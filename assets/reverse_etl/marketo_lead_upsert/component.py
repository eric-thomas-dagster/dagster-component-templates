"""DataFrame -> Marketo Lead createOrUpdate upsert.

Mirrors an upstream DataFrame into Marketo Leads via the native
`POST /rest/v1/leads.json` endpoint with `action: createOrUpdate` --
Marketo handles create-or-update atomically per record, matched on
`lookupField` (typically `email`).

Request shape:
    {
      "action": "createOrUpdate",
      "lookupField": "email",
      "input": [{"email": "...", "firstName": "...", ...}, ...]
    }

Marketo caps `input` at 300 records per request (hard API limit) -- this
component chunks automatically regardless of `batch_size`.

Response shape (one entry per input record, in order):
    {
      "requestId": "...",
      "success": true,
      "result": [
        {"id": 123, "status": "created"},
        {"id": 124, "status": "updated"},
        {"id": 125, "status": "skipped", "reasons": [{"code": "1004", "message": "Lead not found"}]}
      ]
    }

Pairs with:
  - ``marketo_resource`` -- OAuth2 client-credentials auth + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_LEADS_PER_REQUEST = 300  # Marketo hard limit on `input` array size.


def _call_marketo_leads_api(resource, body: dict) -> dict:
    """Isolates the one external-API boundary (Marketo `POST
    /rest/v1/leads.json`) so it can be monkeypatched wholesale in tests --
    mirrors this repo's "mock only the paid/external call" test
    convention."""
    return resource.post("rest/v1/leads.json", json_body=body)


class MarketoLeadUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into Marketo Leads.

    Example:
        ```yaml
        type: dagster_component_templates.MarketoLeadUpsertComponent
        attributes:
          asset_name: marketo_leads_mirror
          upstream_asset_key: dbt_marts_contacts
          resource_key: marketo
          lookup_field: email
          fields_map:
            email: email
            first_name: firstName
            last_name: lastName
            company: company
            lead_score: leadScore__c
        ```

    `fields_map` maps upstream column -> Marketo field API name.
    Standard fields use their Marketo API names (`email`, `firstName`,
    `lastName`, `company`, `title`, `phone`, ...); custom fields pass
    through as-is (e.g. `leadScore__c`).
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes -- supply exactly one.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually exclusive "
            "with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with `upstream_asset_key`. "
            "Shapes: {kind: sql, resource_key/database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    resource_key: str = Field(
        default="marketo",
        description="Resource key registered by MarketoResourceComponent.",
    )

    lookup_field: str = Field(
        default="email",
        description=(
            "Marketo field used as the createOrUpdate match key (Marketo's "
            "`lookupField`). MUST be present in fields_map values. Typically "
            "'email', but can be 'id' or a custom field marked for lookup."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column -> Marketo field API name.",
    )
    batch_size: int = Field(
        default=50000,
        description=(
            "Max upstream rows per run (safety cap). Requests are additionally "
            "chunked at 300 records each (Marketo's hard per-request limit on "
            "the `input` array)."
        ),
    )

    group_name: Optional[str] = Field(default="marketo", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'marketo')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("marketo")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "MarketoLeadUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate lookup_field is in fields_map values.
        mapped_fields = set(self.fields_map.values())
        if self.lookup_field not in mapped_fields:
            raise ValueError(
                f"MarketoLeadUpsertComponent: lookup_field={self.lookup_field!r} "
                f"not in fields_map values. lookup_field must be a Marketo field "
                f"you're upserting. fields_map values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # -- Source resolver (self-contained per no-shared-code rule) -------
        def _resolve_source_df(exec_ctx):
            import pandas as pd
            src = _self.source or {}
            kind = (src.get("kind") or "").lower()
            if kind == "sql":
                query = src.get("query")
                if not query:
                    raise ValueError("source kind=sql requires 'query'")
                rk = src.get("resource_key")
                if rk:
                    resource = getattr(exec_ctx.resources, rk)
                    if hasattr(resource, "get_engine"):
                        return pd.read_sql(query, resource.get_engine())
                    if hasattr(resource, "get_connection"):
                        with resource.get_connection() as conn:
                            if hasattr(conn, "execute") and hasattr(conn, "df"):
                                return conn.execute(query).df()
                            return pd.read_sql(query, conn)
                    raise ValueError(f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()")
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError("source kind=sql requires 'resource_key' OR 'database_url_env_var'")
            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))
            if kind == "inline":
                return pd.DataFrame(src.get("rows") or [])
            raise ValueError(f"MarketoLeadUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            resource = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty -- nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            # Upstream column mapped to lookup_field.
            lookup_col = next(
                (col for col, mkto_field in _self.fields_map.items()
                 if mkto_field == _self.lookup_field),
                None,
            )
            if lookup_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to lookup_field="
                    f"{_self.lookup_field!r}. fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            records: List[dict] = []
            skipped_no_key = 0
            for _, row in df.iterrows():
                lookup_value = _row_value(row[lookup_col])
                if lookup_value is None:
                    skipped_no_key += 1
                    continue
                rec: dict = {}
                for col, mkto_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        rec[mkto_field] = v
                if not rec:
                    continue
                records.append(rec)

            if not records:
                context.log.warning(
                    f"No records with valid {_self.lookup_field} values -- nothing to upsert."
                )
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            created = 0
            updated = 0
            skipped = 0
            errors: List[str] = []
            requests_made = 0

            for chunk_start in range(0, len(records), _LEADS_PER_REQUEST):
                chunk = records[chunk_start:chunk_start + _LEADS_PER_REQUEST]
                body = {
                    "action": "createOrUpdate",
                    "lookupField": _self.lookup_field,
                    "input": chunk,
                }
                try:
                    response = _call_marketo_leads_api(resource, body)
                except Exception as e:  # noqa: BLE001
                    errors.append(
                        f"chunk {chunk_start}-{chunk_start + len(chunk) - 1}: "
                        f"{type(e).__name__}: {e}"
                    )
                    continue
                requests_made += 1

                if not response.get("success", True):
                    errors.append(f"chunk {chunk_start}: Marketo reported success=false")

                for i, item in enumerate(response.get("result") or []):
                    status = item.get("status")
                    if status == "created":
                        created += 1
                    elif status == "updated":
                        updated += 1
                    elif status == "skipped":
                        skipped += 1
                        reasons = item.get("reasons") or []
                        reason_text = "; ".join(
                            f"{r.get('code')}: {r.get('message')}" for r in reasons
                        ) or "unknown reason"
                        errors.append(f"row {chunk_start + i}: skipped -- {reason_text}")
                    else:
                        errors.append(f"row {chunk_start + i}: unexpected status {status!r}")

            context.log.info(
                f"Marketo Lead upsert: {created} created, {updated} updated, "
                f"{skipped} skipped, {skipped_no_key} skipped (missing "
                f"{_self.lookup_field}) -- matched on {_self.lookup_field}, "
                f"{requests_made} request(s)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "marketo_lookup_field": dg.MetadataValue.text(_self.lookup_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_skipped": dg.MetadataValue.int(skipped),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "api_requests": dg.MetadataValue.int(requests_made),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into Marketo Leads (match on "
                f"{_self.lookup_field})."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(required_resource_keys=required_rks, **common_kwargs)
            def _asset(context: dg.AssetExecutionContext):
                df = _resolve_source_df(context)
                return _run_upsert(context, df)
        else:
            @dg.asset(
                ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
                required_resource_keys={_self.resource_key},
                **common_kwargs,
            )
            def _asset(context: dg.AssetExecutionContext, upstream):
                return _run_upsert(context, upstream)

        return dg.Definitions(assets=[_asset])
