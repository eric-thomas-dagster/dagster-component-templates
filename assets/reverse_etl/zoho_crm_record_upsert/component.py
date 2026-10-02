"""DataFrame -> Zoho CRM record upsert (native "Upsert Records" API).

Mirrors an upstream DataFrame into a Zoho CRM module (Leads / Contacts /
Accounts / Deals / etc.) using Zoho's native **Upsert Records** API --
`POST /crm/{version}/{module_api_name}/upsert`. Zoho handles create-or-update
atomically per record, matching on the `duplicate_check_fields` you supply
(Zoho field API names, e.g. `["Email"]` for Leads/Contacts -- Zoho's own
system-defined duplicate-check field for those modules). No search-then-write.

Zoho's documented hard limit is **100 records per request** -- this
component chunks internally at that limit regardless of `batch_size`
(an overall safety cap on total upstream rows per run).

Pairs with:
  - ``zoho_crm_resource`` -- OAuth2 refresh-token auth + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

# Zoho's documented hard limit on records per /upsert call.
_RECORDS_PER_REQUEST = 100


class ZohoCrmRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from an upstream DataFrame into a Zoho CRM module.

    Example:
        ```yaml
        type: dagster_component_templates.ZohoCrmRecordUpsertComponent
        attributes:
          asset_name: zoho_crm_leads_mirror
          upstream_asset_key: dbt_marts_leads
          resource_key: zoho_crm
          module_api_name: Leads
          duplicate_check_fields: [Email]
          fields_map:
            email: Email
            first_name: First_Name
            last_name: Last_Name
            company: Company
          batch_size: 5000
        ```

    For every upstream row: builds a record from `fields_map`, skips rows
    missing a value for any column mapped to a `duplicate_check_fields`
    entry (counted in `rows_skipped_no_key`), chunks the remainder at
    Zoho's 100-records-per-request limit, and calls
    `resource.upsert(module_api_name, chunk, duplicate_check_fields)` per
    chunk. Each response record's `status` / `action` is inspected to
    report `rows_created` / `rows_updated` / `rows_errored`.
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
        default="zoho_crm",
        description="Resource key registered by ZohoCrmResourceComponent.",
    )

    module_api_name: str = Field(
        description=(
            "Target Zoho CRM module API name (e.g. 'Leads', 'Contacts', "
            "'Accounts', 'Deals', or a custom module)."
        ),
    )
    duplicate_check_fields: List[str] = Field(
        description=(
            "Zoho field API names used to detect duplicates on upsert (e.g. "
            "['Email'] for Leads/Contacts). Must be non-empty, and every entry "
            "MUST be present in fields_map values -- fields marked unique/"
            "mandatory on the module work best. Zoho does not publish a hard "
            "maximum count; keep it small (1-3 fields) in practice."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column -> Zoho field API name.",
    )
    batch_size: int = Field(
        default=5000,
        description=(
            "Max upstream rows per run (safety cap). Always chunked at Zoho's "
            "100-records-per-request limit internally, regardless of this value."
        ),
    )

    group_name: Optional[str] = Field(
        default="zoho_crm", description="Dagster asset group name."
    )
    description: Optional[str] = Field(
        default=None, description="Asset description."
    )
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners."
    )
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds (auto-includes 'zoho').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("zoho")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "ZohoCrmRecordUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate duplicate_check_fields is non-empty.
        if not self.duplicate_check_fields:
            raise ValueError(
                "ZohoCrmRecordUpsertComponent: duplicate_check_fields must be "
                "non-empty."
            )

        # Validate duplicate_check_fields is a subset of fields_map values.
        mapped_fields = set(self.fields_map.values())
        unmapped = [f for f in self.duplicate_check_fields if f not in mapped_fields]
        if unmapped:
            raise ValueError(
                f"ZohoCrmRecordUpsertComponent: duplicate_check_fields "
                f"{unmapped} not in fields_map values. duplicate_check_fields "
                f"must be Zoho fields you're also upserting. fields_map "
                f"values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
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
            raise ValueError(f"ZohoCrmRecordUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

        def _run_upsert(context, upstream):
            zoho = getattr(context.resources, _self.resource_key)

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

            # Upstream columns mapped to each duplicate_check_fields entry.
            key_cols: List[str] = []
            for dup_field in _self.duplicate_check_fields:
                col = next(
                    (c for c, zf in _self.fields_map.items() if zf == dup_field),
                    None,
                )
                if col is None:
                    raise dg.Failure(
                        f"fields_map has no column mapping to duplicate_check_field="
                        f"{dup_field!r}. fields_map: {_self.fields_map}"
                    )
                key_cols.append(col)

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            # Build the records list -- skip rows missing ANY key column.
            records: List[dict] = []
            skipped_no_key = 0
            for _, row in df.iterrows():
                if any(_row_value(row[kc]) is None for kc in key_cols):
                    skipped_no_key += 1
                    continue
                rec: dict = {}
                for col, zoho_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        rec[zoho_field] = v
                if not rec:
                    continue
                records.append(rec)

            if not records:
                context.log.warning("No records with valid duplicate-check key values -- nothing to upsert.")
                return dg.MaterializeResult(metadata={
                    "rows_upserted": dg.MetadataValue.int(0),
                    "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
                })

            created = 0
            updated = 0
            errored = 0
            errors: List[str] = []

            for chunk_start in range(0, len(records), _RECORDS_PER_REQUEST):
                chunk = records[chunk_start:chunk_start + _RECORDS_PER_REQUEST]
                try:
                    result = zoho.upsert(
                        _self.module_api_name,
                        chunk,
                        duplicate_check_fields=_self.duplicate_check_fields,
                    )
                except Exception as e:  # noqa: BLE001
                    errored += len(chunk)
                    errors.append(
                        f"chunk {chunk_start}-{chunk_start + len(chunk) - 1}: "
                        f"{type(e).__name__}: {e}"
                    )
                    continue
                for i, rec_result in enumerate(result):
                    status = (rec_result or {}).get("status")
                    action = (rec_result or {}).get("action")
                    if status == "success":
                        if action == "insert":
                            created += 1
                        elif action == "update":
                            updated += 1
                        else:
                            # Unknown-but-successful action -- still count it
                            # toward upserted via `updated` so totals reconcile.
                            updated += 1
                    else:
                        errored += 1
                        errors.append(
                            f"row {chunk_start + i}: "
                            f"{(rec_result or {}).get('message') or 'unknown error'}"
                        )

            context.log.info(
                f"Zoho CRM upsert into {_self.module_api_name}: {created} created, "
                f"{updated} updated, {errored} errors, {skipped_no_key} skipped "
                f"(missing duplicate-check key) -- matched on "
                f"{_self.duplicate_check_fields}."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "zoho_module": dg.MetadataValue.text(_self.module_api_name),
                "duplicate_check_fields": dg.MetadataValue.json(_self.duplicate_check_fields),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(errored),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
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
                f"Upsert DataFrame rows into Zoho CRM {_self.module_api_name} "
                f"(match on {_self.duplicate_check_fields})."
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
