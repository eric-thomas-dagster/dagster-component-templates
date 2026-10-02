"""DataFrame -> Xero Contact upsert.

Mirrors an upstream DataFrame into Xero Accounting Contact records.

Xero's Contacts endpoint is **not a single-call upsert in the usual
sense** -- there is one endpoint, `POST /api.xro/2.0/Contacts`, used for
BOTH create and update, but Xero decides which happens based on whether
the request body carries a `ContactID`:

  - **Create**: body has no `ContactID` -- Xero assigns a new one. If the
    body's `Name` collides with an existing active contact's Name, Xero
    REJECTS the request (`"The contact name is already assigned to
    another contact. The contact name must be unique across all active
    contacts."`) -- Xero enforces Name uniqueness, unlike some other
    fields.
  - **Update**: body includes the existing contact's `ContactID` -- Xero
    updates that record. Unlike QuickBooks' SyncToken, there is no
    optimistic-concurrency token to manage; omitted fields are LEFT
    UNCHANGED (a partial update is the default behavior, not an opt-in).

So a real "upsert" from this component's perspective requires, per row:
  1. **Lookup**: `GET /Contacts?where=<lookup_field>=="<value>"` -- Xero's
     filter-query syntax (`where=`) -- to discover whether a match exists
     and, if so, its `ContactID`.
  2. **Write**: `POST /Contacts` -- with `ContactID` set (update) if a
     match was found, or without it (create) if not.

Two API calls per row, always -- there is no way to combine lookup +
write into one call, and (per Xero's own enforced Name uniqueness) a
blind create-only POST risks a 400 the moment the same Name appears twice.

Only flat (non-nested) Contact fields are supported here -- Xero's Phones
and Addresses are arrays of sub-objects, not simple nested keys, and are
out of scope for this component's fields_map.

Pairs with:
  - ``xero_resource`` -- OAuth2 refresh-token auth + Xero-tenant-id-scoped HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _xero_lookup_contact(resource, lookup_field: str, lookup_value) -> Optional[str]:
    """Isolates the lookup external-API call (GET /Contacts?where=...) so
    it can be monkeypatched wholesale in tests. Returns the ContactID of
    the first match, or None if no contact matches."""
    escaped = str(lookup_value).replace('"', '\\"')
    where = f'{lookup_field}=="{escaped}"'
    response = resource.request("GET", "Contacts", params={"where": where})
    contacts = (response or {}).get("Contacts") or []
    if not contacts:
        return None
    return contacts[0].get("ContactID")


def _xero_write_contact(resource, contact_id: Optional[str], body: dict) -> dict:
    """Isolates the write external-API call (POST /Contacts -- create or
    update, depending on whether `body` carries ContactID) so it can be
    monkeypatched wholesale in tests."""
    payload = dict(body)
    if contact_id:
        payload["ContactID"] = contact_id
    response = resource.request("POST", "Contacts", json_body={"Contacts": [payload]})
    contacts = (response or {}).get("Contacts") or []
    return contacts[0] if contacts else {}


class XeroContactUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Xero Contact records.

    Example:
        ```yaml
        type: dagster_component_templates.XeroContactUpsertComponent
        attributes:
          asset_name: xero_contacts_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: xero
          lookup_field: Name
          fields_map:
            customer_name: Name
            email: EmailAddress
            contact_number: ContactNumber
        ```

    `fields_map` maps upstream column -> Xero Contact field name (flat
    fields only; Phones/Addresses arrays are not supported). `lookup_field`
    (default `Name`, Xero enforces uniqueness on active-contact Name) MUST
    be present in fields_map values, since it's substituted into a Xero
    `where=` filter query.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

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
        default="xero",
        description="Resource key registered by XeroResourceComponent.",
    )

    lookup_field: str = Field(
        default="Name",
        description=(
            "Xero Contact field used to find an existing record via a "
            "where= filter query. MUST be present in fields_map values -- "
            "Xero enforces Name uniqueness across active contacts, making "
            "it the conventional match key (ContactNumber is a common "
            "alternative when you maintain your own external id)."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column -> Xero Contact field name (flat fields only).",
    )
    batch_size: int = Field(
        default=5000,
        description=(
            "Max upstream rows per run (safety cap). Xero has no bulk upsert "
            "-- every row costs one lookup plus one write."
        ),
    )

    group_name: Optional[str] = Field(default="xero", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'xero')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("xero")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "XeroContactUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        mapped_fields = set(self.fields_map.values())
        if self.lookup_field not in mapped_fields:
            raise ValueError(
                f"XeroContactUpsertComponent: lookup_field={self.lookup_field!r} "
                f"not in fields_map values. lookup_field must be a Xero Contact "
                f"field you're upserting. fields_map values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            _rk = self.source.get("resource_key")
            if _rk:
                extra_rks.add(_rk)

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
            raise ValueError(f"XeroContactUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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

            lookup_col = next(
                (col for col, xero_field in _self.fields_map.items()
                 if xero_field == _self.lookup_field),
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

            created = 0
            updated = 0
            skipped_no_key = 0
            errors: List[str] = []
            api_requests = 0

            for row_idx, row in df.iterrows():
                lookup_value = _row_value(row[lookup_col])
                if lookup_value is None:
                    skipped_no_key += 1
                    continue

                body: dict = {}
                for col, xero_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        body[xero_field] = v
                if not body:
                    continue

                try:
                    contact_id = _xero_lookup_contact(resource, _self.lookup_field, lookup_value)
                    api_requests += 1
                    _xero_write_contact(resource, contact_id, body)
                    api_requests += 1
                    if contact_id:
                        updated += 1
                    else:
                        created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(
                        f"row {row_idx} ({_self.lookup_field}={lookup_value!r}): "
                        f"{type(e).__name__}: {e}"
                    )

            context.log.info(
                f"Xero Contact upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing "
                f"{_self.lookup_field}) -- matched on {_self.lookup_field}, "
                f"{api_requests} API request(s)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "xero_lookup_field": dg.MetadataValue.text(_self.lookup_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
                "rows_skipped_no_key": dg.MetadataValue.int(skipped_no_key),
                "api_requests": dg.MetadataValue.int(api_requests),
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
                f"Upsert DataFrame rows into Xero Contact records "
                f"(match on {_self.lookup_field})."
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
