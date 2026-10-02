"""DataFrame -> Zuora Account upsert (generic Object CRUD).

Mirrors an upstream DataFrame into Zuora Account records via Zuora's
generic Object CRUD API (`/v1/object/account`).

Zuora has **no single-call upsert** for Accounts -- `POST /v1/object/account`
always creates, and `PUT /v1/object/account/{id}` always updates (it 404s
if the id doesn't exist). There is also no filter-by-field GET on the
Object CRUD API (only GET-by-id), so discovering whether an account
already exists by a business key (e.g. `AccountNumber`) requires Zuora's
query language -- ZOQL -- via the separate Action Query endpoint,
`POST /v1/action/query`.

So a real "upsert" from this component's perspective requires, per row:

  1. **Query**: `POST /v1/action/query` with a ZOQL `select Id from
     Account where <lookup_field> = '<value>'` -- discovers whether a
     match exists and, if so, its Zuora `Id`.
  2. **Write**:
     - If found: `PUT /v1/object/account/{id}` with the row's fields
       (update, partial-merge semantics).
     - If not found: `POST /v1/object/account` with the row's fields
       (create -- Zuora's lightweight Object CRUD create requires `Name`,
       `Currency`, `BillCycleDay`, and `Status` at minimum; this
       component does not inject defaults for these -- they must come
       from fields_map / the upstream data, or Zuora rejects the create
       with a 400).

Two API calls per row, always -- there's no way to combine the ZOQL
lookup and the create-or-update write into one round trip.

Pairs with:
  - ``zuora_resource`` -- OAuth2 client-credentials auth + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _zuora_lookup_account(resource, lookup_field: str, lookup_value) -> Optional[str]:
    """Isolates the lookup external-API call (ZOQL query via POST
    /v1/action/query) so it can be monkeypatched wholesale in tests.
    Returns the Zuora Account Id of the first match, or None if no
    account matches."""
    escaped = str(lookup_value).replace("'", "\\'")
    zoql = f"select Id from Account where {lookup_field} = '{escaped}'"
    response = resource.query(zoql)
    records = (response or {}).get("records") or []
    if not records:
        return None
    return records[0].get("Id")


def _zuora_write_account(resource, account_id: Optional[str], body: dict) -> dict:
    """Isolates the write external-API call (PUT /v1/object/account/{id}
    to update, POST /v1/object/account to create) so it can be
    monkeypatched wholesale in tests."""
    if account_id:
        resource.request("PUT", f"v1/object/account/{account_id}", json_body=body)
        return {"id": account_id, "action": "updated"}
    created = resource.request("POST", "v1/object/account", json_body=body)
    return {"id": (created or {}).get("Id"), "action": "created"}


class ZuoraAccountUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Zuora Account records
    (generic Object CRUD API).

    Example:
        ```yaml
        type: dagster_component_templates.ZuoraAccountUpsertComponent
        attributes:
          asset_name: zuora_accounts_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: zuora
          lookup_field: AccountNumber
          fields_map:
            customer_id: AccountNumber
            account_name: Name
            currency: Currency
            bill_cycle_day: BillCycleDay
            status: Status
        ```

    `fields_map` maps upstream column -> Zuora Account field name.
    `lookup_field` (default `AccountNumber`, Zuora's unique-per-tenant
    account identifier) MUST be present in fields_map values, since it's
    substituted into a ZOQL `where` clause. Zuora's Object CRUD create
    requires `Name`, `Currency`, `BillCycleDay`, and `Status` -- these
    must be present in fields_map (mapped from upstream columns) for
    create to succeed; this component does not supply defaults.
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
        default="zuora",
        description="Resource key registered by ZuoraResourceComponent.",
    )

    lookup_field: str = Field(
        default="AccountNumber",
        description=(
            "Zuora Account field used to find an existing record via a "
            "ZOQL where clause. MUST be present in fields_map values -- "
            "AccountNumber is unique per Zuora tenant, making it the "
            "conventional match key."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> Zuora Account field name. For create to "
            "succeed, must include Name, Currency, BillCycleDay, and "
            "Status (Zuora's Object CRUD create requirements) -- this "
            "component supplies no defaults for them."
        ),
    )
    batch_size: int = Field(
        default=5000,
        description=(
            "Max upstream rows per run (safety cap). Zuora has no "
            "upsert-by-key endpoint -- every row costs one ZOQL query "
            "plus one write."
        ),
    )

    group_name: Optional[str] = Field(default="zuora", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'zuora')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("zuora")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "ZuoraAccountUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        mapped_fields = set(self.fields_map.values())
        if self.lookup_field not in mapped_fields:
            raise ValueError(
                f"ZuoraAccountUpsertComponent: lookup_field="
                f"{self.lookup_field!r} not in fields_map values. "
                f"lookup_field must be a Zuora field you're upserting. "
                f"fields_map values: {sorted(mapped_fields)}"
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
            raise ValueError(f"ZuoraAccountUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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
                (col for col, zuora_field in _self.fields_map.items()
                 if zuora_field == _self.lookup_field),
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
                for col, zuora_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        body[zuora_field] = v
                if not body:
                    continue

                try:
                    account_id = _zuora_lookup_account(resource, _self.lookup_field, lookup_value)
                    api_requests += 1
                    result = _zuora_write_account(resource, account_id, body)
                    api_requests += 1
                    if result.get("action") == "created":
                        created += 1
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(
                        f"row {row_idx} ({_self.lookup_field}={lookup_value!r}): "
                        f"{type(e).__name__}: {e}"
                    )

            context.log.info(
                f"Zuora Account upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing "
                f"{_self.lookup_field}) -- matched on {_self.lookup_field}, "
                f"{api_requests} API request(s)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "zuora_lookup_field": dg.MetadataValue.text(_self.lookup_field),
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
                f"Upsert DataFrame rows into Zuora Account records "
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
