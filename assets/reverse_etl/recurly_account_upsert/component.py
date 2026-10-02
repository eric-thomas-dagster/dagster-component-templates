"""DataFrame -> Recurly Account upsert.

Mirrors an upstream DataFrame into Recurly Account records.

Recurly's Accounts API has **no single-call upsert** -- `PUT
/accounts/{account_id}` ONLY updates: it returns a 404 if no account
with that id/code exists, it does NOT fall back to creating one. Create
is a separate call, `POST /accounts`.

Recurly lets you reference an account by its `code` (a merchant-assigned
unique identifier) anywhere an `account_id` path parameter is expected,
using the `code-` prefix convention, e.g. `GET /accounts/code-acme-inc`.
So a real "upsert" from this component's perspective requires, per row:

  1. **Lookup**: `GET /accounts/code-<value>` -- using the lookup_field's
     value as the account code. A 404 here means no match (this
     component's `recurly_resource` returns `None` on 404 rather than
     raising, specifically so this check is a simple `is None`).
  2. **Write**:
     - If found: `PUT /accounts/code-<value>` with the row's fields
       (update, partial-merge semantics).
     - If not found: `POST /accounts` with `code` plus the row's other
       fields (create -- Recurly assigns the account its internal `id`,
       but `code` remains the stable external key).

Two API calls per row, always -- there's no way to combine lookup +
write into one, since PUT's 404-vs-200 behavior can only be observed by
actually making the call (and even if it could, you can't tell PUT
"create it if missing" -- it simply won't).

Pairs with:
  - ``recurly_resource`` -- HTTP Basic auth (API key, blank password) + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _recurly_lookup_account(resource, lookup_value) -> bool:
    """Isolates the lookup external-API call (GET /accounts/code-<value>)
    so it can be monkeypatched wholesale in tests. Returns True if an
    account with that code exists, False if not (404)."""
    escaped = str(lookup_value).replace("/", "")
    response = resource.request("GET", f"accounts/code-{escaped}")
    return response is not None


def _recurly_write_account(resource, lookup_value, exists: bool, body: dict) -> dict:
    """Isolates the write external-API call (PUT to update, POST to
    create) so it can be monkeypatched wholesale in tests."""
    escaped = str(lookup_value).replace("/", "")
    if exists:
        response = resource.request("PUT", f"accounts/code-{escaped}", json_body=body)
        return {"id": (response or {}).get("id"), "action": "updated"}
    response = resource.request("POST", "accounts", json_body=body)
    return {"id": (response or {}).get("id"), "action": "created"}


class RecurlyAccountUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Recurly Account records.

    Example:
        ```yaml
        type: dagster_component_templates.RecurlyAccountUpsertComponent
        attributes:
          asset_name: recurly_accounts_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: recurly
          lookup_field: code
          fields_map:
            customer_id: code
            email: email
            first_name: first_name
            last_name: last_name
            company_name: company
        ```

    `fields_map` maps upstream column -> Recurly Account field name.
    `lookup_field` (default `code`, Recurly's merchant-assigned unique
    account identifier) MUST be present in fields_map values, since its
    value is used directly as the `code-<value>` path segment for both
    the existence check and the update call.
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
        default="recurly",
        description="Resource key registered by RecurlyResourceComponent.",
    )

    lookup_field: str = Field(
        default="code",
        description=(
            "Recurly Account field used as the merchant-assigned unique "
            "account code (substituted into the `code-<value>` path "
            "segment). MUST be present in fields_map values."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Upstream column -> Recurly Account field name.",
    )
    batch_size: int = Field(
        default=5000,
        description=(
            "Max upstream rows per run (safety cap). Recurly's PUT /accounts/ "
            "{account_id} only updates (404s if absent) -- every row costs "
            "one existence-check GET plus one write."
        ),
    )

    group_name: Optional[str] = Field(default="recurly", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'recurly')."
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("recurly")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "RecurlyAccountUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        mapped_fields = set(self.fields_map.values())
        if self.lookup_field not in mapped_fields:
            raise ValueError(
                f"RecurlyAccountUpsertComponent: lookup_field="
                f"{self.lookup_field!r} not in fields_map values. "
                f"lookup_field must be a Recurly field you're upserting. "
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
            raise ValueError(f"RecurlyAccountUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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
                (col for col, rc_field in _self.fields_map.items()
                 if rc_field == _self.lookup_field),
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
                for col, rc_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        body[rc_field] = v
                if not body:
                    continue

                try:
                    exists = _recurly_lookup_account(resource, lookup_value)
                    api_requests += 1
                    result = _recurly_write_account(resource, lookup_value, exists, body)
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
                f"Recurly Account upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing "
                f"{_self.lookup_field}) -- matched on {_self.lookup_field}, "
                f"{api_requests} API request(s)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "recurly_lookup_field": dg.MetadataValue.text(_self.lookup_field),
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
                f"Upsert DataFrame rows into Recurly Account records "
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
