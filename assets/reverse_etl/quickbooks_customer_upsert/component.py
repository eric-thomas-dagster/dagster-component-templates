"""DataFrame -> QuickBooks Online Customer upsert.

Mirrors an upstream DataFrame into QuickBooks Online Customer records.

QuickBooks' "upsert" is **not a single API call** -- there is one endpoint,
`POST /v3/company/<realmId>/customer`, used for BOTH create and update, but
which behavior happens depends entirely on the request body:

  - **Create**: body has no `Id` / `SyncToken` -- QBO assigns a new Id.
  - **Update**: body MUST include the record's current `Id` AND its current
    `SyncToken` (QBO's optimistic-concurrency version number -- every
    successful write increments it; a write with a stale SyncToken is
    rejected with a 400). `sparse: true` makes it a partial update (only
    the fields you send are changed); omit `sparse` (or set false) to
    replace fields QBO considers "list" types by replacing the whole list.

So a real "upsert" requires this component to, per row:
  1. Query for an existing match: `GET /query?query=SELECT * FROM Customer
     WHERE <lookup_field> = '<value>'` -- to discover both whether the
     record exists AND, if so, its current `Id` + `SyncToken`.
  2. If found: POST `/customer` with `Id`, `SyncToken`, `sparse: true`, plus
     the row's other fields (update).
  3. If not found: POST `/customer` with just the row's fields (create).

Two API calls per row, no exceptions -- QuickBooks has no way to skip the
query step, since the SyncToken can only be obtained by reading the record.

Pairs with:
  - ``quickbooks_resource`` -- OAuth2 refresh-token auth + workhorse HTTP (required)
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _quickbooks_query_customer(resource, lookup_field: str, lookup_value) -> Optional[dict]:
    """Isolates the query external-API call (GET /query, QBO's SQL-like
    query language) so it can be monkeypatched wholesale in tests. Returns
    the first matching Customer dict (with Id + SyncToken), or None."""
    escaped = str(lookup_value).replace("'", "\\'")
    query_str = f"SELECT * FROM Customer WHERE {lookup_field} = '{escaped}'"
    response = resource.query(query_str)
    customers = ((response or {}).get("QueryResponse") or {}).get("Customer") or []
    if not customers:
        return None
    return customers[0]


def _quickbooks_write_customer(resource, body: dict) -> dict:
    """Isolates the write external-API call (POST /customer -- create or
    update, depending on whether `body` carries Id + SyncToken) so it can
    be monkeypatched wholesale in tests."""
    response = resource.request("POST", "customer", json_body=body)
    return (response or {}).get("Customer") or {}


def _set_nested(target: dict, dotted_key: str, value) -> None:
    """Builds nested QBO objects from a dotted fields_map key, e.g.
    'PrimaryEmailAddr.Address' -> {"PrimaryEmailAddr": {"Address": value}}
    -- mirrors real QBO Customer payload shapes (several fields, like
    PrimaryEmailAddr/PrimaryPhone, are themselves objects, not scalars)."""
    parts = dotted_key.split(".")
    node = target
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class QuickBooksCustomerUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into QuickBooks Online
    Customer records.

    Example:
        ```yaml
        type: dagster_component_templates.QuickBooksCustomerUpsertComponent
        attributes:
          asset_name: quickbooks_customers_mirror
          upstream_asset_key: dbt_marts_customers
          resource_key: quickbooks
          lookup_field: DisplayName
          fields_map:
            customer_name: DisplayName
            company_name: CompanyName
            email: PrimaryEmailAddr.Address
            phone: PrimaryPhone.FreeFormNumber
        ```

    `fields_map` maps upstream column -> QuickBooks Customer field name.
    Dotted keys (e.g. `PrimaryEmailAddr.Address`) build nested QBO objects.
    `lookup_field` (default `DisplayName`, QBO's conventionally-unique
    Customer identifier) MUST be a top-level (non-dotted) field present in
    fields_map, since it's used in a QBO query WHERE clause.
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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
        default="quickbooks",
        description="Resource key registered by QuickBooksResourceComponent.",
    )

    lookup_field: str = Field(
        default="DisplayName",
        description=(
            "QuickBooks Customer field used to find an existing record via "
            "a query WHERE clause. MUST be a top-level (non-dotted) field "
            "present in fields_map values -- QBO enforces DisplayName "
            "uniqueness, making it the conventional match key."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description=(
            "Upstream column -> QuickBooks Customer field name. Use dotted "
            "keys (e.g. 'PrimaryEmailAddr.Address') for nested QBO objects."
        ),
    )
    batch_size: int = Field(
        default=5000,
        description=(
            "Max upstream rows per run (safety cap). QuickBooks has no bulk "
            "upsert -- every row costs one query (for SyncToken discovery) "
            "plus one write."
        ),
    )

    group_name: Optional[str] = Field(default="quickbooks", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds (auto-includes 'quickbooks')."
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("quickbooks")

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "QuickBooksCustomerUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if "." in self.lookup_field:
            raise ValueError(
                f"QuickBooksCustomerUpsertComponent: lookup_field="
                f"{self.lookup_field!r} must be a top-level field (no '.') "
                f"-- it's used directly in a QBO query WHERE clause."
            )

        mapped_fields = set(self.fields_map.values())
        if self.lookup_field not in mapped_fields:
            raise ValueError(
                f"QuickBooksCustomerUpsertComponent: lookup_field="
                f"{self.lookup_field!r} not in fields_map values. "
                f"lookup_field must be a QuickBooks field you're upserting. "
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
            raise ValueError(f"QuickBooksCustomerUpsertComponent source kind={kind!r} not supported (sql / csv / inline)")

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
                (col for col, qbo_field in _self.fields_map.items()
                 if qbo_field == _self.lookup_field),
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
                for col, qbo_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        _set_nested(body, qbo_field, v)
                if not body:
                    continue

                try:
                    existing = _quickbooks_query_customer(resource, _self.lookup_field, lookup_value)
                    api_requests += 1
                    if existing is not None:
                        body["Id"] = existing.get("Id")
                        body["SyncToken"] = existing.get("SyncToken")
                        body["sparse"] = True
                    written = _quickbooks_write_customer(resource, body)
                    api_requests += 1
                    if existing is not None:
                        updated += 1
                    else:
                        created += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(
                        f"row {row_idx} ({_self.lookup_field}={lookup_value!r}): "
                        f"{type(e).__name__}: {e}"
                    )

            context.log.info(
                f"QuickBooks Customer upsert: {created} created, {updated} updated, "
                f"{len(errors)} errors, {skipped_no_key} skipped (missing "
                f"{_self.lookup_field}) -- matched on {_self.lookup_field}, "
                f"{api_requests} API request(s)."
            )
            if errors:
                context.log.error("First few errors:\n" + "\n".join(errors[:5]))

            metadata = {
                "quickbooks_lookup_field": dg.MetadataValue.text(_self.lookup_field),
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
                f"Upsert DataFrame rows into QuickBooks Online Customer "
                f"records (match on {_self.lookup_field})."
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
