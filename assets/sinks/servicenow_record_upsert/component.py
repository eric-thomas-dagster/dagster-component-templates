"""DataFrame → ServiceNow record upsert.

Mirrors a source DataFrame into a ServiceNow table via the Table API. Since
ServiceNow has no native upsert endpoint, this sink does search-then-write per
row: `GET /api/now/table/{table}?sysparm_query={key_field}={value}` → PATCH the
match if found, else POST a new record.

Two source shapes:
  1. `upstream_asset_key:` — chain from an upstream Dagster asset that produces
     a pandas DataFrame. Standard Dagster lineage pattern.
  2. `source:` block — read the DataFrame inline at run time, no upstream asset
     required. Supports:
       - kind: sql — query a database via a Dagster resource (`resource_key`
         with `.get_engine()` / `.get_connection()`) OR a raw
         `database_url_env_var`.
       - kind: csv — read a CSV file at `path`.
       - kind: inline — literal rows in YAML.

Pairs with:
  - ``servicenow_resource`` — connection + auth (required)

Live-validation notes:
  - Rate limits: ServiceNow's default rate limit is 60 req/min for basic auth
    on developer instances. The resource retries on 429 with exponential
    backoff up to `max_retries` (default 3).
  - Uniqueness: if `key_field` is not indexed as unique on the target table,
    concurrent runs racing on the same key can create duplicates. Add a
    unique index in ServiceNow to enforce.
  - Empty strings vs None: string values that are literally "" are sent
    verbatim to ServiceNow (which stores them as blank). None / NaN values
    are omitted from the request body so ServiceNow keeps its current value.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class ServiceNowRecordUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Batch-upsert rows from a source DataFrame into a ServiceNow table.

    Example — upstream asset:
        ```yaml
        type: dagster_component_templates.ServiceNowRecordUpsertComponent
        attributes:
          asset_name: servicenow_incidents_from_alerts
          upstream_asset_key: alerts_to_incidents
          resource_key: servicenow_resource
          table: incident
          key_field: correlation_id
          fields_map:
            alert_id: correlation_id
            title: short_description
            details: description
            severity: impact
        ```

    Example — inline SQL source (no upstream asset):
        ```yaml
        attributes:
          asset_name: servicenow_incidents_from_alerts
          source:
            kind: sql
            resource_key: analytics_postgres  # any resource w/ get_engine / get_connection
            query: |
              SELECT alert_id, title, details, severity
              FROM analytics.open_alerts
              WHERE created_at > now() - interval '1 day'
          resource_key: servicenow_resource
          table: incident
          key_field: correlation_id
          fields_map: {...}
        ```

    For every row in the source DataFrame:
      1. GET the ServiceNow table by `key_field == <row[key_field]>`
      2. If found, PATCH the matched sys_id with the mapped fields
      3. If not, POST a new record with the mapped fields
    """

    asset_name: str = Field(description="Output Dagster asset name.")

    # Two source shapes — supply exactly one.
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
            "Shapes:\n"
            "  {kind: sql, resource_key: <name>, query: <sql>}\n"
            "  {kind: sql, database_url_env_var: <env>, query: <sql>}\n"
            "  {kind: csv, path: <path>, read_csv_kwargs: {...}}\n"
            "  {kind: inline, rows: [{...}, ...]}"
        ),
    )

    resource_key: str = Field(
        default="servicenow_resource",
        description="Resource key registered by ServiceNowResourceComponent.",
    )

    table: str = Field(
        description="Target ServiceNow table name (e.g. 'incident', 'change_request', 'cmdb_ci_server').",
    )
    key_field: str = Field(
        description=(
            "ServiceNow field name used to match existing records. Every row "
            "must supply a value for this field via `fields_map`."
        ),
    )
    fields_map: Dict[str, str] = Field(
        description="Source column → ServiceNow field name.",
    )
    batch_size: int = Field(
        default=500,
        description="Max rows per run (safety cap). Each row is one search-then-write pair (2 REST calls when the record exists, 1 when new).",
    )

    group_name: Optional[str] = Field(
        default="servicenow", description="Dagster asset group name."
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
        description="Asset kinds (auto-includes 'servicenow').",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("servicenow")

        # Validate: exactly one of upstream_asset_key OR source: must be set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "ServiceNowRecordUpsertComponent: supply exactly one of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        # Validate key_field is in fields_map values — otherwise the upsert
        # can't match and every row will be created as new.
        mapped_fields = set(self.fields_map.values())
        if self.key_field not in mapped_fields:
            raise ValueError(
                f"ServiceNowRecordUpsertComponent: key_field={self.key_field!r} not "
                f"present in fields_map values. key_field must be a ServiceNow field "
                f"you're upserting. fields_map values: {sorted(mapped_fields)}"
            )

        use_source = self.source is not None

        # Extra required_resource_keys when source: kind=sql uses a resource.
        extra_rks: set = set()
        if use_source and (self.source.get("kind") or "").lower() == "sql":
            source_rk = self.source.get("resource_key")
            if source_rk:
                extra_rks.add(source_rk)

        # ── Source resolver (self-contained per no-shared-code rule) ──────
        def _resolve_source_df(exec_ctx):
            """Resolve DataFrame from `source:` config (called when use_source=True)."""
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
                        engine = resource.get_engine()
                        return pd.read_sql(query, engine)
                    if hasattr(resource, "get_connection"):
                        conn = resource.get_connection()
                        # DuckDB fast path
                        if hasattr(conn, "execute") and hasattr(conn, "df"):
                            return conn.execute(query).df()
                        return pd.read_sql(query, conn)
                    raise ValueError(
                        f"source kind=sql: resource {rk!r} must expose "
                        ".get_engine() or .get_connection()"
                    )
                env = src.get("database_url_env_var")
                if env:
                    import os
                    from sqlalchemy import create_engine
                    url = os.environ.get(env, "")
                    if not url:
                        raise ValueError(f"database_url_env_var {env!r} is unset")
                    return pd.read_sql(query, create_engine(url))
                raise ValueError(
                    "source kind=sql requires 'resource_key' OR 'database_url_env_var'"
                )

            if kind == "csv":
                path = src.get("path")
                if not path:
                    raise ValueError("source kind=csv requires 'path'")
                return pd.read_csv(path, **(src.get("read_csv_kwargs") or {}))

            if kind == "inline":
                rows = src.get("rows") or []
                return pd.DataFrame(rows)

            raise ValueError(
                f"ServiceNowRecordUpsertComponent source kind={kind!r} not supported "
                "(expected: sql / csv / inline)"
            )

        # ── Shared upsert body ─────────────────────────────────────────
        def _run_upsert(exec_ctx, df):
            sn = getattr(exec_ctx.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame([df]) if isinstance(df, dict) else pd.DataFrame(df)

            if len(df) == 0:
                exec_ctx.log.warning("Source DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                exec_ctx.log.warning(
                    f"Source has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            required_cols = set(_self.fields_map.keys())
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in source: {missing_cols}. Available: {list(df.columns)}"
                )

            key_col = next(
                (col for col, sn_field in _self.fields_map.items() if sn_field == _self.key_field),
                None,
            )
            if key_col is None:
                raise dg.Failure(
                    f"fields_map has no column mapping to key_field={_self.key_field!r}. "
                    f"fields_map: {_self.fields_map}"
                )

            def _row_value(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                return v

            just_created_sys_ids: Dict[str, str] = {}
            created = 0
            updated = 0
            errors: List[str] = []

            for i, row in df.iterrows():
                key_value = _row_value(row[key_col])
                if key_value is None:
                    errors.append(f"row {i}: {key_col!r} is None — skipped")
                    continue

                body: dict = {}
                for col, sn_field in _self.fields_map.items():
                    v = _row_value(row[col])
                    if v is not None:
                        body[sn_field] = v
                if not body:
                    continue

                cached_sys_id = just_created_sys_ids.get(str(key_value))
                try:
                    if cached_sys_id is not None:
                        sn.update_record(_self.table, cached_sys_id, body)
                        updated += 1
                        continue
                    result = sn.upsert_record(_self.table, _self.key_field, key_value, body)
                    if result.get("action") == "created":
                        created += 1
                        rec = result.get("record") or {}
                        if rec.get("sys_id"):
                            just_created_sys_ids[str(key_value)] = rec["sys_id"]
                    else:
                        updated += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"row {i} (key={key_value}): {type(e).__name__}: {e}")

            exec_ctx.log.info(
                f"ServiceNow upsert into {_self.table}: {created} created, "
                f"{updated} updated, {len(errors)} errors (matched on {_self.key_field})."
            )
            if errors:
                exec_ctx.log.error(
                    "First few errors:\n" + "\n".join(errors[:5])
                )

            metadata = {
                "servicenow_table": dg.MetadataValue.text(_self.table),
                "key_field": dg.MetadataValue.text(_self.key_field),
                "rows_created": dg.MetadataValue.int(created),
                "rows_updated": dg.MetadataValue.int(updated),
                "rows_upserted": dg.MetadataValue.int(created + updated),
                "rows_errored": dg.MetadataValue.int(len(errors)),
            }
            if errors:
                metadata["first_errors"] = dg.MetadataValue.json(errors[:5])

            return dg.MaterializeResult(metadata=metadata)

        # ── Two asset shapes based on source configuration ─────────────
        common_kwargs = dict(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            description=_self.description or (
                f"Upsert DataFrame rows into ServiceNow table {_self.table} "
                f"(match on {_self.key_field})."
            ),
        )

        if use_source:
            required_rks = {_self.resource_key} | extra_rks

            @dg.asset(
                required_resource_keys=required_rks,
                **common_kwargs,
            )
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
