"""DataFrame to Braze.

Reverse-ETL sink — batch-POST rows to Braze's REST API for customer
activation flows (segment exports, custom-attribute sync, re-engagement
lists, catalog upserts).

Two source shapes (supply exactly one):

- ``upstream_asset_key`` — read a DataFrame from another Dagster asset.
- ``source:`` — inline config, one of:
    - ``{kind: sql, resource_key | database_url_env_var, query}`` — SQL query
    - ``{kind: csv, path, read_csv_kwargs}`` — CSV file
    - ``{kind: inline, rows: [...]}`` — literal rows

Two Braze endpoints (pick via ``endpoint``):

- ``users_track`` (default) — batched user attribute updates via
  ``/users/track``. Braze batches up to 75 users per call.
- ``catalogs`` — custom catalog item upsert via
  ``/catalogs/{name}/items``. Batches up to 50 items per call.

Auth: reference a ``BrazeResource`` via ``resource_key`` (recommended —
lets multiple sinks share auth) OR supply inline ``api_key_env_var`` +
``rest_endpoint`` on the sink itself (fallback for one-off use).

Docs:
    - /users/track:  https://www.braze.com/docs/api/endpoints/user_data/post_user_track
    - /catalogs:     https://www.braze.com/docs/api/endpoints/catalogs
"""

import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
import requests
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Failure,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    RetryPolicy,
    asset,
)
from pydantic import Field


class DataframeToBrazeComponent(Component, Model, Resolvable):
    """Push a DataFrame (or inline source) to Braze via the REST API."""

    asset_name: str = Field(description="Dagster asset name")

    # ── Source (exactly one of these) ─────────────────────────────
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream Dagster asset providing the DataFrame. Mutually "
            "exclusive with `source:`."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Inline source config. Mutually exclusive with "
            "`upstream_asset_key`. Shapes: "
            "{kind: sql, resource_key|database_url_env_var, query}, "
            "{kind: csv, path, read_csv_kwargs}, {kind: inline, rows}."
        ),
    )

    # ── Auth (prefer resource_key; inline fallback) ────────────────
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Resource key registered by BrazeResourceComponent — the "
            "recommended way to configure Braze auth so multiple sinks "
            "share one config. Mutually exclusive with the inline "
            "`api_key_env_var` + `rest_endpoint` fields."
        ),
    )
    api_key_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Inline fallback: env var holding the Braze REST API key. "
            "Only used when `resource_key` is unset."
        ),
    )
    rest_endpoint: Optional[str] = Field(
        default=None,
        description=(
            "Inline fallback: region-specific Braze REST endpoint URL "
            "(e.g. https://rest.iad-01.braze.com). Only used when "
            "`resource_key` is unset."
        ),
    )

    # ── Endpoint + column mapping ──────────────────────────────────
    endpoint: Literal["users_track", "catalogs"] = Field(
        default="users_track",
        description="Which Braze endpoint to POST to.",
    )
    catalog_name: Optional[str] = Field(
        default=None,
        description="Required when endpoint='catalogs'.",
    )

    fields_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Explicit source_col -> braze_field_name mapping. When set, "
            "columns are renamed on the way to Braze (e.g. "
            "{db_email: email, first: first_name}). Preferred over "
            "`attribute_columns` when you need renaming. For "
            "endpoint='catalogs', the item_id_column value can also be "
            "a source column that gets renamed to Braze's 'id' field."
        ),
    )
    user_id_column: str = Field(
        default="external_id",
        description=(
            "For endpoint='users_track': column holding the user's "
            "external_id. Falls back to 'braze_id' if 'external_id' "
            "isn't in the DataFrame."
        ),
    )
    attribute_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "For endpoint='users_track' (used when fields_map isn't set): "
            "columns to send as top-level user attributes. Default: all "
            "columns except user_id_column and custom_attribute_columns."
        ),
    )
    custom_attribute_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "For endpoint='users_track': columns to send nested under "
            "'custom_attributes' on the user's Braze profile."
        ),
    )
    item_id_column: str = Field(
        default="id",
        description=(
            "For endpoint='catalogs': column holding each catalog item's "
            "unique id."
        ),
    )

    batch_size: Optional[int] = Field(
        default=None,
        description=(
            "Rows per HTTP request. Default = 75 for users_track "
            "(Braze's max) or 50 for catalogs."
        ),
    )
    request_timeout_seconds: int = Field(
        default=30,
        description="Per-request HTTP timeout (only when NOT using a resource).",
    )
    dry_run: bool = Field(
        default=False,
        description=(
            "Build payloads + log without POSTing. Useful for validating "
            "field mappings without hitting Braze."
        ),
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Validate exactly one of upstream_asset_key OR source: is set.
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError(
                "DataframeToBrazeComponent: supply exactly ONE of "
                "`upstream_asset_key` OR `source:` (got both or neither)."
            )

        if self.endpoint == "catalogs" and not self.catalog_name:
            raise ValueError("endpoint='catalogs' requires catalog_name.")

        # Auth validation.
        use_resource = bool(self.resource_key)
        if not use_resource and not (self.api_key_env_var and self.rest_endpoint):
            raise ValueError(
                "DataframeToBrazeComponent: supply `resource_key` "
                "(referring to a BrazeResourceComponent) OR both "
                "`api_key_env_var` + `rest_endpoint` (inline fallback)."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
        )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=self.retry_policy_backoff,  # type: ignore[arg-type]
            )

        cfg = self
        # Which resource keys the asset needs to declare.
        required_resource_keys: set = set()
        if use_resource:
            required_resource_keys.add(cfg.resource_key)  # type: ignore[arg-type]
        if cfg.source and (cfg.source.get("kind") or "").lower() == "sql":
            sql_rk = cfg.source.get("resource_key")
            if sql_rk:
                required_resource_keys.add(sql_rk)

        # ── Body: shared between upstream_asset and source path ──
        def _post_batches(context, df: pd.DataFrame) -> MaterializeResult:
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"upstream must be a DataFrame, got {type(df).__name__}")

            base_url, headers, timeout = _resolve_endpoint_and_headers(cfg, context)
            if cfg.endpoint == "users_track":
                url = f"{base_url}/users/track"
                default_batch = 75
            else:
                url = f"{base_url}/catalogs/{cfg.catalog_name}/items"
                default_batch = 50
            batch_size = cfg.batch_size or default_batch

            records = df.to_dict(orient="records")
            total = len(records)
            sent = failed = batches = 0

            for start in range(0, total, batch_size):
                chunk = records[start : start + batch_size]
                if cfg.endpoint == "users_track":
                    payload = _build_users_track_payload(chunk, cfg)
                else:
                    payload = _build_catalog_items_payload(chunk, cfg)

                if cfg.dry_run:
                    context.log.info(
                        f"[dry_run] Would POST {len(chunk)} rows to {url}"
                    )
                    sent += len(chunk)
                    batches += 1
                    continue

                resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
                batches += 1
                if 200 <= resp.status_code < 300:
                    sent += len(chunk)
                else:
                    failed += len(chunk)
                    body = (resp.text or "")[:400]
                    context.log.warning(
                        f"Braze POST failed: HTTP {resp.status_code} body={body}"
                    )

            context.log.info(
                f"Braze export ({cfg.endpoint}): {sent}/{total} rows in {batches} batches"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(
                metadata={
                    "rows_total": MetadataValue.int(total),
                    "rows_sent": MetadataValue.int(sent),
                    "rows_failed": MetadataValue.int(failed),
                    "batches": MetadataValue.int(batches),
                    "endpoint": MetadataValue.text(cfg.endpoint),
                    "batch_size": MetadataValue.int(batch_size),
                    "dry_run": MetadataValue.bool(cfg.dry_run),
                }
            )

        # ── Two paths depending on source shape ─────────────────
        if cfg.upstream_asset_key:
            upstream_key = AssetKey(cfg.upstream_asset_key.split("/"))

            @asset(
                name=cfg.asset_name,
                ins={"upstream": AssetIn(key=upstream_key)},
                group_name=cfg.group_name,
                description=cfg.description,
                deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
                owners=cfg.owners,
                tags=cfg.asset_tags,
                kinds=cfg.kinds,
                retry_policy=retry_policy,
                partitions_def=partitions_def,
                required_resource_keys=required_resource_keys or None,
            )
            def braze_export_upstream(
                context: AssetExecutionContext, upstream: pd.DataFrame
            ) -> MaterializeResult:
                return _post_batches(context, upstream)

            return Definitions(assets=[braze_export_upstream])

        # source: kind path
        @asset(
            name=cfg.asset_name,
            group_name=cfg.group_name,
            description=cfg.description,
            deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
            owners=cfg.owners,
            tags=cfg.asset_tags,
            kinds=cfg.kinds,
            retry_policy=retry_policy,
            partitions_def=partitions_def,
            required_resource_keys=required_resource_keys or None,
        )
        def braze_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            df = _resolve_source_df(cfg, context)
            return _post_batches(context, df)

        return Definitions(assets=[braze_export_inline])


# ─── Helpers ──────────────────────────────────────────────────────


def _build_partitions_def(
    partition_type: Optional[str],
    partition_start: Optional[str],
    partition_values: Optional[str],
    dynamic_partition_name: Optional[str],
):
    if not partition_type:
        return None
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    _pt = partition_type
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if _pt in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
    if _pt == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if _pt == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if _pt == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if _pt == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if _pt == "static":
        if not _values:
            raise ValueError("partition_type='static' requires non-empty partition_values.")
        return StaticPartitionsDefinition(_values)
    if _pt == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"Unknown partition_type: {_pt!r}")


def _resolve_endpoint_and_headers(
    cfg: DataframeToBrazeComponent, context: AssetExecutionContext
) -> tuple:
    """Return (base_url, headers, timeout) using resource_key OR inline auth."""
    if cfg.resource_key:
        resource = getattr(context.resources, cfg.resource_key)
        # BrazeResource internal helpers — public API stable
        return (
            resource._base_url(),
            resource._headers(),
            resource.request_timeout_seconds,
        )
    # Inline fallback
    token = os.environ.get(cfg.api_key_env_var) if cfg.api_key_env_var else None
    if not token and not cfg.dry_run:
        raise Failure(
            f"env var {cfg.api_key_env_var!r} is empty or unset — set your Braze REST API key."
        )
    return (
        (cfg.rest_endpoint or "").rstrip("/"),
        {
            "Authorization": f"Bearer {token or 'DRY_RUN'}",
            "Content-Type": "application/json",
        },
        cfg.request_timeout_seconds,
    )


def _resolve_source_df(
    cfg: DataframeToBrazeComponent, exec_ctx: AssetExecutionContext
) -> pd.DataFrame:
    """Materialize the inline source: config into a DataFrame."""
    src = cfg.source or {}
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
                conn = resource.get_connection()
                if hasattr(conn, "execute") and hasattr(conn, "df"):
                    return conn.execute(query).df()
                return pd.read_sql(query, conn)
            raise ValueError(
                f"source kind=sql: resource {rk!r} must expose .get_engine() or .get_connection()"
            )
        env = src.get("database_url_env_var")
        if env:
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
    raise ValueError(f"source kind={kind!r} not supported (sql / csv / inline)")


def _pick_user_id(row: Dict[str, Any], cfg: DataframeToBrazeComponent) -> Optional[str]:
    val = row.get(cfg.user_id_column)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        other = "braze_id" if cfg.user_id_column != "braze_id" else "external_id"
        val = row.get(other)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val)


def _apply_fields_map(row: Dict[str, Any], fields_map: Dict[str, str]) -> Dict[str, Any]:
    """Rename row keys per fields_map (source_col -> braze_field). Drops
    columns not in the map."""
    out: Dict[str, Any] = {}
    for src_col, dst_field in fields_map.items():
        if src_col in row and row[src_col] is not None and not (
            isinstance(row[src_col], float) and pd.isna(row[src_col])
        ):
            out[dst_field] = row[src_col]
    return out


def _build_users_track_payload(
    chunk: List[Dict[str, Any]], cfg: DataframeToBrazeComponent
) -> Dict[str, Any]:
    id_key = "external_id" if cfg.user_id_column != "braze_id" else "braze_id"
    custom_set = set(cfg.custom_attribute_columns or [])

    all_cols: set = set()
    for r in chunk:
        all_cols.update(r.keys())

    if cfg.fields_map:
        # fields_map path — user gives explicit source->dest mapping.
        # The user_id_column is applied separately (renamed to external_id).
        users = []
        for row in chunk:
            uid = _pick_user_id(row, cfg)
            if not uid:
                continue
            entry: Dict[str, Any] = {id_key: uid}
            mapped = _apply_fields_map(row, cfg.fields_map)
            # Custom attributes (still by SOURCE column name — apply mapping first)
            if custom_set:
                custom: Dict[str, Any] = {}
                # For each custom col name (source), if it was mapped, use the mapped name;
                # else use the raw name.
                for src_col in custom_set:
                    dst = cfg.fields_map.get(src_col, src_col)
                    if dst in mapped:
                        custom[dst] = mapped.pop(dst)
                if custom:
                    entry["custom_attributes"] = custom
            entry.update(mapped)
            users.append(entry)
        return {"attributes": users}

    # Legacy path: attribute_columns + custom_attribute_columns lists
    if cfg.attribute_columns is not None:
        attr_cols = list(cfg.attribute_columns)
    else:
        attr_cols = [c for c in all_cols if c != cfg.user_id_column and c not in custom_set]

    users = []
    for row in chunk:
        uid = _pick_user_id(row, cfg)
        if not uid:
            continue
        entry = {id_key: uid}
        for c in attr_cols:
            if c in row and row[c] is not None and not (isinstance(row[c], float) and pd.isna(row[c])):
                entry[c] = row[c]
        if custom_set:
            custom = {}
            for c in custom_set:
                if c in row and row[c] is not None and not (isinstance(row[c], float) and pd.isna(row[c])):
                    custom[c] = row[c]
            if custom:
                entry["custom_attributes"] = custom
        users.append(entry)
    return {"attributes": users}


def _build_catalog_items_payload(
    chunk: List[Dict[str, Any]], cfg: DataframeToBrazeComponent
) -> Dict[str, Any]:
    items = []
    for row in chunk:
        if cfg.fields_map:
            item = _apply_fields_map(row, cfg.fields_map)
            # If item_id_column was renamed, its value is now under Braze's
            # target name; find it and re-key as 'id'.
            id_dst = cfg.fields_map.get(cfg.item_id_column, cfg.item_id_column)
            raw_id = item.pop(id_dst, None) or row.get(cfg.item_id_column)
        else:
            raw_id = row.get(cfg.item_id_column)
            item = {c: v for c, v in row.items()
                    if c != cfg.item_id_column and v is not None
                    and not (isinstance(v, float) and pd.isna(v))}
        if raw_id is None or (isinstance(raw_id, float) and pd.isna(raw_id)):
            continue
        item["id"] = str(raw_id)
        items.append(item)
    return {"items": items}
