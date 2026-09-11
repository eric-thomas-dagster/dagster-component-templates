"""DataFrame to Customer.io.

Reverse-ETL sink — batch-POST DataFrame rows to Customer.io via the v2
batch endpoint. Delegates wire concerns to ``CustomerIoResource``.

Two modes via ``mode``:
- ``identify`` (default) — bulk-upsert customer profiles (attribute sync)
- ``event`` — bulk-track events (name + optional data)
"""
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext, AssetIn, AssetKey, Component, ComponentLoadContext,
    Definitions, MaterializeResult, MetadataValue, Model, Resolvable,
    RetryPolicy, asset,
)
from pydantic import Field


class DataframeToCustomerIoComponent(Component, Model, Resolvable):
    """Push DataFrame rows to Customer.io via the /api/v2/batch endpoint."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[str] = Field(default=None)
    source: Optional[Dict[str, Any]] = Field(default=None,
        description="Inline source config: {kind: sql|csv|inline, ...}")

    resource_key: str = Field(description="CustomerIoResourceComponent key.")

    mode: Literal["identify", "event"] = Field(default="identify",
        description="`identify` = upsert customer profiles; `event` = bulk-track events.")

    id_column: Optional[str] = Field(default=None,
        description="Column with your customer id (Customer.io's `id` primary identifier). Preferred.")
    email_column: Optional[str] = Field(default=None,
        description="Column with email (fallback identifier). Rows lacking both id_column and email are dropped.")
    cio_id_column: Optional[str] = Field(default=None,
        description="Column with Customer.io's internal cio_id (rarely used — prefer id or email).")

    event_name_column: Optional[str] = Field(default=None,
        description="For event mode: column with event name. Required for event mode.")
    default_event_name: Optional[str] = Field(default=None,
        description="For event mode: fallback event name when the column is null.")
    event_timestamp_column: Optional[str] = Field(default=None,
        description="For event mode: column with event timestamp (unix seconds, or ISO datetime — coerced).")

    fields_map: Optional[Dict[str, str]] = Field(default=None,
        description="Explicit source_col -> destination_key mapping. For identify mode: lands under `attributes`. For event mode: lands under `data`.")

    attribute_columns: Optional[List[str]] = Field(default=None,
        description="Legacy pass-through: which columns become attributes/data. Default: all except identifier/reserved cols.")

    batch_size: Optional[int] = Field(default=None,
        description="Operations per request. Default 100 (Customer.io max).")
    dry_run: bool = Field(default=False)

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
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("Supply exactly ONE of upstream_asset_key OR source:")
        if not (self.id_column or self.email_column or self.cio_id_column):
            raise ValueError("Supply at least one of id_column, email_column, cio_id_column.")
        if self.mode == "event" and not (self.event_name_column or self.default_event_name):
            raise ValueError("mode='event' requires event_name_column OR default_event_name.")

        cfg = self
        partitions_def = _build_partitions_def(cfg.partition_type, cfg.partition_start,
                                               cfg.partition_values, cfg.dynamic_partition_name)
        retry_policy = None
        if cfg.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(max_retries=cfg.retry_policy_max_retries,
                                       delay=cfg.retry_policy_delay_seconds or 1,
                                       backoff=cfg.retry_policy_backoff)  # type: ignore[arg-type]

        rrks: set = {cfg.resource_key}
        if cfg.source and (cfg.source.get("kind") or "").lower() == "sql":
            sql_rk = cfg.source.get("resource_key")
            if sql_rk: rrks.add(sql_rk)

        def _run(context, df: pd.DataFrame) -> MaterializeResult:
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"upstream must be a DataFrame, got {type(df).__name__}")
            total = len(df)
            cio = getattr(context.resources, cfg.resource_key)
            records = df.to_dict(orient="records")
            ops = _build_ops(records, cfg)
            summary = cio.batch_ops(
                ops, batch_size=cfg.batch_size or 100,
                dry_run=cfg.dry_run, logger=context.log,
            )
            context.log.info(
                f"Customer.io {cfg.mode}: {summary['sent']}/{total} ops in "
                f"{summary['batches']} batches (soft errors: {summary['soft_errors']})"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(metadata={
                "rows_total": MetadataValue.int(total),
                "ops_sent": MetadataValue.int(summary["sent"]),
                "ops_failed": MetadataValue.int(summary["failed"]),
                "soft_errors": MetadataValue.int(summary["soft_errors"]),
                "batches": MetadataValue.int(summary["batches"]),
                "mode": MetadataValue.text(cfg.mode),
                "dry_run": MetadataValue.bool(cfg.dry_run),
            })

        if cfg.upstream_asset_key:
            uk = AssetKey(cfg.upstream_asset_key.split("/"))
            @asset(name=cfg.asset_name, ins={"upstream": AssetIn(key=uk)},
                   group_name=cfg.group_name, description=cfg.description,
                   deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
                   owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
                   retry_policy=retry_policy, partitions_def=partitions_def,
                   required_resource_keys=rrks)
            def customer_io_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[customer_io_export_upstream])

        @asset(name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks)
        def customer_io_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[customer_io_export_inline])


def _coerce(v: Any) -> Any:
    from datetime import date, datetime, timezone
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is None: v = v.tz_localize("UTC")
        return v.isoformat()
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


def _coerce_time_seconds(v: Any) -> Optional[int]:
    from datetime import date, datetime, timezone
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, int):
        return int(v)
    if isinstance(v, float):
        return int(v)
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is None: v = v.tz_localize("UTC")
        return int(v.timestamp())
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp())
    if isinstance(v, date):
        return int(datetime(v.year, v.month, v.day, tzinfo=timezone.utc).timestamp())
    return None


def _build_ops(records: List[Dict[str, Any]], cfg: DataframeToCustomerIoComponent) -> List[Dict[str, Any]]:
    reserved_cols = {
        cfg.id_column, cfg.email_column, cfg.cio_id_column,
        cfg.event_name_column, cfg.event_timestamp_column,
    }
    reserved_cols.discard(None)
    ops: List[Dict[str, Any]] = []
    for row in records:
        ids: Dict[str, Any] = {}
        if cfg.id_column:
            v = _coerce(row.get(cfg.id_column))
            if v is not None: ids["id"] = str(v)
        if cfg.email_column:
            v = _coerce(row.get(cfg.email_column))
            if v is not None: ids["email"] = str(v)
        if cfg.cio_id_column:
            v = _coerce(row.get(cfg.cio_id_column))
            if v is not None: ids["cio_id"] = str(v)
        if not ids:
            continue

        body: Dict[str, Any] = {}
        if cfg.fields_map:
            for src, dst in cfg.fields_map.items():
                if src in reserved_cols: continue
                v = _coerce(row.get(src))
                if v is not None:
                    body[dst] = v
        else:
            cols = cfg.attribute_columns
            if cols is None:
                cols = [c for c in row.keys() if c not in reserved_cols]
            for c in cols:
                v = _coerce(row.get(c))
                if v is not None:
                    body[c] = v

        if cfg.mode == "identify":
            op: Dict[str, Any] = {"type": "identify", "identifiers": ids}
            if body:
                op["attributes"] = body
        else:
            name = row.get(cfg.event_name_column) if cfg.event_name_column else None
            if name is None or (isinstance(name, float) and pd.isna(name)):
                name = cfg.default_event_name
            if not name:
                continue
            op = {"type": "event", "identifiers": ids, "name": str(name)}
            if cfg.event_timestamp_column:
                t = _coerce_time_seconds(row.get(cfg.event_timestamp_column))
                if t is not None:
                    op["timestamp"] = t
            if body:
                op["data"] = body
        ops.append(op)
    return ops


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name):
    if not partition_type: return None
    from dagster import (DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                         MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                         StaticPartitionsDefinition, DynamicPartitionsDefinition)
    _pt = partition_type
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if _pt in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={_pt!r} requires partition_start.")
    if _pt == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if _pt == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if _pt == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if _pt == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if _pt == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if _pt == "dynamic":
        if not dynamic_partition_name: raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"Unknown partition_type: {_pt!r}")


def _resolve_source_df(cfg: DataframeToCustomerIoComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
    src = cfg.source or {}
    kind = (src.get("kind") or "").lower()
    if kind == "sql":
        q = src.get("query")
        if not q: raise ValueError("source kind=sql requires 'query'")
        rk = src.get("resource_key")
        if rk:
            r = getattr(exec_ctx.resources, rk)
            if hasattr(r, "get_engine"): return pd.read_sql(q, r.get_engine())
            if hasattr(r, "get_connection"):
                c = r.get_connection()
                if hasattr(c, "execute") and hasattr(c, "df"): return c.execute(q).df()
                return pd.read_sql(q, c)
            raise ValueError(f"source resource {rk!r} needs .get_engine() or .get_connection()")
        env = src.get("database_url_env_var")
        if env:
            from sqlalchemy import create_engine
            u = os.environ.get(env, "")
            if not u: raise ValueError(f"env var {env!r} unset")
            return pd.read_sql(q, create_engine(u))
        raise ValueError("source kind=sql requires resource_key OR database_url_env_var")
    if kind == "csv":
        p = src.get("path")
        if not p: raise ValueError("source kind=csv requires 'path'")
        return pd.read_csv(p, **(src.get("read_csv_kwargs") or {}))
    if kind == "inline":
        return pd.DataFrame(src.get("rows") or [])
    raise ValueError(f"source kind={kind!r} not supported")
