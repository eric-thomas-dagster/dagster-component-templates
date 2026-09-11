"""DataFrame to Segment.

Reverse-ETL sink — batch-POST DataFrame rows to Segment via the v1
batch endpoint. Delegates wire concerns to ``SegmentResource``.

Two modes via ``mode``:
- ``identify`` (default) — set/update user traits (POST /v1/batch with
  identify ops)
- ``track`` — record events (POST /v1/batch with track ops)
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


class DataframeToSegmentComponent(Component, Model, Resolvable):
    """Push DataFrame rows to Segment via the /v1/batch endpoint."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[str] = Field(default=None)
    source: Optional[Dict[str, Any]] = Field(default=None,
        description="Inline source config: {kind: sql|csv|inline, ...}")

    resource_key: str = Field(description="SegmentResourceComponent key.")

    mode: Literal["identify", "track"] = Field(default="identify",
        description="`identify` = set/update user traits; `track` = record events.")

    user_id_column: Optional[str] = Field(default=None,
        description="Column with userId (preferred Segment identifier).")
    anonymous_id_column: Optional[str] = Field(default=None,
        description="Column with anonymousId (fallback when userId is unknown — Segment requires one of the two).")

    event_column: Optional[str] = Field(default=None,
        description="For track mode: column with the event name. Required for track mode (or set default_event).")
    default_event: Optional[str] = Field(default=None,
        description="For track mode: fallback event name when column is null.")
    timestamp_column: Optional[str] = Field(default=None,
        description="Column with event timestamp (int seconds/ms, or ISO datetime — coerced to ISO 8601).")

    traits_map: Optional[Dict[str, str]] = Field(default=None,
        description="For identify mode: explicit source_col -> traits_key mapping.")
    properties_map: Optional[Dict[str, str]] = Field(default=None,
        description="For track mode: explicit source_col -> properties_key mapping.")

    fields_map: Optional[Dict[str, str]] = Field(default=None,
        description="Alias for traits_map (identify) or properties_map (track). Explicit maps take priority.")

    property_columns: Optional[List[str]] = Field(default=None,
        description="Legacy pass-through: which columns become traits/properties. Default: all except identifier/reserved.")

    batch_size: Optional[int] = Field(default=None,
        description="Cap on ops per HTTP request. Default 100 (Segment enforces ~500KB per batch — the resource repackages).")
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
        if not (self.user_id_column or self.anonymous_id_column):
            raise ValueError("Supply user_id_column OR anonymous_id_column (Segment requires one).")
        if self.mode == "track" and not (self.event_column or self.default_event):
            raise ValueError("mode='track' requires event_column OR default_event.")

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
            seg = getattr(context.resources, cfg.resource_key)
            records = df.to_dict(orient="records")
            ops = _build_ops(records, cfg)
            summary = seg.batch_ops(
                ops, batch_size=cfg.batch_size or 100,
                dry_run=cfg.dry_run, logger=context.log,
            )
            context.log.info(
                f"Segment {cfg.mode}: {summary['sent']}/{total} ops in "
                f"{summary['batches']} batches"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(metadata={
                "rows_total": MetadataValue.int(total),
                "ops_sent": MetadataValue.int(summary["sent"]),
                "ops_failed": MetadataValue.int(summary["failed"]),
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
            def segment_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[segment_export_upstream])

        @asset(name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks)
        def segment_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[segment_export_inline])


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


def _coerce_iso_ts(v: Any) -> Optional[str]:
    from datetime import date, datetime, timezone
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, int) or isinstance(v, float):
        # heuristic — treat 13+ digit values as ms, otherwise seconds
        secs = float(v) / 1000.0 if abs(v) >= 1e12 else float(v)
        return datetime.fromtimestamp(secs, tz=timezone.utc).isoformat()
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is None: v = v.tz_localize("UTC")
        return v.isoformat()
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc).isoformat()
    return str(v)


def _build_ops(records: List[Dict[str, Any]], cfg: DataframeToSegmentComponent) -> List[Dict[str, Any]]:
    reserved_cols = {
        cfg.user_id_column, cfg.anonymous_id_column,
        cfg.event_column, cfg.timestamp_column,
    }
    reserved_cols.discard(None)

    if cfg.mode == "identify":
        active_map = cfg.traits_map or cfg.fields_map
    else:
        active_map = cfg.properties_map or cfg.fields_map

    ops: List[Dict[str, Any]] = []
    for row in records:
        uid = _coerce(row.get(cfg.user_id_column)) if cfg.user_id_column else None
        aid = _coerce(row.get(cfg.anonymous_id_column)) if cfg.anonymous_id_column else None
        if not (uid or aid):
            continue

        body: Dict[str, Any] = {}
        if active_map:
            for src, dst in active_map.items():
                if src in reserved_cols: continue
                v = _coerce(row.get(src))
                if v is not None:
                    body[dst] = v
        else:
            cols = cfg.property_columns
            if cols is None:
                cols = [c for c in row.keys() if c not in reserved_cols]
            for c in cols:
                v = _coerce(row.get(c))
                if v is not None:
                    body[c] = v

        op: Dict[str, Any] = {}
        if cfg.mode == "identify":
            op["type"] = "identify"
            if body:
                op["traits"] = body
        else:
            name = row.get(cfg.event_column) if cfg.event_column else None
            if name is None or (isinstance(name, float) and pd.isna(name)):
                name = cfg.default_event
            if not name:
                continue
            op["type"] = "track"
            op["event"] = str(name)
            if body:
                op["properties"] = body

        if uid is not None: op["userId"] = str(uid)
        if aid is not None: op["anonymousId"] = str(aid)
        if cfg.timestamp_column:
            ts = _coerce_iso_ts(row.get(cfg.timestamp_column))
            if ts is not None:
                op["timestamp"] = ts
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


def _resolve_source_df(cfg: DataframeToSegmentComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
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
