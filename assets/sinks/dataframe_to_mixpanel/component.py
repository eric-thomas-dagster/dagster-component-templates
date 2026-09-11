"""DataFrame to Mixpanel.

Reverse-ETL sink — batch-POST DataFrame rows to Mixpanel via bulk
endpoints. Delegates wire concerns to ``MixpanelResource``.

Two modes via ``mode``:
- ``events`` (default) — ``/import`` (bulk event ingest, service-account auth,
  up to 2000/batch)
- ``profiles`` — ``/engage#profile-set`` (bulk profile ops, project-token
  auth, up to 50/batch)
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


class DataframeToMixpanelComponent(Component, Model, Resolvable):
    """Push DataFrame rows to Mixpanel via bulk events or profile endpoints."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[str] = Field(default=None)
    source: Optional[Dict[str, Any]] = Field(default=None,
        description="Inline source config: {kind: sql|csv|inline, ...}")

    resource_key: str = Field(description="MixpanelResourceComponent key.")

    mode: Literal["events", "profiles"] = Field(default="events",
        description="`events` = bulk-import events via /import; `profiles` = bulk-set profiles via /engage.")

    event_column: Optional[str] = Field(default=None,
        description="For events mode: column with the event name. Required for events mode.")
    default_event: Optional[str] = Field(default=None,
        description="Fallback event name when `event_column` is unset or null.")

    distinct_id_column: Optional[str] = Field(default=None,
        description="Column with Mixpanel distinct_id. Required for both modes.")
    time_column: Optional[str] = Field(default=None,
        description="For events mode: column with event timestamp (unix seconds int, or ISO datetime — coerced to seconds).")
    insert_id_column: Optional[str] = Field(default=None,
        description="For events mode: column with per-event dedup key ($insert_id — Mixpanel drops repeats).")

    properties_map: Optional[Dict[str, str]] = Field(default=None,
        description="Explicit source_col -> destination_key mapping. For events, lands under `properties`. For profiles, lands under `$set`.")

    property_columns: Optional[List[str]] = Field(default=None,
        description="Legacy pass-through: which columns become properties. Default: all except identifier/reserved.")

    batch_size: Optional[int] = Field(default=None,
        description="Rows per HTTP request. Default: 2000 for events (Mixpanel max), 50 for profiles (Mixpanel max).")
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
        if not self.distinct_id_column:
            raise ValueError("distinct_id_column is required.")
        if self.mode == "events" and not (self.event_column or self.default_event):
            raise ValueError("mode='events' requires event_column OR default_event.")

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
            mp = getattr(context.resources, cfg.resource_key)
            records = df.to_dict(orient="records")
            if cfg.mode == "events":
                default_bs = 2000
                rows = _build_events(records, cfg)
                summary = mp.import_events_bulk(
                    rows, batch_size=cfg.batch_size or default_bs,
                    dry_run=cfg.dry_run, logger=context.log,
                )
                imported = summary.get("num_records_imported", 0)
            else:
                default_bs = 50
                rows = _build_profile_ops(records, cfg)
                summary = mp.set_profiles_bulk(
                    rows, batch_size=cfg.batch_size or default_bs,
                    dry_run=cfg.dry_run, logger=context.log,
                )
                imported = 0
            context.log.info(
                f"Mixpanel {cfg.mode}: {summary['sent']}/{total} rows in "
                f"{summary['batches']} batches"
                f"{f' (imported {imported})' if cfg.mode == 'events' else ''}"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            md: Dict[str, Any] = {
                "rows_total": MetadataValue.int(total),
                "rows_sent": MetadataValue.int(summary["sent"]),
                "rows_failed": MetadataValue.int(summary["failed"]),
                "batches": MetadataValue.int(summary["batches"]),
                "mode": MetadataValue.text(cfg.mode),
                "dry_run": MetadataValue.bool(cfg.dry_run),
            }
            if cfg.mode == "events":
                md["num_records_imported"] = MetadataValue.int(imported)
            return MaterializeResult(metadata=md)

        if cfg.upstream_asset_key:
            uk = AssetKey(cfg.upstream_asset_key.split("/"))
            @asset(name=cfg.asset_name, ins={"upstream": AssetIn(key=uk)},
                   group_name=cfg.group_name, description=cfg.description,
                   deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
                   owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
                   retry_policy=retry_policy, partitions_def=partitions_def,
                   required_resource_keys=rrks)
            def mixpanel_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[mixpanel_export_upstream])

        @asset(name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks)
        def mixpanel_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[mixpanel_export_inline])


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


def _build_events(records: List[Dict[str, Any]], cfg: DataframeToMixpanelComponent) -> List[Dict[str, Any]]:
    reserved_cols = {cfg.event_column, cfg.distinct_id_column, cfg.time_column, cfg.insert_id_column}
    reserved_cols.discard(None)
    events: List[Dict[str, Any]] = []
    for row in records:
        name = row.get(cfg.event_column) if cfg.event_column else None
        if name is None or (isinstance(name, float) and pd.isna(name)):
            name = cfg.default_event
        if not name:
            continue
        did = _coerce(row.get(cfg.distinct_id_column))
        if did is None:
            continue
        props: Dict[str, Any] = {"distinct_id": str(did)}
        if cfg.time_column:
            t = _coerce_time_seconds(row.get(cfg.time_column))
            if t is not None:
                props["time"] = t
        if cfg.insert_id_column:
            iid = _coerce(row.get(cfg.insert_id_column))
            if iid is not None:
                props["$insert_id"] = str(iid)

        if cfg.properties_map:
            for src, dst in cfg.properties_map.items():
                if src in reserved_cols: continue
                v = _coerce(row.get(src))
                if v is not None:
                    props[dst] = v
        else:
            cols = cfg.property_columns
            if cols is None:
                cols = [c for c in row.keys() if c not in reserved_cols]
            for c in cols:
                v = _coerce(row.get(c))
                if v is not None:
                    props[c] = v
        events.append({"event": str(name), "properties": props})
    return events


def _build_profile_ops(records: List[Dict[str, Any]], cfg: DataframeToMixpanelComponent) -> List[Dict[str, Any]]:
    reserved_cols = {cfg.distinct_id_column}
    ops: List[Dict[str, Any]] = []
    for row in records:
        did = _coerce(row.get(cfg.distinct_id_column))
        if did is None:
            continue
        set_body: Dict[str, Any] = {}
        if cfg.properties_map:
            for src, dst in cfg.properties_map.items():
                if src in reserved_cols: continue
                v = _coerce(row.get(src))
                if v is not None:
                    set_body[dst] = v
        else:
            cols = cfg.property_columns
            if cols is None:
                cols = [c for c in row.keys() if c not in reserved_cols]
            for c in cols:
                v = _coerce(row.get(c))
                if v is not None:
                    set_body[c] = v
        if not set_body:
            continue
        ops.append({"$distinct_id": str(did), "$set": set_body})
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


def _resolve_source_df(cfg: DataframeToMixpanelComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
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
