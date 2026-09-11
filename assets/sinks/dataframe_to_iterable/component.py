"""DataFrame to Iterable.

Reverse-ETL sink — POST profile updates OR events to Iterable via the
bulk endpoints. Delegates wire concerns to ``IterableResource``.

Two modes via ``mode``:
- ``users`` (default) — ``/api/users/bulkUpdate`` (upsert)
- ``events`` — ``/api/events/trackBulk``
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


class DataframeToIterableComponent(Component, Model, Resolvable):
    """Push DataFrame rows to Iterable via bulk users or events endpoints."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[str] = Field(default=None)
    source: Optional[Dict[str, Any]] = Field(default=None,
        description="Inline source config: {kind: sql|csv|inline, ...}")

    resource_key: Optional[str] = Field(default=None,
        description="IterableResourceComponent key (preferred).")
    api_key_env_var: Optional[str] = Field(default=None,
        description="Inline auth fallback.")
    base_url: Optional[str] = Field(default=None,
        description="Inline auth: Iterable API base URL (default https://api.iterable.com; use https://api.eu.iterable.com for EU).")

    mode: Literal["users", "events"] = Field(default="users",
        description="`users` = bulk-upsert profiles; `events` = bulk-track events.")

    fields_map: Optional[Dict[str, str]] = Field(default=None,
        description="Explicit source_col -> iterable_field mapping. For users, fields other than userId/email land under dataFields; for events, everything except eventName/userId/email/campaignId lands under dataFields.")

    user_id_column: Optional[str] = Field(default=None,
        description="Column holding the userId (Iterable's primary id when different from email).")
    email_column: Optional[str] = Field(default="email",
        description="Column holding the email address.")
    prefer_user_id: bool = Field(default=False,
        description="For users mode: set preferUserId=true on each user object (Iterable disambiguates via userId when both are present).")

    event_name_column: Optional[str] = Field(default=None,
        description="For events mode: column with the eventName. Required for events mode.")
    campaign_id_column: Optional[str] = Field(default=None,
        description="For events mode: column with the campaignId (optional).")

    data_field_columns: Optional[List[str]] = Field(default=None,
        description="Legacy pass-through: which columns become dataFields (users) or event data (events). Default: all except identifier/event columns.")

    batch_size: Optional[int] = Field(default=None,
        description="Rows per HTTP request. Default 1000 (Iterable max for both endpoints).")
    dry_run: bool = Field(default=False)
    request_timeout_seconds: int = Field(default=30)

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
        if not self.resource_key and not self.api_key_env_var:
            raise ValueError("Supply resource_key OR api_key_env_var.")
        if self.mode == "events" and not self.event_name_column:
            raise ValueError("mode='events' requires event_name_column.")

        cfg = self
        partitions_def = _build_partitions_def(cfg.partition_type, cfg.partition_start,
                                               cfg.partition_values, cfg.dynamic_partition_name)
        retry_policy = None
        if cfg.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(max_retries=cfg.retry_policy_max_retries,
                                       delay=cfg.retry_policy_delay_seconds or 1,
                                       backoff=cfg.retry_policy_backoff)  # type: ignore[arg-type]

        rrks: set = set()
        if cfg.resource_key: rrks.add(cfg.resource_key)
        if cfg.source and (cfg.source.get("kind") or "").lower() == "sql":
            sql_rk = cfg.source.get("resource_key")
            if sql_rk: rrks.add(sql_rk)

        def _run(context, df: pd.DataFrame) -> MaterializeResult:
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"upstream must be a DataFrame, got {type(df).__name__}")
            total = len(df)
            iterable = _get_resource(cfg, context)
            records = df.to_dict(orient="records")
            if cfg.mode == "users":
                rows = _build_users(records, cfg)
                summary = iterable.bulk_update_users(
                    rows, batch_size=cfg.batch_size or 1000,
                    dry_run=cfg.dry_run, logger=context.log,
                )
            else:
                rows = _build_events(records, cfg)
                summary = iterable.bulk_track_events(
                    rows, batch_size=cfg.batch_size or 1000,
                    dry_run=cfg.dry_run, logger=context.log,
                )
            context.log.info(
                f"Iterable {cfg.mode}: {summary['sent']}/{total} rows in "
                f"{summary['batches']} batches (soft errors: {summary['soft_errors']})"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(metadata={
                "rows_total": MetadataValue.int(total),
                "rows_sent": MetadataValue.int(summary["sent"]),
                "rows_failed": MetadataValue.int(summary["failed"]),
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
                   required_resource_keys=rrks or None)
            def iterable_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[iterable_export_upstream])

        @asset(name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks or None)
        def iterable_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[iterable_export_inline])


def _get_resource(cfg: DataframeToIterableComponent, context: AssetExecutionContext) -> Any:
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
    from dagster_community_components import IterableResource
    return IterableResource(
        api_key_env_var=cfg.api_key_env_var,
        base_url=cfg.base_url or "https://api.iterable.com",
        request_timeout_seconds=cfg.request_timeout_seconds,
    )


def _coerce(v: Any) -> Any:
    from datetime import date, datetime, timezone
    if v is None or (isinstance(v, float) and pd.isna(v)): return None
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is None: v = v.tz_localize("UTC")
        return v.isoformat()
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


def _build_users(records: List[Dict[str, Any]], cfg: DataframeToIterableComponent) -> List[Dict[str, Any]]:
    id_cols = {cfg.user_id_column, cfg.email_column}
    users: List[Dict[str, Any]] = []
    for row in records:
        u: Dict[str, Any] = {}
        uid = _coerce(row.get(cfg.user_id_column)) if cfg.user_id_column else None
        email = _coerce(row.get(cfg.email_column)) if cfg.email_column else None
        if uid is not None: u["userId"] = uid
        if email is not None: u["email"] = email
        if not (uid or email): continue
        if cfg.prefer_user_id and uid is not None:
            u["preferUserId"] = True

        data_fields: Dict[str, Any] = {}
        if cfg.fields_map:
            for src, dst in cfg.fields_map.items():
                if src in id_cols: continue
                v = _coerce(row.get(src))
                if v is None: continue
                data_fields[dst] = v
        else:
            cols = cfg.data_field_columns
            if cols is None:
                cols = [c for c in row.keys() if c not in id_cols]
            for c in cols:
                v = _coerce(row.get(c))
                if v is None: continue
                data_fields[c] = v
        if data_fields:
            u["dataFields"] = data_fields
        users.append(u)
    return users


def _build_events(records: List[Dict[str, Any]], cfg: DataframeToIterableComponent) -> List[Dict[str, Any]]:
    reserved_cols = {cfg.user_id_column, cfg.email_column, cfg.event_name_column, cfg.campaign_id_column}
    events: List[Dict[str, Any]] = []
    for row in records:
        ename = _coerce(row.get(cfg.event_name_column))
        if not ename: continue
        e: Dict[str, Any] = {"eventName": str(ename)}
        uid = _coerce(row.get(cfg.user_id_column)) if cfg.user_id_column else None
        email = _coerce(row.get(cfg.email_column)) if cfg.email_column else None
        if uid is not None: e["userId"] = uid
        if email is not None: e["email"] = email
        if not (uid or email): continue
        cid = _coerce(row.get(cfg.campaign_id_column)) if cfg.campaign_id_column else None
        if cid is not None:
            try: e["campaignId"] = int(cid)
            except (ValueError, TypeError): pass
        data_fields: Dict[str, Any] = {}
        if cfg.fields_map:
            for src, dst in cfg.fields_map.items():
                if src in reserved_cols: continue
                v = _coerce(row.get(src))
                if v is None: continue
                data_fields[dst] = v
        else:
            cols = cfg.data_field_columns
            if cols is None:
                cols = [c for c in row.keys() if c not in reserved_cols]
            for c in cols:
                v = _coerce(row.get(c))
                if v is None: continue
                data_fields[c] = v
        if data_fields:
            e["dataFields"] = data_fields
        events.append(e)
    return events


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


def _resolve_source_df(cfg: DataframeToIterableComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
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
