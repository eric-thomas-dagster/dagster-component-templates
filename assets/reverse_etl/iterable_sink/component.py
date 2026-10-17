"""Iterable Sink.

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


class IterableSinkComponent(Component, Model, Resolvable):
    """Push DataFrame rows to Iterable via bulk users or events endpoints."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(default=None)
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
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to send to the current static/dynamic partition key.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
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
            from dagster import Backoff as _Backoff

            retry_policy = RetryPolicy(max_retries=cfg.retry_policy_max_retries,
                                       delay=cfg.retry_policy_delay_seconds or 1,
                                       backoff=_Backoff.EXPONENTIAL if str(cfg.retry_policy_backoff).lower() == "exponential" else _Backoff.LINEAR)

        rrks: set = set()
        if cfg.resource_key: rrks.add(cfg.resource_key)
        if cfg.source and (cfg.source.get("kind") or "").lower() == "sql":
            sql_rk = cfg.source.get("resource_key")
            if sql_rk: rrks.add(sql_rk)

        def _run(context, df: pd.DataFrame) -> MaterializeResult:
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"upstream must be a DataFrame, got {type(df).__name__}")

            df = _filter_df_to_partition(df, context, cfg.partition_date_column, cfg.partition_static_column)
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
            @asset(freshness_policy=_freshness_policy_obj, name=cfg.asset_name, ins={"upstream": AssetIn(key=uk)},
                   group_name=cfg.group_name, description=cfg.description,
                   deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
                   owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
                   retry_policy=retry_policy, partitions_def=partitions_def,
                   required_resource_keys=rrks or None)
            def iterable_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[iterable_export_upstream])

        @asset(freshness_policy=_freshness_policy_obj, name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks or None)
        def iterable_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[iterable_export_inline])


# Self-contained: `IterableResource` is inlined from resources/iterable_resource (a component.py must not import
# another component -- the CLI copies only this file). Keep in sync with that resource.
from typing import Any, Dict, List, Optional  # noqa: E402,F401
import dagster as dg  # noqa: E402
from pydantic import Field  # noqa: E402,F401


_MAX_USERS_PER_CALL = 1000
_MAX_EVENTS_PER_CALL = 1000


class IterableResource(dg.ConfigurableResource):
    """Iterable REST API workhorse — auth + typed batch operations."""

    api_key_env_var: str = Field(default="ITERABLE_API_KEY",
        description="Env var holding the Iterable API key.")
    base_url: str = Field(default="https://api.iterable.com",
        description="Iterable API base URL. Use https://api.eu.iterable.com for EU workspaces.")
    request_timeout_seconds: int = Field(default=30)

    def _token(self) -> str:
        import os
        t = os.environ.get(self.api_key_env_var)
        if not t: raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your Iterable API key.")
        return t

    def _headers(self) -> Dict[str, str]:
        return {"Api-Key": self._token(), "Content-Type": "application/json"}

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body, headers=self._headers(),
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Iterable POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def bulk_update_users(
        self, users: List[Dict[str, Any]],
        batch_size: int = _MAX_USERS_PER_CALL,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-upsert Iterable users via /api/users/bulkUpdate.

        Args:
            users: List of user dicts. Each must have ``userId`` OR ``email``
                (or both — set ``preferUserId: true`` on each to disambiguate).
                Extra fields go under ``dataFields``.
            batch_size: Max 1,000 per Iterable docs.
        """
        return self._run_batches("/api/users/bulkUpdate", "users", users,
                                 min(batch_size, _MAX_USERS_PER_CALL),
                                 dry_run, logger, invalid_key="invalidEmails")

    def bulk_track_events(
        self, events: List[Dict[str, Any]],
        batch_size: int = _MAX_EVENTS_PER_CALL,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-track events via /api/events/trackBulk. Each event needs
        ``eventName`` + user identifier (``userId`` or ``email``)."""
        return self._run_batches("/api/events/trackBulk", "events", events,
                                 min(batch_size, _MAX_EVENTS_PER_CALL),
                                 dry_run, logger, invalid_key="disallowedEventNames")

    def _run_batches(
        self, path: str, key_name: str, rows: List[Dict[str, Any]],
        batch_size: int, dry_run: bool, logger: Any, invalid_key: str,
    ) -> Dict[str, Any]:
        total = len(rows)
        sent = failed = soft_errors = batches = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning(f"Iterable {path}: no rows.")
            return {"sent": 0, "failed": 0, "soft_errors": 0, "batches": 0, "error_samples": []}

        for start in range(0, total, batch_size):
            chunk = rows[start : start + batch_size]
            body = {key_name: chunk}
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} to {path}")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw(path, body)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                try:
                    j = resp.json()
                except Exception:
                    j = None
                if isinstance(j, dict):
                    fc = j.get("failCount") or 0
                    if fc:
                        soft_errors += fc
                        inv = j.get(invalid_key) or j.get("filteredOutFields") or []
                        error_samples.extend(inv[: max(0, 3 - len(error_samples))])
                        if logger is not None:
                            logger.warning(f"Iterable {path}: {fc} per-row failures; sample: {inv[:3]}")
            else:
                failed += len(chunk)
                if logger is not None:
                    logger.warning(f"Iterable POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:400]}")
        return {"sent": sent, "failed": failed, "soft_errors": soft_errors, "batches": batches, "error_samples": error_samples}



def _get_resource(cfg: IterableSinkComponent, context: AssetExecutionContext) -> Any:
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
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


def _build_users(records: List[Dict[str, Any]], cfg: IterableSinkComponent) -> List[Dict[str, Any]]:
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


def _build_events(records: List[Dict[str, Any]], cfg: IterableSinkComponent) -> List[Dict[str, Any]]:
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


def _resolve_source_df(cfg: IterableSinkComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


# BEGIN partition row-filter helper (inlined per component; keep copies identical, see tests/test_partition_port_ingestion.py)
from typing import Literal  # noqa: E402,F401


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the frame is returned unchanged."""
    import pandas as pd

    if not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition: no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        return df[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df
# END partition row-filter helper
