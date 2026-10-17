"""Amplitude Event Track.

Reverse-ETL sink — batch-POST DataFrame rows as Amplitude events via the
HTTP V2 API. Delegates all wire concerns (auth, batching, response
parsing) to ``AmplitudeResource``; this module only shapes DataFrame
rows into Amplitude event dicts.

Source shapes (pick one): ``upstream_asset_key`` OR
``source: {kind: sql|csv|inline, ...}``.

Auth: ``resource_key`` (recommended, references an
``AmplitudeResourceComponent``) OR inline ``api_key_env_var`` + ``base_url``.
"""
import os
from typing import Any, Dict, List, Optional

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


class AmplitudeEventTrackComponent(Component, Model, Resolvable):
    """Push DataFrame rows as Amplitude events via the HTTP V2 API."""

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(default=None)
    source: Optional[Dict[str, Any]] = Field(default=None,
        description="Inline source config: {kind: sql|csv|inline, ...}")

    resource_key: Optional[str] = Field(default=None,
        description="AmplitudeResourceComponent key (preferred).")
    api_key_env_var: Optional[str] = Field(default=None,
        description="Inline auth fallback.")
    base_url: Optional[str] = Field(default=None,
        description="Inline auth: Amplitude API base URL (default https://api2.amplitude.com; use https://api.eu.amplitude.com for EU).")

    event_type_column: Optional[str] = Field(default=None,
        description="Column holding the event_type. If unset, use `default_event_type`.")
    default_event_type: Optional[str] = Field(default=None,
        description="Fallback event_type when `event_type_column` is unset or the row's value is null.")

    user_id_column: Optional[str] = Field(default=None,
        description="Column holding the user_id (Amplitude's primary user id).")
    device_id_column: Optional[str] = Field(default=None,
        description="Column holding the device_id (used when user_id is absent — Amplitude requires ONE).")

    time_column: Optional[str] = Field(default=None,
        description="Column holding the event timestamp (unix ms int, or ISO datetime — coerced to ms).")
    insert_id_column: Optional[str] = Field(default=None,
        description="Column holding a per-event de-dup key (Amplitude drops repeated insert_ids for 7d).")

    event_properties_map: Optional[Dict[str, str]] = Field(default=None,
        description="Explicit source_col -> event_properties key mapping.")
    user_properties_map: Optional[Dict[str, str]] = Field(default=None,
        description="Explicit source_col -> user_properties key mapping.")

    event_property_columns: Optional[List[str]] = Field(default=None,
        description="Legacy pass-through: which columns land under event_properties. Default: all cols except identifier/reserved cols.")

    batch_size: Optional[int] = Field(default=None,
        description="Events per HTTP request. Default 1000 (Amplitude V2 max).")
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
        if not self.event_type_column and not self.default_event_type:
            raise ValueError("Supply event_type_column OR default_event_type.")
        if not self.user_id_column and not self.device_id_column:
            raise ValueError("Supply user_id_column OR device_id_column (Amplitude requires one).")

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

            df = _filter_df_to_partition(df, context, cfg.partition_date_column, cfg.partition_static_column)
            total = len(df)
            amp = _get_resource(cfg, context)
            records = df.to_dict(orient="records")
            events = _build_events(records, cfg)
            summary = amp.track_events_bulk(
                events, batch_size=cfg.batch_size or 1000,
                dry_run=cfg.dry_run, logger=context.log,
            )
            context.log.info(
                f"Amplitude /2/httpapi: {summary['sent']}/{total} events in "
                f"{summary['batches']} batches (ingested: {summary['ingested']})"
                f"{' — dry_run' if cfg.dry_run else ''}"
            )
            return MaterializeResult(metadata={
                "rows_total": MetadataValue.int(total),
                "events_sent": MetadataValue.int(summary["sent"]),
                "events_failed": MetadataValue.int(summary["failed"]),
                "events_ingested": MetadataValue.int(summary["ingested"]),
                "batches": MetadataValue.int(summary["batches"]),
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
            def amplitude_export_upstream(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
                return _run(context, upstream)
            return Definitions(assets=[amplitude_export_upstream])

        @asset(freshness_policy=_freshness_policy_obj, name=cfg.asset_name, group_name=cfg.group_name,
               description=cfg.description,
               deps=[AssetKey(d.split("/")) for d in (cfg.deps or [])],
               owners=cfg.owners, tags=cfg.asset_tags, kinds=cfg.kinds,
               retry_policy=retry_policy, partitions_def=partitions_def,
               required_resource_keys=rrks or None)
        def amplitude_export_inline(context: AssetExecutionContext) -> MaterializeResult:
            return _run(context, _resolve_source_df(cfg, context))
        return Definitions(assets=[amplitude_export_inline])


# Self-contained: `AmplitudeResource` is inlined from resources/amplitude_resource (a component.py must not import
# another component -- the CLI copies only this file). Keep in sync with that resource.
from typing import Any, Dict, List, Optional  # noqa: E402,F401
import dagster as dg  # noqa: E402
from pydantic import Field  # noqa: E402,F401


_MAX_EVENTS_PER_CALL = 1000


class AmplitudeResource(dg.ConfigurableResource):
    """Amplitude REST API workhorse — auth + typed batch operations."""

    api_key_env_var: str = Field(default="AMPLITUDE_API_KEY",
        description="Env var holding the Amplitude project API key.")
    base_url: str = Field(default="https://api2.amplitude.com",
        description="Amplitude API base URL. Use https://api.eu.amplitude.com for EU projects.")
    request_timeout_seconds: int = Field(default=30)

    def _token(self) -> str:
        import os
        t = os.environ.get(self.api_key_env_var)
        if not t: raise dg.Failure(f"env var {self.api_key_env_var!r} is unset — set your Amplitude API key.")
        return t

    def _post_raw(self, path: str, json_body: Any) -> Any:
        import requests
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        return requests.post(url, json=json_body,
                             headers={"Content-Type": "application/json", "Accept": "*/*"},
                             timeout=self.request_timeout_seconds)

    def post(self, path: str, json_body: Any) -> Any:
        """Raw escape hatch — POST + parse JSON. Raises Failure on non-2xx."""
        resp = self._post_raw(path, json_body)
        if not (200 <= resp.status_code < 300):
            raise dg.Failure(f"Amplitude POST {path} failed: HTTP {resp.status_code} body={(resp.text or '')[:500]}")
        try:
            return resp.json()
        except Exception:
            return None

    def track_events_bulk(
        self, events: List[Dict[str, Any]],
        batch_size: int = _MAX_EVENTS_PER_CALL,
        dry_run: bool = False, logger: Any = None,
    ) -> Dict[str, Any]:
        """Batch-track Amplitude events via /2/httpapi.

        Args:
            events: List of event dicts. Each MUST have ``user_id`` OR
                ``device_id`` and ``event_type``. Optional ``event_properties``,
                ``user_properties``, ``time`` (unix ms), ``insert_id`` for
                de-dup, ``groups`` for account-level analytics.
            batch_size: Max 1000 events per HTTPV2 request.

        Returns: {sent, failed, batches, ingested, error_samples}
        """
        total = len(events)
        sent = failed = batches = ingested = 0
        error_samples: List[Any] = []
        if total == 0:
            if logger is not None: logger.warning("Amplitude /2/httpapi: no events.")
            return {"sent": 0, "failed": 0, "batches": 0, "ingested": 0, "error_samples": []}
        batch_size = min(batch_size, _MAX_EVENTS_PER_CALL)

        for start in range(0, total, batch_size):
            chunk = events[start : start + batch_size]
            body = {"api_key": self._token(), "events": chunk}
            if dry_run:
                if logger is not None: logger.info(f"[dry_run] Would POST {len(chunk)} events to Amplitude /2/httpapi")
                sent += len(chunk); batches += 1; continue
            resp = self._post_raw("/2/httpapi", body)
            batches += 1
            if 200 <= resp.status_code < 300:
                sent += len(chunk)
                try:
                    j = resp.json()
                    if isinstance(j, dict):
                        ingested += int(j.get("events_ingested") or 0)
                except Exception:
                    pass
            else:
                failed += len(chunk)
                sample = (resp.text or "")[:400]
                if len(error_samples) < 3:
                    error_samples.append(sample)
                if logger is not None:
                    logger.warning(f"Amplitude POST failed: HTTP {resp.status_code} body={sample}")

        return {"sent": sent, "failed": failed, "batches": batches, "ingested": ingested, "error_samples": error_samples}



def _get_resource(cfg: AmplitudeEventTrackComponent, context: AssetExecutionContext) -> Any:
    if cfg.resource_key:
        return getattr(context.resources, cfg.resource_key)
    return AmplitudeResource(
        api_key_env_var=cfg.api_key_env_var,
        base_url=cfg.base_url or "https://api2.amplitude.com",
        request_timeout_seconds=cfg.request_timeout_seconds,
    )


def _coerce_time_ms(v: Any) -> Optional[int]:
    from datetime import date, datetime, timezone
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, (int,)):
        return int(v)
    if isinstance(v, float):
        return int(v)
    if isinstance(v, pd.Timestamp):
        if v.tzinfo is None:
            v = v.tz_localize("UTC")
        return int(v.timestamp() * 1000)
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp() * 1000)
    if isinstance(v, date):
        return int(datetime(v.year, v.month, v.day, tzinfo=timezone.utc).timestamp() * 1000)
    return None


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


def _build_events(records: List[Dict[str, Any]], cfg: AmplitudeEventTrackComponent) -> List[Dict[str, Any]]:
    reserved_cols = {
        cfg.user_id_column, cfg.device_id_column, cfg.event_type_column,
        cfg.time_column, cfg.insert_id_column,
    }
    reserved_cols.discard(None)
    events: List[Dict[str, Any]] = []
    for row in records:
        et = row.get(cfg.event_type_column) if cfg.event_type_column else None
        if et is None or (isinstance(et, float) and pd.isna(et)):
            et = cfg.default_event_type
        if not et:
            continue
        e: Dict[str, Any] = {"event_type": str(et)}

        uid = _coerce(row.get(cfg.user_id_column)) if cfg.user_id_column else None
        did = _coerce(row.get(cfg.device_id_column)) if cfg.device_id_column else None
        if uid is not None: e["user_id"] = str(uid)
        if did is not None: e["device_id"] = str(did)
        if not (uid or did):
            continue

        if cfg.time_column:
            tms = _coerce_time_ms(row.get(cfg.time_column))
            if tms is not None:
                e["time"] = tms

        if cfg.insert_id_column:
            iid = _coerce(row.get(cfg.insert_id_column))
            if iid is not None:
                e["insert_id"] = str(iid)

        ev_props: Dict[str, Any] = {}
        us_props: Dict[str, Any] = {}
        if cfg.event_properties_map:
            for src, dst in cfg.event_properties_map.items():
                if src in reserved_cols: continue
                v = _coerce(row.get(src))
                if v is not None:
                    ev_props[dst] = v
        if cfg.user_properties_map:
            for src, dst in cfg.user_properties_map.items():
                if src in reserved_cols: continue
                v = _coerce(row.get(src))
                if v is not None:
                    us_props[dst] = v
        if not cfg.event_properties_map and not cfg.user_properties_map:
            cols = cfg.event_property_columns
            if cols is None:
                cols = [c for c in row.keys() if c not in reserved_cols]
            for c in cols:
                v = _coerce(row.get(c))
                if v is not None:
                    ev_props[c] = v
        if ev_props:
            e["event_properties"] = ev_props
        if us_props:
            e["user_properties"] = us_props
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


def _resolve_source_df(cfg: AmplitudeEventTrackComponent, exec_ctx: AssetExecutionContext) -> pd.DataFrame:
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
