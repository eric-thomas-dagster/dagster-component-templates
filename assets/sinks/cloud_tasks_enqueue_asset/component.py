"""CloudTasksEnqueueAssetComponent — enqueue Cloud Tasks from DataFrame rows.

For every row of an upstream DataFrame, push one HTTP task onto a Cloud
Tasks queue. The queue's worker (typically a Cloud Run / Cloud Function /
App Engine endpoint) then processes them asynchronously with built-in
retries, rate limits, and scheduling.

Common patterns:
  - Materialize a Dagster asset that emits N work items, fan-out to N
    workers without waiting in-line
  - Schedule deferred work (`schedule_time_column`) — e.g. "send the
    reminder email in 24 hours"
  - Throttle outbound API calls through the queue's rate limit

Uses HTTP tasks (most flexible). For App Engine tasks, see Google's docs
and roll a custom asset.
"""

import datetime as dt
import json
import os
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
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


class CloudTasksEnqueueAssetComponent(Component, Model, Resolvable):
    """Enqueue one Cloud Tasks HTTP task per row of an upstream DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)
    location: str = Field(description="Queue region (e.g. 'us-central1').")
    queue_id: str = Field(description="Cloud Tasks queue id.")

    target_url: str = Field(
        description=(
            "HTTPS URL the worker hits (e.g. a Cloud Run service endpoint). "
            "Can include `{column}` placeholders to template per-row."
        ),
    )
    http_method: Literal["POST", "GET", "PUT", "DELETE", "PATCH"] = Field(default="POST")

    body_columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to bundle into the request JSON body. Default: all columns.",
    )
    headers: Optional[Dict[str, str]] = Field(default=None)

    schedule_time_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column of datetimes — when each task should fire (defaults to now).",
    )

    # OIDC auth for hitting a private Cloud Run / Cloud Function endpoint
    oidc_service_account_email: Optional[str] = Field(
        default=None,
        description="Service account to mint an OIDC token for. Required for private invocation targets.",
    )
    oidc_audience: Optional[str] = Field(
        default=None,
        description="OIDC audience (default: target_url's origin).",
    )

    dispatch_deadline_seconds: Optional[int] = Field(default=None, description="Override Cloud Tasks dispatch_deadline.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key (e.g. 'customer_id').",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        queue_id = self.queue_id
        target_url = self.target_url
        http_method = self.http_method
        body_columns = self.body_columns
        headers = self.headers or {}
        schedule_time_column = self.schedule_time_column
        oidc_sa = self.oidc_service_account_email
        oidc_audience = self.oidc_audience
        dispatch_deadline_seconds = self.dispatch_deadline_seconds

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Cloud Tasks → {location}/{queue_id}",
            group_name=self.group_name,
            kinds={"google", "cloud-tasks"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # Filter to current partition if partitioned
            if context.has_partition_key or context.has_partition_key_range:
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        upstream = upstream[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
            try:
                from google.cloud import tasks_v2
                from google.oauth2 import service_account
                from google.protobuf import timestamp_pb2
            except ImportError:
                raise ImportError("pip install google-cloud-tasks google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = tasks_v2.CloudTasksClient(credentials=sa_creds)
            queue_path = client.queue_path(project_id, location, queue_id)

            method_map = {
                "POST":   tasks_v2.HttpMethod.POST,
                "GET":    tasks_v2.HttpMethod.GET,
                "PUT":    tasks_v2.HttpMethod.PUT,
                "DELETE": tasks_v2.HttpMethod.DELETE,
                "PATCH":  tasks_v2.HttpMethod.PATCH,
            }
            http_method_enum = method_map[http_method]

            cols = body_columns if body_columns is not None else list(upstream.columns)

            enqueued = 0
            failed = 0
            errors: List[str] = []
            for i, row in upstream.iterrows():
                row_dict = {c: row[c] for c in cols if c in upstream.columns}
                # Stringify non-JSON-serializable values
                body: Dict[str, Any] = {}
                for k, v in row_dict.items():
                    try:
                        json.dumps(v, default=str)
                        body[k] = v
                    except Exception:
                        body[k] = str(v)

                # Render URL placeholders {column} from row values
                rendered_url = target_url
                try:
                    rendered_url = target_url.format(**{k: row[k] for k in upstream.columns})
                except (KeyError, IndexError):
                    pass

                http_request: Dict[str, Any] = {
                    "url":         rendered_url,
                    "http_method": http_method_enum,
                    "headers":     {**headers, "Content-Type": "application/json"} if http_method in ("POST", "PUT", "PATCH") else dict(headers),
                }
                if http_method in ("POST", "PUT", "PATCH"):
                    http_request["body"] = json.dumps(body, default=str).encode("utf-8")

                if oidc_sa:
                    http_request["oidc_token"] = {
                        "service_account_email": oidc_sa,
                        **({"audience": oidc_audience} if oidc_audience else {}),
                    }

                task: Dict[str, Any] = {"http_request": http_request}

                if schedule_time_column and schedule_time_column in upstream.columns:
                    sched = row[schedule_time_column]
                    sched_dt: Optional[dt.datetime] = None
                    if isinstance(sched, str) and sched:
                        sched_dt = dt.datetime.fromisoformat(sched.replace("Z", "+00:00"))
                    elif isinstance(sched, dt.datetime):
                        sched_dt = sched
                    elif isinstance(sched, pd.Timestamp):
                        sched_dt = sched.to_pydatetime()
                    if sched_dt is not None:
                        if sched_dt.tzinfo is None:
                            sched_dt = sched_dt.replace(tzinfo=dt.timezone.utc)
                        ts = timestamp_pb2.Timestamp()
                        ts.FromDatetime(sched_dt)
                        task["schedule_time"] = ts

                if dispatch_deadline_seconds:
                    task["dispatch_deadline"] = {"seconds": int(dispatch_deadline_seconds)}

                try:
                    client.create_task(request={"parent": queue_path, "task": task})
                    enqueued += 1
                except Exception as e:
                    failed += 1
                    if len(errors) < 5:
                        errors.append(str(e))

            result_df = pd.DataFrame({
                "metric": ["enqueued", "failed", "rows_in"],
                "value":  [enqueued, failed, len(upstream)],
            })
            return Output(
                value=result_df,
                metadata={
                    "queue":     MetadataValue.text(f"{location}/{queue_id}"),
                    "target":    MetadataValue.text(target_url),
                    "enqueued":  MetadataValue.int(enqueued),
                    "failed":    MetadataValue.int(failed),
                    "errors":    MetadataValue.json(errors) if errors else MetadataValue.text("(none)"),
                },
            )

        return Definitions(assets=[_asset])


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
