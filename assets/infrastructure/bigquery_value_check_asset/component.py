"""BigQueryValueCheckAssetComponent.

Run a scalar SQL query against BigQuery and assert the result matches an
expected value (within an optional tolerance) -- the Airflow
`BigQueryCheckOperator` / `BigQueryValueCheckOperator` equivalent. Real
`google-cloud-bigquery` client calls (`client.query(...).result()`),
verified against the installed library's `QueryJob` API.
"""
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

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


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01').")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _bigquery_client(credentials, credentials_path, project_id, location):
    import json
    import os

    from google.cloud import bigquery
    from google.oauth2 import service_account

    creds_dict = credentials
    if creds_dict is None:
        cred_path = credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path:
            with open(cred_path, "r") as fh:
                creds_dict = json.load(fh)
    if creds_dict is None:
        raise ValueError("provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")
    sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
    project = project_id or creds_dict.get("project_id")
    return bigquery.Client(credentials=sa_creds, project=project, location=location), project


_COMPARISONS = {
    "equal": lambda actual, expected, tol: (abs(actual - expected) <= tol) if tol else (actual == expected),
    "not_equal": lambda actual, expected, tol: not ((abs(actual - expected) <= tol) if tol else (actual == expected)),
    "greater_than": lambda actual, expected, tol: actual > expected,
    "greater_than_or_equal": lambda actual, expected, tol: actual >= expected,
    "less_than": lambda actual, expected, tol: actual < expected,
    "less_than_or_equal": lambda actual, expected, tol: actual <= expected,
}


class BigQueryValueCheckAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a scalar SQL query against BigQuery and assert the result against an expectation.

    Reads the first column of the first row of `query`'s result and compares
    it to `expected_value` via `comparison` (default `'equal'`), with an
    optional numeric `tolerance` for `equal`/`not_equal`. Raises (fails the
    run) on a mismatch unless `fail_on_mismatch: false`, in which case the
    asset still materializes with `check_passed: false` in its metadata --
    useful for a check meant to inform downstream logic rather than halt the
    pipeline.
    """

    asset_name: str = Field(description="Dagster asset key.")
    credentials: Optional[Dict[str, Any]] = Field(default=None, description="Service-account JSON as a dict.")
    credentials_path: Optional[str] = Field(default=None, description="Path to a service-account JSON file. Falls back to GOOGLE_APPLICATION_CREDENTIALS.")
    project_id: Optional[str] = Field(default=None, description="Billing project. Defaults to the service account's own project.")
    location: Optional[str] = Field(default=None, description="BQ location (e.g. 'US', 'EU'). Default: auto-detect.")
    query: str = Field(description="Scalar-returning SELECT statement. Supports {placeholder} substitution from query_params.")
    query_params: Dict[str, Any] = Field(default_factory=dict, description="Values substituted into `query` via str.format.")
    expected_value: Union[float, int, str, bool] = Field(description="Value the query's first result column is compared against.")
    comparison: Literal["equal", "not_equal", "greater_than", "greater_than_or_equal", "less_than", "less_than_or_equal"] = Field(default="equal", description="Comparison applied between the query's actual result and expected_value.")
    tolerance: Optional[float] = Field(default=None, description="Absolute numeric tolerance for comparison='equal'/'not_equal' (ignored otherwise). None means an exact match.")
    fail_on_mismatch: bool = Field(default=True, description="Raise dg.Failure on a mismatch. False materializes anyway with check_passed=false in metadata.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['gcp', 'bigquery'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d")
    partition_end_offset: int = Field(default=0)
    partition_timezone: Optional[str] = Field(default=None)

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None)
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), csv=("partition_values",), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Big Query Value Check Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["gcp", "bigquery"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(int(self.freshness_max_lag_minutes), self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = dg.BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)

        @dg.asset(
            key=dg.AssetKey.from_user_string(self.asset_name),
            group_name=self.group_name,
            description=self.description or f"BigQuery value check ({self.comparison} {self.expected_value!r})",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            backfill_policy=backfill_policy,
        )
        def _bigquery_value_check_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            client, _project = _bigquery_client(self.credentials, self.credentials_path, self.project_id, self.location)

            params = dict(self.query_params or {})
            if context.has_partition_key:
                params.setdefault("partition_key", context.partition_key)
            try:
                rendered = self.query.format(**params) if params else self.query
            except KeyError as e:
                raise ValueError(f"query references missing placeholder {e}; available: {list(params.keys())}")

            context.log.info(f"bigquery_value_check_asset: running query (expected {self.comparison} {self.expected_value!r})")
            job = client.query(rendered)
            rows = list(job.result())
            if not rows:
                raise dg.Failure(description="bigquery_value_check_asset: query returned no rows.")
            actual_value = list(rows[0].values())[0]

            compare = _COMPARISONS[self.comparison]
            try:
                passed = compare(actual_value, self.expected_value, self.tolerance or 0)
            except TypeError:
                # non-numeric comparison, e.g. string equality
                passed = (actual_value == self.expected_value) if self.comparison == "equal" else (actual_value != self.expected_value) if self.comparison == "not_equal" else False

            metadata: Dict[str, Any] = {
                "actual_value": dg.MetadataValue.text(str(actual_value)),
                "expected_value": dg.MetadataValue.text(str(self.expected_value)),
                "comparison": dg.MetadataValue.text(self.comparison),
                "check_passed": dg.MetadataValue.bool(bool(passed)),
                "rendered_query": dg.MetadataValue.text(rendered),
            }

            if not passed and self.fail_on_mismatch:
                raise dg.Failure(
                    description=(
                        f"bigquery_value_check_asset: expected {self.comparison} {self.expected_value!r} "
                        f"(tolerance={self.tolerance}), got {actual_value!r}."
                    ),
                    metadata=metadata,
                )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_bigquery_value_check_asset])
