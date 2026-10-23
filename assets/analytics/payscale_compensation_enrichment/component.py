"""PayScale Compensation Enrichment Component.

Enriches an upstream DataFrame of job records (title + location, optionally
years of experience / education / skills) with compensation-benchmark
columns from PayScale's **Jobalyzer** API -- one lookup per row, the same
shape as this repo's `geocoder`/`reverse_geocoder` components. See
`resources/payscale_resource/README.md` for the full verified API facts
(OAuth2 client_credentials auth, async submit-then-poll report retrieval,
per-report metered billing).

*** PayScale credentials are NOT self-serve. *** `payscale_resource` (the
resource this component depends on) requires a direct commercial agreement
with PayScale -- there is no public signup. See that resource's README
before configuring this component.

This component is self-contained: it duplicates the small amount of shared
logic (partition-def building, catalog metadata) rather than importing it
from `geocoder` or any other component in this repo.
"""
import time
from typing import Any, Dict, List, Optional, Union

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
    Resolvable,
    asset,
)
from pydantic import Field

# Sub-reports documented under PayScale's Pay Report (see
# resources/payscale_resource README "Pay report shape"). Only BasePay and
# TotalPay are flattened into output columns by default -- see
# `include_total_pay`.
_BASE_PAY_KEY = "BasePayReport"
_TOTAL_PAY_KEY = "TotalPayReport"


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Duplicated (not imported) from this repo's other analytics components,
    per the self-contained-logic convention -- strict: raises ValueError on
    misconfigured combinations rather than silently picking a default.
    """
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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        if isinstance(value, float) and pd.isna(value):
            return True
    except Exception:
        pass
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _build_answers(
    row: Dict[str, Any],
    job_title_column: str,
    city_column: Optional[str],
    state_column: Optional[str],
    country_column: Optional[str],
    default_country: str,
    years_experience_column: Optional[str],
    education_column: Optional[str],
    skills_column: Optional[str],
) -> Dict[str, Any]:
    """Build the Jobalyzer `answers` payload for one row.

    Field names (`JobTitle`, `City`, `State`, `Country`, `YearsExperience`,
    `HighestDegreeEarned`, `Skills`) are PayScale's documented compensable
    factors -- see resources/payscale_resource/component.py's module
    docstring and README for the verified source. Only `JobTitle` and a
    `Country` are documented as strictly required; everything else is
    included only when present and non-blank so a sparse upstream row still
    produces a valid request.
    """
    answers: Dict[str, Any] = {"JobTitle": str(row.get(job_title_column))}

    city = row.get(city_column) if city_column else None
    if not _is_blank(city):
        answers["City"] = str(city)

    state = row.get(state_column) if state_column else None
    if not _is_blank(state):
        answers["State"] = str(state)

    country = row.get(country_column) if country_column else None
    answers["Country"] = str(country) if not _is_blank(country) else default_country

    years_experience = row.get(years_experience_column) if years_experience_column else None
    if not _is_blank(years_experience):
        try:
            answers["YearsExperience"] = int(float(years_experience))
        except (TypeError, ValueError):
            answers["YearsExperience"] = years_experience

    education = row.get(education_column) if education_column else None
    if not _is_blank(education):
        answers["HighestDegreeEarned"] = str(education)

    skills = row.get(skills_column) if skills_column else None
    if not _is_blank(skills):
        if isinstance(skills, (list, tuple)):
            answers["Skills"] = [str(s) for s in skills]
        else:
            answers["Skills"] = [s.strip() for s in str(skills).split(",") if s.strip()]

    return answers


def _fetch_compensation_report(resource, answers: Dict[str, Any]) -> Dict[str, Any]:
    """The ONE place this component calls out to PayScale.

    Delegates the actual (OAuth2 + submit-then-poll) HTTP work to the
    injected `payscale_resource`, but isolates that single call behind one
    module-level function so tests can monkeypatch exactly this -- no need
    to fake an entire resource object or reach into `requests`.
    """
    return resource.get_pay_report(answers)


def _flatten_pay_report(report: Dict[str, Any], include_total_pay: bool) -> Dict[str, Any]:
    """Flatten PayScale's nested Pay report JSON into flat output-column
    values. Field names (Percentile10/25/50/75/90, CurrencyName,
    ReportRating, Context.MatchedJobTitle, ...) are PayScale's documented
    Pay report shape -- see resources/payscale_resource README."""
    base = report.get(_BASE_PAY_KEY) or {}
    context = report.get("Context") or {}

    flat: Dict[str, Any] = {
        "median_base_pay": base.get("Percentile50"),
        "base_pay_p10": base.get("Percentile10"),
        "base_pay_p25": base.get("Percentile25"),
        "base_pay_p75": base.get("Percentile75"),
        "base_pay_p90": base.get("Percentile90"),
        "base_pay_average": base.get("Average"),
        "currency": base.get("CurrencyName"),
        "report_rating": report.get("ReportRating"),
        "total_profiles_analyzed": report.get("TotalProfilesAnalyzed"),
        "matched_job_title": context.get("MatchedJobTitle"),
        "job_title_rating": context.get("JobTitleRating"),
    }
    if include_total_pay:
        total = report.get(_TOTAL_PAY_KEY) or {}
        flat["median_total_pay"] = total.get("Percentile50")
        flat["total_pay_p10"] = total.get("Percentile10")
        flat["total_pay_p90"] = total.get("Percentile90")
    return flat


_OUTPUT_COLUMNS = [
    "median_base_pay", "base_pay_p10", "base_pay_p25", "base_pay_p75", "base_pay_p90",
    "base_pay_average", "currency", "report_rating", "total_profiles_analyzed",
    "matched_job_title", "job_title_rating",
]
_TOTAL_PAY_COLUMNS = ["median_total_pay", "total_pay_p10", "total_pay_p90"]



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


# --- backfill_policy support -------------------------------------------------
# A single_run backfill hands the asset a whole partition RANGE, on which
# `context.partition_key` raises. The compute below was written against one
# key at a time, so a range is run key-by-key through a thin context proxy and
# the per-partition frames are concatenated.
from typing import Literal  # noqa: E402,F401


class _SinglePartitionContext:
    """Presents ONE partition key of a range run to compute code that reads
    ``context.partition_key``; everything else delegates to the real context.
    ``add_output_metadata`` is captured (the real context only accepts one call
    per output) and merged by ``_run_per_partition``."""

    def __init__(self, context, partition_key):
        self._ctx = context
        self._key = partition_key
        self.captured_metadata = {}

    @property
    def has_partition_key(self):
        return self._key is not None

    @property
    def has_partition_key_range(self):
        return False

    @property
    def partition_key(self):
        if self._key is None:
            raise AttributeError("no partition key on this run")
        return self._key

    @property
    def partition_keys(self):
        return [] if self._key is None else [self._key]

    def add_output_metadata(self, metadata, output_name=None, mapping_key=None):
        self.captured_metadata.update(metadata or {})

    def __getattr__(self, name):
        return getattr(self._ctx, name)


def _run_per_partition(context, compute, partitions_def, loop, args, kwargs):
    """Run ``compute(context, *args, **kwargs)``; for a multi-partition range
    run it once per key (when ``loop``) and concatenate the DataFrames."""
    import pandas as pd
    from dagster import MetadataValue, Output

    if not (context.has_partition_key or context.has_partition_key_range):
        return compute(context, *args, **kwargs)
    keys = list(context.partition_keys)
    if len(keys) <= 1 and context.has_partition_key:
        return compute(context, *args, **kwargs)  # ordinary single-partition run

    if not loop:
        # Partition key does not change the computation: run once.
        proxy = _SinglePartitionContext(context, None)
        result = compute(proxy, *args, **kwargs)
        if isinstance(result, Output):
            proxy.captured_metadata.update(result.metadata or {})
            result = result.value
        if proxy.captured_metadata:
            context.add_output_metadata(proxy.captured_metadata)
        return result

    frames, merged = [], {}
    for key in keys:
        if hasattr(partitions_def, "partitions_defs") and not hasattr(key, "keys_by_dimension"):
            key = partitions_def.get_partition_key_from_str(key)
        proxy = _SinglePartitionContext(context, key)
        result = compute(proxy, *args, **kwargs)
        merged.update(proxy.captured_metadata)
        if isinstance(result, Output):
            merged.update(result.metadata or {})
            result = result.value
        if not isinstance(result, pd.DataFrame):
            raise TypeError(
                f"backfill_policy='single_run' needs a DataFrame result per partition; "
                f"got {type(result).__name__} for partition {key!r}"
            )
        result = result.copy()
        if "_partition_key" not in result.columns:
            result["_partition_key"] = str(key)
        frames.append(result)
    merged["partition_keys"] = MetadataValue.json([str(k) for k in keys])
    context.add_output_metadata(merged)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class PayscaleCompensationEnrichmentComponent(Component, Model, Resolvable):
    """Enrich an upstream DataFrame of job records with PayScale Jobalyzer
    compensation-benchmark columns -- one API lookup per row.

    *** PayScale credentials are NOT self-serve. *** See
    `resources/payscale_resource/README.md` -- `customer_id`/`client_id`/
    `client_secret` require a direct commercial agreement with PayScale.

    Example:
        ```yaml
        type: dagster_component_templates.PayscaleCompensationEnrichmentComponent
        attributes:
          asset_name: compensation_benchmarked_roles
          upstream_asset_key: open_roles
          resource_key: payscale_resource
          job_title_column: job_title
          city_column: city
          state_column: state
          country_column: country
          years_experience_column: years_experience
          group_name: analytics
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    upstream_asset_key: Union[str, List[str]] = Field(
        description="Upstream asset key providing a DataFrame with job title/location data"
    )

    resource_key: str = Field(
        default="payscale_resource",
        description="Resource key registered by PayscaleResourceComponent.",
    )

    job_title_column: Union[str, int] = Field(
        description="Column with the job title to send as PayScale's 'JobTitle' compensable factor"
    )
    city_column: Optional[Union[str, int]] = Field(
        default=None, description="Optional column with city name ('City')"
    )
    state_column: Optional[Union[str, int]] = Field(
        default=None, description="Optional column with state/province ('State')"
    )
    country_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column with country name ('Country'). Falls back to `default_country` when absent or blank -- PayScale documents Country as a required compensable factor.",
    )
    default_country: str = Field(
        default="United States",
        description="Country sent when `country_column` is unset or blank for a row.",
    )
    years_experience_column: Optional[Union[str, int]] = Field(
        default=None, description="Optional column with years of experience ('YearsExperience')"
    )
    education_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column with highest degree earned ('HighestDegreeEarned'), e.g. \"Bachelor's Degree\"",
    )
    skills_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column with a comma-separated or list-valued set of skills ('Skills')",
    )

    auto_resolve_job_title: bool = Field(
        default=True,
        description="Let PayScale auto-match free-text JobTitle input to a standardized PayScale title (PayScale's documented 'AutoResolveJobTitle' flag).",
    )
    include_total_pay: bool = Field(
        default=True,
        description="Also append TotalPayReport columns (median_total_pay, total_pay_p10, total_pay_p90) alongside BasePayReport.",
    )
    output_column_prefix: str = Field(
        default="payscale_",
        description="Prefix applied to every appended compensation-benchmark column.",
    )
    continue_on_error: bool = Field(
        default=True,
        description="If a row's PayScale lookup fails (bad data, rate limit, timeout), log a warning and leave that row's columns null rather than failing the whole asset.",
    )
    batch_delay: float = Field(
        default=0.0,
        description="Seconds to sleep between rows -- PayScale charges per report, so a small delay can help stay under any account-level rate limit.",
    )

    group_name: Optional[str] = Field(default=None, description="Asset group for organization")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'hr', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Auto-inferred ('python') if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from.",
    )
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output data in metadata (rows as a markdown table).",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows to include in the preview metadata when `include_preview_metadata` is True.",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill range in one run: the analysis runs once per partition in the range, each on the upstream rows filtered to that partition (via partition_date_column / partition_static_column), and the per-partition outputs are concatenated with a '_partition_key' column added. 'multi_run' launches one run per partition (optionally capped via backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, uncapped). Ignored when partition_type is not set. 'single_run' requires a partition-range-aware IO manager (e.g. a DB-table IO manager with partition_expr set) -- a generic filesystem/default IO manager cannot persist a single_run output spanning multiple partitions and will raise a clear error at materialization time. If neither partition_date_column nor partition_static_column is set the partition key does not change the computation, so a range runs the analysis once. Components that persist side effects (e.g. model files) write them once per partition, so the last partition in the range wins."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None, description="Seconds between retries (default 1)."
    )
    retry_policy_backoff: str = Field(
        default="exponential", description="Backoff strategy: 'linear' or 'exponential'."
    )
    description: Optional[str] = Field(
        default=None, description="Asset description shown in the Dagster catalog."
    )
    deps: Optional[List[str]] = Field(
        default=None, description="Lineage-only upstream asset keys (no data passed at runtime)."
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        resource_key = self.resource_key
        job_title_column = self.job_title_column
        city_column = self.city_column
        state_column = self.state_column
        country_column = self.country_column
        default_country = self.default_country
        years_experience_column = self.years_experience_column
        education_column = self.education_column
        skills_column = self.skills_column
        auto_resolve_job_title = self.auto_resolve_job_title
        include_total_pay = self.include_total_pay
        output_column_prefix = self.output_column_prefix
        continue_on_error = self.continue_on_error
        batch_delay = self.batch_delay
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _inferred_kinds = list(self.kinds) if self.kinds else ["python"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )
        _deco_payscale_compensation_enrichment_asset = asset(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Rows enriched with PayScale Jobalyzer compensation benchmarks",
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            required_resource_keys={resource_key},
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def payscale_compensation_enrichment_asset_compute(
            context: AssetExecutionContext, upstream: pd.DataFrame
        ) -> pd.DataFrame:
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = (
                    next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                )
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]

            resource = getattr(context.resources, resource_key)

            df = upstream.copy()
            output_cols = list(_OUTPUT_COLUMNS) + (list(_TOTAL_PAY_COLUMNS) if include_total_pay else [])
            collected: Dict[str, List[Any]] = {c: [] for c in output_cols}
            error_col: List[Optional[str]] = []

            context.log.info(f"Fetching PayScale compensation benchmarks for {len(df)} rows")

            success_count = 0
            for i, row in df.iterrows():
                row_dict = row.to_dict()
                try:
                    answers = _build_answers(
                        row_dict,
                        job_title_column,
                        city_column,
                        state_column,
                        country_column,
                        default_country,
                        years_experience_column,
                        education_column,
                        skills_column,
                    )
                    report = _fetch_compensation_report(resource, answers)
                    flat = _flatten_pay_report(report, include_total_pay)
                    for col in output_cols:
                        collected[col].append(flat.get(col))
                    error_col.append(None)
                    success_count += 1
                except Exception as e:  # noqa: BLE001
                    msg = f"{type(e).__name__}: {e}"
                    if not continue_on_error:
                        raise
                    context.log.warning(f"row {i}: PayScale lookup failed: {msg}")
                    for col in output_cols:
                        collected[col].append(None)
                    error_col.append(msg)

                if batch_delay:
                    time.sleep(batch_delay)

                if (i + 1) % 10 == 0:
                    context.log.info(f"Progress: {i + 1}/{len(df)} rows processed")

            for col in output_cols:
                df[f"{output_column_prefix}{col}"] = collected[col]
            df[f"{output_column_prefix}error"] = error_col

            success_rate = success_count / len(df) * 100 if len(df) > 0 else 0
            context.log.info(
                f"PayScale enrichment complete: {success_count}/{len(df)} succeeded ({success_rate:.1f}%)"
            )

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col])) for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "rows_succeeded": MetadataValue.int(success_count),
                "rows_failed": MetadataValue.int(len(df) - success_count),
            }
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {
                        str(out_col): [TableColumnDep(asset_key=_upstream_key, column_name=str(ic)) for ic in in_cols]
                        for out_col, in_cols in _effective_lineage.items()
                    }
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df
        @_deco_payscale_compensation_enrichment_asset
        def payscale_compensation_enrichment_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            return _run_per_partition(context, payscale_compensation_enrichment_asset_compute, partitions_def, bool(partition_date_column or partition_static_column), [upstream], {})


        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[payscale_compensation_enrichment_asset])

        return Definitions(
            assets=[payscale_compensation_enrichment_asset], asset_checks=list(_schema_checks)
        )


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
