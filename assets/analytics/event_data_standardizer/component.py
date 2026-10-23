"""Event Data Standardizer Component.

Transform platform-specific event tracking data (Segment, Rudderstack, Snowplow, Custom) into a
standardized common schema for cross-platform event analysis.
"""

from typing import Any, Dict, List, Literal, Optional, Union
import pandas as pd
from dagster import (
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


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

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
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
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
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


class EventDataStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing event tracking data across platforms.

    Transforms platform-specific schemas (Segment, Rudderstack, Snowplow, Custom) into a
    unified event tracking data model with consistent field names and structure.

    Standard Schema Output:
    - event_id: Event identifier
    - event_name: Event name/type
    - event_type: Event category (track, page, screen, identify)
    - platform: Source platform (segment, rudderstack, snowplow, custom)
    - user_id: User identifier
    - anonymous_id: Anonymous identifier
    - session_id: Session identifier
    - timestamp: Event timestamp
    - date: Event date
    - hour: Event hour
    - properties: Event properties (JSON)
    - context: Event context (JSON) - device, location, etc.
    - page_url: Page URL
    - page_path: Page path
    - page_title: Page title
    - referrer: Referrer URL
    - device_type: Device type (mobile, desktop, tablet)
    - browser: Browser name
    - os: Operating system
    - country: Country
    - city: City
    - event_value: Numeric value (for conversion events)
    - is_conversion: Boolean flag

    Example:
        ```yaml
        type: dagster_component_templates.EventDataStandardizerComponent
        attributes:
          asset_name: standardized_events
          platform: "segment"
          source_asset: "segment_events"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized output asset"
    )

    platform: Literal["segment", "rudderstack", "custom", "snowplow"] = Field(
        description="Source event tracking platform to standardize"
    )

    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)",
    )

    event_id_field: Optional[str] = Field(
        default=None,
        description="Field name for event ID (auto-detected if not provided)"
    )

    event_name_field: Optional[str] = Field(
        default=None,
        description="Field name for event name (auto-detected if not provided)"
    )

    user_id_field: Optional[str] = Field(
        default=None,
        description="Field name for user ID (auto-detected if not provided)"
    )

    timestamp_field: Optional[str] = Field(
        default=None,
        description="Field name for timestamp (auto-detected if not provided)"
    )

    # Optional filters
    filter_event_name: Optional[str] = Field(
        default=None,
        description="Filter by event name (comma-separated)"
    )

    filter_event_type: Optional[str] = Field(
        default=None,
        description="Filter by event type (comma-separated)"
    )

    filter_date_from: Optional[str] = Field(
        default=None,
        description="Filter events from this date (YYYY-MM-DD)"
    )

    filter_date_to: Optional[str] = Field(
        default=None,
        description="Filter events to this date (YYYY-MM-DD)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="events",
        description="Asset group for organization"
    )
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
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
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
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
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

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        source_asset = self.upstream_asset_key
        event_id_field = self.event_id_field
        event_name_field = self.event_name_field
        user_id_field = self.user_id_field
        timestamp_field = self.timestamp_field
        filter_event_name = self.filter_event_name
        filter_event_type = self.filter_event_type
        filter_date_from = self.filter_date_from
        filter_date_to = self.filter_date_to
        description = self.description or f"Standardized {platform} event tracking data"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Parse upstream asset keys
        upstream_keys = []
        if source_asset:
            upstream_keys = [source_asset]

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "event_data_standardizer"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


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
        _deco_event_data_standardizer_asset = asset(retry_policy=_retry_policy, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def event_data_standardizer_asset_compute(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that standardizes platform-specific event tracking data."""

            context.log.info(f"Standardizing {platform} event tracking data")

            # Load upstream data
            if upstream_keys and hasattr(context, 'load_asset_value'):
                context.log.info(f"Loading data from upstream asset: {source_asset}")
                raw_data = context.load_asset_value(AssetKey(source_asset))
            elif kwargs:
                raw_data = list(kwargs.values())[0]
            else:
                raise ValueError(
                    f"Event Data Standardizer '{asset_name}' requires upstream data. "
                    f"Connect to an event tracking ingestion component (Segment, Rudderstack, etc.)"
                )

            # Convert to DataFrame if needed
            if isinstance(raw_data, dict):
                if 'data' in raw_data:
                    df = pd.DataFrame(raw_data['data'])
                elif 'rows' in raw_data:
                    df = pd.DataFrame(raw_data['rows'])
                else:
                    df = pd.DataFrame([raw_data])
            elif isinstance(raw_data, pd.DataFrame):
                df = raw_data
            else:
                raise TypeError(f"Unexpected data type: {type(raw_data)}")

            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                if partition_date_column and partition_date_column in df.columns and _date_key:
                    _col_dates = pd.to_datetime(df[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    df = df[_col_dates == _date_key]
                if partition_static_column and partition_static_column in df.columns and _static_key:
                    df = df[df[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in df.columns and not _is_multi:
                    df = df[df[partition_static_column].astype(str) == str(_pk)]

            context.log.info(f"Raw data: {len(df)} rows, {len(df.columns)} columns")
            original_rows = len(df)

            # Platform-specific field mappings
            field_mappings = {
                "segment": {
                    "event_id": ["messageId", "message_id", "id"],
                    "event_name": ["event", "event_name"],
                    "event_type": ["type"],
                    "user_id": ["userId", "user_id"],
                    "anonymous_id": ["anonymousId", "anonymous_id"],
                    "session_id": ["context.sessionId", "session_id"],
                    "timestamp": ["timestamp", "sent_at", "originalTimestamp"],
                    "properties": ["properties"],
                    "context": ["context"],
                    "page_url": ["context.page.url", "properties.url"],
                    "page_path": ["context.page.path", "properties.path"],
                    "page_title": ["context.page.title", "properties.title"],
                    "referrer": ["context.page.referrer", "properties.referrer"],
                    "device_type": ["context.device.type"],
                    "browser": ["context.userAgent", "context.browser"],
                    "os": ["context.os.name"],
                    "country": ["context.location.country"],
                    "city": ["context.location.city"],
                },
                "rudderstack": {
                    "event_id": ["messageId", "message_id", "id"],
                    "event_name": ["event", "event_name"],
                    "event_type": ["type"],
                    "user_id": ["userId", "user_id"],
                    "anonymous_id": ["anonymousId", "anonymous_id"],
                    "session_id": ["context.sessionId", "session_id"],
                    "timestamp": ["timestamp", "sent_at", "originalTimestamp"],
                    "properties": ["properties"],
                    "context": ["context"],
                    "page_url": ["context.page.url", "properties.url"],
                    "page_path": ["context.page.path", "properties.path"],
                    "page_title": ["context.page.title", "properties.title"],
                    "referrer": ["context.page.referrer"],
                    "device_type": ["context.device.type"],
                    "browser": ["context.userAgent"],
                    "os": ["context.os.name"],
                    "country": ["context.location.country"],
                    "city": ["context.location.city"],
                },
                "snowplow": {
                    "event_id": ["event_id"],
                    "event_name": ["event_name", "event"],
                    "event_type": ["event_type"],
                    "user_id": ["user_id", "domain_userid"],
                    "anonymous_id": ["domain_userid"],
                    "session_id": ["domain_sessionid"],
                    "timestamp": ["collector_tstamp", "derived_tstamp"],
                    "page_url": ["page_url"],
                    "page_path": ["page_urlpath"],
                    "page_title": ["page_title"],
                    "referrer": ["page_referrer", "refr_urlhost"],
                    "device_type": ["dvce_type"],
                    "browser": ["br_name"],
                    "os": ["os_name"],
                    "country": ["geo_country"],
                    "city": ["geo_city"],
                },
                "custom": {
                    "event_id": ["id", "event_id", "_id"],
                    "event_name": ["event", "event_name", "name"],
                    "event_type": ["type", "event_type"],
                    "user_id": ["user_id", "userId"],
                    "anonymous_id": ["anonymous_id", "anonymousId"],
                    "session_id": ["session_id", "sessionId"],
                    "timestamp": ["timestamp", "time", "created_at"],
                    "properties": ["properties", "data"],
                    "context": ["context", "meta"],
                    "page_url": ["url", "page_url"],
                    "page_path": ["path", "page_path"],
                    "page_title": ["title", "page_title"],
                    "referrer": ["referrer", "referer"],
                    "device_type": ["device_type", "device"],
                    "browser": ["browser"],
                    "os": ["os", "operating_system"],
                    "country": ["country"],
                    "city": ["city"],
                },
            }

            mapping = field_mappings.get(platform)
            if not mapping:
                raise ValueError(f"Unsupported platform: {platform}")

            # Helper function to find field in DataFrame
            def find_field(possible_names, custom_field=None):
                if custom_field and custom_field in df.columns:
                    return custom_field
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            # Build standardized DataFrame
            standardized_data = {}

            # Platform identifier
            standardized_data['platform'] = platform

            # Event ID
            event_id_col = find_field(mapping['event_id'], event_id_field)
            if event_id_col:
                standardized_data['event_id'] = df[event_id_col].astype(str)

            # Event name
            event_name_col = find_field(mapping['event_name'], event_name_field)
            if event_name_col:
                standardized_data['event_name'] = df[event_name_col]

            # Event type
            event_type_col = find_field(mapping.get('event_type', []))
            if event_type_col:
                standardized_data['event_type'] = df[event_type_col]

            # User ID
            user_id_col = find_field(mapping['user_id'], user_id_field)
            if user_id_col:
                standardized_data['user_id'] = df[user_id_col].astype(str)

            # Anonymous ID
            anonymous_id_col = find_field(mapping.get('anonymous_id', []))
            if anonymous_id_col:
                standardized_data['anonymous_id'] = df[anonymous_id_col].astype(str)

            # Session ID
            session_id_col = find_field(mapping.get('session_id', []))
            if session_id_col:
                standardized_data['session_id'] = df[session_id_col].astype(str)

            # Timestamp
            timestamp_col = find_field(mapping['timestamp'], timestamp_field)
            if timestamp_col:
                standardized_data['timestamp'] = pd.to_datetime(df[timestamp_col], errors='coerce')
                # Derive date and hour
                standardized_data['date'] = standardized_data['timestamp'].dt.date
                standardized_data['hour'] = standardized_data['timestamp'].dt.hour

            # Properties (JSON field)
            properties_col = find_field(mapping.get('properties', []))
            if properties_col:
                standardized_data['properties'] = df[properties_col]

            # Context (JSON field)
            context_col = find_field(mapping.get('context', []))
            if context_col:
                standardized_data['context'] = df[context_col]

            # Page URL
            page_url_col = find_field(mapping.get('page_url', []))
            if page_url_col:
                standardized_data['page_url'] = df[page_url_col]

            # Page path
            page_path_col = find_field(mapping.get('page_path', []))
            if page_path_col:
                standardized_data['page_path'] = df[page_path_col]

            # Page title
            page_title_col = find_field(mapping.get('page_title', []))
            if page_title_col:
                standardized_data['page_title'] = df[page_title_col]

            # Referrer
            referrer_col = find_field(mapping.get('referrer', []))
            if referrer_col:
                standardized_data['referrer'] = df[referrer_col]

            # Device type
            device_type_col = find_field(mapping.get('device_type', []))
            if device_type_col:
                standardized_data['device_type'] = df[device_type_col]

            # Browser
            browser_col = find_field(mapping.get('browser', []))
            if browser_col:
                standardized_data['browser'] = df[browser_col]

            # OS
            os_col = find_field(mapping.get('os', []))
            if os_col:
                standardized_data['os'] = df[os_col]

            # Country
            country_col = find_field(mapping.get('country', []))
            if country_col:
                standardized_data['country'] = df[country_col]

            # City
            city_col = find_field(mapping.get('city', []))
            if city_col:
                standardized_data['city'] = df[city_col]

            # Create standardized DataFrame
            std_df = pd.DataFrame(standardized_data)

            # Add derived fields (placeholders - would need actual logic)
            std_df['event_value'] = pd.NA
            std_df['is_conversion'] = False

            # Apply filters
            if filter_event_name and 'event_name' in std_df.columns:
                events = [e.strip() for e in filter_event_name.split(',')]
                std_df = std_df[std_df['event_name'].isin(events)]
                context.log.info(f"Filtered to events: {events}")

            if filter_event_type and 'event_type' in std_df.columns:
                types = [t.strip() for t in filter_event_type.split(',')]
                std_df = std_df[std_df['event_type'].isin(types)]
                context.log.info(f"Filtered to types: {types}")

            if filter_date_from and 'date' in std_df.columns:
                std_df = std_df[std_df['date'] >= pd.to_datetime(filter_date_from).date()]
                context.log.info(f"Filtered from date: {filter_date_from}")

            if filter_date_to and 'date' in std_df.columns:
                std_df = std_df[std_df['date'] <= pd.to_datetime(filter_date_to).date()]
                context.log.info(f"Filtered to date: {filter_date_to}")

            # Replace inf and -inf with NaN
            std_df = std_df.replace([float('inf'), float('-inf')], pd.NA)

            final_rows = len(std_df)
            context.log.info(
                f"Standardization complete: {original_rows} → {final_rows} rows, "
                f"{len(std_df.columns)} columns"
            )

            # Add metadata
            metadata = {
                "platform": platform,
                "original_rows": original_rows,
                "final_rows": final_rows,
                "columns": list(std_df.columns),
            }

            # Add event-specific metadata
            if 'event_name' in std_df.columns:
                metadata["unique_events"] = int(std_df['event_name'].nunique())

            if 'user_id' in std_df.columns:
                metadata["unique_users"] = int(std_df['user_id'].nunique())

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_preview and len(std_df) > 0:
                context.add_output_metadata({
                    "row_count": len(std_df),
                    "columns": std_df.columns.tolist(),
                    "preview": MetadataValue.md(std_df.head(10).to_markdown())
                })
                return std_df
            else:
                # Build column schema metadata
                from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
                _col_schema = TableSchema(columns=[
                    TableColumn(name=str(col), type=str(std_df.dtypes[col]))
                    for col in std_df.columns
                ])
                _metadata = {
                    "dagster/row_count": MetadataValue.int(len(std_df)),
                    "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                }
                # Use explicit lineage, or auto-infer passthrough columns at runtime
                _effective_lineage = column_lineage
                if not _effective_lineage:
                    try:
                        _upstream_cols = set(df.columns)
                        _effective_lineage = {
                            col.name: [col.name] for col in _col_schema.columns
                            if col.name in _upstream_cols
                        }
                    except Exception:
                        pass
                if _effective_lineage:
                    _upstream_key = AssetKey.from_user_string(source_asset) if source_asset else None
                    if _upstream_key:
                        _lineage_deps = {}
                        for out_col, in_cols in _effective_lineage.items():
                            _lineage_deps[str(out_col)] = [
                                TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                                for ic in in_cols
                            ]
                        _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                            TableColumnLineage(_lineage_deps)
                        )
                context.add_output_metadata(_metadata)
                return std_df
        @_deco_event_data_standardizer_asset
        def event_data_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            return _run_per_partition(context, event_data_standardizer_asset_compute, partitions_def, bool(partition_date_column or partition_static_column), [], kwargs)


        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[event_data_standardizer_asset])


        return Definitions(assets=[event_data_standardizer_asset], asset_checks=list(_schema_checks))


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
