"""Time Series Generator Asset Component."""

from typing import Dict, List, Literal, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


class TimeSeriesGeneratorComponent(Component, Model, Resolvable):
    """
    Component for generating synthetic time-series data with various patterns.

    This component creates realistic time-series data with configurable patterns,
    frequencies, and noise. Perfect for demonstrating analytics, forecasting,
    anomaly detection, and time-series transformations.

    Supports multiple patterns:
    - trend: Linear upward or downward trend
    - seasonal: Recurring daily/weekly/monthly patterns
    - random_walk: Random incremental changes
    - sine_wave: Smooth periodic oscillation
    - step_function: Sudden level changes
    - spike: Occasional random spikes
    - complex: Combination of trend + seasonal + noise
    """

    asset_name: str = Field(description="Name of the asset")

    pattern_type: Literal[
        "trend",
        "seasonal",
        "random_walk",
        "sine_wave",
        "step_function",
        "spike",
        "complex"
    ] = Field(
        default="complex",
        description="Pattern to generate in the time series"
    )

    start_date: str = Field(
        description="Start date (YYYY-MM-DD format)",
        default=""
    )

    end_date: str = Field(
        description="End date (YYYY-MM-DD format)",
        default=""
    )

    frequency: Literal["1min", "5min", "15min", "30min", "1h", "1d"] = Field(
        default="1h",
        description="Data point frequency"
    )

    base_value: float = Field(
        default=100.0,
        description="Starting/baseline value for the series"
    )

    noise_level: float = Field(
        default=0.1,
        description="Amount of random noise to add (0.0 = none, 1.0 = high)",
        ge=0.0,
        le=1.0
    )

    random_seed: Optional[int] = Field(
        default=None,
        description="Random seed for reproducible data generation (leave empty for random)"
    )

    metric_name: str = Field(
        default="value",
        description="Name of the metric/value column"
    )

    series_count: int = Field(
        default=1,
        description="Number of parallel series to emit. With >1, each series is tagged via group_column.",
        ge=1,
        le=1000,
    )

    group_column: str = Field(
        default="series_id",
        description="Column name used to tag each series when series_count > 1.",
    )

    dropout_rate: float = Field(
        default=0.0,
        description="Fraction of rows to randomly drop (0=dense, 0.25=leave ~25%% gaps). Useful for gap-fill demos.",
        ge=0.0,
        le=0.95,
    )

    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )

    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
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
        default=False,
        description="Include a preview of the output data in metadata (first 5 rows as markdown table). Used by builder UIs to render asset shape without warehouse access."
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

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


    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the time series generator."""

        # Capture fields for closure
        asset_name = self.asset_name
        pattern_type = self.pattern_type
        start_date = self.start_date
        end_date = self.end_date
        frequency = self.frequency
        base_value = self.base_value
        noise_level = self.noise_level
        random_seed = self.random_seed
        metric_name = self.metric_name
        series_count = self.series_count
        group_column = self.group_column
        dropout_rate = self.dropout_rate
        description = self.description or f"Time series with {pattern_type} pattern"
        group_name = self.group_name or None
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "time_series_generator"  # component directory name
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
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

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



        @asset(retry_policy=_retry_policy, 
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def time_series_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Generate time series data based on pattern type."""

            # Set random seed if provided
            if random_seed is not None:
                np.random.seed(random_seed)

            # Check if running in partitioned mode
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    # For partitioned mode, generate data for just this day
                    start = partition_date
                    end = partition_date + timedelta(days=1) - timedelta(seconds=1)
                    context.log.info(
                        f"Generating {pattern_type} time series for partition {context.partition_key} "
                        f"at {frequency} frequency"
                    )
                except ValueError:
                    context.log.warning(
                        f"Could not parse partition key '{context.partition_key}' as date, "
                        f"falling back to configured dates"
                    )
                    # Fall back to configured dates
                    if start_date:
                        start = pd.to_datetime(start_date)
                    else:
                        start = datetime.now() - timedelta(days=30)

                    if end_date:
                        end = pd.to_datetime(end_date)
                    else:
                        end = datetime.now()
            else:
                # Non-partitioned mode: use configured dates or defaults
                context.log.info(f"Generating {pattern_type} time series (non-partitioned)")

                if start_date:
                    start = pd.to_datetime(start_date)
                else:
                    start = datetime.now() - timedelta(days=30)

                if end_date:
                    end = pd.to_datetime(end_date)
                else:
                    end = datetime.now()

                context.log.info(
                    f"Generating {pattern_type} time series from {start} to {end} "
                    f"at {frequency} frequency"
                )

            # Generate datetime index
            date_range = pd.date_range(start=start, end=end, freq=frequency)
            n_points = len(date_range)

            context.log.info(f"Generating {n_points} data points")

            # Generate base pattern
            if pattern_type == "trend":
                values = _generate_trend(n_points, base_value, slope=0.5)
            elif pattern_type == "seasonal":
                values = _generate_seasonal(n_points, base_value, date_range)
            elif pattern_type == "random_walk":
                values = _generate_random_walk(n_points, base_value)
            elif pattern_type == "sine_wave":
                values = _generate_sine_wave(n_points, base_value)
            elif pattern_type == "step_function":
                values = _generate_step_function(n_points, base_value)
            elif pattern_type == "spike":
                values = _generate_spike(n_points, base_value)
            elif pattern_type == "complex":
                values = _generate_complex(n_points, base_value, date_range)
            else:
                raise ValueError(f"Unknown pattern type: {pattern_type}")

            def _build_one_series(seed_offset: int, base: float) -> np.ndarray:
                """Generate the configured pattern at the requested length."""
                if pattern_type == "trend":
                    v = _generate_trend(n_points, base, slope=0.5)
                elif pattern_type == "seasonal":
                    v = _generate_seasonal(n_points, base, date_range)
                elif pattern_type == "random_walk":
                    v = _generate_random_walk(n_points, base)
                elif pattern_type == "sine_wave":
                    v = _generate_sine_wave(n_points, base)
                elif pattern_type == "step_function":
                    v = _generate_step_function(n_points, base)
                elif pattern_type == "spike":
                    v = _generate_spike(n_points, base)
                elif pattern_type == "complex":
                    v = _generate_complex(n_points, base, date_range)
                else:
                    raise ValueError(f"Unknown pattern type: {pattern_type}")
                if noise_level > 0:
                    v = v + np.random.normal(0, base * noise_level, n_points)
                return v

            if series_count == 1:
                # Single-series path keeps the original output shape.
                if noise_level > 0:
                    values = values + np.random.normal(0, base_value * noise_level, n_points)
                df = pd.DataFrame({"timestamp": date_range, metric_name: values})
            else:
                # Multi-series: regenerate per series, tag with group_column.
                # Each series gets a slightly offset baseline so curves are visually distinct.
                frames = []
                for s in range(series_count):
                    base = base_value + (s - series_count / 2.0) * (base_value * 0.05)
                    sv = _build_one_series(s, base)
                    frames.append(pd.DataFrame({
                        "timestamp": date_range,
                        group_column: f"series_{s}",
                        metric_name: sv,
                    }))
                df = pd.concat(frames, ignore_index=True)

            # Apply dropout last so it punches holes in whatever shape we built.
            if dropout_rate > 0:
                keep_mask = np.random.random(len(df)) >= dropout_rate
                df = df.loc[keep_mask].reset_index(drop=True)

            context.log.info(
                f"Generated {len(df)} points "
                f"(series_count={series_count}, dropout_rate={dropout_rate}), "
                f"min={df[metric_name].min():.2f}, "
                f"max={df[metric_name].max():.2f}, "
                f"mean={df[metric_name].mean():.2f}"
            )

            if include_preview and len(df) > 0:
                # Return with sample metadata
                context.add_output_metadata({
                    "row_count": len(df),
                    "columns": df.columns.tolist(),
                    "preview": MetadataValue.md(df.head().to_markdown())
                })
                return df
            else:
                # Build column schema metadata
                from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
                _col_schema = TableSchema(columns=[
                    TableColumn(name=str(col), type=str(df.dtypes[col]))
                    for col in df.columns
                ])
                _metadata = {
                    "dagster/row_count": MetadataValue.int(len(df)),
                    "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                }
                # Add column lineage if defined
                if column_lineage:
                    _upstream_key = AssetKey.from_user_string(upstream_asset_key) if 'upstream_asset_key' in dir() else None
                    if _upstream_key:
                        _lineage_deps = {}
                        for out_col, in_cols in column_lineage.items():
                            _lineage_deps[out_col] = [
                                TableColumnDep(asset_key=_upstream_key, column_name=ic)
                                for ic in in_cols
                            ]
                        _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                            TableColumnLineage(_lineage_deps)
                        )
                context.add_output_metadata(_metadata)
                return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[time_series_asset])


        return Definitions(assets=[time_series_asset], asset_checks=list(_schema_checks))


def _generate_trend(n: int, base: float, slope: float = 0.5) -> np.ndarray:
    """Generate linear trend."""
    return base + slope * np.arange(n)


def _generate_seasonal(n: int, base: float, date_range: pd.DatetimeIndex) -> np.ndarray:
    """Generate seasonal pattern based on hour of day."""
    # Daily seasonality - higher during business hours
    hours = date_range.hour
    # Peak at hour 14 (2 PM), low at hour 2 (2 AM)
    seasonal = base * (1 + 0.3 * np.sin((hours - 2) * 2 * np.pi / 24))
    return seasonal


def _generate_random_walk(n: int, base: float) -> np.ndarray:
    """Generate random walk."""
    steps = np.random.normal(0, base * 0.02, n)
    return base + np.cumsum(steps)


def _generate_sine_wave(n: int, base: float) -> np.ndarray:
    """Generate sine wave."""
    x = np.linspace(0, 4 * np.pi, n)
    return base + base * 0.2 * np.sin(x)


def _generate_step_function(n: int, base: float) -> np.ndarray:
    """Generate step function with level changes."""
    values = np.ones(n) * base
    # Add 3 random steps
    num_steps = min(3, n // 10)
    step_positions = sorted(np.random.choice(range(n // 4, 3 * n // 4), num_steps, replace=False))

    current_level = base
    prev_pos = 0
    for pos in step_positions:
        step_change = np.random.choice([-1, 1]) * base * np.random.uniform(0.2, 0.4)
        current_level += step_change
        values[pos:] = current_level

    return values


def _generate_spike(n: int, base: float) -> np.ndarray:
    """Generate mostly flat with occasional spikes."""
    values = np.ones(n) * base
    # Add 5-10 random spikes
    num_spikes = min(np.random.randint(5, 11), n // 10)
    spike_positions = np.random.choice(n, num_spikes, replace=False)

    for pos in spike_positions:
        spike_magnitude = base * np.random.uniform(2, 5)
        values[pos] = spike_magnitude

    return values


def _generate_complex(n: int, base: float, date_range: pd.DatetimeIndex) -> np.ndarray:
    """Generate complex pattern: trend + seasonal + noise."""
    # Upward trend
    trend = base + 0.1 * np.arange(n)

    # Daily seasonality
    hours = date_range.hour
    seasonal = base * 0.2 * np.sin((hours - 2) * 2 * np.pi / 24)

    # Combine
    return trend + seasonal
