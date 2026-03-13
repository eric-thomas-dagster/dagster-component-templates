"""Customer Journey Mapping Component.

Map and analyze customer journeys from first touch to conversion, identifying common paths,
drop-off points, and journey performance metrics.
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


class CustomerJourneyMappingComponent(Component, Model, Resolvable):
    """Component for mapping and analyzing customer journeys.

    This component traces customer interactions from first touch to conversion,
    identifying:
    - Common journey paths and sequences
    - Average journey length and duration
    - Drop-off points and bottlenecks
    - Conversion rates by journey stage
    - Journey performance by segment

    Example:
        ```yaml
        type: dagster_component_templates.CustomerJourneyMappingComponent
        attributes:
          asset_name: customer_journeys
          source_asset: customer_events
          journey_stages: ["awareness", "consideration", "purchase"]
          max_journey_length: 20
          description: "Customer journey analysis"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with customer event/touchpoint data (set via lineage)"
    )

    journey_stages: Optional[list] = Field(
        default=None,
        description="Ordered list of journey stages to track (optional)"
    )

    max_journey_length: int = Field(
        default=20,
        description="Maximum events to include in a journey path"
    )

    conversion_event: Optional[str] = Field(
        default=None,
        description="Event name that indicates conversion (e.g., 'purchase', 'signup')"
    )

    time_window_hours: int = Field(
        default=720,  # 30 days
        description="Maximum time between first and last event in a journey (hours)"
    )

    include_path_analysis: bool = Field(
        default=True,
        description="Include detailed path analysis and visualization"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column (auto-detected if not specified)"
    )

    event_field: Optional[str] = Field(
        default=None,
        description="Event name/type column (auto-detected if not specified)"
    )

    timestamp_field: Optional[str] = Field(
        default=None,
        description="Event timestamp column (auto-detected if not specified)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="customer_analytics",
        description="Asset group for organization"
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

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        journey_stages = self.journey_stages
        max_length = self.max_journey_length
        conversion_event = self.conversion_event
        time_window = self.time_window_hours
        include_paths = self.include_path_analysis
        customer_id_field = self.customer_id_field
        event_field = self.event_field
        timestamp_field = self.timestamp_field
        description = self.description or "Customer journey mapping and analysis"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Set up dependencies
        upstream_keys = []
        if source_asset:
            upstream_keys.append(source_asset)

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
        _comp_name = "customer_journey_mapping"  # component directory name
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


        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def customer_journey_mapping_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Asset that maps customer journeys."""

            # Load upstream data
            upstream_data = {}
            if upstream_keys and hasattr(context, 'load_asset_value'):
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(AssetKey(key))
                        upstream_data[key] = value
                        context.log.info(f"Loaded {len(value)} rows from {key}")
                    except Exception as e:
                        context.log.warning(f"Could not load {key}: {e}")
            else:
                upstream_data = kwargs

            if not upstream_data:
                context.log.warning("No upstream data available")
                return pd.DataFrame()

            # Get the source DataFrame
            df = list(upstream_data.values())[0]
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} events for journey mapping")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            customer_col = find_column(
                ['customer_id', 'user_id', 'customerId', 'userId', 'id'],
                customer_id_field
            )
            event_col = find_column(
                ['event', 'event_name', 'event_type', 'action', 'activity'],
                event_field
            )
            timestamp_col = find_column(
                ['timestamp', 'date', 'created_at', 'event_time', 'time'],
                timestamp_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not event_col:
                missing.append("event/event_name")
            if not timestamp_col:
                missing.append("timestamp/date")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Event: {event_col}, Timestamp: {timestamp_col}")

            # Prepare data
            events_df = df[[customer_col, event_col, timestamp_col]].copy()
            events_df.columns = ['customer_id', 'event', 'timestamp']

            # Parse timestamps
            events_df['timestamp'] = pd.to_datetime(events_df['timestamp'], errors='coerce')
            events_df = events_df.dropna(subset=['timestamp'])

            # Sort by customer and timestamp
            events_df = events_df.sort_values(['customer_id', 'timestamp'])

            context.log.info(f"Analyzing journeys for {events_df['customer_id'].nunique()} customers")

            # Build journeys for each customer
            journeys = []

            for customer_id, customer_events in events_df.groupby('customer_id'):
                # Get events for this customer
                events = customer_events.sort_values('timestamp')

                # Check if journey is within time window
                first_event = events.iloc[0]
                last_event = events.iloc[-1]
                journey_duration_hours = (last_event['timestamp'] - first_event['timestamp']).total_seconds() / 3600

                if journey_duration_hours > time_window:
                    # Split into multiple journeys if exceeds time window
                    # For simplicity, we'll take the most recent journey
                    cutoff_time = last_event['timestamp'] - pd.Timedelta(hours=time_window)
                    events = events[events['timestamp'] >= cutoff_time]

                # Limit journey length
                if len(events) > max_length:
                    events = events.tail(max_length)

                # Build journey path
                journey_path = ' → '.join(events['event'].tolist())
                journey_length = len(events)

                # Calculate journey duration
                if journey_length > 1:
                    journey_duration = (events.iloc[-1]['timestamp'] - events.iloc[0]['timestamp']).total_seconds() / 3600
                else:
                    journey_duration = 0

                # Check if journey resulted in conversion
                has_conversion = False
                if conversion_event:
                    has_conversion = conversion_event in events['event'].values
                else:
                    # If no conversion event specified, assume last event is outcome
                    has_conversion = True

                # Get first and last event
                first_event_name = events.iloc[0]['event']
                last_event_name = events.iloc[-1]['event']

                # Count unique event types
                unique_events = events['event'].nunique()

                journeys.append({
                    'customer_id': customer_id,
                    'journey_path': journey_path,
                    'journey_length': journey_length,
                    'journey_duration_hours': round(journey_duration, 2),
                    'first_event': first_event_name,
                    'last_event': last_event_name,
                    'unique_events': unique_events,
                    'converted': has_conversion,
                    'first_timestamp': events.iloc[0]['timestamp'],
                    'last_timestamp': events.iloc[-1]['timestamp']
                })

            result_df = pd.DataFrame(journeys)

            if len(result_df) == 0:
                context.log.warning("No customer journeys found")
                return pd.DataFrame()

            context.log.info(f"Mapped {len(result_df)} customer journeys")

            # Calculate conversion rate
            conversion_rate = (result_df['converted'].sum() / len(result_df) * 100).round(2)
            context.log.info(f"Overall conversion rate: {conversion_rate}%")

            # Analyze path patterns
            if include_paths:
                # Most common paths
                path_counts = result_df['journey_path'].value_counts().head(20)
                context.log.info(f"\nTop 10 Journey Paths:")
                for path, count in path_counts.head(10).items():
                    pct = (count / len(result_df) * 100).round(1)
                    context.log.info(f"  ({count} customers, {pct}%): {path}")

                # Conversion rate by journey length
                length_conv_rate = result_df.groupby('journey_length')['converted'].mean() * 100
                context.log.info(f"\nConversion Rate by Journey Length:")
                for length, rate in length_conv_rate.head(10).items():
                    count = len(result_df[result_df['journey_length'] == length])
                    context.log.info(f"  {length} events: {rate:.1f}% ({count} journeys)")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "avg_journey_length": round(result_df['journey_length'].mean(), 2),
                "avg_journey_duration_hours": round(result_df['journey_duration_hours'].mean(), 2),
                "conversion_rate": conversion_rate,
                "conversions": result_df['converted'].sum(),
                "max_journey_length": max_length,
                "time_window_hours": time_window,
            }

            # Return with metadata
            if include_sample and len(result_df) > 0:
                # Show sample of converted and non-converted journeys
                converted_sample = result_df[result_df['converted']].head(5)
                non_converted_sample = result_df[~result_df['converted']].head(5)
                sample_df = pd.concat([converted_sample, non_converted_sample])

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(sample_df.to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(sample_df)
                    }
                )
            else:
                context.add_output_metadata(metadata)
                # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
                return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[customer_journey_mapping_asset])


        return Definitions(assets=[customer_journey_mapping_asset], asset_checks=list(_schema_checks))
