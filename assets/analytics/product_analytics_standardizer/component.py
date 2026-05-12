"""Product Analytics Standardizer Component.

Transform platform-specific product analytics data (GA4, Matomo, Mixpanel, Amplitude) into a
standardized common schema for cross-platform analytics analysis.
"""

from typing import Any, Dict, List, Literal, Optional
import pandas as pd
import numpy as np
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
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
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


class ProductAnalyticsStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing product analytics data across platforms.

    Transforms platform-specific schemas (GA4, Matomo, Mixpanel, Amplitude) into a
    unified product analytics data model with consistent field names and metrics.

    Standard Schema Output:
    - date: Event date
    - platform: Source platform (google_analytics_4, matomo, mixpanel, amplitude)
    - event_name: Event identifier
    - user_id: User identifier (if available)
    - session_id: Session identifier
    - page_path: Page/screen path
    - page_title: Page/screen title
    - sessions: Number of sessions
    - users: Number of users
    - new_users: Number of new users
    - page_views: Number of page views
    - events: Number of events
    - conversions: Number of conversions
    - bounce_rate: Bounce rate (%)
    - avg_session_duration: Average session duration (seconds)
    - engagement_rate: Engagement rate (%)

    Example:
        ```yaml
        type: dagster_component_templates.ProductAnalyticsStandardizerComponent
        attributes:
          asset_name: standardized_product_analytics
          platform: "google_analytics_4"
          source_asset: "ga4_events"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized output asset"
    )

    platform: Literal["google_analytics_4", "matomo", "mixpanel", "amplitude"] = Field(
        description="Source platform to standardize"
    )

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)",
    )

    date_field: Optional[str] = Field(
        default=None,
        description="Field name for date (auto-detected if not provided)"
    )

    event_name_field: Optional[str] = Field(
        default=None,
        description="Field name for event name (auto-detected if not provided)"
    )

    user_id_field: Optional[str] = Field(
        default=None,
        description="Field name for user ID (auto-detected if not provided)"
    )

    # Optional filters
    filter_date_from: Optional[str] = Field(
        default=None,
        description="Filter data from this date (YYYY-MM-DD)"
    )

    filter_date_to: Optional[str] = Field(
        default=None,
        description="Filter data to this date (YYYY-MM-DD)"
    )

    filter_event_name: Optional[str] = Field(
        default=None,
        description="Filter by event name (comma-separated)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="analytics",
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
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        source_asset = self.upstream_asset_key
        date_field = self.date_field
        event_name_field = self.event_name_field
        user_id_field = self.user_id_field
        filter_date_from = self.filter_date_from
        filter_date_to = self.filter_date_to
        filter_event_name = self.filter_event_name
        description = self.description or f"Standardized {platform} product analytics data"
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
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "product_analytics_standardizer"  # component directory name
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
            deps=upstream_keys if upstream_keys else None,
        )
        def product_analytics_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
            """Asset that standardizes platform-specific product analytics data."""

            context.log.info(f"Standardizing {platform} product analytics data")

            # Load upstream data
            if upstream_keys and hasattr(context, 'load_asset_value'):
                context.log.info(f"Loading data from upstream asset: {source_asset}")
                raw_data = context.load_asset_value(AssetKey(source_asset))
            elif kwargs:
                raw_data = list(kwargs.values())[0]
            else:
                raise ValueError(
                    f"Product Analytics Standardizer '{asset_name}' requires upstream data. "
                    f"Connect to a product analytics ingestion component (GA4, Matomo, etc.)"
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

            context.log.info(f"Raw data: {len(df)} rows, {len(df.columns)} columns")
            original_rows = len(df)

            # Platform-specific field mappings
            field_mappings = {
                "google_analytics_4": {
                    "date": ["event_date", "date", "eventDate"],
                    "event_name": ["event_name", "eventName"],
                    "user_id": ["user_pseudo_id", "user_id", "userId"],
                    "session_id": ["ga_session_id", "session_id"],
                    "page_path": ["page_location", "page_path", "pagePath"],
                    "page_title": ["page_title", "pageTitle"],
                    "sessions": ["sessions", "ga_sessions"],
                    "users": ["totalUsers", "users", "active_users"],
                    "new_users": ["newUsers", "new_users"],
                    "page_views": ["screenPageViews", "page_views", "pageviews"],
                    "events": ["eventCount", "events"],
                    "conversions": ["conversions", "keyEvents"],
                    "bounce_rate": ["bounceRate", "bounce_rate"],
                    "avg_session_duration": ["averageSessionDuration", "avg_session_duration"],
                    "engagement_rate": ["engagementRate", "engagement_rate"],
                },
                "matomo": {
                    "date": ["date", "label", "period"],
                    "event_name": ["label", "event_name", "action_name"],
                    "user_id": ["userId", "visitorId"],
                    "session_id": ["idVisit", "visitId"],
                    "page_path": ["url", "page_url"],
                    "page_title": ["label", "page_title"],
                    "sessions": ["nb_visits", "visits"],
                    "users": ["nb_uniq_visitors", "unique_visitors"],
                    "new_users": ["nb_new_visits", "new_visits"],
                    "page_views": ["nb_pageviews", "pageviews"],
                    "events": ["nb_events", "events"],
                    "conversions": ["nb_conversions", "conversions", "goals"],
                    "bounce_rate": ["bounce_rate", "bounceRate"],
                    "avg_session_duration": ["avg_time_on_site", "averageTimeOnSite"],
                },
                "mixpanel": {
                    "date": ["date", "time"],
                    "event_name": ["event", "event_name"],
                    "user_id": ["distinct_id", "user_id"],
                    "session_id": ["$session_id", "session_id"],
                    "page_path": ["$current_url", "page_url"],
                    "page_title": ["$page_title", "page_title"],
                    "sessions": ["$sessions", "sessions"],
                    "users": ["users", "unique_users"],
                    "new_users": ["new_users"],
                    "page_views": ["$pageviews", "pageviews"],
                    "events": ["count", "event_count"],
                    "conversions": ["conversions"],
                },
                "amplitude": {
                    "date": ["event_time", "date", "server_upload_time"],
                    "event_name": ["event_type", "event_name"],
                    "user_id": ["user_id", "amplitude_id"],
                    "session_id": ["session_id"],
                    "page_path": ["page_url", "url"],
                    "page_title": ["page_title"],
                    "sessions": ["sessions"],
                    "users": ["users", "active_users"],
                    "new_users": ["new_users"],
                    "page_views": ["page_views"],
                    "events": ["event_count", "events"],
                    "conversions": ["conversions"],
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

            # Date field
            date_col = find_field(mapping['date'], date_field)
            if date_col:
                standardized_data['date'] = pd.to_datetime(df[date_col]).dt.date
            else:
                context.log.warning("Date field not found")

            # Event name
            event_name_col = find_field(mapping['event_name'], event_name_field)
            if event_name_col:
                standardized_data['event_name'] = df[event_name_col]

            # User ID
            user_id_col = find_field(mapping['user_id'], user_id_field)
            if user_id_col:
                standardized_data['user_id'] = df[user_id_col].astype(str)

            # Session ID
            session_id_col = find_field(mapping.get('session_id', []))
            if session_id_col:
                standardized_data['session_id'] = df[session_id_col].astype(str)

            # Page path
            page_path_col = find_field(mapping.get('page_path', []))
            if page_path_col:
                standardized_data['page_path'] = df[page_path_col]

            # Page title
            page_title_col = find_field(mapping.get('page_title', []))
            if page_title_col:
                standardized_data['page_title'] = df[page_title_col]

            # Metrics
            sessions_col = find_field(mapping.get('sessions', []))
            if sessions_col:
                standardized_data['sessions'] = pd.to_numeric(df[sessions_col], errors='coerce')

            users_col = find_field(mapping.get('users', []))
            if users_col:
                standardized_data['users'] = pd.to_numeric(df[users_col], errors='coerce')

            new_users_col = find_field(mapping.get('new_users', []))
            if new_users_col:
                standardized_data['new_users'] = pd.to_numeric(df[new_users_col], errors='coerce')

            page_views_col = find_field(mapping.get('page_views', []))
            if page_views_col:
                standardized_data['page_views'] = pd.to_numeric(df[page_views_col], errors='coerce')

            events_col = find_field(mapping.get('events', []))
            if events_col:
                standardized_data['events'] = pd.to_numeric(df[events_col], errors='coerce')

            conversions_col = find_field(mapping.get('conversions', []))
            if conversions_col:
                standardized_data['conversions'] = pd.to_numeric(df[conversions_col], errors='coerce')

            bounce_rate_col = find_field(mapping.get('bounce_rate', []))
            if bounce_rate_col:
                standardized_data['bounce_rate'] = pd.to_numeric(df[bounce_rate_col], errors='coerce')

            avg_duration_col = find_field(mapping.get('avg_session_duration', []))
            if avg_duration_col:
                standardized_data['avg_session_duration'] = pd.to_numeric(df[avg_duration_col], errors='coerce')

            engagement_rate_col = find_field(mapping.get('engagement_rate', []))
            if engagement_rate_col:
                standardized_data['engagement_rate'] = pd.to_numeric(df[engagement_rate_col], errors='coerce')

            # Create standardized DataFrame
            std_df = pd.DataFrame(standardized_data)

            # Calculate derived metrics if not present
            if 'bounce_rate' not in std_df.columns and 'sessions' in std_df.columns:
                # Placeholder calculation - would need actual bounce session data
                std_df['bounce_rate'] = np.nan

            if 'engagement_rate' not in std_df.columns and 'sessions' in std_df.columns:
                # Placeholder calculation - would need actual engagement data
                std_df['engagement_rate'] = np.nan

            # Apply filters
            if filter_date_from and 'date' in std_df.columns:
                std_df = std_df[std_df['date'] >= pd.to_datetime(filter_date_from).date()]
                context.log.info(f"Filtered from date: {filter_date_from}")

            if filter_date_to and 'date' in std_df.columns:
                std_df = std_df[std_df['date'] <= pd.to_datetime(filter_date_to).date()]
                context.log.info(f"Filtered to date: {filter_date_to}")

            if filter_event_name and 'event_name' in std_df.columns:
                events = [e.strip() for e in filter_event_name.split(',')]
                std_df = std_df[std_df['event_name'].isin(events)]
                context.log.info(f"Filtered to events: {events}")

            # Replace inf and -inf with NaN
            std_df = std_df.replace([float('inf'), float('-inf')], pd.NA)

            final_rows = len(std_df)
            context.log.info(
                f"Standardization complete: {original_rows} → {final_rows} rows, "
                f"{len(std_df.columns)} columns"
            )

            # Add metadata

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
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            # Return DataFrame
            if include_preview and len(std_df) > 0:
                context.add_output_metadata({
                    "row_count": len(std_df),
                    "columns": std_df.columns.tolist(),
                    "preview": MetadataValue.md(std_df.head(10).to_markdown())
                })
                return std_df
            else:
                return std_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[product_analytics_standardizer_asset])


        return Definitions(assets=[product_analytics_standardizer_asset], asset_checks=list(_schema_checks))
