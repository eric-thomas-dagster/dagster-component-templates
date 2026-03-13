"""Event Data Standardizer Component.

Transform platform-specific event tracking data (Segment, Rudderstack, Snowplow, Custom) into a
standardized common schema for cross-platform event analysis.
"""

from typing import Dict, List, Literal, Optional
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

    source_asset: Optional[str] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)"
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
        platform = self.platform
        source_asset = self.source_asset
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
        include_sample = self.include_sample_metadata

        # Parse upstream asset keys
        upstream_keys = []
        if source_asset:
            upstream_keys = [source_asset]

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
        def event_data_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
            if include_sample and len(std_df) > 0:
                return Output(
                    value=std_df,
                    metadata={
                        "row_count": len(std_df),
                        "columns": std_df.columns.tolist(),
                        "sample": MetadataValue.md(std_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(std_df.head(10))
                    }
                )
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
                return std_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[event_data_standardizer_asset])


        return Definitions(assets=[event_data_standardizer_asset], asset_checks=list(_schema_checks))
