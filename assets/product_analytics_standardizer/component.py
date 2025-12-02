"""Product Analytics Standardizer Component.

Transform platform-specific product analytics data (GA4, Matomo, Mixpanel, Amplitude) into a
standardized common schema for cross-platform analytics analysis.
"""

from typing import Optional, Literal
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

    source_asset: Optional[str] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)"
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

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        source_asset = self.source_asset
        date_field = self.date_field
        event_name_field = self.event_name_field
        user_id_field = self.user_id_field
        filter_date_from = self.filter_date_from
        filter_date_to = self.filter_date_to
        filter_event_name = self.filter_event_name
        description = self.description or f"Standardized {platform} product analytics data"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Parse upstream asset keys
        upstream_keys = []
        if source_asset:
            upstream_keys = [source_asset]

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def product_analytics_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
            context.add_output_metadata({
                "platform": platform,
                "original_rows": original_rows,
                "final_rows": final_rows,
                "columns": list(std_df.columns),
                "date_range": f"{std_df['date'].min()} to {std_df['date'].max()}" if 'date' in std_df.columns else "N/A",
                "total_sessions": int(std_df['sessions'].sum()) if 'sessions' in std_df.columns else 0,
                "total_users": int(std_df['users'].sum()) if 'users' in std_df.columns else 0,
                "total_page_views": int(std_df['page_views'].sum()) if 'page_views' in std_df.columns else 0,
                "total_events": int(std_df['events'].sum()) if 'events' in std_df.columns else 0,
            })

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
                return std_df

        return Definitions(assets=[product_analytics_standardizer_asset])
