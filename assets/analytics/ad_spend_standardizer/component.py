"""Ad Spend Standardizer Component.

Unifies advertising spend data from multiple platforms (Google Ads, Facebook Ads, etc.)
into a standardized schema for cross-platform analysis and attribution.
"""

from typing import Any, Dict, List, Optional, Union
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


class AdSpendStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing advertising spend data across platforms.

    Unifies ad campaign data from Google Ads, Facebook Ads, and other platforms
    into a common schema. Essential for cross-platform campaign comparison and
    marketing attribution analysis.

    Standard output schema:
    - campaign_id: Unique campaign identifier
    - campaign_name: Campaign display name
    - platform: Advertising platform (google_ads, facebook_ads, etc.)
    - campaign_type: Campaign type (search, display, shopping, social)
    - date: Campaign date
    - impressions: Number of ad impressions
    - clicks: Number of clicks
    - spend: Amount spent (normalized currency)
    - conversions: Number of conversions
    - conversion_value: Value of conversions
    - ctr: Click-through rate (clicks/impressions)
    - cpc: Cost per click
    - cpa: Cost per acquisition
    - roas: Return on ad spend
    - ad_group_id: Ad group identifier (if available)
    - ad_group_name: Ad group name (if available)
    - keyword: Target keyword (for search campaigns)
    - device_type: Device type (mobile, desktop, tablet)
    - location: Geographic location/region

    Example:
        ```yaml
        type: dagster_component_templates.AdSpendStandardizerComponent
        attributes:
          asset_name: standardized_ad_spend
          google_ads_asset_key: "google_ads_data"
          facebook_ads_asset_key: "facebook_ads_data"
          currency_normalization: "USD"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized ad spend output asset"
    )

    # Input assets (set via visual lineage)
    google_ads_asset_key: Optional[str] = Field(
        default=None,
        description="Google Ads data asset (automatically set via lineage)"
    )

    facebook_ads_asset_key: Optional[str] = Field(
        default=None,
        description="Facebook Ads data asset (automatically set via lineage)"
    )

    other_ad_platform_asset_key: Optional[str] = Field(
        default=None,
        description="Other ad platform data asset (automatically set via lineage)"
    )

    # Configuration
    currency_normalization: str = Field(
        default="USD",
        description="Normalize all spend to this currency (USD, EUR, GBP)"
    )

    date_column_name: Union[str, int] = Field(
        default="date",
        description="Name of the date column in source data"
    )

    aggregate_by_day: bool = Field(
        default=True,
        description="Aggregate metrics by day (recommended for consistency)"
    )

    include_zero_spend_days: bool = Field(
        default=False,
        description="Include days with zero spend in output"
    )

    calculate_derived_metrics: bool = Field(
        default=True,
        description="Calculate CTR, CPC, CPA, ROAS automatically"
    )

    description: Optional[str] = Field(
        default="",
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="standardizers",
        description="Asset group name"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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

    partition_values: Optional[str] = Field(
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
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access."
        ),
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
        """Build Dagster definitions for ad spend standardization."""

        # Capture configuration
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        google_ads_asset_key = self.google_ads_asset_key
        facebook_ads_asset_key = self.facebook_ads_asset_key
        other_ad_platform_asset_key = self.other_ad_platform_asset_key
        currency_normalization = self.currency_normalization
        date_column_name = self.date_column_name
        aggregate_by_day = self.aggregate_by_day
        include_zero_spend_days = self.include_zero_spend_days
        calculate_derived_metrics = self.calculate_derived_metrics
        description = self.description or "Standardized advertising spend data"
        group_name = self.group_name or "standardizers"

        # Build ins= so Dagster's IO manager loads each platform DataFrame
        # in the right order. Each named role maps to a kwarg name below.
        from dagster import AssetIn
        _ins: dict = {}
        if google_ads_asset_key:
            _ins["google_ads"] = AssetIn(key=AssetKey.from_user_string(google_ads_asset_key))
        if facebook_ads_asset_key:
            _ins["facebook_ads"] = AssetIn(key=AssetKey.from_user_string(facebook_ads_asset_key))
        if other_ad_platform_asset_key:
            _ins["other_ads"] = AssetIn(key=AssetKey.from_user_string(other_ad_platform_asset_key))

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
        _comp_name = "ad_spend_standardizer"  # component directory name
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
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins=_ins or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def ad_spend_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Standardize ad spend data from multiple platforms."""

            standardized_datasets = []

            # Upstream DataFrames arrive via Dagster's IO manager (see ins= above).
            google_ads_df = kwargs.get("google_ads")
            if google_ads_df is not None:
                try:
                    context.log.info(f"Loaded Google Ads data: {len(google_ads_df)} rows")
                    standardized_google = self._standardize_google_ads(
                        google_ads_df, date_column_name, currency_normalization,
                    )
                    standardized_datasets.append(standardized_google)
                    context.log.info(f"Standardized Google Ads: {len(standardized_google)} rows")
                except Exception as e:
                    context.log.warning(f"Could not process Google Ads data: {e}")

            facebook_ads_df = kwargs.get("facebook_ads")
            if facebook_ads_df is not None:
                try:
                    context.log.info(f"Loaded Facebook Ads data: {len(facebook_ads_df)} rows")
                    standardized_facebook = self._standardize_facebook_ads(
                        facebook_ads_df, date_column_name, currency_normalization,
                    )
                    standardized_datasets.append(standardized_facebook)
                    context.log.info(f"Standardized Facebook Ads: {len(standardized_facebook)} rows")
                except Exception as e:
                    context.log.warning(f"Could not process Facebook Ads data: {e}")

            other_ads_df = kwargs.get("other_ads")
            if other_ads_df is not None:
                try:
                    context.log.info(f"Loaded other ad platform data: {len(other_ads_df)} rows")
                    standardized_other = self._standardize_generic_ads(
                        other_ads_df, date_column_name, currency_normalization,
                    )
                    standardized_datasets.append(standardized_other)
                    context.log.info(f"Standardized other ads: {len(standardized_other)} rows")
                except Exception as e:
                    context.log.warning(f"Could not process other ad platform data: {e}")

            if not standardized_datasets:
                context.log.warning("No ad platform data available, returning empty DataFrame")
                return pd.DataFrame(columns=self._get_standard_columns())

            # Combine all standardized data
            combined_df = pd.concat(standardized_datasets, ignore_index=True)
            context.log.info(f"Combined data: {len(combined_df)} rows from {len(standardized_datasets)} platforms")

            # Aggregate by day if requested
            if aggregate_by_day:
                combined_df = self._aggregate_by_day(combined_df)
                context.log.info(f"Aggregated by day: {len(combined_df)} rows")

            # Calculate derived metrics
            if calculate_derived_metrics:
                combined_df = self._calculate_derived_metrics(combined_df)

            # Filter zero spend days if requested
            if not include_zero_spend_days:
                original_count = len(combined_df)
                combined_df = combined_df[combined_df['spend'] > 0]
                context.log.info(f"Filtered zero spend days: {original_count} → {len(combined_df)} rows")

            # Sort by date and platform (only include columns the source actually has)
            sort_cols = [c for c in ['date', 'platform', 'campaign_name'] if c in combined_df.columns]
            if sort_cols:
                combined_df = combined_df.sort_values(sort_cols)

            context.log.info(f"✅ Standardized ad spend data: {len(combined_df)} rows")
            context.log.info(f"Platforms: {combined_df['platform'].unique().tolist()}")
            context.log.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
            context.log.info(f"Total spend: {combined_df['spend'].sum():.2f} {currency_normalization}")

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(combined_df.dtypes[col]))
                for col in combined_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(combined_df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Add column lineage if defined
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if 'upstream_asset_key' in dir() else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            context.add_output_metadata({
                "row_count": len(combined_df),
                "platforms": MetadataValue.text(", ".join(combined_df['platform'].unique())),
                "total_spend": float(combined_df['spend'].sum()),
                "total_clicks": int(combined_df['clicks'].sum()),
                "total_impressions": int(combined_df['impressions'].sum()),
                "avg_ctr": float(combined_df['ctr'].mean()),
                "date_range": f"{combined_df['date'].min()} to {combined_df['date'].max()}",
                "preview": MetadataValue.md(combined_df.head(10).to_markdown()),
            })
            return combined_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[ad_spend_standardizer_asset])


        return Definitions(assets=[ad_spend_standardizer_asset], asset_checks=list(_schema_checks))

    def _get_standard_columns(self) -> list[str]:
        """Get list of standard column names."""
        return [
            'campaign_id', 'campaign_name', 'platform', 'campaign_type', 'date',
            'impressions', 'clicks', 'spend', 'conversions', 'conversion_value',
            'ctr', 'cpc', 'cpa', 'roas', 'ad_group_id', 'ad_group_name',
            'keyword', 'device_type', 'location'
        ]

    def _standardize_google_ads(
        self,
        df: pd.DataFrame,
        date_col: str,
        currency: str
    ) -> pd.DataFrame:
        """Standardize Google Ads data format."""

        # Common Google Ads field mappings
        field_mappings = {
            # Campaign fields
            'campaign_id': ['campaign_id', 'campaignId', 'id'],
            'campaign_name': ['campaign_name', 'campaignName', 'campaign', 'name'],
            'campaign_type': ['campaign_type', 'campaignType', 'advertising_channel_type'],

            # Date
            'date': [date_col, 'date', 'day', 'segments_date'],

            # Metrics
            'impressions': ['impressions', 'metrics_impressions'],
            'clicks': ['clicks', 'metrics_clicks'],
            'spend': ['cost', 'spend', 'cost_micros', 'metrics_cost_micros'],
            'conversions': ['conversions', 'metrics_conversions', 'all_conversions'],
            'conversion_value': ['conversion_value', 'metrics_conversions_value', 'conversions_value'],

            # Additional fields
            'ad_group_id': ['ad_group_id', 'adGroupId', 'adgroup_id'],
            'ad_group_name': ['ad_group_name', 'adGroupName', 'adgroup_name', 'adgroup'],
            'keyword': ['keyword_text', 'keyword', 'criteria', 'search_term'],
            'device_type': ['device', 'device_type', 'segments_device'],
            'location': ['location', 'geographic_location', 'country', 'region'],
        }

        standardized = pd.DataFrame()

        # Map fields
        for std_field, possible_names in field_mappings.items():
            for name in possible_names:
                if name in df.columns:
                    standardized[std_field] = df[name]
                    break

            # Set default if not found
            if std_field not in standardized.columns:
                if std_field in ['impressions', 'clicks', 'conversions']:
                    standardized[std_field] = 0
                elif std_field == 'spend':
                    standardized[std_field] = 0.0
                else:
                    standardized[std_field] = None

        # Convert Google Ads micros to currency units
        if 'cost_micros' in df.columns or 'metrics_cost_micros' in df.columns:
            standardized['spend'] = standardized['spend'] / 1_000_000

        # Add platform identifier
        standardized['platform'] = 'google_ads'

        # Ensure date is datetime
        standardized['date'] = pd.to_datetime(standardized['date'])

        # Set defaults for calculated metrics (will be calculated later)
        standardized['ctr'] = None
        standardized['cpc'] = None
        standardized['cpa'] = None
        standardized['roas'] = None

        return standardized

    def _standardize_facebook_ads(
        self,
        df: pd.DataFrame,
        date_col: str,
        currency: str
    ) -> pd.DataFrame:
        """Standardize Facebook Ads data format."""

        field_mappings = {
            'campaign_id': ['campaign_id', 'campaignId', 'id'],
            'campaign_name': ['campaign_name', 'campaignName', 'campaign', 'name'],
            'campaign_type': ['objective', 'campaign_objective', 'buying_type'],
            'date': [date_col, 'date', 'date_start'],
            'impressions': ['impressions', 'reach'],
            'clicks': ['clicks', 'link_clicks', 'inline_link_clicks'],
            'spend': ['spend', 'amount_spent', 'cost'],
            'conversions': ['conversions', 'actions', 'purchases'],
            'conversion_value': ['conversion_value', 'action_values', 'purchase_value'],
            'ad_group_id': ['adset_id', 'adsetId', 'ad_set_id'],
            'ad_group_name': ['adset_name', 'adsetName', 'ad_set_name'],
            'device_type': ['platform_position', 'device_platform'],
            'location': ['region', 'country', 'location'],
        }

        standardized = pd.DataFrame()

        for std_field, possible_names in field_mappings.items():
            for name in possible_names:
                if name in df.columns:
                    standardized[std_field] = df[name]
                    break

            if std_field not in standardized.columns:
                if std_field in ['impressions', 'clicks', 'conversions']:
                    standardized[std_field] = 0
                elif std_field == 'spend':
                    standardized[std_field] = 0.0
                else:
                    standardized[std_field] = None

        standardized['platform'] = 'facebook_ads'
        standardized['date'] = pd.to_datetime(standardized['date'])
        standardized['keyword'] = None  # Facebook doesn't use keywords
        standardized['ctr'] = None
        standardized['cpc'] = None
        standardized['cpa'] = None
        standardized['roas'] = None

        return standardized

    def _standardize_generic_ads(
        self,
        df: pd.DataFrame,
        date_col: str,
        currency: str
    ) -> pd.DataFrame:
        """Standardize generic ad platform data."""

        # Attempt to map common fields
        standardized = pd.DataFrame()

        # Try to identify standard fields
        for col in df.columns:
            col_lower = col.lower()
            if 'campaign' in col_lower and 'id' in col_lower:
                standardized['campaign_id'] = df[col]
            elif 'campaign' in col_lower and 'name' in col_lower:
                standardized['campaign_name'] = df[col]
            elif col_lower == 'date' or date_col.lower() in col_lower:
                standardized['date'] = df[col]
            elif 'impression' in col_lower:
                standardized['impressions'] = df[col]
            elif 'click' in col_lower:
                standardized['clicks'] = df[col]
            elif 'spend' in col_lower or 'cost' in col_lower:
                standardized['spend'] = df[col]
            elif 'conversion' in col_lower and 'value' in col_lower:
                standardized['conversion_value'] = df[col]
            elif 'conversion' in col_lower:
                standardized['conversions'] = df[col]

        # Fill missing columns
        for col in self._get_standard_columns():
            if col not in standardized.columns:
                if col in ['impressions', 'clicks', 'conversions']:
                    standardized[col] = 0
                elif col == 'spend':
                    standardized[col] = 0.0
                else:
                    standardized[col] = None

        standardized['platform'] = 'other'
        standardized['date'] = pd.to_datetime(standardized['date'])

        return standardized

    def _aggregate_by_day(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate metrics by day, platform, and campaign."""

        group_cols = ['date', 'platform', 'campaign_id', 'campaign_name', 'campaign_type']
        group_cols = [col for col in group_cols if col in df.columns and df[col].notna().any()]

        agg_dict = {
            'impressions': 'sum',
            'clicks': 'sum',
            'spend': 'sum',
            'conversions': 'sum',
            'conversion_value': 'sum',
        }

        aggregated = df.groupby(group_cols).agg(agg_dict).reset_index()

        # Preserve non-aggregated fields from first row of each group
        for col in ['ad_group_id', 'ad_group_name', 'keyword', 'device_type', 'location']:
            if col in df.columns:
                aggregated[col] = df.groupby(group_cols)[col].first().values

        return aggregated

    def _calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate CTR, CPC, CPA, ROAS.

        np.where() evaluates both branches eagerly, so wrapping
        `df['x'] / df['y']` in np.where() still divides element-wise
        first and raises on a zero denominator. Replace 0 with NaN in
        the denominator so the division yields NaN, then fillna(0).
        """
        impressions_safe = df['impressions'].replace(0, np.nan)
        clicks_safe = df['clicks'].replace(0, np.nan)
        conversions_safe = df['conversions'].replace(0, np.nan)
        spend_safe = df['spend'].replace(0, np.nan)

        df['ctr'] = ((df['clicks'] / impressions_safe) * 100).fillna(0)
        df['cpc'] = (df['spend'] / clicks_safe).fillna(0)
        df['cpa'] = (df['spend'] / conversions_safe).fillna(0)
        df['roas'] = (df['conversion_value'] / spend_safe).fillna(0)

        return df
