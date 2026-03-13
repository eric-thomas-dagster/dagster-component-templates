"""Propensity Scoring Component.

Calculate propensity scores for various customer actions (purchase, upgrade, churn, refer)
using heuristic scoring based on behavior patterns.
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
    Output,
)
from pydantic import Field


class PropensityScoringComponent(Component, Model, Resolvable):
    """Component for calculating customer propensity scores.

    Calculate likelihood scores for customer actions:
    - **Purchase Propensity**: Likelihood to make next purchase
    - **Upgrade Propensity**: Likelihood to upgrade tier/plan
    - **Referral Propensity**: Likelihood to refer others
    - **Engagement Propensity**: Likelihood to engage with content

    Uses heuristic scoring based on:
    - Recent activity levels
    - Historical behavior patterns
    - Engagement metrics
    - RFM characteristics

    Example:
        ```yaml
        type: dagster_component_templates.PropensityScoringComponent
        attributes:
          asset_name: customer_propensity_scores
          upstream_asset_key: customer_behavior
          propensity_type: purchase
          scoring_window_days: 90
          description: "Customer purchase propensity scores"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with customer behavior data"
    )

    propensity_type: str = Field(
        default="purchase",
        description="Type of propensity: purchase, upgrade, referral, engagement"
    )

    scoring_window_days: int = Field(
        default=90,
        description="Days of historical data to use for scoring"
    )

    score_threshold_high: float = Field(
        default=70.0,
        description="Score threshold for 'high propensity' classification"
    )

    score_threshold_medium: float = Field(
        default=40.0,
        description="Score threshold for 'medium propensity' classification"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column (auto-detected)"
    )

    last_activity_field: Optional[str] = Field(
        default=None,
        description="Last activity date column (auto-detected)"
    )

    activity_count_field: Optional[str] = Field(
        default=None,
        description="Activity count column (auto-detected)"
    )

    engagement_score_field: Optional[str] = Field(
        default=None,
        description="Engagement score column (optional)"
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
        upstream_asset_key = self.upstream_asset_key
        propensity_type = self.propensity_type
        scoring_window = self.scoring_window_days
        threshold_high = self.score_threshold_high
        threshold_medium = self.score_threshold_medium
        customer_id_field = self.customer_id_field
        last_activity_field = self.last_activity_field
        activity_count_field = self.activity_count_field
        engagement_score_field = self.engagement_score_field
        description = self.description or f"Customer {propensity_type} propensity scores"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

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
        _comp_name = "propensity_scoring"  # component directory name
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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def propensity_scoring_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that calculates customer propensity scores."""

            df = upstream
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} customer records for propensity scoring")

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
            last_activity_col = find_column(
                ['last_activity_date', 'last_activity', 'last_seen', 'last_purchase_date'],
                last_activity_field
            )
            activity_count_col = find_column(
                ['activity_count', 'total_activities', 'event_count', 'interactions'],
                activity_count_field
            )
            engagement_col = find_column(
                ['engagement_score', 'engagement', 'activity_score'],
                engagement_score_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not last_activity_col:
                missing.append("last_activity_date")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Last Activity: {last_activity_col}")

            # Prepare data
            cols_to_use = [customer_col, last_activity_col]
            col_names = ['customer_id', 'last_activity_date']

            if activity_count_col:
                cols_to_use.append(activity_count_col)
                col_names.append('activity_count')

            if engagement_col:
                cols_to_use.append(engagement_col)
                col_names.append('engagement_score')

            propensity_df = df[cols_to_use].copy()
            propensity_df.columns = col_names

            # Parse dates
            propensity_df['last_activity_date'] = pd.to_datetime(propensity_df['last_activity_date'], errors='coerce')
            propensity_df = propensity_df.dropna(subset=['last_activity_date'])

            # Calculate days since last activity
            current_date = pd.Timestamp.now()
            propensity_df['days_since_activity'] = (current_date - propensity_df['last_activity_date']).dt.days

            context.log.info(f"Calculating {propensity_type} propensity for {len(propensity_df)} customers")

            # Calculate propensity score based on type
            if propensity_type == 'purchase':
                # Purchase propensity based on recency and frequency
                # Recency score (0-40 points): Recent activity = high score
                propensity_df['recency_score'] = propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 40 - (days / 7) * 5)  # Decay 5 points per week
                ).clip(0, 40)

                # Frequency score (0-40 points)
                if 'activity_count' in propensity_df.columns:
                    max_activities = propensity_df['activity_count'].quantile(0.95)
                    propensity_df['frequency_score'] = (
                        propensity_df['activity_count'] / max_activities * 40
                    ).clip(0, 40)
                else:
                    propensity_df['frequency_score'] = 20  # Default mid-range

                # Engagement score (0-20 points)
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['engagement_component'] = (
                            propensity_df['engagement_score'] / max_engagement * 20
                        ).clip(0, 20)
                    else:
                        propensity_df['engagement_component'] = 10
                else:
                    propensity_df['engagement_component'] = 10  # Default mid-range

                # Total propensity score (0-100)
                propensity_df['propensity_score'] = (
                    propensity_df['recency_score'] +
                    propensity_df['frequency_score'] +
                    propensity_df['engagement_component']
                ).round(2)

            elif propensity_type == 'upgrade':
                # Upgrade propensity based on engagement and activity growth
                # High engagement = high upgrade propensity
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['propensity_score'] = (
                            propensity_df['engagement_score'] / max_engagement * 70
                        ).clip(0, 70)
                    else:
                        propensity_df['propensity_score'] = 35
                else:
                    propensity_df['propensity_score'] = 35

                # Boost for recent activity
                propensity_df['propensity_score'] += propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 30 - days)  # Up to 30 points for very recent
                ).clip(0, 30)

                propensity_df['propensity_score'] = propensity_df['propensity_score'].round(2).clip(0, 100)

            elif propensity_type == 'referral':
                # Referral propensity based on engagement and satisfaction indicators
                # Highly engaged customers are more likely to refer
                if 'engagement_score' in propensity_df.columns:
                    max_engagement = propensity_df['engagement_score'].max()
                    if max_engagement > 0:
                        propensity_df['propensity_score'] = (
                            propensity_df['engagement_score'] / max_engagement * 60
                        ).clip(0, 60)
                    else:
                        propensity_df['propensity_score'] = 30
                else:
                    propensity_df['propensity_score'] = 30

                # Boost for moderate recency (not too new, not too old)
                propensity_df['propensity_score'] += propensity_df['days_since_activity'].apply(
                    lambda days: 40 if 7 <= days <= 60 else (20 if days < 7 else max(0, 40 - (days - 60) / 10))
                ).clip(0, 40)

                propensity_df['propensity_score'] = propensity_df['propensity_score'].round(2).clip(0, 100)

            elif propensity_type == 'engagement':
                # Engagement propensity based on recent activity patterns
                # Recent and frequent activity = high engagement propensity
                propensity_df['recency_score'] = propensity_df['days_since_activity'].apply(
                    lambda days: max(0, 50 - days)  # 50 points max, decays daily
                ).clip(0, 50)

                if 'activity_count' in propensity_df.columns:
                    max_activities = propensity_df['activity_count'].quantile(0.95)
                    propensity_df['frequency_score'] = (
                        propensity_df['activity_count'] / max_activities * 50
                    ).clip(0, 50)
                else:
                    propensity_df['frequency_score'] = 25

                propensity_df['propensity_score'] = (
                    propensity_df['recency_score'] + propensity_df['frequency_score']
                ).round(2).clip(0, 100)

            else:
                context.log.error(f"Unknown propensity type: {propensity_type}")
                return pd.DataFrame()

            # Classify propensity level
            def classify_propensity(score):
                if score >= threshold_high:
                    return 'High'
                elif score >= threshold_medium:
                    return 'Medium'
                else:
                    return 'Low'

            propensity_df['propensity_level'] = propensity_df['propensity_score'].apply(classify_propensity)

            # Select output columns
            output_cols = [
                'customer_id',
                'propensity_score',
                'propensity_level',
                'days_since_activity'
            ]

            result_df = propensity_df[output_cols].copy()

            context.log.info(f"Propensity scoring complete: {len(result_df)} customers scored")

            # Log propensity distribution
            level_dist = result_df['propensity_level'].value_counts()
            context.log.info("\nPropensity Level Distribution:")
            for level, count in level_dist.items():
                pct = (count / len(result_df) * 100).round(1)
                avg_score = result_df[result_df['propensity_level'] == level]['propensity_score'].mean().round(2)
                context.log.info(f"  {level}: {count} customers ({pct}%), avg score: {avg_score}")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "propensity_type": propensity_type,
                "avg_propensity_score": round(result_df['propensity_score'].mean(), 2),
                "high_propensity_count": int(level_dist.get('High', 0)),
                "medium_propensity_count": int(level_dist.get('Medium', 0)),
                "low_propensity_count": int(level_dist.get('Low', 0)),
                "scoring_window_days": scoring_window,
            }

            # Return with metadata
            if include_sample and len(result_df) > 0:
                # Sort by propensity score descending
                result_sorted = result_df.sort_values('propensity_score', ascending=False)

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_sorted.head(20).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_sorted.head(20))
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
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
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


        _schema_checks = build_column_schema_change_checks(assets=[propensity_scoring_asset])


        return Definitions(assets=[propensity_scoring_asset], asset_checks=list(_schema_checks))
