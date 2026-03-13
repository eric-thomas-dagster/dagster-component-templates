"""Churn Prediction Component.

Predict customer churn risk using heuristic scoring based on activity patterns.
Identifies customers at risk of churning with actionable recommendations.
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


class ChurnPredictionComponent(Component, Model, Resolvable):
    """Component for predicting customer churn risk using heuristic scoring.

    Churn prediction identifies customers at risk of stopping their relationship
    with your business. This component uses a weighted heuristic approach based on:
    - Inactivity: Days since last activity
    - Activity Decline: Recent vs historical activity levels
    - Value Decline: Recent vs historical spending
    - Frequency Decline: Recent vs historical purchase frequency

    Each factor is scored 0-10 and combined into a final churn risk score (0-100)
    with risk levels: Low, Medium, High, Critical.

    Example:
        ```yaml
        type: dagster_component_templates.ChurnPredictionComponent
        attributes:
          asset_name: customer_churn_risk
          upstream_asset_key: customer_metrics
          inactivity_threshold_days: 90
          lookback_days: 365
          include_risk_factors: true
          description: "Customer churn risk prediction"
          group_name: customer_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with customer activity data"
    )

    inactivity_threshold_days: int = Field(
        default=90,
        description="Days of inactivity to consider high risk"
    )

    lookback_days: int = Field(
        default=365,
        description="Days to look back for historical comparison"
    )

    include_risk_factors: bool = Field(
        default=True,
        description="Include detailed risk factors in output"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column name (auto-detected if not specified)"
    )

    last_activity_field: Optional[str] = Field(
        default=None,
        description="Last activity date column (auto-detected if not specified)"
    )

    total_orders_field: Optional[str] = Field(
        default=None,
        description="Total orders column (auto-detected if not specified)"
    )

    total_revenue_field: Optional[str] = Field(
        default=None,
        description="Total revenue column (auto-detected if not specified)"
    )

    lifetime_days_field: Optional[str] = Field(
        default=None,
        description="Customer lifetime days column (auto-detected if not specified)"
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
        inactivity_threshold_days = self.inactivity_threshold_days
        lookback_days = self.lookback_days
        include_risk_factors = self.include_risk_factors
        customer_id_field = self.customer_id_field
        last_activity_field = self.last_activity_field
        total_orders_field = self.total_orders_field
        total_revenue_field = self.total_revenue_field
        lifetime_days_field = self.lifetime_days_field
        description = self.description or "Customer churn risk prediction"
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
        _comp_name = "churn_prediction"  # component directory name
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
        def churn_prediction_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that predicts customer churn risk."""

            df = upstream
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} customer records for churn prediction")

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
                ['last_activity_date', 'last_order_date', 'last_purchase_date', 'last_seen'],
                last_activity_field
            )
            total_orders_col = find_column(
                ['total_orders', 'order_count', 'num_orders', 'orders'],
                total_orders_field
            )
            total_revenue_col = find_column(
                ['total_revenue', 'lifetime_value', 'ltv', 'total_spend', 'revenue'],
                total_revenue_field
            )
            lifetime_col = find_column(
                ['lifetime_days', 'customer_age_days', 'days_since_first_order', 'tenure_days'],
                lifetime_days_field
            )

            # Validate required columns
            missing = []
            if not customer_col:
                missing.append("customer_id")
            if not last_activity_col:
                missing.append("last_activity_date")
            if not total_orders_col:
                missing.append("total_orders")
            if not total_revenue_col:
                missing.append("total_revenue")
            if not lifetime_col:
                missing.append("lifetime_days")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Customer: {customer_col}, Last Activity: {last_activity_col}, Orders: {total_orders_col}, Revenue: {total_revenue_col}, Lifetime: {lifetime_col}")

            # Prepare data
            churn_df = df[[customer_col, last_activity_col, total_orders_col, total_revenue_col, lifetime_col]].copy()
            churn_df.columns = ['customer_id', 'last_activity_date', 'total_orders', 'total_revenue', 'lifetime_days']

            # Parse dates
            churn_df['last_activity_date'] = pd.to_datetime(churn_df['last_activity_date'], errors='coerce')
            churn_df = churn_df.dropna(subset=['last_activity_date'])

            # Convert numeric columns
            churn_df['total_orders'] = pd.to_numeric(churn_df['total_orders'], errors='coerce').fillna(0)
            churn_df['total_revenue'] = pd.to_numeric(churn_df['total_revenue'], errors='coerce').fillna(0)
            churn_df['lifetime_days'] = pd.to_numeric(churn_df['lifetime_days'], errors='coerce').fillna(0)

            # Exclude very new customers (< 30 days)
            churn_df = churn_df[churn_df['lifetime_days'] >= 30]

            if len(churn_df) == 0:
                context.log.warning("No customers with sufficient lifetime (>= 30 days)")
                return pd.DataFrame()

            context.log.info(f"Analyzing churn risk for {len(churn_df)} customers")

            # Calculate days inactive
            current_date = pd.Timestamp.now()
            churn_df['days_inactive'] = (current_date - churn_df['last_activity_date']).dt.days

            # Calculate historical averages
            churn_df['avg_orders_per_30_days'] = (churn_df['total_orders'] / (churn_df['lifetime_days'] / 30)).replace([np.inf, -np.inf], np.nan)
            churn_df['avg_revenue_per_30_days'] = (churn_df['total_revenue'] / (churn_df['lifetime_days'] / 30)).replace([np.inf, -np.inf], np.nan)

            # Heuristic scoring functions
            def calculate_inactivity_score(days):
                """Score inactivity: 0 (active) to 10 (very inactive)."""
                if days < 30:
                    return 0
                elif days < 60:
                    return 3
                elif days < 90:
                    return 6
                elif days < 180:
                    return 8
                else:
                    return 10

            def calculate_decline_score(recent_value, historical_avg):
                """Score decline: 0 (no decline) to 10 (severe decline)."""
                if pd.isna(historical_avg) or historical_avg == 0:
                    return 5  # Neutral score for uncertain cases

                decline_ratio = 1 - (recent_value / historical_avg)
                score = min(max(decline_ratio * 10, 0), 10)
                return score

            # Calculate inactivity score
            churn_df['inactivity_score'] = churn_df['days_inactive'].apply(calculate_inactivity_score)

            # For this heuristic, we'll estimate recent activity (last 30 days)
            # Since we don't have time-series data, we'll use a simplified approach:
            # If inactive > 30 days, recent activity = 0
            # Otherwise, use historical average
            churn_df['recent_orders_estimate'] = churn_df.apply(
                lambda row: 0 if row['days_inactive'] > 30 else row['avg_orders_per_30_days'],
                axis=1
            )
            churn_df['recent_revenue_estimate'] = churn_df.apply(
                lambda row: 0 if row['days_inactive'] > 30 else row['avg_revenue_per_30_days'],
                axis=1
            )

            # Calculate decline scores
            churn_df['activity_decline_score'] = churn_df.apply(
                lambda row: calculate_decline_score(row['recent_orders_estimate'], row['avg_orders_per_30_days']),
                axis=1
            )
            churn_df['value_decline_score'] = churn_df.apply(
                lambda row: calculate_decline_score(row['recent_revenue_estimate'], row['avg_revenue_per_30_days']),
                axis=1
            )

            # Frequency decline (same as activity decline for this heuristic)
            churn_df['frequency_decline_score'] = churn_df['activity_decline_score']

            # Weighted churn score (0-100)
            churn_df['churn_risk_score'] = (
                0.40 * churn_df['inactivity_score'] +
                0.25 * churn_df['activity_decline_score'] +
                0.20 * churn_df['value_decline_score'] +
                0.15 * churn_df['frequency_decline_score']
            ) * 10

            churn_df['churn_risk_score'] = churn_df['churn_risk_score'].round(2)

            # Assign risk levels
            def assign_risk_level(score):
                if score < 25:
                    return 'Low', 'Monitor'
                elif score < 50:
                    return 'Medium', 'Engage with Campaigns'
                elif score < 75:
                    return 'High', 'Personalized Outreach'
                else:
                    return 'Critical', 'Intervene Immediately'

            churn_df[['churn_risk_level', 'recommended_action']] = churn_df['churn_risk_score'].apply(
                lambda x: pd.Series(assign_risk_level(x))
            )

            # Determine activity trend
            def determine_trend(row):
                if row['days_inactive'] < 30:
                    return 'Active'
                elif row['days_inactive'] < 60:
                    return 'Declining'
                elif row['days_inactive'] < 90:
                    return 'At Risk'
                else:
                    return 'Inactive'

            churn_df['activity_trend'] = churn_df.apply(determine_trend, axis=1)

            # Add risk factors if requested
            if include_risk_factors:
                def identify_risk_factors(row):
                    factors = []
                    if row['inactivity_score'] >= 6:
                        factors.append(f"Inactive for {row['days_inactive']} days")
                    if row['activity_decline_score'] >= 6:
                        factors.append("Significant activity decline")
                    if row['value_decline_score'] >= 6:
                        factors.append("Significant revenue decline")
                    if row['total_orders'] == 1:
                        factors.append("Single transaction customer")
                    return ', '.join(factors) if factors else 'None'

                churn_df['risk_factors'] = churn_df.apply(identify_risk_factors, axis=1)

            # Select output columns
            output_cols = [
                'customer_id',
                'days_inactive',
                'activity_trend',
                'churn_risk_score',
                'churn_risk_level',
                'recommended_action'
            ]
            if include_risk_factors:
                output_cols.append('risk_factors')

            result_df = churn_df[output_cols].copy()

            context.log.info(f"Churn prediction complete: {len(result_df)} customers analyzed")

            # Log risk distribution
            risk_distribution = result_df['churn_risk_level'].value_counts()
            for level, count in risk_distribution.items():
                pct = (count / len(result_df)) * 100
                context.log.info(f"  {level} Risk: {count} customers ({pct:.1f}%)")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_customers": len(result_df),
                "avg_churn_risk_score": round(result_df['churn_risk_score'].mean(), 2),
                "inactivity_threshold_days": inactivity_threshold_days,
                "lookback_days": lookback_days,
                "risk_distribution": MetadataValue.md(
                    risk_distribution.to_frame('count').to_markdown()
                )
            }

            # Count high-risk customers
            high_risk_count = len(result_df[result_df['churn_risk_level'].isin(['High', 'Critical'])])
            metadata['high_risk_customers'] = high_risk_count

            # Return with metadata
            if include_sample and len(result_df) > 0:
                # Sort by risk score descending to show highest risk first
                result_sorted = result_df.sort_values('churn_risk_score', ascending=False)

                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_sorted.head(10).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_sorted.head(10))
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


        _schema_checks = build_column_schema_change_checks(assets=[churn_prediction_asset])


        return Definitions(assets=[churn_prediction_asset], asset_checks=list(_schema_checks))
