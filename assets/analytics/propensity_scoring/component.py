"""Propensity Scoring Component.

Calculate propensity scores for various customer actions (purchase, upgrade, churn, refer)
using heuristic scoring based on behavior patterns.
"""

from typing import Any, Dict, List, Optional, Union
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
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

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
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
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
            if include_preview and len(result_df) > 0:
                # Sort by propensity score descending
                result_sorted = result_df.sort_values('propensity_score', ascending=False)

                _prev = result_sorted.sample(preview_rows) if len(result_sorted) > preview_rows * 10 else result_sorted.head(preview_rows)
                metadata['preview'] = MetadataValue.md(_prev.to_markdown(index=False))
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
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return result_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[propensity_scoring_asset])


        return Definitions(assets=[propensity_scoring_asset], asset_checks=list(_schema_checks))
