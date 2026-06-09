"""Campaign Performance Analytics Component.

Analyze marketing campaign performance across channels, calculating ROI, conversion rates,
and cost-per-acquisition metrics.
"""

from typing import Any, Dict, List, Optional
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


class CampaignPerformanceComponent(Component, Model, Resolvable):
    """Component for analyzing marketing campaign performance.

    Calculates key campaign metrics including:
    - Return on Ad Spend (ROAS)
    - Cost Per Acquisition (CPA)
    - Conversion Rate
    - Click-Through Rate (CTR)
    - Cost Per Click (CPC)
    - Revenue attribution by campaign

    Example:
        ```yaml
        type: dagster_component_templates.CampaignPerformanceComponent
        attributes:
          asset_name: campaign_performance
          upstream_asset_key: campaign_metrics
          target_roas: 3.0
          description: "Campaign performance analysis"
          group_name: marketing_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with campaign metrics (impressions, clicks, spend)"
    )

    target_roas: Optional[float] = Field(
        default=None,
        description="Target ROAS for performance comparison (optional)"
    )

    target_cpa: Optional[float] = Field(
        default=None,
        description="Target CPA for performance comparison (optional)"
    )

    include_benchmarks: bool = Field(
        default=True,
        description="Include industry benchmark comparisons"
    )

    campaign_id_field: Optional[str] = Field(
        default=None,
        description="Campaign ID column (auto-detected)"
    )

    campaign_name_field: Optional[str] = Field(
        default=None,
        description="Campaign name column (auto-detected)"
    )

    channel_field: Optional[str] = Field(
        default=None,
        description="Marketing channel column (auto-detected)"
    )

    spend_field: Optional[str] = Field(
        default=None,
        description="Campaign spend column (auto-detected)"
    )

    impressions_field: Optional[str] = Field(
        default=None,
        description="Impressions column (auto-detected)"
    )

    clicks_field: Optional[str] = Field(
        default=None,
        description="Clicks column (auto-detected)"
    )

    conversions_field: Optional[str] = Field(
        default=None,
        description="Conversions column (auto-detected)"
    )

    revenue_field: Optional[str] = Field(
        default=None,
        description="Revenue column (auto-detected)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="marketing_analytics",
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
        upstream_asset_key = self.upstream_asset_key
        target_roas = self.target_roas
        target_cpa = self.target_cpa
        include_benchmarks = self.include_benchmarks
        campaign_id_field = self.campaign_id_field
        campaign_name_field = self.campaign_name_field
        channel_field = self.channel_field
        spend_field = self.spend_field
        impressions_field = self.impressions_field
        clicks_field = self.clicks_field
        conversions_field = self.conversions_field
        revenue_field = self.revenue_field
        description = self.description or "Campaign performance analytics"
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
        _comp_name = "campaign_performance"  # component directory name
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
        def campaign_performance_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
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
            """Asset that analyzes campaign performance."""

            campaign_df = upstream
            if not isinstance(campaign_df, pd.DataFrame):
                context.log.error("Campaign data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(campaign_df)} campaign records")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in campaign_df.columns:
                    return custom_name
                for name in possible_names:
                    if name in campaign_df.columns:
                        return name
                return None

            campaign_id_col = find_column(
                ['campaign_id', 'campaignId', 'id', 'campaign'],
                campaign_id_field
            )
            campaign_name_col = find_column(
                ['campaign_name', 'name', 'campaignName', 'campaign'],
                campaign_name_field
            )
            channel_col = find_column(
                ['channel', 'source', 'marketing_channel', 'platform'],
                channel_field
            )
            spend_col = find_column(
                ['spend', 'cost', 'budget', 'amount'],
                spend_field
            )
            impressions_col = find_column(
                ['impressions', 'views', 'reach'],
                impressions_field
            )
            clicks_col = find_column(
                ['clicks', 'link_clicks', 'clicks_total'],
                clicks_field
            )
            conversions_col = find_column(
                ['conversions', 'purchases', 'leads', 'conversion_count'],
                conversions_field
            )
            revenue_col = find_column(
                ['revenue', 'sales', 'conversion_value', 'total_revenue'],
                revenue_field
            )

            # Validate required columns
            missing = []
            if not campaign_id_col and not campaign_name_col:
                missing.append("campaign_id or campaign_name")
            if not spend_col:
                missing.append("spend/cost")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(campaign_df.columns)}")
                return pd.DataFrame()

            # Use campaign_id or campaign_name as identifier
            campaign_key = campaign_id_col if campaign_id_col else campaign_name_col
            context.log.info(f"Using campaign key: {campaign_key}")

            # Prepare data
            cols_to_use = [campaign_key, spend_col]
            col_names = ['campaign_id', 'spend']

            optional_cols = [
                (campaign_name_col, 'campaign_name'),
                (channel_col, 'channel'),
                (impressions_col, 'impressions'),
                (clicks_col, 'clicks'),
                (conversions_col, 'conversions'),
                (revenue_col, 'revenue')
            ]

            for col, name in optional_cols:
                if col:
                    cols_to_use.append(col)
                    col_names.append(name)

            performance_df = campaign_df[cols_to_use].copy()
            performance_df.columns = col_names

            # Convert numeric columns
            numeric_cols = ['spend', 'impressions', 'clicks', 'conversions', 'revenue']
            for col in numeric_cols:
                if col in performance_df.columns:
                    performance_df[col] = pd.to_numeric(performance_df[col], errors='coerce').fillna(0)

            # Calculate metrics
            context.log.info("Calculating performance metrics...")

            # Click-Through Rate (CTR)
            if 'impressions' in performance_df.columns and 'clicks' in performance_df.columns:
                performance_df['ctr'] = (performance_df['clicks'] / performance_df['impressions'] * 100).round(2)
                performance_df['ctr'] = performance_df['ctr'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Cost Per Click (CPC)
            if 'clicks' in performance_df.columns:
                performance_df['cpc'] = (performance_df['spend'] / performance_df['clicks']).round(2)
                performance_df['cpc'] = performance_df['cpc'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Conversion Rate
            if 'clicks' in performance_df.columns and 'conversions' in performance_df.columns:
                performance_df['conversion_rate'] = (performance_df['conversions'] / performance_df['clicks'] * 100).round(2)
                performance_df['conversion_rate'] = performance_df['conversion_rate'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Cost Per Acquisition (CPA)
            if 'conversions' in performance_df.columns:
                performance_df['cpa'] = (performance_df['spend'] / performance_df['conversions']).round(2)
                performance_df['cpa'] = performance_df['cpa'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Return on Ad Spend (ROAS)
            if 'revenue' in performance_df.columns:
                performance_df['roas'] = (performance_df['revenue'] / performance_df['spend']).round(2)
                performance_df['roas'] = performance_df['roas'].replace([np.inf, -np.inf], np.nan).fillna(0)

                # ROI (percentage)
                performance_df['roi'] = ((performance_df['revenue'] - performance_df['spend']) / performance_df['spend'] * 100).round(2)
                performance_df['roi'] = performance_df['roi'].replace([np.inf, -np.inf], np.nan).fillna(0)

            # Performance scoring
            if target_roas and 'roas' in performance_df.columns:
                performance_df['roas_vs_target'] = ((performance_df['roas'] / target_roas - 1) * 100).round(2)
                performance_df['roas_performance'] = performance_df['roas'].apply(
                    lambda x: 'Excellent' if x >= target_roas * 1.2 else (
                        'Good' if x >= target_roas else (
                            'Below Target' if x >= target_roas * 0.8 else 'Poor'
                        )
                    )
                )

            if target_cpa and 'cpa' in performance_df.columns:
                performance_df['cpa_vs_target'] = ((performance_df['cpa'] / target_cpa - 1) * 100).round(2)
                performance_df['cpa_performance'] = performance_df['cpa'].apply(
                    lambda x: 'Excellent' if x <= target_cpa * 0.8 else (
                        'Good' if x <= target_cpa else (
                            'Above Target' if x <= target_cpa * 1.2 else 'Poor'
                        )
                    )
                )

            # Sort by performance (ROAS or ROI if available, otherwise by spend)
            if 'roas' in performance_df.columns:
                performance_df = performance_df.sort_values('roas', ascending=False)
            elif 'roi' in performance_df.columns:
                performance_df = performance_df.sort_values('roi', ascending=False)
            else:
                performance_df = performance_df.sort_values('spend', ascending=False)

            context.log.info(f"Performance analysis complete: {len(performance_df)} campaigns analyzed")

            # Log summary statistics
            total_spend = performance_df['spend'].sum()
            context.log.info(f"\nCampaign Summary:")
            context.log.info(f"  Total Campaigns: {len(performance_df)}")
            context.log.info(f"  Total Spend: ${total_spend:,.2f}")

            if 'revenue' in performance_df.columns:
                total_revenue = performance_df['revenue'].sum()
                overall_roas = total_revenue / total_spend if total_spend > 0 else 0
                context.log.info(f"  Total Revenue: ${total_revenue:,.2f}")
                context.log.info(f"  Overall ROAS: {overall_roas:.2f}x")

            if 'conversions' in performance_df.columns:
                total_conversions = performance_df['conversions'].sum()
                overall_cpa = total_spend / total_conversions if total_conversions > 0 else 0
                context.log.info(f"  Total Conversions: {total_conversions:,.0f}")
                context.log.info(f"  Overall CPA: ${overall_cpa:.2f}")

            # Log top/bottom performers
            if len(performance_df) >= 3:
                context.log.info(f"\nTop 3 Campaigns:")
                for idx, row in performance_df.head(3).iterrows():
                    campaign_name = row.get('campaign_name', row['campaign_id'])
                    if 'roas' in row:
                        context.log.info(f"  {campaign_name}: ROAS {row['roas']}x, Spend ${row['spend']:,.2f}")
                    else:
                        context.log.info(f"  {campaign_name}: Spend ${row['spend']:,.2f}")

            # Add metadata (coerce numpy types → native Python for serialization)
            metadata = {
                "row_count": len(performance_df),
                "total_campaigns": len(performance_df),
                "total_spend": float(round(total_spend, 2)),
            }

            if 'revenue' in performance_df.columns:
                metadata['total_revenue'] = float(round(total_revenue, 2))
                metadata['overall_roas'] = float(round(overall_roas, 2))

            if 'conversions' in performance_df.columns:
                metadata['total_conversions'] = int(total_conversions)
                metadata['overall_cpa'] = float(round(overall_cpa, 2))

            if target_roas:
                metadata['target_roas'] = target_roas

            if target_cpa:
                metadata['target_cpa'] = target_cpa

            # Return with metadata
            if include_preview and len(performance_df) > 0:
                try:
                    _prev = performance_df.sample(min(preview_rows, len(performance_df))) if len(performance_df) > preview_rows * 10 else performance_df.head(preview_rows)
                    metadata['preview'] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(metadata)
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(performance_df.dtypes[col]))
                for col in performance_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(performance_df)),
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
            return performance_df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[campaign_performance_asset])


        return Definitions(assets=[campaign_performance_asset], asset_checks=list(_schema_checks))
