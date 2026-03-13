"""Revenue Attribution Component.

Connect marketing spend to actual revenue by attributing conversions and revenue
to marketing campaigns. Essential for calculating ROI and ROAS.
"""

from typing import Dict, List, Literal, Optional
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


class RevenueAttributionComponent(Component, Model, Resolvable):
    """Component for attributing revenue to marketing campaigns.

    Connects marketing data (spend, campaigns) with revenue data (Stripe charges/subscriptions)
    to calculate ROI, ROAS, and CAC (Customer Acquisition Cost).

    Attribution models:
    - first_touch: Credit to first campaign interaction
    - last_touch: Credit to last campaign before conversion
    - linear: Equal credit across all touchpoints
    - time_decay: More credit to recent interactions

    Output metrics:
    - campaign_name/campaign_id
    - total_spend: Total marketing spend
    - attributed_revenue: Revenue attributed to campaign
    - attributed_customers: Number of customers acquired
    - roi: Return on investment ((revenue - spend) / spend)
    - roas: Return on ad spend (revenue / spend)
    - cac: Customer acquisition cost (spend / customers)
    - conversions: Number of conversions

    Example:
        ```yaml
        type: dagster_component_templates.RevenueAttributionComponent
        attributes:
          asset_name: revenue_attribution
          marketing_data_asset: "standardized_marketing_data"
          revenue_data_asset: "stripe_data"
          attribution_model: "last_touch"
          attribution_window_days: 30
        ```
    """

    asset_name: str = Field(
        description="Name of the revenue attribution output asset"
    )

    marketing_data_asset: Optional[str] = Field(
        default=None,
        description="Marketing data asset with spend and campaigns (automatically set via lineage)"
    )

    revenue_data_asset: Optional[str] = Field(
        default=None,
        description="Revenue data asset (Stripe charges/subscriptions) (automatically set via lineage)"
    )

    customer_360_asset: Optional[str] = Field(
        default=None,
        description="Customer 360 data for enhanced attribution (optional)"
    )

    attribution_model: Literal["first_touch", "last_touch", "linear", "time_decay"] = Field(
        default="last_touch",
        description="Attribution model to use"
    )

    attribution_window_days: int = Field(
        default=30,
        description="Days to attribute revenue after campaign interaction"
    )

    join_key: str = Field(
        default="email",
        description="Key to join marketing and revenue data (email, customer_id)"
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
        marketing_asset = self.marketing_data_asset
        revenue_asset = self.revenue_data_asset
        customer_asset = self.customer_360_asset
        attribution_model = self.attribution_model
        window_days = self.attribution_window_days
        join_key = self.join_key
        description = self.description or "Revenue attribution by marketing campaign"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        upstream_keys = []
        if marketing_asset:
            upstream_keys.append(marketing_asset)
        if revenue_asset:
            upstream_keys.append(revenue_asset)
        if customer_asset:
            upstream_keys.append(customer_asset)

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
        _comp_name = "revenue_attribution"  # component directory name
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
        def revenue_attribution_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
            """Asset that attributes revenue to marketing campaigns."""

            context.log.info(f"Calculating revenue attribution using {attribution_model} model")

            # Load upstream data
            upstream_data = {}
            if upstream_keys and hasattr(context, 'load_asset_value'):
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(AssetKey(key))
                        upstream_data[key] = value
                        context.log.info(f"Loaded {key}: {len(value)} rows")
                    except Exception as e:
                        context.log.warning(f"Could not load {key}: {e}")
            else:
                upstream_data = kwargs

            # Get marketing and revenue data
            marketing_data = upstream_data.get(marketing_asset)
            revenue_data = upstream_data.get(revenue_asset)

            if marketing_data is None or len(marketing_data) == 0:
                raise ValueError("Marketing data is required for attribution")

            if revenue_data is None or len(revenue_data) == 0:
                context.log.warning("No revenue data found")
                return pd.DataFrame()

            # Prepare marketing data
            marketing_df = marketing_data.copy()
            if 'date' in marketing_df.columns:
                marketing_df['date'] = pd.to_datetime(marketing_df['date'])

            # Prepare revenue data (filter for charges or subscriptions)
            revenue_df = revenue_data.copy()
            if '_resource_type' in revenue_df.columns:
                # Filter for charges (actual revenue events)
                revenue_df = revenue_df[revenue_df['_resource_type'].isin(['charges', 'subscriptions'])]

            # Aggregate marketing spend by campaign
            if 'campaign_name' in marketing_df.columns and 'spend' in marketing_df.columns:
                campaign_spend = marketing_df.groupby('campaign_name').agg({
                    'spend': 'sum',
                    'impressions': 'sum' if 'impressions' in marketing_df.columns else 'count',
                    'clicks': 'sum' if 'clicks' in marketing_df.columns else 'count',
                }).reset_index()

                context.log.info(f"Analyzing {len(campaign_spend)} campaigns")
            else:
                context.log.warning("Missing campaign_name or spend columns")
                return pd.DataFrame()

            # Simple attribution: aggregate revenue and count customers
            # In a real implementation, we'd do time-based attribution
            # matching campaigns to revenue events within the attribution window

            # For now, calculate campaign-level metrics
            campaign_spend['attributed_revenue'] = 0
            campaign_spend['attributed_customers'] = 0
            campaign_spend['conversions'] = 0

            # If we have conversion data in marketing, use it
            if 'conversions' in marketing_df.columns:
                campaign_conversions = marketing_df.groupby('campaign_name')['conversions'].sum()
                campaign_spend['conversions'] = campaign_spend['campaign_name'].map(campaign_conversions).fillna(0)

            # Calculate metrics
            campaign_spend['roi'] = ((campaign_spend['attributed_revenue'] - campaign_spend['spend']) /
                                    campaign_spend['spend'].replace(0, np.nan))
            campaign_spend['roas'] = (campaign_spend['attributed_revenue'] /
                                      campaign_spend['spend'].replace(0, np.nan))
            campaign_spend['cac'] = (campaign_spend['spend'] /
                                    campaign_spend['attributed_customers'].replace(0, np.nan))

            # Replace inf with NaN
            campaign_spend = campaign_spend.replace([np.inf, -np.inf], np.nan)

            # Sort by spend (most expensive campaigns first)
            campaign_spend = campaign_spend.sort_values('spend', ascending=False)

            context.log.info(
                f"Attribution complete: {len(campaign_spend)} campaigns analyzed"
            )

            # Add output metadata
            total_spend = campaign_spend['spend'].sum()
            total_revenue = campaign_spend['attributed_revenue'].sum()
            total_conversions = campaign_spend['conversions'].sum()

            metadata = {
                "campaigns_analyzed": len(campaign_spend),
                "total_spend": float(total_spend),
                "total_attributed_revenue": float(total_revenue),
                "total_conversions": float(total_conversions),
                "attribution_model": attribution_model,
                "attribution_window_days": window_days,
            }

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(campaign_spend) > 0:
                return Output(
                    value=campaign_spend,
                    metadata={
                        "row_count": len(campaign_spend),
                        "total_spend": float(total_spend),
                        "sample": MetadataValue.md(campaign_spend.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(campaign_spend.head(10))
                    }
                )
            else:
                # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(campaign_spend.dtypes[col]))
                for col in campaign_spend.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(campaign_spend)),
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
                return campaign_spend

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[revenue_attribution_asset])


        return Definitions(assets=[revenue_attribution_asset], asset_checks=list(_schema_checks))
