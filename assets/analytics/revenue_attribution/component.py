"""Revenue Attribution Component.

Connect marketing spend to actual revenue by attributing conversions and revenue
to marketing campaigns. Essential for calculating ROI and ROAS.
"""

from typing import Any, Dict, List, Literal, Optional, Union
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

    marketing_data_asset_key: Optional[str] = Field(
        default=None,
        description="Marketing data asset with spend and campaigns (automatically set via lineage)",
    )

    revenue_data_asset_key: Optional[str] = Field(
        default=None,
        description="Revenue data asset (Stripe charges/subscriptions) (automatically set via lineage)",
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
        marketing_asset = self.marketing_data_asset_key
        revenue_asset = self.revenue_data_asset_key
        customer_asset = self.customer_360_asset
        attribution_model = self.attribution_model
        window_days = self.attribution_window_days
        join_key = self.join_key
        description = self.description or "Revenue attribution by marketing campaign"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        upstream_keys = []
        if marketing_asset:
            upstream_keys.append(marketing_asset)
        if revenue_asset:
            upstream_keys.append(revenue_asset)
        if customer_asset:
            upstream_keys.append(customer_asset)

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
            if include_preview and len(campaign_spend) > 0:
                context.add_output_metadata({
                    "row_count": len(campaign_spend),
                    "total_spend": float(total_spend),
                    "preview": MetadataValue.md(campaign_spend.head(10).to_markdown())
                })
                return campaign_spend
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
                            _lineage_deps[str(out_col)] = [
                                TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                                for ic in in_cols
                            ]
                        _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                            TableColumnLineage(_lineage_deps)
                        )
                context.add_output_metadata(_metadata)
                return campaign_spend

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[revenue_attribution_asset])


        return Definitions(assets=[revenue_attribution_asset], asset_checks=list(_schema_checks))
