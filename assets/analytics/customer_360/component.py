"""Customer 360 Component.

Unify customer data from multiple sources (marketing, CRM, payments, analytics) into
a single comprehensive customer profile. Core component for CDP functionality.
"""

from typing import Dict, List, Optional
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


class Customer360Component(Component, Model, Resolvable):
    """Component for creating unified customer profiles (Customer 360 view).

    This component unifies customer data from multiple sources into a single
    comprehensive customer profile, providing a complete view of each customer's
    interactions, transactions, and behavior across all touchpoints.

    Input sources (via visual connections):
    - Stripe customers: Payment and subscription data
    - Marketing data: Campaign interactions, conversions
    - GA4 data: Website behavior, sessions
    - CRM data: Contact information, interactions

    Output fields:
    - customer_id: Unified customer identifier
    - email: Primary email address
    - first_name, last_name: Customer name
    - created_at: First seen date
    - total_revenue: Lifetime revenue
    - total_orders: Number of transactions
    - avg_order_value: Average transaction size
    - total_sessions: Website sessions
    - total_page_views: Page views
    - acquisition_source: First touch source
    - acquisition_medium: First touch medium
    - acquisition_campaign: First touch campaign
    - last_interaction_date: Most recent activity
    - customer_lifetime_days: Days since first interaction
    - is_active: Active in last 30 days

    Example:
        ```yaml
        type: dagster_component_templates.Customer360Component
        attributes:
          asset_name: customer_360
          stripe_customers_asset: "stripe_data"
          marketing_data_asset: "standardized_marketing_data"
          ga4_data_asset: "google_analytics_data"
          join_key: "email"
        ```
    """

    asset_name: str = Field(
        description="Name of the unified customer profile asset"
    )

    # Input assets (set via visual lineage)
    stripe_customers_asset: Optional[str] = Field(
        default=None,
        description="Stripe customers data asset (automatically set via lineage)"
    )

    marketing_data_asset: Optional[str] = Field(
        default=None,
        description="Marketing data asset (automatically set via lineage)"
    )

    ga4_data_asset: Optional[str] = Field(
        default=None,
        description="Google Analytics data asset (automatically set via lineage)"
    )

    crm_data_asset: Optional[str] = Field(
        default=None,
        description="CRM data asset (automatically set via lineage)"
    )

    # Join configuration
    join_key: str = Field(
        default="email",
        description="Primary key for joining customer data across sources (email, user_id, customer_id)"
    )

    secondary_join_keys: Optional[str] = Field(
        default=None,
        description="Additional join keys (comma-separated). E.g., 'user_id,phone'"
    )

    # Activity window
    active_days_threshold: int = Field(
        default=30,
        description="Days to consider a customer active (default: 30)"
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
        stripe_asset = self.stripe_customers_asset
        marketing_asset = self.marketing_data_asset
        ga4_asset = self.ga4_data_asset
        crm_asset = self.crm_data_asset
        join_key = self.join_key
        secondary_keys_str = self.secondary_join_keys
        active_threshold = self.active_days_threshold
        description = self.description or "Unified customer 360 profiles"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Build dependency list
        upstream_keys = []
        if stripe_asset:
            upstream_keys.append(stripe_asset)
        if marketing_asset:
            upstream_keys.append(marketing_asset)
        if ga4_asset:
            upstream_keys.append(ga4_asset)
        if crm_asset:
            upstream_keys.append(crm_asset)

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
        _comp_name = "customer_360"  # component directory name
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
        def customer_360_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
            """Asset that creates unified customer profiles from multiple sources."""

            context.log.info("Building Customer 360 unified profiles")

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

            if not upstream_data:
                raise ValueError(
                    f"Customer 360 '{asset_name}' requires at least one upstream data source. "
                    "Connect Stripe customers, marketing data, GA4 data, or CRM data."
                )

            context.log.info(f"Processing {len(upstream_data)} data sources")

            # Parse secondary join keys
            secondary_keys = []
            if secondary_keys_str:
                secondary_keys = [k.strip() for k in secondary_keys_str.split(',')]

            all_join_keys = [join_key] + secondary_keys

            # Initialize base customer DataFrame
            customers = pd.DataFrame()

            # Process Stripe data (payment/subscription info)
            stripe_data = upstream_data.get(stripe_asset)
            if stripe_data is not None and len(stripe_data) > 0:
                context.log.info("Processing Stripe customer data...")

                # Filter for customers resource if multiple resources present
                if '_resource_type' in stripe_data.columns:
                    stripe_customers = stripe_data[stripe_data['_resource_type'] == 'customers'].copy()
                else:
                    stripe_customers = stripe_data.copy()

                if len(stripe_customers) > 0:
                    # Extract key customer fields
                    stripe_profile = stripe_customers[[col for col in stripe_customers.columns if col in [
                        'id', 'email', 'name', 'created', 'description'
                    ]]].copy()

                    stripe_profile.rename(columns={
                        'id': 'stripe_customer_id',
                        'created': 'stripe_created_at'
                    }, inplace=True)

                    customers = stripe_profile
                    context.log.info(f"Added {len(customers)} Stripe customers")

            # Process marketing data (acquisition, campaigns)
            marketing_data = upstream_data.get(marketing_asset)
            if marketing_data is not None and len(marketing_data) > 0:
                context.log.info("Processing marketing data...")

                # Aggregate marketing data by customer
                # Assuming standardized schema with: email, campaign_name, spend, conversions, date
                if 'email' in marketing_data.columns or join_key in marketing_data.columns:
                    key_col = join_key if join_key in marketing_data.columns else 'email'

                    # Get first touch attribution (earliest campaign interaction)
                    if 'date' in marketing_data.columns:
                        marketing_data['date'] = pd.to_datetime(marketing_data['date'])
                        first_touch = marketing_data.sort_values('date').groupby(key_col).first().reset_index()

                        first_touch = first_touch[[col for col in first_touch.columns if col in [
                            key_col, 'campaign_name', 'platform', 'date'
                        ]]].copy()

                        first_touch.rename(columns={
                            'campaign_name': 'acquisition_campaign',
                            'platform': 'acquisition_source',
                            'date': 'first_interaction_date'
                        }, inplace=True)

                        # Merge with customers
                        if len(customers) == 0:
                            customers = first_touch
                        else:
                            customers = customers.merge(first_touch, on=key_col, how='outer')

                        context.log.info(f"Added marketing attribution for {len(first_touch)} customers")

            # Process GA4 data (website behavior)
            ga4_data = upstream_data.get(ga4_asset)
            if ga4_data is not None and len(ga4_data) > 0:
                context.log.info("Processing GA4 data...")

                # Aggregate GA4 metrics by user
                # Assuming fields: user_id, sessions, screenPageViews, date
                if 'user_id' in ga4_data.columns or join_key in ga4_data.columns:
                    key_col = join_key if join_key in ga4_data.columns else 'user_id'

                    ga4_agg = ga4_data.groupby(key_col).agg({
                        'sessions': 'sum' if 'sessions' in ga4_data.columns else 'count',
                        'screenPageViews': 'sum' if 'screenPageViews' in ga4_data.columns else 'count',
                    }).reset_index()

                    ga4_agg.rename(columns={
                        'sessions': 'total_sessions',
                        'screenPageViews': 'total_page_views'
                    }, inplace=True)

                    # Merge with customers
                    if len(customers) == 0:
                        customers = ga4_agg
                    else:
                        customers = customers.merge(ga4_agg, on=key_col, how='outer')

                    context.log.info(f"Added GA4 metrics for {len(ga4_agg)} users")

            # Process CRM data if available
            crm_data = upstream_data.get(crm_asset)
            if crm_data is not None and len(crm_data) > 0:
                context.log.info("Processing CRM data...")
                # Add CRM fields as needed
                # This is a placeholder for future CRM integration

            # Ensure we have customers
            if len(customers) == 0:
                context.log.warning("No customer data found")
                return pd.DataFrame()

            # Create unified customer ID
            customers['customer_id'] = customers.index + 1

            # Calculate derived metrics
            # Active status (if we have interaction dates)
            if 'first_interaction_date' in customers.columns:
                customers['first_interaction_date'] = pd.to_datetime(customers['first_interaction_date'])
                customers['customer_lifetime_days'] = (pd.Timestamp.now() - customers['first_interaction_date']).dt.days
                customers['is_active'] = customers['customer_lifetime_days'] <= active_threshold
            else:
                customers['is_active'] = True

            # Clean up and standardize
            # Ensure email is clean
            if 'email' in customers.columns:
                customers['email'] = customers['email'].str.lower().str.strip()

            # Sort by most valuable customers (if we have revenue data)
            if 'total_revenue' in customers.columns:
                customers = customers.sort_values('total_revenue', ascending=False)
            elif 'total_sessions' in customers.columns:
                customers = customers.sort_values('total_sessions', ascending=False)

            context.log.info(
                f"Created {len(customers)} unified customer profiles with "
                f"{len(customers.columns)} attributes"
            )

            # Calculate summary statistics
            active_customers = customers['is_active'].sum() if 'is_active' in customers.columns else 0
            total_revenue = customers['total_revenue'].sum() if 'total_revenue' in customers.columns else 0

            # Add output metadata
            metadata = {
                "total_customers": len(customers),
                "active_customers": int(active_customers),
                "total_revenue": float(total_revenue),
                "data_sources": len(upstream_data),
                "attributes": len(customers.columns),
                "columns": list(customers.columns),
            }

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(customers) > 0:
                return Output(
                    value=customers,
                    metadata={
                        "row_count": len(customers),
                        "column_count": len(customers.columns),
                        "active_customers": int(active_customers),
                        "sample": MetadataValue.md(customers.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(customers.head(10))
                    }
                )
            else:
                # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(customers.dtypes[col]))
                for col in customers.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(customers)),
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
                return customers

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[customer_360_asset])


        return Definitions(assets=[customer_360_asset], asset_checks=list(_schema_checks))
