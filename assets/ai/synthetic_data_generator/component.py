"""Synthetic Data Generator Asset Component."""

from typing import Any, Dict, List, Literal, Optional
import pandas as pd
from datetime import datetime, timedelta
import random
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
    asset,
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
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
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


class SyntheticDataGeneratorComponent(Component, Model, Resolvable):
    """
    Component for generating synthetic/mock data for demos and testing.

    This component creates realistic fake data based on pre-defined schemas.
    Perfect for demonstrations, testing pipelines, and prototyping without
    needing external data sources.

    Supports multiple data schemas:
    - customers: Customer profiles with contact info
    - orders: E-commerce orders with products and amounts
    - products: Product catalog with pricing
    - transactions: Financial transactions
    - events: Event logs with timestamps
    - sensors: IoT sensor readings
    - users: User accounts with activity data
    """

    asset_name: str = Field(description="Name of the asset")

    schema_type: Literal[
        "customers",
        "orders",
        "products",
        "transactions",
        "ab_experiment",
        "events",
        "sensors",
        "users",
        "subscriptions",
        "sparse_sensors",
        "customer_churn_metrics",
        "stripe_charges",
        "stripe_subscriptions",
        "support_tickets",
        "product_reviews",
        "audio_samples",
        "image_prompts",
        "employees",
        "fhir_patients",
        "hl7_messages",
        "iso20022_payments",
        "x12_messages",
        "fix_messages",
    ] = Field(
        default="customers",
        description="Type of data schema to generate"
    )

    row_count: int = Field(
        default=100,
        description="Number of rows to generate",
        ge=1,
        le=100000
    )

    random_state: Optional[int] = Field(
        default=None,
        description="Random seed for reproducible data generation (leave empty for random)",
    )

    schema_options: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Per-schema knobs. Keys recognized by specific schemas:\n"
            "  subscriptions:\n"
            "    tiers: list of {name, weight, daily_churn_rate, max_days}\n"
            "      defaults: free (55%, 2%/day, 180d), pro (35%, 0.5%/day, 365d), enterprise (10%, 0.1%/day, 730d)\n"
            "  sparse_sensors:\n"
            "    sensor_count: int (default 3)\n"
            "    duration_hours: int (default 336 = 14 days)\n"
            "    dropout_rate: float in [0,1] (default 0.25)\n"
            "    base_temp: float (default 22.0)\n"
            "    noise_amplitude: float (default 2.0)\n"
            "    start_date: 'YYYY-MM-DD' (default '2026-04-01')"
        ),
    )

    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )

    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
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
        default=False,
        description="Include a preview of the output data in metadata (first 5 rows as markdown table). Used by builder UIs to render asset shape without warehouse access."
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

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

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


    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the synthetic data generator."""

        # Capture fields for closure
        asset_name = self.asset_name
        schema_type = self.schema_type
        row_count = self.row_count
        random_seed = self.random_state
        description = self.description or f"Synthetic {schema_type} data"
        group_name = self.group_name or None
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
        _comp_name = "synthetic_data_generator"  # component directory name
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
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def synthetic_data_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Generate synthetic data based on schema type."""

            # Set random seed if provided
            if random_seed is not None:
                random.seed(random_seed)

            # Check if running in partitioned mode
            target_date = None
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    target_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    context.log.info(f"Generating {row_count} rows of {schema_type} data for partition {context.partition_key}")
                except ValueError:
                    context.log.warning(f"Could not parse partition key '{context.partition_key}' as date, using current date")
                    target_date = None
            else:
                context.log.info(f"Generating {row_count} rows of {schema_type} data (non-partitioned)")

            # Generate data based on schema type
            if schema_type == "customers":
                df = _generate_customers(row_count, target_date)
            elif schema_type == "orders":
                df = _generate_orders(row_count, target_date)
            elif schema_type == "products":
                df = _generate_products(row_count)
            elif schema_type == "transactions":
                df = _generate_transactions(row_count, target_date)
            elif schema_type == "events":
                df = _generate_events(row_count, target_date)
            elif schema_type == "sensors":
                df = _generate_sensors(row_count, target_date)
            elif schema_type == "users":
                df = _generate_users(row_count, target_date)
            elif schema_type == "subscriptions":
                df = _generate_subscriptions(row_count, self.schema_options or {})
            elif schema_type == "sparse_sensors":
                df = _generate_sparse_sensors(row_count, self.schema_options or {})
            elif schema_type == "customer_churn_metrics":
                df = _generate_customer_churn_metrics(row_count, self.schema_options or {})
            elif schema_type == "stripe_charges":
                df = _generate_stripe_charges(row_count, self.schema_options or {})
            elif schema_type == "stripe_subscriptions":
                df = _generate_stripe_subscriptions(row_count, self.schema_options or {})
            elif schema_type == "ab_experiment":
                df = _generate_ab_experiment(row_count, self.schema_options or {})
            elif schema_type == "support_tickets":
                df = _generate_support_tickets(row_count, self.schema_options or {})
            elif schema_type == "product_reviews":
                df = _generate_product_reviews(row_count, self.schema_options or {})
            elif schema_type == "audio_samples":
                df = _generate_audio_samples(row_count, self.schema_options or {})
            elif schema_type == "image_prompts":
                df = _generate_image_prompts(row_count, self.schema_options or {})
            elif schema_type == "employees":
                df = _generate_employees(row_count, self.schema_options or {})
            elif schema_type == "fhir_patients":
                df = _generate_fhir_patients(row_count, self.schema_options or {})
            elif schema_type == "hl7_messages":
                df = _generate_hl7_messages(row_count, self.schema_options or {})
            elif schema_type == "iso20022_payments":
                df = _generate_iso20022_payments(row_count, self.schema_options or {})
            elif schema_type == "x12_messages":
                df = _generate_x12_messages(row_count, self.schema_options or {})
            elif schema_type == "fix_messages":
                df = _generate_fix_messages(row_count, self.schema_options or {})
            else:
                raise ValueError(f"Unknown schema type: {schema_type}")

            context.log.info(f"Generated DataFrame with shape {df.shape}")

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if 'upstream_asset_key' in dir() else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[synthetic_data_asset])


        return Definitions(assets=[synthetic_data_asset], asset_checks=list(_schema_checks))


def _generate_customers(n: int, target_date: Optional[datetime] = None) -> pd.DataFrame:
    """Generate customer data.

    Args:
        n: Number of customers to generate
        target_date: If provided, all customers will have signup_date on this date
    """
    first_names = ["John", "Jane", "Michael", "Emily", "David", "Sarah", "James", "Emma", "Robert", "Olivia"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA"]

    data = []
    for i in range(n):
        customer_id = f"CUST{i+1:06d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@example.com"
        phone = f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        city_idx = random.randint(0, len(cities) - 1)

        # Use target_date if partitioned, otherwise random date
        if target_date:
            signup_date = target_date
        else:
            signup_date = datetime.now() - timedelta(days=random.randint(1, 730))

        data.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": phone,
            "city": cities[city_idx],
            "state": states[city_idx],
            "signup_date": signup_date.strftime("%Y-%m-%d"),
            "lifetime_value": round(random.uniform(100, 10000), 2),
            "is_active": random.choice([True, True, True, False])  # 75% active
        })

    return pd.DataFrame(data)


def _generate_orders(n: int, target_date: Optional[datetime] = None) -> pd.DataFrame:
    """Generate order data.

    Args:
        n: Number of orders to generate
        target_date: If provided, all orders will be placed on this date
    """
    product_categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food"]
    statuses = ["pending", "shipped", "delivered", "cancelled"]

    data = []
    for i in range(n):
        order_id = f"ORD{i+1:08d}"
        customer_id = f"CUST{random.randint(1, 1000):06d}"

        # Use target_date if partitioned, otherwise random date
        if target_date:
            # Add random hours/minutes within the day
            order_date = target_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
        else:
            order_date = datetime.now() - timedelta(days=random.randint(0, 365))

        num_items = random.randint(1, 5)
        item_total = sum(random.uniform(10, 200) for _ in range(num_items))
        shipping = random.choice([0, 5.99, 9.99, 14.99])
        tax = item_total * 0.08

        data.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "category": random.choice(product_categories),
            "num_items": num_items,
            "subtotal": round(item_total, 2),
            "shipping": shipping,
            "tax": round(tax, 2),
            "total": round(item_total + shipping + tax, 2),
            "status": random.choice(statuses)
        })

    return pd.DataFrame(data)


def _generate_products(n: int) -> pd.DataFrame:
    """Generate product catalog data."""
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food"]
    adjectives = ["Premium", "Deluxe", "Classic", "Modern", "Vintage", "Pro", "Essential", "Ultimate"]
    nouns = ["Widget", "Gadget", "Tool", "Device", "Kit", "Set", "Bundle", "Collection"]

    data = []
    for i in range(n):
        product_id = f"PROD{i+1:06d}"
        category = random.choice(categories)
        name = f"{random.choice(adjectives)} {random.choice(nouns)}"
        price = round(random.uniform(9.99, 999.99), 2)
        cost = round(price * random.uniform(0.4, 0.7), 2)

        data.append({
            "product_id": product_id,
            "name": name,
            "category": category,
            "price": price,
            "cost": cost,
            "margin_pct": round(((price - cost) / price) * 100, 1),
            "stock_quantity": random.randint(0, 500),
            "rating": round(random.uniform(3.0, 5.0), 1),
            "num_reviews": random.randint(0, 1000),
            "is_available": random.choice([True, True, True, False])
        })

    return pd.DataFrame(data)


def _generate_transactions(n: int, target_date: Optional[datetime] = None) -> pd.DataFrame:
    """Generate financial transaction data.

    Args:
        n: Number of transactions to generate
        target_date: If provided, all transactions will occur on this date
    """
    transaction_types = ["deposit", "withdrawal", "transfer", "payment", "refund"]
    merchants = ["Amazon", "Walmart", "Target", "Starbucks", "Shell", "Uber", "Netflix", "Apple"]

    data = []
    for i in range(n):
        transaction_id = f"TXN{i+1:010d}"

        # Use target_date if partitioned, otherwise random date in last 90 days
        if target_date:
            timestamp = target_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
        else:
            timestamp = datetime.now() - timedelta(
                days=random.randint(0, 90),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )

        txn_type = random.choice(transaction_types)
        amount = round(random.uniform(5, 1000), 2)

        data.append({
            "transaction_id": transaction_id,
            "account_id": f"ACC{random.randint(1, 1000):06d}",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "type": txn_type,
            "amount": amount if txn_type in ["deposit", "refund"] else -amount,
            "merchant": random.choice(merchants) if txn_type in ["payment", "withdrawal"] else None,
            "category": random.choice(["shopping", "food", "gas", "entertainment", "utilities"]),
            "status": random.choice(["completed", "completed", "completed", "pending"])
        })

    return pd.DataFrame(data)


def _generate_events(n: int, target_date: Optional[datetime] = None) -> pd.DataFrame:
    """Generate event log data.

    Args:
        n: Number of events to generate
        target_date: If provided, all events will occur on this date
    """
    event_types = ["page_view", "click", "form_submit", "download", "purchase", "signup", "login", "logout"]
    pages = ["/home", "/products", "/about", "/contact", "/checkout", "/account", "/blog", "/help"]

    data = []
    for i in range(n):
        event_id = f"EVT{i+1:010d}"

        # Use target_date if partitioned, otherwise random date in last 30 days
        if target_date:
            timestamp = target_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
        else:
            timestamp = datetime.now() - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )

        data.append({
            "event_id": event_id,
            "user_id": f"USER{random.randint(1, 500):06d}",
            "session_id": f"SESS{random.randint(1, 2000):08d}",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": random.choice(event_types),
            "page": random.choice(pages),
            "duration_seconds": random.randint(1, 300),
            "device": random.choice(["desktop", "mobile", "tablet"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"])
        })

    return pd.DataFrame(data)


def _generate_sensors(n: int, target_date: Optional[datetime] = None) -> pd.DataFrame:
    """Generate IoT sensor reading data.

    Args:
        n: Number of sensor readings to generate
        target_date: If provided, all readings will be from this date
    """
    sensor_types = ["temperature", "humidity", "pressure", "motion", "light", "sound"]
    locations = ["Building-A", "Building-B", "Warehouse", "Factory-Floor", "Office-1", "Office-2"]

    data = []

    # Use target_date if partitioned, otherwise last 24 hours
    if target_date:
        base_time = target_date
    else:
        base_time = datetime.now() - timedelta(hours=24)

    for i in range(n):
        timestamp = base_time + timedelta(minutes=i * (1440 / n))  # Spread over 24 hours
        sensor_type = random.choice(sensor_types)

        # Generate realistic values based on sensor type
        if sensor_type == "temperature":
            value = round(random.uniform(18, 28), 1)
            unit = "°C"
        elif sensor_type == "humidity":
            value = round(random.uniform(30, 70), 1)
            unit = "%"
        elif sensor_type == "pressure":
            value = round(random.uniform(980, 1020), 1)
            unit = "hPa"
        elif sensor_type == "motion":
            value = random.choice([0, 1])
            unit = "detected"
        elif sensor_type == "light":
            value = round(random.uniform(0, 1000), 0)
            unit = "lux"
        else:  # sound
            value = round(random.uniform(30, 90), 1)
            unit = "dB"

        data.append({
            "sensor_id": f"SENS{random.randint(1, 50):03d}",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "sensor_type": sensor_type,
            "location": random.choice(locations),
            "value": value,
            "unit": unit,
            "status": random.choice(["normal", "normal", "normal", "warning"])
        })

    return pd.DataFrame(data)


def _generate_users(n: int, target_date: Optional[datetime] = None) -> pd.DataFrame:
    """Generate user account data.

    Args:
        n: Number of user accounts to generate
        target_date: If provided, all users will be created on this date
    """
    roles = ["admin", "user", "moderator", "guest"]
    plans = ["free", "basic", "premium", "enterprise"]

    data = []
    for i in range(n):
        user_id = f"USER{i+1:06d}"
        username = f"user{i+1}"

        # Use target_date if partitioned, otherwise random date
        if target_date:
            created_at = target_date
            # Last login should be on or after created_at
            last_login = target_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
        else:
            created_at = datetime.now() - timedelta(days=random.randint(1, 1000))
            last_login = created_at + timedelta(days=random.randint(0, (datetime.now() - created_at).days))

        data.append({
            "user_id": user_id,
            "username": username,
            "email": f"{username}@example.com",
            "role": random.choice(roles),
            "plan": random.choice(plans),
            "created_at": created_at.strftime("%Y-%m-%d"),
            "last_login": last_login.strftime("%Y-%m-%d %H:%M:%S"),
            "login_count": random.randint(1, 500),
            "storage_used_gb": round(random.uniform(0.1, 100), 2),
            "is_verified": random.choice([True, True, False]),
            "is_active": random.choice([True, True, True, False])
        })

    return pd.DataFrame(data)


def _generate_subscriptions(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Generate SaaS subscription rows with tier-dependent churn.

    Schema: subscription_id, plan_tier, days_active, cancelled, signup_date.
    Designed for survival-analysis demos (Kaplan-Meier, Cox).

    opts:
      tiers: list of {name, weight, daily_churn_rate, max_days}.
        Defaults: free (0.55w, 0.020/day, 180d), pro (0.35w, 0.005/day, 365d),
        enterprise (0.10w, 0.001/day, 730d).
    """
    default_tiers = [
        {"name": "free",       "weight": 0.55, "daily_churn_rate": 0.020, "max_days": 180},
        {"name": "pro",        "weight": 0.35, "daily_churn_rate": 0.005, "max_days": 365},
        {"name": "enterprise", "weight": 0.10, "daily_churn_rate": 0.001, "max_days": 730},
    ]
    tiers = opts.get("tiers") or default_tiers
    weights = [t["weight"] for t in tiers]

    data = []
    today = datetime.now()
    for i in range(n):
        r = random.random(); cum = 0.0; chosen = tiers[-1]
        for t, w in zip(tiers, weights):
            cum += w
            if r <= cum:
                chosen = t; break
        churn = chosen["daily_churn_rate"]
        max_days = chosen["max_days"]
        days = 0; cancelled = 0
        while days < max_days:
            days += 1
            if random.random() < churn:
                cancelled = 1; break
        # Censor still-active subs at a random earlier point
        if cancelled == 0:
            days = random.randint(int(max_days * 0.3), max_days)
        signup = today - timedelta(days=days + random.randint(0, 30))
        data.append({
            "subscription_id": f"sub_{i:05d}",
            "plan_tier": chosen["name"],
            "days_active": days,
            "cancelled": cancelled,
            "signup_date": signup.strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(data)


def _generate_sparse_sensors(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Generate IoT sensor readings with random gaps (for gap-fill demos).

    Schema: reading_ts, sensor_id, temperature_c. Each sensor produces a
    diurnal-cycle temperature reading; ~dropout_rate fraction of rows are
    skipped to simulate flaky devices.

    opts:
      sensor_count: int (default 3)
      duration_hours: int (default 336 = 14 days)
      dropout_rate: float in [0,1] (default 0.25)
      base_temp: float (default 22.0)
      noise_amplitude: float (default 2.0)
      start_date: 'YYYY-MM-DD' (default '2026-04-01')

    `n` is treated as a soft cap. The full grid is sensor_count*duration_hours;
    after dropouts the result is roughly grid*(1-dropout_rate). If `n` is
    smaller than the resulting size, the result is truncated to `n`.
    """
    import math
    sensor_count = int(opts.get("sensor_count", 3))
    duration_hours = int(opts.get("duration_hours", 14 * 24))
    dropout_rate = float(opts.get("dropout_rate", 0.25))
    base_temp = float(opts.get("base_temp", 22.0))
    noise_amp = float(opts.get("noise_amplitude", 2.0))
    start_str = opts.get("start_date", "2026-04-01")
    start = datetime.strptime(start_str, "%Y-%m-%d")

    sensors = [
        (f"sensor_{chr(ord('a') + i)}", base_temp + (i - sensor_count / 2) * 2.0)
        for i in range(sensor_count)
    ]

    rows = []
    for sid, sensor_base in sensors:
        for hour in range(duration_hours):
            if random.random() < dropout_rate:
                continue
            ts = start + timedelta(hours=hour)
            diurnal = math.sin(2 * math.pi * (ts.hour / 24.0))
            temp = sensor_base + diurnal * noise_amp + random.gauss(0, 0.3)
            rows.append({
                "reading_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "sensor_id": sid,
                "temperature_c": round(temp, 2),
            })
    rows.sort(key=lambda r: (r["sensor_id"], r["reading_ts"]))
    if len(rows) > n:
        rows = rows[:n]
    return pd.DataFrame(rows)


def _generate_customer_churn_metrics(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Per-customer aggregated state used for churn modeling / scoring.

    Schema: customer_id, last_activity, total_orders, total_revenue, lifetime_days.

    opts:
      reference_date: 'YYYY-MM-DD' (default today). last_activity is computed
        relative to this so demos remain reproducible.
      activity_mix: list of (low_days, high_days, weight) tuples controlling
        the distribution of days_since_last_activity. Default biases toward
        recent activity with a long tail.
    """
    ref_str = opts.get("reference_date")
    today = datetime.strptime(ref_str, "%Y-%m-%d") if ref_str else datetime.now()
    activity_mix = opts.get("activity_mix") or [
        (0, 30, 5),
        (31, 90, 3),
        (91, 365, 2),
    ]
    bands = [(lo, hi) for lo, hi, _ in activity_mix]
    weights = [w for _, _, w in activity_mix]

    rows = []
    for i in range(1, n + 1):
        lo, hi = random.choices(bands, weights=weights)[0]
        days_since = random.randint(lo, hi)
        last_activity = today - timedelta(days=days_since)
        lifetime_days = random.randint(60, 800)
        total_orders = max(1, int(random.gauss(15, 8)))
        total_revenue = round(total_orders * random.uniform(20, 250), 2)
        rows.append({
            "customer_id": f"cus_{i:04d}",
            "last_activity": last_activity.strftime("%Y-%m-%d"),
            "total_orders": total_orders,
            "total_revenue": total_revenue,
            "lifetime_days": lifetime_days,
        })
    return pd.DataFrame(rows)


def _generate_stripe_charges(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Stripe-shaped charge events for revenue/attribution demos.

    Schema: id, _resource_type, customer_id, amount (cents), created (epoch),
    status. Matches the column shape of Stripe's `charges` API.

    opts:
      plans: list of plan-tier dollar amounts. Default [29, 49, 99, 199, 499].
      lookback_days: int (default 90). Charges are spread over the last N days.
    """
    plans = opts.get("plans") or [29, 49, 99, 199, 499]
    lookback_days = int(opts.get("lookback_days", 90))
    now = int(datetime.now().timestamp())
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id": f"ch_{i:05d}",
            "_resource_type": "charges",
            "customer_id": f"cus_{random.randint(1, max(2, n // 2)):03d}",
            "amount": random.choice(plans) * 100,
            "created": now - random.randint(0, lookback_days) * 86400,
            "status": "succeeded",
        })
    return pd.DataFrame(rows)


def _generate_stripe_subscriptions(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Stripe-shaped subscription rows for SaaS metrics / MRR demos.

    Schema: id, _resource_type, customer_id, status (active/trialing/canceled),
    created (epoch), canceled_at (epoch or ''), current_period_end (epoch),
    plan_amount (cents), plan_interval, plan_nickname.

    opts:
      plans: list of (cents-amount-as-int, nickname) tuples. Default mirrors
        a SaaS price ladder: starter/basic/pro/business/enterprise.
      plan_weights: list of relative weights, same length as plans.
      status_mix: dict {active, trialing, canceled} → fraction. Defaults to
        {active: 0.65, trialing: 0.10, canceled: 0.25}.
      lookback_days: int (default 540).
    """
    plans = opts.get("plans") or [
        (10, "starter"), (29, "basic"), (49, "pro"), (99, "business"), (199, "enterprise")
    ]
    weights = opts.get("plan_weights") or [3, 4, 3, 2, 1]
    status_mix = opts.get("status_mix") or {"active": 0.65, "trialing": 0.10, "canceled": 0.25}
    lookback_days = int(opts.get("lookback_days", 540))
    now = int(datetime.now().timestamp())

    cum_active = status_mix.get("active", 0.65)
    cum_trial = cum_active + status_mix.get("trialing", 0.10)

    rows = []
    for i in range(1, n + 1):
        days_ago = random.randint(0, lookback_days)
        created = now - days_ago * 86400
        plan_amount, plan_name = random.choices(plans, weights=weights)[0]
        r = random.random()
        if r < cum_active:
            status, canceled_at = "active", ""
            cpe = created + 30 * 86400
        elif r < cum_trial:
            status, canceled_at = "trialing", ""
            cpe = created + 14 * 86400
        else:
            status = "canceled"
            canceled_at = created + random.randint(30, 300) * 86400
            cpe = canceled_at
        rows.append({
            "id": f"sub_{i:04d}",
            "_resource_type": "subscriptions",
            "customer_id": f"cus_{i:04d}",
            "status": status,
            "created": created,
            "canceled_at": canceled_at,
            "current_period_end": cpe,
            "plan_amount": plan_amount * 100,
            "plan_interval": "month",
            "plan_nickname": plan_name,
        })
    return pd.DataFrame(rows)


def _generate_ab_experiment(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Per-user A/B experiment exposure rows for stat-test demos.

    Schema: experiment_id, user_id, variant ('control'|'treatment'), converted (0/1), exposed_at.

    The treatment lift defaults to 30% relative — large enough that a
    sample of 1000 will register as significant in a typical demo. Set
    `lift` to 0.0 to simulate a null result.

    opts:
      experiment_id: str (default 'exp_001')
      control_conversion: float (default 0.10 = 10%)
      lift: float (default 0.3 = treatment is 30% better than control,
        relative — i.e. treatment_conversion = control * (1 + lift))
      treatment_share: float (default 0.5)
      lookback_days: int (default 14)
    """
    experiment_id = opts.get("experiment_id", "exp_001")
    control_conv = float(opts.get("control_conversion", 0.10))
    lift = float(opts.get("lift", 0.3))
    treatment_share = float(opts.get("treatment_share", 0.5))
    lookback_days = int(opts.get("lookback_days", 14))
    treatment_conv = max(0.0, min(1.0, control_conv * (1.0 + lift)))
    today = datetime.now()

    rows = []
    for i in range(1, n + 1):
        is_treatment = random.random() < treatment_share
        variant = "treatment" if is_treatment else "control"
        p = treatment_conv if is_treatment else control_conv
        converted = 1 if random.random() < p else 0
        exposed_at = today - timedelta(
            days=random.randint(0, lookback_days),
            seconds=random.randint(0, 86399),
        )
        rows.append({
            "experiment_id": experiment_id,
            "user_id": f"user_{i:06d}",
            "variant": variant,
            "converted": converted,
            "exposed_at": exposed_at.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def _generate_support_tickets(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Generate synthetic multilingual support tickets with embedded PII.

    Useful for: NLP demos (keyword/language/PII detection, embeddings,
    sentiment analysis, classification, summarization). Each ticket has
    realistic ticket_text including names, emails, phones, and credit
    card fragments — designed to exercise PII detectors AND multilingual
    NLP pipelines.

    Columns:
      ticket_id, customer_id, channel (email|chat|web), priority,
      created_at, ticket_text (rich free text)
    """
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))

    ENGLISH_TEMPLATES = [
        ("Hi, my name is {name} and my email is {email}. My order #{order} hasn't arrived. Can you check status?", "en"),
        ("I want to cancel my subscription. Account email: {email}. Phone: {phone}.", "en"),
        ("Login fails with 2FA. Email: {email}. Please reset.", "en"),
        ("Refund request for order {order} — product arrived damaged.", "en"),
        ("Quick question — does the Pro plan include API access?", "en"),
        ("How do I export my data? Compliance team needs everything by Friday.", "en"),
        ("Site is down for me — getting 502 errors since 9am EST.", "en"),
        ("Bug report: search results show duplicates when filtering by date range.", "en"),
        ("Can I get an enterprise quote? Contact: {email} or {phone}.", "en"),
        ("Feature request: add dark mode to the mobile app please.", "en"),
        ("Account security alert — got an email about login from Russia, was that real?", "en"),
        ("API rate limit too low for production use. Account: {email}.", "en"),
        ("Charge my card ending in {cc4}.", "en"),
    ]
    SPANISH_TEMPLATES = [
        ("Hola, soy {name}. Mi número de pedido es {order} y aún no lo recibo.", "es"),
        ("Mi tarjeta de crédito {cc} fue rechazada. ¿Por qué?", "es"),
        ("El sistema está caído. Llámame al {phone} lo antes posible.", "es"),
    ]
    FRENCH_TEMPLATES = [
        ("Bonjour, je m'appelle {name}. Mon paiement échoue toujours sur le site. Aide?", "fr"),
        ("Le tableau de bord ne charge plus depuis hier. Email: {email}", "fr"),
    ]
    GERMAN_TEMPLATES = [
        ("Guten Tag! Mein Name ist {name}. Ich brauche eine Rechnung für letzten Monat.", "de"),
        ("Bekommen ich Rabatte als jährlicher Abonnent?", "de"),
    ]
    ALL_TEMPLATES = ENGLISH_TEMPLATES * 4 + SPANISH_TEMPLATES + FRENCH_TEMPLATES + GERMAN_TEMPLATES
    NAMES = ["Alice Johnson", "Carlos Hernández", "Marie Dupont", "Klaus Müller",
             "Bob Smith", "Charlie Brown", "Jane Doe", "Maria Garcia",
             "Hiroshi Tanaka", "Olga Petrova"]
    EMAIL_DOMAINS = ["example.com", "enterprise.org", "test.io", "gmail.com",
                     "bigcorp.com", "startup.io", "support.example.fr"]

    rows = []
    for i in range(n):
        template, lang = ALL_TEMPLATES[rng.integers(0, len(ALL_TEMPLATES))]
        name = NAMES[rng.integers(0, len(NAMES))]
        email = f"{name.lower().replace(' ', '.').replace('é', 'e').replace('ñ', 'n').replace('ü', 'u')}@{EMAIL_DOMAINS[rng.integers(0, len(EMAIL_DOMAINS))]}"
        phone = f"555-{rng.integers(1000, 9999):04d}"
        cc4 = f"{rng.integers(1000, 9999):04d}"
        cc = f"4532-{rng.integers(1000, 9999):04d}-{rng.integers(1000, 9999):04d}-{cc4}"
        order = f"{rng.integers(10000, 99999)}"
        text = template.format(name=name, email=email, phone=phone, order=order, cc=cc, cc4=cc4)
        rows.append({
            "ticket_id":   f"T{i+1:05d}",
            "customer_id": f"CUST{rng.integers(1000, 9999):04d}",
            "channel":     ["email", "chat", "web"][rng.integers(0, 3)],
            "priority":    ["low", "medium", "high", "urgent"][rng.integers(0, 4)],
            "created_at":  pd.Timestamp.now() - pd.Timedelta(minutes=int(rng.integers(0, 60 * 24 * 30))),
            "ticket_text": text,
            # `expected_language` is for ground-truth comparison in language_detector demos
            "expected_language": lang,
        })
    return pd.DataFrame(rows)


def _generate_product_reviews(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Generate synthetic product reviews with sentiment-rich text.

    Useful for: sentiment analysis, classification, embeddings, RAG.
    """
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))

    POSITIVE = [
        "Absolutely love this product. Quality is fantastic, would buy again. {detail}",
        "Best purchase I've made this year. {detail}",
        "Exceeded expectations — fast shipping, perfect packaging. {detail}",
        "Top notch. {detail}",
    ]
    NEUTRAL = [
        "Product is OK. Does what it says. {detail}",
        "Average quality for the price. {detail}",
        "It works. Nothing exciting. {detail}",
    ]
    NEGATIVE = [
        "Disappointed. Build quality is poor. {detail}",
        "Returned within a week. {detail}",
        "Customer service was slow and unhelpful. {detail}",
        "Don't recommend — broke after second use. {detail}",
    ]
    DETAILS = [
        "Battery life could be better.",
        "The color is exactly as pictured.",
        "Setup took 10 minutes.",
        "Heavier than expected.",
        "App integration is smooth.",
        "Wish there were more size options.",
    ]
    PRODUCTS = ["Wireless Headphones", "Coffee Maker", "Yoga Mat", "Mechanical Keyboard",
                "Office Chair", "Smart Bulb", "Tablet Stand", "Laptop Backpack"]

    rows = []
    for i in range(n):
        rating = int(rng.integers(1, 6))
        if rating >= 4:
            template = POSITIVE[rng.integers(0, len(POSITIVE))]; sentiment = "positive"
        elif rating == 3:
            template = NEUTRAL[rng.integers(0, len(NEUTRAL))]; sentiment = "neutral"
        else:
            template = NEGATIVE[rng.integers(0, len(NEGATIVE))]; sentiment = "negative"
        detail = DETAILS[rng.integers(0, len(DETAILS))]
        rows.append({
            "review_id":   f"R{i+1:05d}",
            "product_name": PRODUCTS[rng.integers(0, len(PRODUCTS))],
            "rating":      rating,
            "expected_sentiment": sentiment,  # ground truth for sentiment demos
            "review_text": template.format(detail=detail),
            "created_at":  pd.Timestamp.now() - pd.Timedelta(days=int(rng.integers(0, 365))),
        })
    return pd.DataFrame(rows)


def _generate_audio_samples(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Public-domain GCS sample audio URIs — the gs:// ones Google ships.

    Columns: audio_id, language_code, gcs_uri, expected_phrase (where known).
    """
    samples = [
        ("brooklyn_bridge", "en-US", "gs://cloud-samples-data/speech/brooklyn_bridge.mp3",
         "how old is the Brooklyn Bridge"),
        ("commercial_mono", "en-US", "gs://cloud-samples-data/speech/commercial_mono.wav",
         None),
        ("hello",           "en-US", "gs://cloud-samples-data/speech/hello.wav",
         "hello"),
        ("multi_speaker",   "en-US", "gs://cloud-samples-data/speech/multi.wav",
         None),
    ]
    rows = [
        {"audio_id": s[0], "language_code": s[1], "gcs_uri": s[2], "expected_phrase": s[3]}
        for s in samples[: max(1, min(n, len(samples)))]
    ]
    return pd.DataFrame(rows)


def _generate_image_prompts(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Synthetic image-generation prompts. Columns: prompt_id, prompt, kind."""
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))
    subjects = [
        "a vintage racing car", "a futuristic city skyline at sunset",
        "a cozy mountain cabin in winter", "a fierce dragon in a misty forest",
        "a serene japanese garden", "a steampunk airship",
        "an astronaut on the moon", "a deep-sea anglerfish",
        "a marble greek statue", "a neon-lit night market",
    ]
    styles = [
        "photorealistic", "oil painting", "watercolor sketch",
        "anime", "studio ghibli", "low-poly 3d render",
        "cinematic", "isometric pixel art",
    ]
    kinds = ["hero", "thumbnail", "banner", "social", "marketing"]
    rows = []
    for i in range(n):
        s = subjects[rng.integers(0, len(subjects))]
        st = styles[rng.integers(0, len(styles))]
        rows.append({
            "prompt_id": f"P{i+1:04d}",
            "prompt": f"{s}, {st}",
            "kind": kinds[rng.integers(0, len(kinds))],
        })
    return pd.DataFrame(rows)


def _generate_employees(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Synthetic HRIS-style employee export.

    Designed for hris_normalizer / HR analytics demos. Vendor-y column names
    (employee_number, work_email, employment_type, hire_dt) plus mixed-format
    values for status/employment_type to exercise normalization paths.
    """
    import numpy as np
    from datetime import timedelta
    rng = np.random.default_rng(opts.get("random_state"))
    first_names = ["Alice", "Bob", "Carlos", "Diana", "Eli", "Fatima", "Grace",
                   "Hiroshi", "Ivy", "Jamal", "Klaus", "Lin", "Marco", "Nia",
                   "Olga", "Pria", "Quinn", "Rashid", "Sofia", "Theo"]
    last_names  = ["Johnson", "Hernandez", "Smith", "Tanaka", "Petrova",
                   "Mueller", "Dupont", "Garcia", "Patel", "Brown", "Lee",
                   "Cohen", "Nguyen", "Khan", "Rossi", "Singh"]
    departments = ["Engineering", "Sales", "Marketing", "Finance", "Operations",
                   "Customer Success", "Product", "Data"]
    # Vendor-y status values — should normalize to ACTIVE / INACTIVE etc.
    statuses    = ["Active", "active", "ACTIVE", "Terminated", "term", "On Leave"]
    emp_types   = ["FT", "Full-Time", "FULL_TIME", "PT", "Part-Time", "Contractor", "Intern"]
    today = pd.Timestamp.now().normalize()
    rows = []
    for i in range(n):
        fn = first_names[rng.integers(0, len(first_names))]
        ln = last_names[rng.integers(0, len(last_names))]
        hire_days_ago = int(rng.integers(30, 365 * 8))
        rows.append({
            "employee_number": f"E{i+1:06d}",
            "first_name":      fn,
            "last_name":       ln,
            "work_email":      f"{fn.lower()}.{ln.lower()}@example.com",
            "department":      departments[rng.integers(0, len(departments))],
            "status":          statuses[rng.integers(0, len(statuses))],
            "employment_type": emp_types[rng.integers(0, len(emp_types))],
            "hire_dt":         (today - timedelta(days=hire_days_ago)).strftime("%Y-%m-%d"),
        })
    return pd.DataFrame(rows)


def _generate_fhir_patients(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Synthetic FHIR R4/R5 Patient + Observation resources.

    Emits a DataFrame with one row per resource and a `resource` column
    holding the parsed FHIR JSON (as a dict, ready for downstream parsing
    by `fhir_resource_normalizer`). Mix is: each "patient" gets 1 Patient
    resource plus ~3 Observations (vitals: HR, BP, temperature).
    """
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))

    first_names = ["Alice", "Bob", "Carlos", "Diana", "Eli", "Fatima", "Grace",
                   "Hiroshi", "Ivy", "Jamal", "Klaus", "Lin", "Marco", "Nia"]
    last_names  = ["Johnson", "Hernandez", "Smith", "Tanaka", "Petrova",
                   "Mueller", "Dupont", "Garcia", "Patel", "Brown"]
    cities = ["Boston", "Austin", "Seattle", "Denver", "Atlanta", "Phoenix"]
    states = ["MA", "TX", "WA", "CO", "GA", "AZ"]
    genders = ["M", "F"]   # intentionally messy — exercises value_maps
    rows: List[Dict[str, Any]] = []

    # Each "patient" cycle emits 1 Patient + ~3 Observations.
    pid = 1
    while len(rows) < n:
        pat_id = f"pat-{pid:05d}"
        fn = first_names[rng.integers(0, len(first_names))]
        ln = last_names[rng.integers(0, len(last_names))]
        gender = genders[rng.integers(0, len(genders))]
        birth = f"19{rng.integers(50, 99):02d}-{rng.integers(1, 12):02d}-{rng.integers(1, 28):02d}"
        city_idx = int(rng.integers(0, len(cities)))
        rows.append({
            "row_id":   f"r-{len(rows)+1:05d}",
            "resource": {
                "resourceType": "Patient",
                "id":           pat_id,
                "name":         [{"given": [fn], "family": ln}],
                "gender":       gender,
                "birthDate":    birth,
                "address":      [{"city": cities[city_idx], "state": states[city_idx],
                                  "country": "US", "postalCode": f"{rng.integers(10000, 99999)}"}],
            },
        })
        if len(rows) >= n: break

        # Heart rate observation
        rows.append({
            "row_id":   f"r-{len(rows)+1:05d}",
            "resource": {
                "resourceType":      "Observation",
                "id":                f"obs-{pid:05d}-hr",
                "status":            "final",
                "subject":           {"reference": f"Patient/{pat_id}"},
                "code":              {"coding": [{"system": "http://loinc.org", "code": "8867-4", "display": "Heart rate"}]},
                "effectiveDateTime": "2025-01-15T09:00:00Z",
                "valueQuantity":     {"value": int(rng.integers(55, 105)), "unit": "/min"},
            },
        })
        if len(rows) >= n: break

        # Body temperature observation
        rows.append({
            "row_id":   f"r-{len(rows)+1:05d}",
            "resource": {
                "resourceType":      "Observation",
                "id":                f"obs-{pid:05d}-temp",
                "status":            "final",
                "subject":           {"reference": f"Patient/{pat_id}"},
                "code":              {"coding": [{"system": "http://loinc.org", "code": "8310-5", "display": "Body temperature"}]},
                "effectiveDateTime": "2025-01-15T09:01:00Z",
                "valueQuantity":     {"value": round(36.5 + float(rng.normal(0, 0.5)), 1), "unit": "Cel"},
            },
        })
        pid += 1

    return pd.DataFrame(rows[:n])


def _generate_hl7_messages(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Synthetic HL7 v2 ADT^A01 and ORU^R01 messages.

    Emits a DataFrame with `message_id` and `message` columns; `message` is
    the raw pipe-delimited HL7 string with \\r between segments — ready for
    `hl7_v2_parser` to flatten.
    """
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))

    last_names  = ["Doe", "Smith", "Garcia", "Tanaka", "Patel", "Mueller", "Romano"]
    first_names = ["John", "Jane", "Maria", "Hiroshi", "Anand", "Ingrid", "Marco"]
    rows: List[Dict[str, Any]] = []

    for i in range(n):
        msg_id = f"MSG{i+1:07d}"
        pat_id = f"PAT{rng.integers(10000, 99999)}"
        ln = last_names[rng.integers(0, len(last_names))]
        fn = first_names[rng.integers(0, len(first_names))]
        dob = f"19{rng.integers(40, 99):02d}{rng.integers(1, 12):02d}{rng.integers(1, 28):02d}"
        sex = "M" if rng.integers(0, 2) == 0 else "F"

        if i % 2 == 0:
            # ADT^A01 — admit
            msg = (
                f"MSH|^~\\&|HOSPITAL|EHR|LAB|HOSPITAL|202501151200||ADT^A01|{msg_id}|P|2.5\r"
                f"EVN|A01|202501151200\r"
                f"PID|1||{pat_id}||{ln}^{fn}^A||{dob}|{sex}|||123 Main St^^Boston^MA^02101^US\r"
                f"PV1|1|I|ICU^301^A||||1234^Smith^Jane^^^DR\r"
            )
            msg_type = "ADT^A01"
        else:
            # ORU^R01 — lab result with 2 OBX observations
            glu = float(rng.integers(70, 250))
            hr  = int(rng.integers(55, 110))
            msg = (
                f"MSH|^~\\&|LAB|HOSPITAL|EHR|HOSPITAL|202501151200||ORU^R01|{msg_id}|P|2.5\r"
                f"PID|1||{pat_id}||{ln}^{fn}^A||{dob}|{sex}|||123 Main St^^Boston^MA^02101^US\r"
                f"OBR|1|||LAB001^Lab Panel^L\r"
                f"OBX|1|NM|GLU^Glucose^L||{glu}|mg/dL|70-100|H||F\r"
                f"OBX|2|NM|HR^Heart Rate^L||{hr}|/min|60-100|N||F\r"
            )
            msg_type = "ORU^R01"

        rows.append({"message_id": msg_id, "message_type": msg_type, "message": msg})

    return pd.DataFrame(rows)


def _generate_iso20022_payments(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Synthetic ISO 20022 pacs.008 and pacs.002 XML messages.

    Emits a DataFrame with `message_id`, `message_type`, and `xml` columns.
    Alternates between pacs.008 (credit transfer) and pacs.002 (status report).
    """
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))

    debtors  = [("Acme Corp",        "DE89370400440532013000", "DEUTDEFFXXX"),
                ("Globex Industries","GB29NWBK60161331926819", "NWBKGB2L"),
                ("Initech LLC",      "US12345678901234567890", "CHASUS33"),
                ("Soylent SA",       "FR1420041010050500013M02606", "BNPAFRPP")]
    creditors = [("Hooli Inc",       "US98765432109876543210", "BOFAUS3N"),
                 ("Pied Piper Ltd",  "GB94BARC10201530093459", "BARCGB22"),
                 ("Stark Industries","DE12500105170648489890", "INGDDEFFXXX")]

    rows = []
    for i in range(n):
        msg_id = f"MSG{i+1:08d}"
        e2e_id = f"E2E{i+1:08d}"
        amt = round(float(rng.uniform(100, 50000)), 2)
        ccy = ["USD", "EUR", "GBP"][int(rng.integers(0, 3))]
        d_name, d_iban, d_bic = debtors[rng.integers(0, len(debtors))]
        c_name, c_iban, c_bic = creditors[rng.integers(0, len(creditors))]

        if i % 2 == 0:
            # pacs.008 — Customer Credit Transfer
            xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08">
  <FIToFICstmrCdtTrf>
    <GrpHdr>
      <MsgId>{msg_id}</MsgId>
      <CreDtTm>2025-01-15T09:00:00Z</CreDtTm>
      <NbOfTxs>1</NbOfTxs>
      <SttlmInf><SttlmMtd>CLRG</SttlmMtd></SttlmInf>
    </GrpHdr>
    <CdtTrfTxInf>
      <PmtId><InstrId>{msg_id}-1</InstrId><EndToEndId>{e2e_id}</EndToEndId></PmtId>
      <IntrBkSttlmAmt Ccy="{ccy}">{amt}</IntrBkSttlmAmt>
      <Dbtr><Nm>{d_name}</Nm></Dbtr>
      <DbtrAcct><Id><IBAN>{d_iban}</IBAN></Id></DbtrAcct>
      <DbtrAgt><FinInstnId><BICFI>{d_bic}</BICFI></FinInstnId></DbtrAgt>
      <CdtrAgt><FinInstnId><BICFI>{c_bic}</BICFI></FinInstnId></CdtrAgt>
      <Cdtr><Nm>{c_name}</Nm></Cdtr>
      <CdtrAcct><Id><IBAN>{c_iban}</IBAN></Id></CdtrAcct>
      <RmtInf><Ustrd>Invoice {i+1:05d}</Ustrd></RmtInf>
    </CdtTrfTxInf>
  </FIToFICstmrCdtTrf>
</Document>'''
            mt = "pacs.008"
        else:
            # pacs.002 — Payment Status Report
            status = ["ACCP", "ACSC", "RJCT", "PDNG"][int(rng.integers(0, 4))]
            reason = "AC01" if status == "RJCT" else ""
            xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10">
  <FIToFIPmtStsRpt>
    <GrpHdr>
      <MsgId>{msg_id}</MsgId>
      <CreDtTm>2025-01-15T09:30:00Z</CreDtTm>
    </GrpHdr>
    <TxInfAndSts>
      <OrgnlInstrId>{msg_id}-1</OrgnlInstrId>
      <OrgnlEndToEndId>{e2e_id}</OrgnlEndToEndId>
      <TxSts>{status}</TxSts>
      {f"<StsRsnInf><Rsn><Cd>{reason}</Cd></Rsn></StsRsnInf>" if reason else ""}
    </TxInfAndSts>
  </FIToFIPmtStsRpt>
</Document>'''
            mt = "pacs.002"

        rows.append({"message_id": msg_id, "message_type": mt, "xml": xml})

    return pd.DataFrame(rows)


def _generate_x12_messages(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Synthetic ASC X12 EDI messages.

    Emits 270 (eligibility inquiry), 271 (eligibility response), 835 (remittance),
    837 (healthcare claim), and 850 (purchase order) transactions wrapped in ISA/GS/ST
    envelopes. Default delimiter is '~' for segment, '*' for element, ':' for sub-element.

    Output DataFrame columns: `control_number`, `transaction_set`, `message`.
    """
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))

    seg = "~"
    el = "*"
    senders = ["SENDERID01", "PAYER001ABC", "PROVIDER12"]
    receivers = ["RECEIVERID", "CLEARNGHSE", "VENDORACME"]
    members = [("DOE", "JOHN", "M123456789"), ("SMITH", "JANE", "M987654321"),
               ("LEE", "WEI", "M444555666"), ("PATEL", "RAJ", "M777888999")]
    payers = ["AETNA", "BCBS", "UNITEDHEALTH", "CIGNA"]
    providers = [("DR HOUSE", "1234567890"), ("DR GREY", "2345678901"), ("DR STRANGE", "3456789012")]
    products = [("WIDGET-A", 12.50), ("WIDGET-B", 25.00), ("GADGET-C", 99.99), ("PART-D", 7.25)]

    tx_types = ["270", "271", "835", "837", "850"]
    rows = []
    for i in range(n):
        ctrl = f"{i+1:09d}"
        gs_ctrl = f"{i+1:06d}"
        st_ctrl = f"{i+1:04d}"
        tx = tx_types[i % len(tx_types)]
        snd = senders[rng.integers(0, len(senders))]
        rcv = receivers[rng.integers(0, len(receivers))]

        isa = el.join([
            "ISA", "00", "          ", "00", "          ",
            "ZZ", snd.ljust(15), "ZZ", rcv.ljust(15),
            "250115", "0900", "U", "00401", ctrl, "0", "P", ":"
        ]) + seg

        if tx == "270":
            # Eligibility inquiry
            ln, fn, mid = members[rng.integers(0, len(members))]
            payer = payers[rng.integers(0, len(payers))]
            segs = [
                f"GS{el}HS{el}{snd}{el}{rcv}{el}20250115{el}0900{el}{gs_ctrl}{el}X{el}005010X279A1",
                f"ST{el}270{el}{st_ctrl}{el}005010X279A1",
                f"BHT{el}0022{el}13{el}{ctrl}{el}20250115{el}0900",
                f"HL{el}1{el}{el}20{el}1",
                f"NM1{el}PR{el}2{el}{payer}{el}{el}{el}{el}{el}PI{el}{payer[:5]}",
                f"HL{el}2{el}1{el}21{el}1",
                f"NM1{el}1P{el}2{el}PROVIDER GROUP{el}{el}{el}{el}{el}XX{el}1234567890",
                f"HL{el}3{el}2{el}22{el}0",
                f"NM1{el}IL{el}1{el}{ln}{el}{fn}{el}{el}{el}{el}MI{el}{mid}",
                f"DMG{el}D8{el}19800515{el}M",
                f"EQ{el}30",
                f"SE{el}11{el}{st_ctrl}",
                f"GE{el}1{el}{gs_ctrl}",
                f"IEA{el}1{el}{ctrl}",
            ]
        elif tx == "271":
            # Eligibility response
            ln, fn, mid = members[rng.integers(0, len(members))]
            payer = payers[rng.integers(0, len(payers))]
            active = rng.choice([True, False])
            segs = [
                f"GS{el}HB{el}{snd}{el}{rcv}{el}20250115{el}0930{el}{gs_ctrl}{el}X{el}005010X279A1",
                f"ST{el}271{el}{st_ctrl}{el}005010X279A1",
                f"BHT{el}0022{el}11{el}{ctrl}{el}20250115{el}0930",
                f"HL{el}1{el}{el}20{el}1",
                f"NM1{el}PR{el}2{el}{payer}",
                f"HL{el}2{el}1{el}21{el}1",
                f"NM1{el}1P{el}2{el}PROVIDER GROUP",
                f"HL{el}3{el}2{el}22{el}0",
                f"NM1{el}IL{el}1{el}{ln}{el}{fn}{el}{el}{el}{el}MI{el}{mid}",
                f"EB{el}{'1' if active else '6'}{el}IND{el}30",
                f"SE{el}10{el}{st_ctrl}",
                f"GE{el}1{el}{gs_ctrl}",
                f"IEA{el}1{el}{ctrl}",
            ]
        elif tx == "835":
            # Remittance advice
            payer = payers[rng.integers(0, len(payers))]
            prv_name, prv_npi = providers[rng.integers(0, len(providers))]
            ln, fn, mid = members[rng.integers(0, len(members))]
            chrg = round(float(rng.uniform(100, 2000)), 2)
            paid = round(chrg * float(rng.uniform(0.6, 0.95)), 2)
            segs = [
                f"GS{el}HP{el}{snd}{el}{rcv}{el}20250115{el}1000{el}{gs_ctrl}{el}X{el}005010X221A1",
                f"ST{el}835{el}{st_ctrl}",
                f"BPR{el}I{el}{paid}{el}C{el}ACH{el}CCP{el}{el}{el}{el}{el}{el}{el}{el}{el}{el}{el}20250115",
                f"TRN{el}1{el}TRACE{ctrl}{el}1{payer[:9].ljust(9, '0')}",
                f"N1{el}PR{el}{payer}",
                f"N1{el}PE{el}{prv_name}{el}XX{el}{prv_npi}",
                f"LX{el}1",
                f"CLP{el}CLAIM{ctrl}{el}1{el}{chrg}{el}{paid}{el}{el}MC{el}PAYER{i+1:06d}",
                f"NM1{el}QC{el}1{el}{ln}{el}{fn}{el}{el}{el}{el}MI{el}{mid}",
                f"SVC{el}HC:99213{el}{chrg}{el}{paid}",
                f"SE{el}10{el}{st_ctrl}",
                f"GE{el}1{el}{gs_ctrl}",
                f"IEA{el}1{el}{ctrl}",
            ]
        elif tx == "837":
            # Healthcare claim
            ln, fn, mid = members[rng.integers(0, len(members))]
            prv_name, prv_npi = providers[rng.integers(0, len(providers))]
            payer = payers[rng.integers(0, len(payers))]
            chrg = round(float(rng.uniform(100, 2000)), 2)
            segs = [
                f"GS{el}HC{el}{snd}{el}{rcv}{el}20250115{el}1100{el}{gs_ctrl}{el}X{el}005010X222A1",
                f"ST{el}837{el}{st_ctrl}{el}005010X222A1",
                f"BHT{el}0019{el}00{el}{ctrl}{el}20250115{el}1100{el}CH",
                f"NM1{el}41{el}2{el}SUBMITTER{el}{el}{el}{el}{el}46{el}{snd}",
                f"NM1{el}40{el}2{el}{payer}{el}{el}{el}{el}{el}46{el}{rcv}",
                f"HL{el}1{el}{el}20{el}1",
                f"NM1{el}85{el}2{el}{prv_name}{el}{el}{el}{el}{el}XX{el}{prv_npi}",
                f"HL{el}2{el}1{el}22{el}0",
                f"SBR{el}P{el}18{el}{el}{el}{el}{el}{el}{el}CI",
                f"NM1{el}IL{el}1{el}{ln}{el}{fn}{el}{el}{el}{el}MI{el}{mid}",
                f"NM1{el}PR{el}2{el}{payer}",
                f"CLM{el}CLAIM{ctrl}{el}{chrg}{el}{el}{el}11:B:1{el}Y{el}A{el}Y{el}Y",
                f"LX{el}1",
                f"SV1{el}HC:99213{el}{chrg}{el}UN{el}1{el}{el}{el}1",
                f"DTP{el}472{el}D8{el}20250114",
                f"SE{el}15{el}{st_ctrl}",
                f"GE{el}1{el}{gs_ctrl}",
                f"IEA{el}1{el}{ctrl}",
            ]
        else:
            # 850 — purchase order
            qty = int(rng.integers(1, 100))
            sku, unit = products[rng.integers(0, len(products))]
            total = round(qty * unit, 2)
            segs = [
                f"GS{el}PO{el}{snd}{el}{rcv}{el}20250115{el}1200{el}{gs_ctrl}{el}X{el}004010",
                f"ST{el}850{el}{st_ctrl}",
                f"BEG{el}00{el}SA{el}PO{ctrl}{el}{el}20250115",
                f"REF{el}DP{el}DEPT{i+1:04d}",
                f"DTM{el}002{el}20250130",
                f"N1{el}ST{el}SHIP TO LOCATION",
                f"PO1{el}1{el}{qty}{el}EA{el}{unit}{el}{el}VP{el}{sku}",
                f"CTT{el}1{el}{qty}",
                f"AMT{el}TT{el}{total}",
                f"SE{el}9{el}{st_ctrl}",
                f"GE{el}1{el}{gs_ctrl}",
                f"IEA{el}1{el}{ctrl}",
            ]

        msg = isa + seg.join(segs) + seg
        rows.append({"control_number": ctrl, "transaction_set": tx, "message": msg})

    return pd.DataFrame(rows)


def _generate_fix_messages(n: int, opts: Dict[str, Any]) -> pd.DataFrame:
    """Synthetic FIX 4.4 trading messages.

    Emits NewOrderSingle (D) and ExecutionReport (8) messages with realistic
    tag mappings. Delimiter is SOH (\\x01) per canonical FIX; tests can use
    `delimiter='|'` via schema_options to emit pipe-delimited logs instead.

    Output DataFrame columns: `seq_num`, `msg_type`, `message`.
    """
    import numpy as np
    rng = np.random.default_rng(opts.get("random_state"))
    delim = opts.get("delimiter", "\x01")

    senders = ["BUYSIDE01", "HEDGE_FUND_A", "PROP_DESK_B"]
    targets = ["BROKER_XYZ", "EXCHANGE_NYSE", "ECN_BATS"]
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "SPY"]
    sides = ["1", "2"]  # buy, sell
    ord_types = ["1", "2"]  # market, limit
    tifs = ["0", "1", "3", "4"]  # day, gtc, ioc, fok

    rows = []
    for i in range(n):
        seq = i + 1
        snd = senders[rng.integers(0, len(senders))]
        tgt = targets[rng.integers(0, len(targets))]
        sym = symbols[rng.integers(0, len(symbols))]
        side = sides[rng.integers(0, len(sides))]
        ord_type = ord_types[rng.integers(0, len(ord_types))]
        tif = tifs[rng.integers(0, len(tifs))]
        qty = int(rng.integers(100, 10000))
        px = round(float(rng.uniform(50, 500)), 2)
        ts = f"20250115-09:30:{(i % 60):02d}.000"

        if i % 3 == 0:
            # NewOrderSingle
            mt = "D"
            body_fields = [
                f"35={mt}", f"34={seq}", f"49={snd}", f"56={tgt}", f"52={ts}",
                f"11=CLORD{seq:08d}",
                f"55={sym}",
                f"54={side}",
                f"60={ts}",
                f"38={qty}",
                f"40={ord_type}",
            ]
            if ord_type == "2":
                body_fields.append(f"44={px}")
            body_fields.append(f"59={tif}")
        else:
            # ExecutionReport
            mt = "8"
            exec_type = "F"  # trade
            ord_status = "1" if i % 5 == 0 else "2"  # partial vs filled
            last_qty = qty if ord_status == "2" else int(qty * 0.5)
            cum_qty = last_qty
            leaves = qty - cum_qty if ord_status == "1" else 0
            body_fields = [
                f"35={mt}", f"34={seq}", f"49={snd}", f"56={tgt}", f"52={ts}",
                f"37=ORD{seq:08d}",
                f"17=EXEC{seq:08d}",
                f"11=CLORD{seq:08d}",
                f"150={exec_type}",
                f"39={ord_status}",
                f"55={sym}",
                f"54={side}",
                f"38={qty}",
                f"32={last_qty}",
                f"31={px}",
                f"14={cum_qty}",
                f"151={leaves}",
                f"6={px}",
            ]

        # Compute body length and checksum per FIX wire format
        body = delim.join(body_fields) + delim
        prefix_for_length = body
        body_length = len(prefix_for_length)
        header = f"8=FIX.4.4{delim}9={body_length}{delim}"
        # FIX checksum = sum of all bytes (header + body) mod 256, zero-padded to 3 digits
        pre_checksum = (header + body).encode("ascii")
        checksum = sum(pre_checksum) % 256
        message = header + body + f"10={checksum:03d}{delim}"

        rows.append({"seq_num": seq, "msg_type": mt, "message": message})

    return pd.DataFrame(rows)
