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
