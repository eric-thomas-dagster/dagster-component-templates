"""Synthetic Data Generator Asset Component."""

from typing import Optional, Literal
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
    asset,
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
        "events",
        "sensors",
        "users"
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

    random_seed: Optional[int] = Field(
        default=None,
        description="Random seed for reproducible data generation (leave empty for random)"
    )

    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )

    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the synthetic data generator."""

        # Capture fields for closure
        asset_name = self.asset_name
        schema_type = self.schema_type
        row_count = self.row_count
        random_seed = self.random_seed
        description = self.description or f"Synthetic {schema_type} data"
        group_name = self.group_name or None

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
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
            else:
                raise ValueError(f"Unknown schema type: {schema_type}")

            context.log.info(f"Generated DataFrame with shape {df.shape}")
            return df

        return Definitions(assets=[synthetic_data_asset])


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
