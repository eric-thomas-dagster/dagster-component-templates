# Synthetic Data Generator

Generate realistic synthetic/mock data for demos, testing, and prototyping without needing external data sources.

## Overview

This component creates realistic fake data based on pre-defined schemas. Perfect for:
- **Demonstrations**: Show off your pipeline without setting up real data sources
- **Testing**: Test data transformations and quality checks with known data
- **Prototyping**: Build pipelines before connecting to production data
- **Training**: Create safe, realistic datasets for learning and experimentation

## Features

- **13 Pre-defined Schemas**: Customers, Orders, Products, Transactions, Events, Sensors, Users, Subscriptions, Sparse Sensors, Customer Churn Metrics, Stripe Charges, Stripe Subscriptions, A/B Experiment
- **Configurable Size**: Generate 1 to 100,000 rows
- **Reproducible**: Optional random seed for consistent data generation
- **No External Dependencies**: Pure Python, no external APIs or services needed
- **DataFrame Output**: Returns pandas DataFrame ready for downstream processing

## Available Schemas

### Customers
Realistic customer profiles with:
- Customer ID, name, email, phone
- Address (city, state)
- Signup date
- Lifetime value
- Active status

### Orders
E-commerce order data with:
- Order ID, customer ID, date
- Product category
- Item counts and pricing (subtotal, shipping, tax, total)
- Order status

### Products
Product catalog with:
- Product ID, name, category
- Pricing (price, cost, margin)
- Stock quantity
- Ratings and reviews
- Availability status

### Transactions
Financial transaction records with:
- Transaction ID, account ID, timestamp
- Transaction type (deposit, withdrawal, transfer, payment, refund)
- Amount and merchant
- Category and status

### Events
Web/app event logs with:
- Event ID, user ID, session ID, timestamp
- Event type (page_view, click, form_submit, download, purchase, signup, login, logout)
- Page path
- Duration
- Device and browser info

### Sensors
IoT sensor readings with:
- Sensor ID, timestamp
- Sensor type (temperature, humidity, pressure, motion, light, sound)
- Location
- Value with appropriate units
- Status (normal, warning)

### Users
User account data with:
- User ID, username, email
- Role and subscription plan
- Account creation and last login dates
- Login count
- Storage usage
- Verification and active status

### Subscriptions
SaaS subscription rows shaped for **survival analysis** (Kaplan-Meier, Cox):
- subscription_id, plan_tier, days_active, cancelled (event indicator), signup_date
- Tier-dependent churn rates: free churns fast, enterprise barely churns
- Customize via `schema_options.tiers`:
  ```yaml
  schema_options:
    tiers:
      - {name: free, weight: 0.55, daily_churn_rate: 0.020, max_days: 180}
      - {name: pro, weight: 0.35, daily_churn_rate: 0.005, max_days: 365}
      - {name: enterprise, weight: 0.10, daily_churn_rate: 0.001, max_days: 730}
  ```

### Sparse Sensors
IoT sensor readings with **random gaps** for gap-fill and resample demos:
- reading_ts, sensor_id, temperature_c
- Multiple sensors over a configurable timespan with diurnal cycle + noise
- Configurable dropout rate to simulate flaky devices
- Customize via `schema_options`:
  ```yaml
  schema_options:
    sensor_count: 3
    duration_hours: 336      # 14 days * 24
    dropout_rate: 0.25       # ~25% of rows dropped
    base_temp: 22.0
    noise_amplitude: 2.0
    start_date: "2026-04-01"
  ```

### Customer Churn Metrics
Per-customer aggregated state for churn modeling / scoring:
- customer_id, last_activity, total_orders, total_revenue, lifetime_days
- Activity-recency distribution biased toward recent with a long tail
- Customize via `schema_options`:
  ```yaml
  schema_options:
    reference_date: "2026-05-01"   # last_activity computed relative to this
    activity_mix:
      - [0, 30, 5]                 # (low, high, weight)
      - [31, 90, 3]
      - [91, 365, 2]
  ```

### Stripe Charges
Stripe-shaped charge events for revenue / attribution demos. Matches the column shape of Stripe's `charges` API:
- id, _resource_type, customer_id, amount (cents), created (epoch), status
- `schema_options`: `plans` (list of dollar amounts), `lookback_days`

### Stripe Subscriptions
Stripe-shaped subscription rows for SaaS metrics / MRR demos:
- id, _resource_type, customer_id, status (active/trialing/canceled), created, canceled_at, current_period_end, plan_amount (cents), plan_interval, plan_nickname
- Customize via `schema_options`:
  ```yaml
  schema_options:
    plans: [[10, "starter"], [29, "basic"], [49, "pro"], [99, "business"], [199, "enterprise"]]
    plan_weights: [3, 4, 3, 2, 1]
    status_mix: {active: 0.65, trialing: 0.10, canceled: 0.25}
    lookback_days: 540
  ```

### A/B Experiment
Per-user exposure rows for stat-test demos (z-test, t-test, chi-squared):
- experiment_id, user_id, variant (`control`/`treatment`), converted (0/1), exposed_at
- Configurable lift so the treatment shows the level of effect you want
- Customize via `schema_options`:
  ```yaml
  schema_options:
    experiment_id: "exp_001"
    control_conversion: 0.10        # 10% baseline
    lift: 0.3                        # treatment is 30% better (relative)
    treatment_share: 0.5
    lookback_days: 14
  ```
  Set `lift: 0.0` to simulate a null result.

## Configuration

### Required Fields

- **asset_name** (string): Name of the asset to create
- **schema_type** (enum): Type of data to generate
  - Options: `customers`, `orders`, `products`, `transactions`, `events`, `sensors`, `users`, `subscriptions`, `sparse_sensors`, `customer_churn_metrics`, `stripe_charges`, `stripe_subscriptions`, `ab_experiment`
- **row_count** (integer): Number of rows to generate (1-100,000)

### Optional Fields

- **random_state** (integer, optional): Random seed for reproducible data. If not specified, data will be different each time.
- **schema_options** (object, optional): Per-schema knobs. Only used by `subscriptions` (tiers list) and `sparse_sensors` (sensor_count, duration_hours, dropout_rate, base_temp, noise_amplitude, start_date).
- **description** (string): Asset description
- **group_name** (string): Asset group for organization

## Example Usage

### Basic Customer Data
```yaml
type: synthetic_data_generator.SyntheticDataGeneratorComponent

attributes:
  asset_name: demo_customers
  schema_type: customers
  row_count: 1000
```

### Reproducible Orders Data
```yaml
type: synthetic_data_generator.SyntheticDataGeneratorComponent

attributes:
  asset_name: test_orders
  schema_type: orders
  row_count: 500
  random_state: 42
  description: "Test orders dataset for pipeline validation"
  group_name: test_data
```

### IoT Sensor Readings
```yaml
type: synthetic_data_generator.SyntheticDataGeneratorComponent

attributes:
  asset_name: sensor_readings
  schema_type: sensors
  row_count: 10000
  description: "Simulated IoT sensor data over 24 hours"
```

## Output

Returns a pandas DataFrame with columns appropriate for the selected schema type. The DataFrame can be consumed by:
- **DataFrame Transformer**: Apply transformations
- **DuckDB Writer**: Persist to local database
- **File Writer**: Save to CSV/Parquet
- **Data Quality Checks**: Validate with assertion checks

## Demo Pipeline Example

Build a complete demo pipeline:

```
Synthetic Data Generator (customers)
  → DuckDB Writer (raw_customers table)
    → DuckDB Query Reader (SELECT * FROM raw_customers WHERE is_active = true)
      → DataFrame Transformer (add customer_tier column)
        → Enhanced Data Quality Checks
```

## Tips

1. **Start Small**: Use 100-1000 rows during development, scale up for demos
2. **Use Seeds**: Set `random_state` for reproducible demos and tests
3. **Mix Schemas**: Generate multiple schemas to create related datasets
4. **Chain Components**: Combine with transformers, writers, and checks for complete workflows

## Technical Notes

- All dates are generated relative to the current time
- Data distributions are designed to be realistic (e.g., 75% active customers)
- No external APIs or internet connection required
- Lightweight - only depends on pandas

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
