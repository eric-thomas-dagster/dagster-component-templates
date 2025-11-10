# Synthetic Data Generator

Generate realistic synthetic/mock data for demos, testing, and prototyping without needing external data sources.

## Overview

This component creates realistic fake data based on pre-defined schemas. Perfect for:
- **Demonstrations**: Show off your pipeline without setting up real data sources
- **Testing**: Test data transformations and quality checks with known data
- **Prototyping**: Build pipelines before connecting to production data
- **Training**: Create safe, realistic datasets for learning and experimentation

## Features

- **7 Pre-defined Schemas**: Customers, Orders, Products, Transactions, Events, Sensors, Users
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

## Configuration

### Required Fields

- **asset_name** (string): Name of the asset to create
- **schema_type** (enum): Type of data to generate
  - Options: `customers`, `orders`, `products`, `transactions`, `events`, `sensors`, `users`
- **row_count** (integer): Number of rows to generate (1-100,000)

### Optional Fields

- **random_seed** (integer, optional): Random seed for reproducible data. If not specified, data will be different each time.
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
  random_seed: 42
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
2. **Use Seeds**: Set `random_seed` for reproducible demos and tests
3. **Mix Schemas**: Generate multiple schemas to create related datasets
4. **Chain Components**: Combine with transformers, writers, and checks for complete workflows

## Technical Notes

- All dates are generated relative to the current time
- Data distributions are designed to be realistic (e.g., 75% active customers)
- No external APIs or internet connection required
- Lightweight - only depends on pandas
