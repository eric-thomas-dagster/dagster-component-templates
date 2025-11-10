# DuckDB Table Writer

Write pandas DataFrames to DuckDB tables with automatic schema creation and flexible write modes.

## Overview

This component reads data from an upstream asset (as a DataFrame) and writes it to a DuckDB database table. Perfect for persisting data locally without needing a database server.

## Features

- **Zero Setup**: No database server needed - DuckDB runs embedded
- **Automatic Schema**: Creates tables automatically from DataFrame structure
- **Flexible Modes**: Create, replace, or append data
- **Efficient Storage**: Columnar storage format for fast queries
- **SQL Ready**: Query data immediately with full SQL support
- **Portable**: Single-file database can be easily shared

## Why DuckDB?

DuckDB is perfect for local data workflows:
- Fast analytical queries on local data
- No server to install or configure
- Integrates seamlessly with pandas
- Full SQL support (joins, aggregations, window functions, etc.)
- Lightweight and portable

## Configuration

### Required Fields

- **asset_name** (string): Name of the asset to create
- **database_path** (string): Path to the DuckDB database file (created if doesn't exist)
- **table_name** (string): Name of the table to create/update

### Optional Fields

- **write_mode** (enum): How to write data
  - `"replace"` (default): Drop table if exists, create new with data
  - `"create"`: Fail if table already exists
  - `"append"`: Add rows to existing table (create if doesn't exist)
- **description** (string): Asset description
- **group_name** (string): Asset group for organization

### Connecting Upstream Data

This component expects to receive a DataFrame from an upstream asset. Use the **Custom Lineage UI** in Dagster Designer to connect an upstream asset (like Synthetic Data Generator, CSV File Ingestion, or DataFrame Transformer) to this writer component. The lineage connection will automatically pass the DataFrame to the writer.

## Example Usage

### Basic Usage
```yaml
type: duckdb_table_writer.DuckDBTableWriterComponent

attributes:
  asset_name: customers_to_db
  database_path: "{{ project_root }}/data/demo.duckdb"
  table_name: customers
  write_mode: replace
```

Then use the Custom Lineage UI to connect `demo_customers` → `customers_to_db`.

### Append Mode for Accumulating Data
```yaml
type: duckdb_table_writer.DuckDBTableWriterComponent

attributes:
  asset_name: sensor_data_writer
  database_path: "{{ project_root }}/data/sensors.duckdb"
  table_name: readings
  write_mode: append
  description: "Append new sensor readings to historical table"
```

Connect `sensor_readings` → `sensor_data_writer` in the Custom Lineage UI.

### Multiple Tables in One Database
```yaml
# Write customers
type: duckdb_table_writer.DuckDBTableWriterComponent
attributes:
  asset_name: write_customers
  database_path: "{{ project_root }}/data/ecommerce.duckdb"
  table_name: customers
  write_mode: replace

---
# Write orders (same database)
type: duckdb_table_writer.DuckDBTableWriterComponent
attributes:
  asset_name: write_orders
  database_path: "{{ project_root }}/data/ecommerce.duckdb"
  table_name: orders
  write_mode: replace
```

Connect the upstream assets in the Custom Lineage UI:
- `customers_data` → `write_customers`
- `orders_data` → `write_orders`

## Upstream Dependencies

This component requires an upstream asset that produces a pandas DataFrame. Compatible with:
- **Synthetic Data Generator**: Mock data
- **CSV File Ingestion**: File data
- **REST API Fetcher**: API responses
- **DataFrame Transformer**: Transformed data
- **Database Query**: Query results
- Any custom asset returning a DataFrame

## Write Modes Explained

### Replace Mode (Default)
- **Use when**: You want fresh data each run
- **Behavior**: Drops existing table and creates new one
- **Result**: Table always matches source DataFrame exactly

### Create Mode
- **Use when**: You want to ensure table doesn't exist
- **Behavior**: Fails if table already exists
- **Result**: Prevents accidental overwrites

### Append Mode
- **Use when**: You want to accumulate data over time
- **Behavior**: Adds rows to existing table, creates if doesn't exist
- **Result**: Table grows with each run
- **Note**: Ensure DataFrame schema matches existing table

## Demo Pipeline Example

```
Synthetic Data Generator (customers, 1000 rows)
  → DuckDB Table Writer (write to demo.duckdb/customers)
    → DuckDB Query Reader (SELECT * FROM customers WHERE lifetime_value > 5000)
      → Data Quality Checks
```

## Querying Your Data

After writing data, you can query it with SQL:

```python
import duckdb

# Connect to your database
con = duckdb.connect('data/demo.duckdb')

# Run queries
result = con.execute("""
    SELECT
        state,
        COUNT(*) as customer_count,
        AVG(lifetime_value) as avg_ltv
    FROM customers
    WHERE is_active = true
    GROUP BY state
    ORDER BY customer_count DESC
""").df()

print(result)
```

Or use the **DuckDB Query Reader** component to create assets from queries!

## Tips

1. **Organize by Database**: Group related tables in the same database file
2. **Use Replace for Dev**: Replace mode ensures clean state during development
3. **Use Append for History**: Append mode for accumulating time-series or event data
4. **Path Placeholders**: Use `{{ project_root }}` for portable paths
5. **Performance**: DuckDB is optimized for analytical queries - perfect for aggregations

## Technical Notes

- Database file is created automatically if it doesn't exist
- Parent directories are created as needed
- Tables are created with schema inferred from DataFrame
- DuckDB supports most pandas dtypes automatically
- Single database file can contain many tables
- No additional setup or server configuration required
