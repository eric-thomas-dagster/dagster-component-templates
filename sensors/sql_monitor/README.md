# SQL Monitor Sensor

Monitor a SQL database table for new or updated rows and trigger jobs automatically.

## Overview

This sensor queries a database table for rows where a watermark column (e.g., `updated_at`, `created_at`, or an auto-increment `id`) is greater than the last-seen value. The watermark is stored in the sensor cursor. One RunRequest is created per matching row.

Works with any SQLAlchemy-compatible database: **PostgreSQL**, **MySQL**, **SQL Server**, **SQLite**, **Snowflake**, **BigQuery**, **Redshift**, and more.

## Configuration

### Required
- **sensor_name** - Unique sensor name
- **connection_string_env_var** - Env var containing the SQLAlchemy connection string
- **table_name** - Table to monitor (use `schema.table` for non-default schemas)
- **watermark_column** - Column to track new/updated rows (e.g., `updated_at`, `id`)
- **job_name** - Job to trigger per row

### Optional
- **id_column** (default: same as `watermark_column`) — Column used as the run_key for deduplication
- **batch_size** (default `100`) — Max rows returned per evaluation
- **minimum_interval_seconds** (default `60`)
- **default_status** (`running` | `stopped`)

## Connection Strings

```bash
# PostgreSQL
DATABASE_URL=postgresql://user:pass@host:5432/mydb

# MySQL
DATABASE_URL=mysql+pymysql://user:pass@host:3306/mydb

# SQL Server
DATABASE_URL=mssql+pyodbc://user:pass@host/mydb?driver=ODBC+Driver+17+for+SQL+Server

# Snowflake
DATABASE_URL=snowflake://user:pass@account/mydb/myschema?warehouse=compute_wh

# SQLite (local file)
DATABASE_URL=sqlite:///path/to/database.db
```

## Usage

### Monitor new orders by timestamp
```yaml
type: dagster_component_templates.SQLMonitorSensorComponent
attributes:
  sensor_name: new_orders_sensor
  connection_string_env_var: DATABASE_URL
  table_name: orders
  watermark_column: created_at
  id_column: order_id
  job_name: process_new_order
  batch_size: 50
  minimum_interval_seconds: 60
```

### Monitor updated records by auto-increment ID
```yaml
type: dagster_component_templates.SQLMonitorSensorComponent
attributes:
  sensor_name: events_sensor
  connection_string_env_var: DATABASE_URL
  table_name: events
  watermark_column: id
  job_name: process_event
  batch_size: 200
  minimum_interval_seconds: 30
```

### Monitor a non-default schema
```yaml
type: dagster_component_templates.SQLMonitorSensorComponent
attributes:
  sensor_name: staging_sensor
  connection_string_env_var: DATABASE_URL
  table_name: staging.inbound_records
  watermark_column: inserted_at
  id_column: record_id
  job_name: process_inbound_record
```

## Run Config Schema

```python
{
  "ops": {
    "config": {
      "table_name": str,          # Table name
      "watermark_column": str,    # Watermark column name
      "watermark_value": str,     # Watermark value for this row
      "row_id_column": str,       # ID column name
      "row_id": str,              # ID value for this row
      "row": str,                 # Full row as JSON string (all values stringified)
      "columns": str              # Column names as JSON list
    }
  }
}
```

## Usage with Assets

```python
from dagster import asset, Config, AssetExecutionContext
import json

class SQLRowConfig(Config):
    table_name: str
    watermark_column: str
    watermark_value: str
    row_id_column: str
    row_id: str
    row: str
    columns: str

@asset
def process_new_order(context: AssetExecutionContext, config: SQLRowConfig):
    row = json.loads(config.row)
    order_id = row["order_id"]
    customer_id = row["customer_id"]
    total = row["total"]

    context.log.info(f"Processing order {order_id} for customer {customer_id}: ${total}")
    return row
```

## First-Run Behavior

On the very first evaluation, the sensor initializes the watermark to the current `MAX(watermark_column)` and returns a skip — it does **not** process historical rows. This prevents a thundering herd of RunRequests on startup. All rows inserted/updated after the first run will be processed.

To process historical rows, reset the sensor cursor to `""` or a specific watermark value in the Dagster UI.

## Performance Considerations

1. **Index the watermark column** — The sensor runs `WHERE watermark_column > :val ORDER BY watermark_column` on every evaluation. A B-tree index on the watermark column is essential for large tables.
2. **Use `id` as watermark for append-only tables** — Auto-increment integer IDs are faster to index and compare than timestamps.
3. **Avoid wide tables** — The sensor returns full rows. For very wide tables, consider using a view with only the columns your job needs.

## Troubleshooting

### Issue: "Failed to create database engine"
Verify the connection string is correct and the database is reachable:
```bash
python -c "from sqlalchemy import create_engine; create_engine('$DATABASE_URL').connect()"
```

### Issue: Rows processed multiple times
Check that `watermark_column` values are strictly increasing. For `updated_at` columns using second-precision timestamps, two rows updated in the same second can cause re-processing. Use millisecond-precision timestamps or an auto-increment ID instead.

### Issue: Sensor never finds new rows
Confirm the `watermark_column` is being set on insert/update:
```sql
SELECT MAX(updated_at) FROM orders;
```

## Requirements

- Python 3.8+, Dagster 1.5.0+, sqlalchemy>=2.0.0
- Database-specific driver (see requirements.txt)

## License

MIT License
