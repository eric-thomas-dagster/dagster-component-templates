# EventLogToSnowflakeJobComponent

Op-shaped job that streams new Dagster event log entries into a
Snowflake table via `snowflake-connector-python` INSERT batches.
Ideal for BI on Dagster reliability from Snowflake-native BI tools.

## Target table DDL

Create the table once:

```sql
CREATE TABLE dagster_events (
  storage_id NUMBER,
  timestamp NUMBER,
  timestamp_ts TIMESTAMP_NTZ,
  run_id STRING,
  job_name STRING,
  event_type STRING,
  step_key STRING,
  asset_key STRING,
  message STRING
);
```

## YAML example

```yaml
type: dagster_component_templates.EventLogToSnowflakeJobComponent
attributes:
  job_name: dagster_events_to_snowflake
  schedule: "*/15 * * * *"
  default_status: RUNNING
  account: abc12345.us-east-1
  user: DAGSTER_INGEST
  password_env: SNOWFLAKE_PASSWORD
  warehouse: LOAD_WH
  database: ANALYTICS
  snowflake_schema: PLATFORM_OBSERVABILITY
  table: DAGSTER_EVENTS
```

## Auth

Username + password (from env). For SSO / key-pair auth, extend the
component or fall back to the sink-asset alternative.

## Behavior

- Cursor via run tag; first run pulls `initial_lookback_hours` of history.
- Uses `INSERT ... VALUES (...)` via `cursor.executemany`. For very large batches consider swapping in `PUT` + `COPY INTO` for better throughput.
