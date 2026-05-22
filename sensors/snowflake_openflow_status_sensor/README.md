# Snowflake OpenFlow Status Sensor

Polls `SNOWFLAKE.TELEMETRY.EVENTS` for `openflow_metric` rows on a named flow and emits `AssetMaterialization` when the flow's most recent activity timestamp advances. Optionally fires a `RunRequest` to chain downstream Dagster work.

OpenFlow owns the schedule and the runtime; Dagster surfaces flow activity in the catalog by polling Snowflake's telemetry. Pair with `external_snowflake_openflow_flow` so the catalog node + materialization history come from the same `asset_key`.

## Three operating modes

| `asset_key` | `job_name` | Behavior |
|---|---|---|
| set | unset | **Observe-only** — emit AssetMaterialization on activity advance, no downstream trigger |
| unset | set | **Trigger-only** — fire RunRequest on each activity advance |
| set | set | **Both** — record + trigger |

## Example

```yaml
type: dagster_community_components.SnowflakeOpenflowStatusSensorComponent
attributes:
  sensor_name: customer_sync_done
  flow_name: customer_sync
  asset_key: snowflake/openflow/customer_sync
  account_env_var: SNOWFLAKE_ACCOUNT
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
  warehouse_env_var: SNOWFLAKE_WAREHOUSE
  minimum_interval_seconds: 120
  default_status: stopped
```

## How activity is detected

The sensor queries:

```sql
SELECT MAX(TIMESTAMP) AS latest_activity, COUNT(*) AS event_count,
       MAX(RECORD['runtime_id']::STRING) AS runtime_id
FROM SNOWFLAKE.TELEMETRY.EVENTS
WHERE RECORD_TYPE = 'openflow_metric'
  AND RECORD['process_group_name']::STRING = '<flow_name>'
  AND TIMESTAMP >= DATEADD('minute', -<lookback_minutes>, CURRENT_TIMESTAMP())
```

If `latest_activity` is newer than the sensor cursor, the sensor fires.

## Requirements

```
snowflake-connector-python>=3.0.0
```
