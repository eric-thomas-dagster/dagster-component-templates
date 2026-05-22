# External Snowflake OpenFlow Flow Asset

Declares a Snowflake [OpenFlow](https://docs.snowflake.com/en/user-guide/openflow/about) process group as an external Dagster asset. OpenFlow runs the flow on its own infra (BYOC EKS or Snowflake-managed); Dagster declares + observes via the paired sensor.

## Pattern

1. **Declare** the OpenFlow flow:
   ```yaml
   type: dagster_community_components.ExternalSnowflakeOpenflowFlowAsset
   attributes:
     asset_key: snowflake/openflow/customer_sync
     flow_name: customer_sync
   ```
2. **Pair** with [`snowflake_openflow_status_sensor`](https://dagster-component-ui.vercel.app/c/snowflake_openflow_status_sensor) (same `asset_key`):
   ```yaml
   type: dagster_community_components.SnowflakeOpenflowStatusSensorComponent
   attributes:
     sensor_name: customer_sync_done
     flow_name: customer_sync
     asset_key: snowflake/openflow/customer_sync
     account_env_var: SNOWFLAKE_ACCOUNT
     # ...
   ```

The sensor polls `SNOWFLAKE.TELEMETRY.EVENTS` for `openflow_metric` rows; on completion it emits `AssetMaterialization` for the asset_key — lighting up this external asset's materialization history.

## See also

- [`snowflake_openflow_status_sensor`](https://dagster-component-ui.vercel.app/c/snowflake_openflow_status_sensor) — paired sensor
- [`snowflake_workspace`](https://dagster-component-ui.vercel.app/c/snowflake_workspace) — multi-entity auto-discovery (includes OpenFlow flows as observable assets)
