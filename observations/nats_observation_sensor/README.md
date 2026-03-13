# Nats Observation Sensor

Periodically connects to Nats and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Nats using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalNatsAsset to observe |
| `servers` | `str` | `**required**` | Comma-separated NATS server URLs |
| `stream_name` | `str` | `**required**` | JetStream stream name |
| `credentials_env_var` | `Optional[str]` | `None` | Env var with path to NATS .creds file |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.NatsObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  servers: my_servers
  stream_name: my_stream_name
  # credentials_env_var: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalNatsAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalNatsAsset
attributes:
  asset_key: external/nats

---

# 2. Observe it
type: dagster_component_templates.NatsObservationSensorComponent
attributes:
  sensor_name: nats_observation_sensor_sensor
  asset_key: external/nats
```

## Requirements

```
nats-py>=2.3.0
```
