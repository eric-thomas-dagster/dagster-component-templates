# Pulsar Observation Sensor

Periodically connects to Pulsar and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Pulsar using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalPulsarAsset to observe |
| `service_url` | `str` | `**required**` | Pulsar service URL |
| `topic` | `str` | `**required**` | Pulsar topic name |
| `admin_url` | `Optional[str]` | `None` |  |
| `jwt_token_env_var` | `Optional[str]` | `None` | Env var with JWT auth token |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.PulsarObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  service_url: my_service_url
  topic: my_topic
  # admin_url: None  # optional
  # jwt_token_env_var: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalPulsarAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalPulsarAsset
attributes:
  asset_key: external/pulsar

---

# 2. Observe it
type: dagster_component_templates.PulsarObservationSensorComponent
attributes:
  sensor_name: pulsar_observation_sensor_sensor
  asset_key: external/pulsar
```

## Requirements

```
pulsar-client>=3.0.0
```
