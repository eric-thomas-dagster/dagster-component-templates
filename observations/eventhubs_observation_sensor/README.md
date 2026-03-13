# Eventhubs Observation Sensor

Periodically connects to Eventhubs and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Eventhubs using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalEventHubsAsset to observe |
| `namespace` | `str` | `**required**` | Azure Event Hubs namespace |
| `eventhub_name` | `str` | `**required**` | Event Hub name |
| `connection_string_env_var` | `Optional[str]` | `None` | Env var with connection string |
| `check_interval_seconds` | `int` | `300` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.EventHubsObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  namespace: my_namespace
  eventhub_name: my_eventhub_name
  # connection_string_env_var: None  # optional
  # check_interval_seconds: 300  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalEventHubsAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalEventHubsAsset
attributes:
  asset_key: external/eventhubs

---

# 2. Observe it
type: dagster_component_templates.EventHubsObservationSensorComponent
attributes:
  sensor_name: eventhubs_observation_sensor_sensor
  asset_key: external/eventhubs
```

## Requirements

```
azure-eventhub>=5.10.0
azure-identity>=1.10.0
```
