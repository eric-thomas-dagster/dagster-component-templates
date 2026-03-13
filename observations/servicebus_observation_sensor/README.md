# Servicebus Observation Sensor

Periodically connects to Servicebus and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Servicebus using the provided credentials
2. Queries the resource for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalServiceBusAsset to observe |
| `namespace` | `str` | `**required**` | Azure Service Bus namespace |
| `queue_name` | `Optional[str]` | `None` |  |
| `topic_name` | `Optional[str]` | `None` | Topic name |
| `subscription_name` | `Optional[str]` | `None` |  |
| `connection_string_env_var` | `Optional[str]` | `None` | Env var with connection string |
| `check_interval_seconds` | `int` | `60` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.ServiceBusObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  namespace: my_namespace
  # queue_name: None  # optional
  # topic_name: None  # optional
  # subscription_name: None  # optional
  # connection_string_env_var: None  # optional
  # check_interval_seconds: 60  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalServiceBusAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalServiceBusAsset
attributes:
  asset_key: external/servicebus

---

# 2. Observe it
type: dagster_component_templates.ServiceBusObservationSensorComponent
attributes:
  sensor_name: servicebus_observation_sensor_sensor
  asset_key: external/servicebus
```

## Requirements

```
azure-servicebus>=7.0.0
azure-identity>=1.10.0
```
