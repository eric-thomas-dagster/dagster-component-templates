# Rabbitmq Observation Sensor

Periodically connects to Rabbitmq and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Rabbitmq using the provided credentials
2. Queries the queue for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalRabbitmqAsset to observe |
| `host` | `str` | `**required**` | RabbitMQ host |
| `queue_name` | `str` | `**required**` | RabbitMQ queue name |
| `virtual_host` | `str` | `"/"` | RabbitMQ virtual host |
| `port` | `int` | `5672` | AMQP port |
| `username_env_var` | `Optional[str]` | `None` | Env var with username |
| `password_env_var` | `Optional[str]` | `None` | Env var with password |
| `check_interval_seconds` | `int` | `60` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.RabbitmqObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  host: my-service.internal.company.com
  queue_name: my_queue_name
  # virtual_host: "/"  # optional
  # port: 5672  # optional
  # username_env_var: None  # optional
  # password_env_var: None  # optional
  # check_interval_seconds: 60  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalRabbitmqAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalRabbitmqAsset
attributes:
  asset_key: external/rabbitmq

---

# 2. Observe it
type: dagster_component_templates.RabbitmqObservationSensorComponent
attributes:
  sensor_name: rabbitmq_observation_sensor_sensor
  asset_key: external/rabbitmq
```

## Requirements

```
pika>=1.3.0
```
