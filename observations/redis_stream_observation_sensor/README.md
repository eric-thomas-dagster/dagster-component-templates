# Redis Stream Observation Sensor

Periodically connects to Redis Stream and records an `AssetObservation` on the target external asset — surfacing health metrics in the Dagster UI without triggering a pipeline run.

## What this does

On each sensor tick, this component:
1. Connects to Redis Stream using the provided credentials
2. Queries the stream for current health metrics
3. Records an `AssetObservation` on the target asset with the collected data

Observations appear in the **Asset Activity** timeline, giving you a health history viewable in the Dagster UI.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `**required**` | Unique sensor name |
| `asset_key` | `str` | `**required**` | Asset key of the ExternalRedisStreamAsset to observe |
| `stream_name` | `str` | `**required**` | Redis stream name |
| `host` | `str` | `"localhost"` | Redis host |
| `port` | `int` | `6379` | Redis port |
| `db` | `int` | `0` | Redis database index |
| `password_env_var` | `Optional[str]` | `None` | Env var with Redis password |
| `check_interval_seconds` | `int` | `60` | Seconds between health checks |
| `resource_key` | `Optional[str]` | `None` | Optional Dagster resource key. |

## Example

```yaml
type: dagster_component_templates.RedisStreamObservationSensorComponent
attributes:
  sensor_name: my_sensor_name
  asset_key: my_service/asset
  stream_name: my_stream_name
  # host: "localhost"  # optional
  # port: 6379  # optional
  # db: 0  # optional
  # password_env_var: None  # optional
  # check_interval_seconds: 60  # optional
  # resource_key: None  # optional
```

## Pair with the external asset

This sensor observes an external asset declared with `ExternalRedisStreamAsset`. Declare the external asset first:

```yaml
# 1. Declare the external asset
type: dagster_component_templates.ExternalRedisStreamAsset
attributes:
  asset_key: external/redis_stream

---

# 2. Observe it
type: dagster_component_templates.RedisStreamObservationSensorComponent
attributes:
  sensor_name: redis_stream_observation_sensor_sensor
  asset_key: external/redis_stream
```

## Requirements

```
redis>=4.0.0
```
