# External Rabbitmq Asset

Declares a Rabbitmq queue as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Rabbitmq queue visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Rabbitmq. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `host` | `str` | `**required**` | RabbitMQ host |
| `queue_name` | `str` | `**required**` | RabbitMQ queue name |
| `virtual_host` | `str` | `"/"` | RabbitMQ virtual host |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalRabbitmqAsset
attributes:
  asset_key: my_service/asset
  host: my-service.internal.company.com
  queue_name: my_queue_name
  # virtual_host: "/"  # optional
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the queue and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalRabbitmqAsset
attributes:
  asset_key: external/rabbitmq
  host: my-rabbitmq.internal
  queue_name: my_queue

---

# 2. Observe it on a schedule
type: dagster_component_templates.RabbitmqObservationSensorComponent
attributes:
  sensor_name: rabbitmq_asset_observer
  asset_key: external/rabbitmq
  asset_key: external/rabbitmq
  host: my-rabbitmq.internal
  queue_name: my_queue
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
