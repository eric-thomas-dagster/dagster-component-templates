# External Servicebus Asset

Declares a Servicebus resource as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Servicebus resource visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Servicebus. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `namespace` | `str` | `**required**` | Azure Service Bus namespace |
| `queue_name` | `Optional[str]` | `None` |  |
| `topic_name` | `Optional[str]` | `None` |  |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalServiceBusAsset
attributes:
  asset_key: my_service/asset
  namespace: my_namespace
  # queue_name: None  # optional
  # topic_name: None  # optional
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the resource and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalServiceBusAsset
attributes:
  asset_key: external/servicebus
  namespace: my_namespace

---

# 2. Observe it on a schedule
type: dagster_component_templates.ServiceBusObservationSensorComponent
attributes:
  sensor_name: servicebus_asset_observer
  asset_key: external/servicebus
  asset_key: external/servicebus
  namespace: my_namespace
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
