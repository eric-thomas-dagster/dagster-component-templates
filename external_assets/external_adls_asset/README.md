# External Adls Asset

Declares a Adls storage container as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Adls storage container visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Adls. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `account_name` | `str` | `**required**` | Azure storage account name |
| `container_name` | `str` | `**required**` | ADLS container / filesystem name |
| `path_prefix` | `str` | `""` | Path prefix within the container |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalAdlsAsset
attributes:
  asset_key: my_service/asset
  account_name: my_account_name
  container_name: my_container_name
  # path_prefix: ""  # optional
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the storage container and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalAdlsAsset
attributes:
  asset_key: external/adls
  account_name: my_account
  container_name: my_container

---

# 2. Observe it on a schedule
type: dagster_component_templates.AdlsObservationSensorComponent
attributes:
  sensor_name: adls_asset_observer
  asset_key: external/adls
  asset_key: external/adls
  account_name: my_account
  container_name: my_container
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
