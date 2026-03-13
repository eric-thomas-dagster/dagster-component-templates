# External Sftp Path

Declares a Sftp Path path as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Sftp Path path visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Sftp Path. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `host` | `str` | `**required**` | SFTP host |
| `remote_path` | `str` | `**required**` | Remote directory path |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalSftpPathAsset
attributes:
  asset_key: my_service/asset
  host: my-service.internal.company.com
  remote_path: ./path/to/file
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the path and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalSftpPathAsset
attributes:
  asset_key: external/sftp_path
  host: my-sftp-path.internal
  remote_path: REMOTE_PATH

---

# 2. Observe it on a schedule
type: dagster_component_templates.SftpPathObservationSensorComponent
attributes:
  sensor_name: sftp_path_observer
  asset_key: external/sftp_path
  asset_key: external/sftp_path
  host: my-sftp-path.internal
  remote_path: REMOTE_PATH
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
