# External Sharepoint Library

Declares a Sharepoint Library library as an external asset in the Dagster asset graph — making it visible for lineage without materializing it from Dagster.

## What this does

External assets represent data sources managed outside of Dagster. Adding one to your project:
- Makes the Sharepoint Library library visible as a node in the **Dagster asset graph**
- Enables downstream assets to declare `deps` on it, drawing lineage edges
- Records connection metadata in the **Asset Catalog**

This component does not read from or write to Sharepoint Library. It is a **lineage declaration only**.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_key` | `str` | `**required**` | Dagster asset key |
| `site_url` | `str` | `**required**` | SharePoint site URL |
| `library_name` | `str` | `**required**` | Document library name |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |
| `description` | `Optional[str]` | `None` | Human-readable description |

## Example

```yaml
type: dagster_component_templates.ExternalSharePointLibraryAsset
attributes:
  asset_key: my_service/asset
  site_url: my_site_url
  library_name: my_library_name
  # group_name: None  # optional
  # description: None  # optional
```

## Pair with the observation sensor

Use the companion observation sensor to periodically health-check the library and record metrics as `AssetObservation` events:

```yaml
# 1. Declare the external asset (lineage node)
type: dagster_component_templates.ExternalSharePointLibraryAsset
attributes:
  asset_key: external/sharepoint_library
  site_url: SITE_URL
  library_name: my_library

---

# 2. Observe it on a schedule
type: dagster_component_templates.SharePointLibraryObservationSensorComponent
attributes:
  sensor_name: sharepoint_library_observer
  asset_key: external/sharepoint_library
  asset_key: external/sharepoint_library
  site_url: SITE_URL
  library_name: my_library
```

## Requirements

No additional packages required — this component only creates a `dg.AssetSpec`.
