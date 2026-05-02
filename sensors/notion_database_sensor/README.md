# NotionDatabaseSensor

Polls a Notion database via the official API. Triggers a RunRequest for each row whose `last_edited_time` is after the cursor. Token-authed.

## Example

```yaml
type: dagster_component_templates.NotionDatabaseSensorComponent
attributes:
  sensor_name: notion_release_notes_sensor
  asset_keys: [release_notes_doc]
  database_id: "abcd1234-..."
  api_key_env_var: NOTION_API_KEY
  minimum_interval_seconds: 600
```

## Requirements

```
requests
```
