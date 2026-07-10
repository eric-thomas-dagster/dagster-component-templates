# NotionWorkspaceComponent

Auto-emit one Dagster asset per **Notion database** that your integration has been shared to. `StateBackedComponent` — discovery cached to disk. Materializing an asset queries the database and flattens each page's properties into a DataFrame row.

The workspace-shape peer of `notion_ingestion`.

## Example

```yaml
type: dagster_community_components.NotionWorkspaceComponent
attributes:
  access_token_env_var: NOTION_ACCESS_TOKEN
  database_selector:
    by_pattern: ["*Roadmap*", "*Tasks*"]
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Property flattening

Notion properties come back as `{name: {type: <t>, <t>: <value>}}`. The component flattens common types:

| Notion type | Emitted as |
|---|---|
| `title`, `rich_text` | concatenated plain-text string |
| `number` | number |
| `select` | option name |
| `multi_select` | comma-separated option names |
| `date` | `start` value |
| `checkbox` | bool |
| `url`, `email` | string |
| `people` | comma-separated names |
| other | raw value (fallback) |

## Access

The Notion integration only sees databases explicitly shared with it. Share your relevant databases with the integration in Notion's UI (Share → Connections) before running the workspace refresh.

## Related

- `notion_resource`
- `notion_ingestion` — single-database counterpart
- `notion_database_sensor` — event-drive on database changes
