# `NotionDatabaseUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into a **Notion database**. Each row is matched against existing pages by `key_property`; matches are updated, misses are inserted.

Property values are serialized based on the database's schema, retrieved from the Notion API at materialize time -- unlike a hand-maintained property-type map, this auto-detects each property's type (title/rich_text/number/select/etc.) from the live schema.

## When to use

- Sync computed data (scores, statuses) from a warehouse INTO a Notion database so non-technical teams can view/filter/sort it in Notion.

## Pairs with

- **`notion_resource`** — connection (required).
- **`notion_page_sync`** — single-page analogue (for keeping one specific page in sync instead of a whole table).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `database_id` | required | Notion database ID (UUID). |
| `resource_key` | optional (default `notion_resource`) | Resource key registered by NotionResourceComponent. |
| `key_property` | required | Notion property name that uniquely identifies a row. |
| `key_column` | required | Upstream column holding the value that matches key_property. |
| `properties_map` | required | Upstream column -> Notion property name. |
| `delete_missing` | optional (default `false`) | Archive pages whose key value is not in the upstream DataFrame. |
| `batch_size` | optional (default `100`) | Max upstream rows to process per run. |

## Example
```yaml
type: dagster_component_templates.NotionDatabaseUpsertComponent
attributes:
  asset_name: notion_incidents_mirror
  upstream_asset_key: incidents_current
  database_id: "abc123def456"
  resource_key: notion_resource
  key_property: Incident ID
  key_column: incident_id
  properties_map:
    incident_id: Incident ID
    title: Name
    severity: Severity
    status: Status
    opened_at: Opened
```
