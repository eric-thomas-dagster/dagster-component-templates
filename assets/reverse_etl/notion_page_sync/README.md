# `NotionPageSyncComponent`

Reverse-ETL sink: keep a **specific Notion page's** properties (and optionally its markdown body) in sync with the first row of an upstream DataFrame. Property values are serialized based on the page's existing property types, which Notion returns on `pages.retrieve`. Set a property to `None` to clear it.

## When to use

- Keep a single "status dashboard" or "KPI snapshot" Notion page's properties updated from a warehouse query, without needing a whole database.

For creating brand-new pages on every run, use `notion_resource` directly from your own asset -- that's not this component's materialization pattern (this is for one persistent page, not spawning many).

## Pairs with

- **`notion_resource`** — connection (required).
- **`notion_database_upsert`** — multi-row analogue (for mirroring a whole table into a database instead of one page).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `page_id` | required | Notion page ID to keep in sync (UUID with or without dashes). |
| `resource_key` | optional (default `notion_resource`) | Resource key registered by NotionResourceComponent. |
| `properties_map` | optional | Upstream column -> Notion property name. |
| `markdown_column` | optional | Column whose value is written to the page body as markdown. |
| `row_index` | optional (default `0`) | Which upstream row to sync from. |

## Example
```yaml
type: dagster_component_templates.NotionPageSyncComponent
attributes:
  asset_name: notion_kpi_dashboard
  upstream_asset_key: kpi_snapshot
  page_id: "abc123def456"
  resource_key: notion_resource
  properties_map:
    revenue: Revenue
    active_users: Active Users
    status: Status
  markdown_column: report_markdown
```
