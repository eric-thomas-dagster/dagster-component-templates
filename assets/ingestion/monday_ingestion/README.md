# Monday.com Ingestion

Ingest monday.com board/item/user data via its GraphQL API.

> **Note:** monday.com's API is GraphQL-only (a single POST endpoint at `api.monday.com/v2`) -- there is no REST surface. This connector does **not** use dlt's generic `rest_api_source` (that machinery assumes REST paths/pagination and would be a poor, misleading fit). Instead it defines plain `@dlt.resource` generators that POST fixed GraphQL query strings directly via `requests`, then hands those resources to a normal dlt pipeline -- so destination flexibility (snowflake/bigquery/postgres/filesystem/duckdb/etc, same as every other ingestion component in this repo) is preserved even though the fetch mechanism differs.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | monday.com personal or API token, sent raw in the Authorization header. |
| `board_id` | optional | Specific board ID to pull items from (enables the `items` resource). |
| `resources` | optional | Comma-separated list of resources to extract: boards, users, items. Default: `boards,users` |

## Partitions

None of `boards`, `users`, or `items` are bound to the partition window -- `boards`/`users` are account-level snapshots, and `items` pages through ALL items on the configured board regardless of partition. Declaring `partition_type` controls Dagster's materialization/backfill schedule only.

## Scope note

`boards` and `users` are single-page pulls (up to 100 each) -- monday.com's pagination story for these top-level list queries was lower-confidence than `items`' cursor mechanism (`items_page`/`next_items_page`), which this connector paginates fully. If you have more than 100 boards or users, extend the query loop.

## Example
```yaml
type: dagster_component_templates.MondayIngestionComponent
attributes:
  asset_name: monday_ingestion
  api_token: "${MONDAY_API_TOKEN}"
  board_id: "1234567890"
  resources: "boards,users,items"
```
