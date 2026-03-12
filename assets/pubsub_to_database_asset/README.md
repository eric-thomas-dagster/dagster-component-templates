# Pub/Sub to Database Asset

Pulls messages from a Google Cloud Pub/Sub subscription and writes them to a database table via SQLAlchemy. Messages are acknowledged after successful write.

## Required packages

```
google-cloud-pubsub>=2.18.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `project_id` | str | yes | — | GCP project ID |
| `subscription_id` | str | yes | — | Pub/Sub subscription ID |
| `credentials_env_var` | str | no | null | Env var pointing to GCP service account JSON path |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_messages` | int | no | 10000 | Max messages to pull per run |
| `pull_batch_size` | int | no | 1000 | Messages per pull request (max 1000) |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.PubSubToDatabaseAssetComponent
attributes:
  asset_name: pubsub_events_ingest
  project_id: my-gcp-project
  subscription_id: events-sub
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  max_messages: 10000
```

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
