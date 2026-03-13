# Pulsar to Database Asset

Consumes a batch of messages from an Apache Pulsar topic using a shared subscription and writes them to a database table via SQLAlchemy. Messages are acknowledged after a successful write.

## Required packages

```
pulsar-client>=3.3.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `service_url_env_var` | str | yes | — | Env var with Pulsar service URL (pulsar://host:6650) |
| `topic` | str | yes | — | Pulsar topic (e.g. persistent://public/default/events) |
| `subscription_name` | str | no | `dagster-ingest` | Pulsar subscription name |
| `subscription_type` | str | no | `Shared` | Exclusive, Shared, Failover, or KeyShared |
| `token_env_var` | str | no | null | Env var with Pulsar JWT auth token |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_messages` | int | no | 10000 | Max messages to consume per run |
| `receive_timeout_ms` | int | no | 5000 | Timeout in ms for each receive call |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.PulsarToDatabaseAssetComponent
attributes:
  asset_name: pulsar_events_ingest
  service_url_env_var: PULSAR_SERVICE_URL
  topic: persistent://public/default/events
  subscription_name: dagster-ingest
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
