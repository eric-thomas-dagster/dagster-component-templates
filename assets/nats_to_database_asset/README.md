# NATS to Database Asset

Fetches messages from a NATS subject or JetStream consumer and writes them to a database table via SQLAlchemy. Supports both core NATS (ephemeral) and JetStream (durable, acknowledged).

## Required packages

```
nats-py>=2.6.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `nats_url_env_var` | str | yes | — | Env var with NATS server URL (nats://host:4222) |
| `subject` | str | yes | — | NATS subject (supports wildcards * and >) |
| `use_jetstream` | bool | no | false | Use JetStream for durable consumption |
| `stream_name` | str | no | null | JetStream stream name (required if use_jetstream=true) |
| `consumer_name` | str | no | `dagster-ingest` | JetStream durable consumer name |
| `credentials_env_var` | str | no | null | Env var with path to NATS credentials file |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_messages` | int | no | 10000 | Max messages to fetch per run |
| `fetch_timeout_seconds` | float | no | 5.0 | Seconds to wait for each fetch batch |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.NATSToDatabaseAssetComponent
attributes:
  asset_name: nats_events_ingest
  nats_url_env_var: NATS_URL
  subject: events.>
  use_jetstream: true
  stream_name: EVENTS
  consumer_name: dagster-ingest
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  max_messages: 10000
```
