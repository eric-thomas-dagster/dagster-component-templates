# Kinesis to Database Asset

Reads records from an Amazon Kinesis Data Stream and writes them to a database table via SQLAlchemy. Iterates all shards (or a specific shard when triggered by `kinesis_monitor`).

## Required packages

```
boto3>=1.26.0
pandas>=1.3.0
sqlalchemy>=2.0.0
```

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | str | yes | — | Dagster asset name |
| `stream_name` | str | yes | — | Kinesis stream name |
| `database_url_env_var` | str | yes | — | Env var with SQLAlchemy database URL |
| `table_name` | str | yes | — | Destination table name |
| `schema_name` | str | no | null | Destination schema name |
| `if_exists` | str | no | `append` | fail, replace, or append |
| `max_records` | int | no | 10000 | Max records to consume per run |
| `shard_iterator_type` | str | no | `TRIM_HORIZON` | TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER |
| `region_name` | str | no | `us-east-1` | AWS region |
| `aws_access_key_env_var` | str | no | null | Env var with AWS access key ID |
| `aws_secret_key_env_var` | str | no | null | Env var with AWS secret access key |
| `column_mapping` | dict | no | null | Rename columns: `{old: new}` |
| `group_name` | str | no | `ingestion` | Asset group name |
| `partition_type` | str | no | `none` | none, daily, weekly, or monthly |
| `partition_start_date` | str | no | null | Partition start date YYYY-MM-DD |

## Example

```yaml
type: dagster_component_templates.KinesisToDatabaseAssetComponent
attributes:
  asset_name: kinesis_events_ingest
  stream_name: my-stream
  database_url_env_var: DATABASE_URL
  table_name: raw_events
  max_records: 10000
  region_name: us-east-1
```
