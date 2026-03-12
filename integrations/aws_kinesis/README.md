# AWS Kinesis

Imports AWS Kinesis Firehose delivery streams and Kinesis Data Analytics applications as Dagster assets. Materialization triggers stream delivery or starts an analytics application; an optional observation sensor emits `AssetMaterialization` events for active streams and running applications.

## Required packages

```
boto3>=1.26.0
botocore>=1.29.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `aws_region` | Yes | — | AWS region (e.g. `us-east-1`) |
| `aws_access_key_id` | No | `null` | AWS access key ID (omit to use IAM role) |
| `aws_secret_access_key` | No | `null` | AWS secret access key |
| `aws_session_token` | No | `null` | AWS session token for temporary credentials |
| `import_firehose_streams` | No | `true` | Import Firehose delivery streams |
| `import_analytics_applications` | No | `true` | Import Data Analytics applications |
| `filter_by_name_pattern` | No | `null` | Regex to filter entities by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude entities by name |
| `filter_by_tags` | No | `null` | Comma-separated tag keys to filter by |
| `generate_sensor` | No | `true` | Generate an observation sensor |
| `poll_interval_seconds` | No | `60` | Sensor poll interval |
| `group_name` | No | `aws_kinesis` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.AWSKinesisComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
  import_firehose_streams: true
  import_analytics_applications: true
  filter_by_name_pattern: "^prod-"
  group_name: aws_kinesis
```
