# DynamoDB Writer Component

Write a DataFrame to an AWS DynamoDB table as a Dagster sink asset.

## Overview

The `DynamodbWriterComponent` accepts an upstream DataFrame asset and writes its records to a DynamoDB table using batch writes for efficiency. Automatically converts Python `float` values to `Decimal` as required by DynamoDB. Skips `None` values in records.

## Use Cases

- **Serverless backends**: Load processed data into DynamoDB for serverless applications
- **Feature store**: Write ML features to DynamoDB for low-latency serving
- **Session state**: Store computed session data in DynamoDB
- **IoT device data**: Write processed IoT telemetry to DynamoDB

## Configuration

### Basic write

```yaml
type: dagster_component_templates.DynamodbWriterComponent
attributes:
  asset_name: write_users_to_dynamo
  upstream_asset_key: processed_users
  table_name: users
  aws_region: us-east-1
  group_name: sinks
```

### With explicit AWS credentials

```yaml
type: dagster_component_templates.DynamodbWriterComponent
attributes:
  asset_name: write_events_to_dynamo
  upstream_asset_key: enriched_events
  table_name: events
  aws_region: eu-west-1
  aws_access_key_env_var: AWS_ACCESS_KEY_ID
  aws_secret_key_env_var: AWS_SECRET_ACCESS_KEY
```

## Notes

- Float values are automatically converted to `Decimal` as required by DynamoDB
- `None` values in records are skipped (not written to DynamoDB)
- The DynamoDB table must already exist with a compatible primary key schema

## Environment Variables

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key (optional if using instance role) |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key (optional if using instance role) |

## Dependencies

- `pandas>=1.5.0`
- `boto3>=1.26.0`
