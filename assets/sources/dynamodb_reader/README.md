# DynamoDB Reader Component

Scan an AWS DynamoDB table and return all items as a Dagster asset DataFrame.

## Overview

The `DynamodbReaderComponent` performs a full table scan on an AWS DynamoDB table, handling pagination automatically to retrieve all matching items. Results are returned as a pandas DataFrame.

## Use Cases

- **Event sourcing**: Pull DynamoDB event streams into analytics pipelines
- **Session data**: Read user session or state records
- **IoT data**: Extract device telemetry stored in DynamoDB
- **Serverless backends**: Sync DynamoDB application data to a data warehouse

## Configuration

### Basic scan

```yaml
type: dagster_component_templates.DynamodbReaderComponent
attributes:
  asset_name: dynamo_users
  table_name: users
  aws_region: us-west-2
```

### Scan with limit using GSI

```yaml
type: dagster_component_templates.DynamodbReaderComponent
attributes:
  asset_name: recent_orders
  table_name: orders
  aws_region: us-east-1
  index_name: created_at-index
  limit: 5000
  group_name: sources
```

### With explicit AWS credentials

```yaml
type: dagster_component_templates.DynamodbReaderComponent
attributes:
  asset_name: prod_records
  table_name: production_data
  aws_region: eu-west-1
  aws_access_key_env_var: AWS_ACCESS_KEY_ID
  aws_secret_key_env_var: AWS_SECRET_ACCESS_KEY
```

## Output Schema

Returns all DynamoDB item attributes as DataFrame columns. DynamoDB Decimal types are preserved as-is.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key (optional if using instance role) |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key (optional if using instance role) |

## Dependencies

- `pandas>=1.5.0`
- `boto3>=1.26.0`
