# DynamoDBResourceComponent

A Dagster component that provides a DynamoDB `ConfigurableResource` backed by `boto3`.

## Installation

```
pip install boto3
```

## Configuration

```yaml
type: dynamodb_resource.component.DynamoDBResourceComponent
attributes:
  resource_key: dynamodb_resource
  region_name: us-east-1
  aws_access_key_id_env_var: AWS_ACCESS_KEY_ID
  aws_secret_access_key_env_var: AWS_SECRET_ACCESS_KEY
```

## Auth

Set the environment variables named in `aws_access_key_id_env_var` and `aws_secret_access_key_env_var` to your AWS credentials. If omitted, boto3 will fall back to its standard credential chain (IAM roles, `~/.aws/credentials`, etc.). Use `endpoint_url` to point at DynamoDB Local for local development.
