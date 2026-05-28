# AWS

The AWS community surface is **~32 components** across object storage (S3), streaming (Kinesis, SQS), warehouse (Redshift, Athena), Glue + DMS + SageMaker, CDK + CloudFormation infrastructure-as-asset, CloudWatch observability, and DynamoDB.

## Official Dagster integration (`dagster-aws`)

For the core AWS resources (S3, EMR, ECR, Redshift, Athena) **prefer the official `dagster-aws` package**. It's the supported path with first-class auth via boto3's standard credential chain.

The community components below are **complements** — they cover declarative-YAML wrappers (often easier to set up than the official Python-API path), the long tail (DMS, SageMaker, DynamoDB CRUD, CDK), monitoring patterns (S3/SQS/Kinesis observation sensors), and security-event ingestion (CloudTrail).

## Components — by sub-area

### Object storage (S3)

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`aws_s3_resource`](https://dagster-component-ui.vercel.app/c/aws_s3_resource) | resource | boto3 S3 client + bucket helpers | `code` |
| [`s3_parquet_io_manager`](https://dagster-component-ui.vercel.app/c/s3_parquet_io_manager) | io_manager | Pandas → Parquet on S3 | `code` |
| [`s3_csv_io_manager`](https://dagster-component-ui.vercel.app/c/s3_csv_io_manager) | io_manager | Pandas → CSV on S3 | `code` |
| [`s3_monitor`](https://dagster-component-ui.vercel.app/c/s3_monitor) | sensor | Watch S3 prefix for new objects → dynamic-partition emission | `live` |
| [`s3_observation_sensor`](https://dagster-component-ui.vercel.app/c/s3_observation_sensor) | observation | Periodic prefix size + object-count observation | `code` |
| [`s3_cleanup_job`](https://dagster-component-ui.vercel.app/c/s3_cleanup_job) | jobs | Scheduled S3-prefix purge | `code` |
| [`s3_to_database_asset`](https://dagster-component-ui.vercel.app/c/s3_to_database_asset) | ingestion | S3 → relational DB (Sling under the hood) | `code` |
| [`external_s3_asset`](https://dagster-component-ui.vercel.app/c/external_s3_asset) | external | Declare-only S3 object as Dagster asset | `live` |

### Streaming (Kinesis + SQS + MSK)

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`aws_kinesis`](https://dagster-component-ui.vercel.app/c/aws_kinesis) | integration | Kinesis stream consumer | `code` |
| [`kinesis_monitor`](https://dagster-component-ui.vercel.app/c/kinesis_monitor) | sensor | Watch stream for new records | `code` |
| [`kinesis_observation_sensor`](https://dagster-component-ui.vercel.app/c/kinesis_observation_sensor) | observation | Periodic record-count observation | `code` |
| [`kinesis_to_database_asset`](https://dagster-component-ui.vercel.app/c/kinesis_to_database_asset) | ingestion | Kinesis → relational DB | `code` |
| [`external_kinesis_asset`](https://dagster-component-ui.vercel.app/c/external_kinesis_asset) | external | Declare-only Kinesis stream | `live` |
| [`sqs_monitor`](https://dagster-component-ui.vercel.app/c/sqs_monitor) | sensor | Watch SQS queue for new messages | `code` |
| [`sqs_observation_sensor`](https://dagster-component-ui.vercel.app/c/sqs_observation_sensor) | observation | Queue-depth + age observation | `code` |
| [`sqs_to_database_asset`](https://dagster-component-ui.vercel.app/c/sqs_to_database_asset) | ingestion | SQS → relational DB | `code` |
| [`external_sqs_asset`](https://dagster-component-ui.vercel.app/c/external_sqs_asset) | external | Declare-only SQS queue | `live` |

### Warehouse (Redshift + Athena)

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`redshift_resource`](https://dagster-component-ui.vercel.app/c/redshift_resource) | resource | Redshift connection | `code` |
| [`redshift_io_manager`](https://dagster-component-ui.vercel.app/c/redshift_io_manager) | io_manager | Pandas → Redshift | `code` |
| [`dataframe_to_redshift`](https://dagster-component-ui.vercel.app/c/dataframe_to_redshift) | sink | DataFrame → Redshift table | `live` |
| [`aws_redshift`](https://dagster-component-ui.vercel.app/c/aws_redshift) | integration | Redshift admin actions | `code` |
| [`athena_io_manager`](https://dagster-component-ui.vercel.app/c/athena_io_manager) | io_manager | Athena query + Pandas roundtrip | `code` |

### DynamoDB

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`dynamodb_resource`](https://dagster-component-ui.vercel.app/c/dynamodb_resource) | resource | DynamoDB client | `code` |
| [`dynamodb_reader`](https://dagster-component-ui.vercel.app/c/dynamodb_reader) | source | Table scan → Pandas | `code` |
| [`dynamodb_writer`](https://dagster-component-ui.vercel.app/c/dynamodb_writer) | sink | Pandas → DynamoDB BatchWrite | `code` |

### Glue + DMS + SageMaker

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`aws_glue`](https://dagster-component-ui.vercel.app/c/aws_glue) | integration | Glue Job + Crawler triggers | `code` |
| [`aws_dms`](https://dagster-component-ui.vercel.app/c/aws_dms) | integration | DMS replication task trigger + status observation | `code` |
| [`aws_sagemaker`](https://dagster-component-ui.vercel.app/c/aws_sagemaker) | integration | SageMaker training / inference job orchestration | `code` |

### Infrastructure-as-asset

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`aws_cdk_asset`](https://dagster-component-ui.vercel.app/c/aws_cdk_asset) | infrastructure | `cdk deploy` a stack as a Dagster asset | `code` |

### Observability + security

| Component | Category | What it does | Validation |
|---|---|---|---|
| [`aws_cloudwatch_logs_insights_query`](https://dagster-component-ui.vercel.app/c/aws_cloudwatch_logs_insights_query) | source | CloudWatch Logs Insights query → Pandas | `live` |
| [`aws_cloudwatch_metrics_query`](https://dagster-component-ui.vercel.app/c/aws_cloudwatch_metrics_query) | source | CloudWatch Metrics query → Pandas | `live` |
| [`aws_cloudtrail_ingestion`](https://dagster-component-ui.vercel.app/c/aws_cloudtrail_ingestion) | ingestion | CloudTrail event ingestion for governance / SIEM | `code` |

## Walkthroughs

| Topic | Walkthrough |
|---|---|
| 23 external-asset declarations (S3, SQS, Kinesis included) | [`examples/external_assets.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/external_assets.md) |
| Warehouse migration (Redshift target) | [`examples/warehouse_migration.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/warehouse_migration.md) |

## Connection / auth — quick reference

All AWS components use **boto3's standard credential chain**:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars (with optional `AWS_SESSION_TOKEN` for STS)
2. `~/.aws/credentials` profile (override via `AWS_PROFILE`)
3. EC2 instance profile / ECS task role / EKS IRSA
4. CLI / SSO cache

Components rarely need explicit credential fields; `region:` is the most common config you'll set. Components requiring a Redshift / RDS endpoint take a connection-string env var.

## Gotchas

- **boto3 credential chain order matters** — if `AWS_ACCESS_KEY_ID` is set in the environment, it overrides EVERY other source including the AWS_PROFILE-selected profile. Helpful in CI; surprising on dev laptops.
- **`s3_monitor` partition key shape** — the monitor emits dynamic partitions keyed by the object's S3 key (or a parsed substring via `partition_key_pattern`). Customers using this with `dataframe_to_*` partition by the same key get one Dagster run per S3 object.
- **Athena query state machine** — Athena queries are async via Polling-via-`get_query_execution`. The `athena_io_manager` handles this; if writing a custom op, don't `start_query_execution` + immediately try to read results.
- **DynamoDB BatchWriteItem 25-item limit** — `dynamodb_writer` paginates automatically.
- **Glue job param shape** — Glue's `start_job_run` accepts `Arguments={...}` not `parameters={...}` (common confusion). `aws_glue` handles the conversion.

## Roadmap

- **Step Functions execution asset** — trigger + watch (close cousin of the precisely_connect_job_trigger pattern).
- **EventBridge schedule → Dagster sensor** — receive EventBridge events as Dagster sensor triggers (useful for cross-account fan-out).
- **EMR Serverless** — submit a Spark job, watch for completion.

## See also

- Official `dagster-aws` package — preferred for S3, EMR, Redshift, Athena resource + IO-manager paths
- [boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [`examples/external_assets.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/external_assets.md)
