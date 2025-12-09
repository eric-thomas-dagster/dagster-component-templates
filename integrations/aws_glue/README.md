# AWS Glue Component

Import AWS Glue services as Dagster assets with comprehensive orchestration and observation capabilities.

## Features

- **Glue Jobs**: Trigger Spark/Python ETL jobs on demand
- **Glue Crawlers**: Run metadata discovery crawlers
- **Glue Workflows**: Trigger multi-job orchestration workflows
- **Glue DataBrew Jobs**: Execute visual data preparation recipes
- **Glue Data Quality Rulesets**: Monitor data validation rules
- **Observation Sensor**: Track job runs, crawler runs, and workflow runs

## Configuration

### Basic Example
```yaml
type: dagster_component_templates.AWSGlueComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
  import_jobs: true
```

### Advanced Example
```yaml
type: dagster_component_templates.AWSGlueComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
  
  import_jobs: true
  import_crawlers: true
  import_workflows: true
  import_databrew_jobs: true
  import_data_quality_rulesets: true
  
  filter_by_name_pattern: ^prod_.*
  filter_by_tags: environment,team
  generate_sensor: true
```

## Entity Types

### Glue Jobs (Materializable)
- Trigger Spark/Python ETL jobs
- Waits for completion
- Returns run ID, execution time, and status
- Supports Spark, Python Shell, and Ray jobs

### Glue Crawlers (Materializable)
- Trigger metadata discovery
- Scans data sources and updates Data Catalog
- Returns tables created/updated/deleted
- Monitors crawler state

### Glue Workflows (Materializable)
- Trigger multi-step ETL workflows
- Orchestrates jobs and crawlers
- Returns workflow run ID and status
- Monitors workflow execution

### Glue DataBrew Jobs (Materializable)
- Execute visual data preparation recipes
- No-code data transformations
- Returns job run ID
- Integrates DataBrew with Dagster

### Glue Data Quality Rulesets (Observable)
- Monitor data validation rules
- Track data quality metrics
- Observable only (no trigger capability)
- Integrates DQ results with lineage

## Authentication

Three options:
1. **IAM Role** (recommended for EC2/ECS): Omit credentials
2. **Access Keys**: Provide `aws_access_key_id` and `aws_secret_access_key`
3. **Temporary Credentials**: Add `aws_session_token`

## Permissions

Required IAM permissions:
- `glue:GetJobs`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns`
- `glue:GetCrawlers`, `glue:StartCrawler`, `glue:GetCrawlerMetrics`
- `glue:ListWorkflows`, `glue:BatchGetWorkflows`, `glue:StartWorkflowRun`, `glue:GetWorkflowRun`, `glue:GetWorkflowRuns`
- `databrew:ListJobs`, `databrew:StartJobRun`
- `glue:ListDataQualityRulesets`, `glue:GetDataQualityRuleset`

## Limitations

- Glue jobs wait for completion (can be long-running)
- Crawlers start but don't wait (use sensor for completion)
- Workflows start but don't wait (use sensor for completion)
- Data Quality rulesets are read-only (no trigger capability)

## Use Cases

- Trigger Glue ETL jobs from Dagster orchestration
- Coordinate crawler runs with downstream pipelines
- Monitor data quality in your data lake
- Integrate DataBrew transformations
- Centralize AWS Glue observability
