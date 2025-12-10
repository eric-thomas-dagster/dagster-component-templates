# AWS Glue Component

Import AWS Glue services as Dagster assets with comprehensive orchestration, lineage tracking, and observation capabilities.

## Features

- **Glue Jobs**: Trigger Spark/Python ETL jobs on demand with automatic lineage to Data Catalog tables
- **Data Catalog Tables**: Observable source assets for lineage tracking
- **Glue Crawlers**: Run metadata discovery crawlers
- **Glue Workflows**: Trigger multi-job orchestration workflows
- **Glue DataBrew Jobs**: Execute visual data preparation recipes
- **Glue Data Quality Rulesets**: Monitor data validation rules
- **Automatic Lineage**: Extract dependencies from job metadata to create lineage graph
- **Observation Sensor**: Track job runs, crawler runs, and workflow runs

## Configuration

### Basic Example (with Lineage)
```yaml
type: dagster_component_templates.AWSGlueComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
  import_jobs: true
  import_catalog_tables: true
  catalog_database_filter: production,analytics
```

### Advanced Example
```yaml
type: dagster_component_templates.AWSGlueComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"

  # Import all entity types
  import_jobs: true
  import_crawlers: true
  import_workflows: true
  import_databrew_jobs: true
  import_data_quality_rulesets: true

  # Lineage tracking
  import_catalog_tables: true
  catalog_database_filter: production,staging,analytics

  # Filtering
  filter_by_name_pattern: ^prod_.*
  filter_by_tags: environment,team

  # Sensor
  generate_sensor: true
```

## Entity Types

### Data Catalog Tables (Observable)
- Observable source assets representing tables in Glue Data Catalog
- Tracks table metadata (location, schema, update time, row count)
- Used for lineage tracking to jobs that read these tables
- Filter by database using `catalog_database_filter`
- Asset key format: `glue_table_{database}_{table}`

### Glue Jobs (Materializable)
- Trigger Spark/Python ETL jobs
- Waits for completion
- Returns run ID, execution time, and status
- Supports Spark, Python Shell, and Ray jobs
- **Automatic lineage**: Jobs include `deps` on Data Catalog tables they read
- Lineage extracted from job `DefaultArguments` (`--database_name`, `--table_name`, etc.)

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

## Lineage Tracking

This component automatically creates lineage from Data Catalog tables to Glue jobs:

1. **Data Catalog tables** are imported as observable source assets
2. **Job metadata** is parsed to extract table references from `DefaultArguments`
3. **Dependencies** are automatically added via the `deps` parameter
4. **Lineage graph** shows data flow: `table → job → output_table`

### Example Lineage
```
glue_table_production_customers (observable)
          ↓ (deps)
glue_job_customer_etl (materializable)
          ↓ (produces)
glue_table_production_customer_summary (observable)
```

### How Lineage is Extracted

The component looks for these patterns in job `DefaultArguments`:
- `--database_name` or `--database`: Database name
- `--table_name`, `--input_table`, `--source_table`: Table names

Jobs automatically get `deps` on any matching Data Catalog tables.

## Permissions

Required IAM permissions:
- **Jobs**: `glue:GetJobs`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns`
- **Crawlers**: `glue:GetCrawlers`, `glue:StartCrawler`, `glue:GetCrawlerMetrics`
- **Workflows**: `glue:ListWorkflows`, `glue:BatchGetWorkflows`, `glue:StartWorkflowRun`, `glue:GetWorkflowRun`, `glue:GetWorkflowRuns`
- **Data Catalog**: `glue:GetDatabases`, `glue:GetTables`, `glue:GetTable`
- **DataBrew**: `databrew:ListJobs`, `databrew:StartJobRun`
- **Data Quality**: `glue:ListDataQualityRulesets`, `glue:GetDataQualityRuleset`

## Limitations

- Glue jobs wait for completion (can be long-running)
- Crawlers start but don't wait (use sensor for completion)
- Workflows start but don't wait (use sensor for completion)
- Data Quality rulesets are read-only (no trigger capability)

## Use Cases

- **Lineage Tracking**: Visualize data flow from Data Catalog tables through Glue jobs
- **Orchestration**: Trigger Glue ETL jobs from Dagster with automatic upstream dependencies
- **Pipeline Coordination**: Coordinate crawler runs with downstream pipelines
- **Data Quality**: Monitor data quality in your data lake
- **Visual ETL**: Integrate DataBrew transformations
- **Observability**: Centralize AWS Glue observability in Dagster

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `import_jobs` | boolean | `true` | Import Glue ETL jobs as materializable assets with lineage |
| `import_catalog_tables` | boolean | `true` | Import Data Catalog tables as observable source assets |
| `catalog_database_filter` | string | `null` | Comma-separated database names to import (e.g., `production,analytics`) |
| `import_crawlers` | boolean | `false` | Import Glue crawlers as materializable assets |
| `import_workflows` | boolean | `false` | Import Glue workflows as materializable assets |
| `import_databrew_jobs` | boolean | `false` | Import DataBrew jobs as materializable assets |
| `import_data_quality_rulesets` | boolean | `false` | Import data quality rulesets as observable assets |
| `filter_by_name_pattern` | string | `null` | Regex pattern to filter entities by name |
| `exclude_name_pattern` | string | `null` | Regex pattern to exclude entities by name |
| `filter_by_tags` | string | `null` | Comma-separated tag keys to filter entities |
| `generate_sensor` | boolean | `true` | Create sensor to observe Glue runs |
| `poll_interval_seconds` | number | `60` | Sensor poll interval in seconds |
