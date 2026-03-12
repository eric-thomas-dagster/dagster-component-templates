# AWS Glue Component

Import AWS Glue services as Dagster assets with state-backed caching, comprehensive
orchestration, lineage tracking, and observation capabilities.

## How It Works

This component uses `StateBackedComponent` to separate discovery from definition
building:

1. **`write_state_to_path`** — called once at prepare time (or via
   `dg utils refresh-defs-state`). Paginates the Glue APIs and writes a JSON
   snapshot to disk.
2. **`build_defs_from_state`** — called on every code-server reload. Reads the
   cached JSON and constructs all asset definitions with **zero network calls**,
   keeping startup fast.

On first load (before the cache is populated), the component returns an empty
`Definitions`. Run `dg utils refresh-defs-state` or `dagster dev` to populate.

## Features

- **Glue Jobs**: Trigger Spark/Python ETL jobs with polling until completion,
  automatic lineage to Data Catalog tables, and `MaterializeResult` metadata.
- **Data Catalog Tables**: Observable source assets for lineage tracking.
- **Glue Crawlers**: Run metadata discovery crawlers.
- **Glue Workflows**: Trigger multi-job orchestration workflows.
- **Glue DataBrew Jobs**: Execute visual data preparation recipes.
- **Glue Data Quality Rulesets**: Monitor data validation rules (observable).
- **Observation Sensor**: Track externally-triggered job, crawler, and workflow runs.

## Quickstart

```yaml
type: dagster_component_templates.AWSGlueComponent
attributes:
  region_name: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
  import_jobs: true
  import_catalog_tables: true
  catalog_database_filter: production,analytics
```

After adding the component, populate the cache:

```bash
dg utils refresh-defs-state
# or simply:
dagster dev
```

## Advanced Example

```yaml
type: dagster_component_templates.AWSGlueComponent
attributes:
  region_name: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"

  # Entity discovery
  import_jobs: true
  import_crawlers: true
  import_workflows: true
  import_databrew_jobs: true
  import_data_quality_rulesets: true

  # Lineage tracking
  import_catalog_tables: true
  catalog_database_filter: production,staging,analytics

  # Filtering
  job_name_prefix: prod_
  exclude_name_pattern: test|dev
  filter_by_tags: environment,team

  # Asset presentation
  group_name: aws_glue_workspace
  key_prefix: glue

  # Sensor
  generate_sensor: true
  poll_interval_seconds: 60
```

## Entity Types

### Glue Jobs (Materializable)
- Calls `glue.start_job_run()`, polls `glue.get_job_run()` until a terminal state.
- Raises an exception on `FAILED`/`STOPPED`/`TIMEOUT`/`ERROR`.
- Returns `MaterializeResult` with `run_id`, `execution_time_seconds`, timestamps.
- Asset key format: `glue_job_{sanitized_name}` (optionally prefixed with `key_prefix`).
- Automatic `deps` on any Data Catalog table assets extracted from `DefaultArguments`.

### Data Catalog Tables (Observable)
- Asset key format: `glue_table_{database}_{table}`.
- Filter by database using `catalog_database_filter`.

### Glue Crawlers (Materializable)
- Asset key format: `glue_crawler_{sanitized_name}`.
- Starts the crawler; returns state and last-crawl time.

### Glue Workflows (Materializable)
- Asset key format: `glue_workflow_{sanitized_name}`.
- Starts the workflow run; returns run ID and status.

### Glue DataBrew Jobs (Materializable)
- Asset key format: `databrew_job_{sanitized_name}`.
- Starts the DataBrew job run; returns run ID.

### Glue Data Quality Rulesets (Observable)
- Asset key format: `data_quality_{sanitized_name}`.
- Verifies the ruleset exists; returns no data.

### Observation Sensor
- Polls `get_job_runs`, `get_crawler_metrics`, and `get_workflow_runs`.
- Yields `AssetMaterialization` events for externally-triggered successful runs.
- Enabled when `generate_sensor: true` (the default).

## Filtering

| Field | Description |
|-------|-------------|
| `job_name_prefix` | Only include entities whose names start with this prefix |
| `exclude_jobs` | Explicit list of job names to skip |
| `filter_by_name_pattern` | Regex pattern — only include matching names |
| `exclude_name_pattern` | Regex pattern — exclude matching names |
| `filter_by_tags` | Comma-separated tag keys; entity must have at least one |
| `catalog_database_filter` | Comma-separated database names for table discovery |

## Authentication

Three options (listed in order of preference for production):

1. **IAM Role** (EC2/ECS/EKS): omit all credential fields.
2. **Access Keys**: provide `aws_access_key_id` + `aws_secret_access_key`.
3. **Temporary Credentials**: add `aws_session_token` to option 2.

## Required IAM Permissions

```json
{
  "Effect": "Allow",
  "Action": [
    "glue:GetJobs", "glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
    "glue:GetCrawlers", "glue:StartCrawler", "glue:GetCrawler", "glue:GetCrawlerMetrics",
    "glue:ListWorkflows", "glue:BatchGetWorkflows",
    "glue:StartWorkflowRun", "glue:GetWorkflowRun", "glue:GetWorkflowRuns",
    "glue:GetDatabases", "glue:GetTables", "glue:GetTable",
    "glue:ListDataQualityRulesets", "glue:GetDataQualityRuleset",
    "databrew:ListJobs", "databrew:StartJobRun"
  ],
  "Resource": "*"
}
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `region_name` | string | **required** | AWS region (e.g., `us-east-1`) |
| `aws_access_key_id` | string | `null` | AWS access key (optional) |
| `aws_secret_access_key` | string | `null` | AWS secret key (optional) |
| `aws_session_token` | string | `null` | Session token for temporary creds |
| `import_jobs` | boolean | `true` | Discover Glue ETL jobs |
| `import_crawlers` | boolean | `false` | Discover Glue crawlers |
| `import_workflows` | boolean | `false` | Discover Glue workflows |
| `import_databrew_jobs` | boolean | `false` | Discover DataBrew jobs |
| `import_data_quality_rulesets` | boolean | `false` | Discover data quality rulesets |
| `import_catalog_tables` | boolean | `true` | Discover Data Catalog tables (lineage) |
| `catalog_database_filter` | string | `null` | Comma-separated databases for table discovery |
| `job_name_prefix` | string | `null` | Only include entities with this name prefix |
| `exclude_jobs` | list | `null` | Job names to explicitly exclude |
| `filter_by_name_pattern` | string | `null` | Regex to include entities by name |
| `exclude_name_pattern` | string | `null` | Regex to exclude entities by name |
| `filter_by_tags` | string | `null` | Comma-separated tag keys to filter entities |
| `group_name` | string | `aws_glue` | Dagster asset group name |
| `key_prefix` | string | `null` | Optional prefix prepended to all asset keys |
| `poll_interval_seconds` | number | `30` | Job run poll interval in seconds |
| `generate_sensor` | boolean | `true` | Generate observation sensor |
