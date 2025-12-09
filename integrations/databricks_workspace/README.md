# Databricks Workspace Component

Import Databricks workspace entities (jobs, notebooks, DLT pipelines, model endpoints) as Dagster assets with automatic lineage discovery and observation.

## Features

- **Jobs Import**: Automatically detects root vs downstream jobs
  - Root jobs (no upstream deps) → Regular assets (Dagster can materialize)
  - Downstream jobs (have upstream deps) → Observable source assets (Databricks orchestrates)
- **Notebooks**: Import standalone notebooks as materializable assets
- **Delta Live Tables**: Import DLT pipelines as materializable assets (trigger pipeline updates from Dagster)
- **Model Endpoints**: Import ML serving endpoints as observable assets
- **Filtering**: Filter by tags, name patterns, or exclude patterns
- **Observation Sensor**: Automatically track job runs from Databricks

## Configuration

### Basic Example

```yaml
type: dagster_component_templates.DatabricksWorkspaceComponent
attributes:
  workspace_url: https://dbc-abc123.cloud.databricks.com
  access_token: "{{ env('DATABRICKS_TOKEN') }}"
  import_jobs: true
```

### Advanced Example with Filtering

```yaml
type: dagster_component_templates.DatabricksWorkspaceComponent
attributes:
  workspace_url: https://dbc-abc123.cloud.databricks.com
  access_token: "{{ env('DATABRICKS_TOKEN') }}"

  # Entity types
  import_jobs: true
  import_notebooks: false
  import_dlt_pipelines: true
  import_model_endpoints: true

  # Filtering
  filter_by_tags: production,analytics
  filter_by_name_pattern: ^prod_.*
  exclude_name_pattern: test|dev|staging

  # Sensor configuration
  generate_sensor: true
  poll_interval_seconds: 60

  # Organization
  group_name: databricks_prod
  description: Production Databricks workspace
```

## How It Works

### Root vs Downstream Jobs

The component automatically analyzes job dependencies:

- **Root Jobs**: Jobs with no upstream Databricks dependencies become regular `@asset`
  - You can materialize these from Dagster
  - Materializing triggers the Databricks job

- **Downstream Jobs**: Jobs with upstream dependencies become `observable_source_asset`
  - Databricks triggers these automatically when upstream jobs complete
  - Dagster observes completions but doesn't trigger them

### Observation Sensor

When `generate_sensor: true`, a sensor is created that:
1. Polls Databricks for completed job runs and DLT pipeline updates
2. Emits `AssetMaterialization` events for successful completions
3. Tracks runs that happen outside Dagster (manual runs, scheduled runs, etc.)
4. Monitors both jobs and DLT pipelines for completions

This ensures your lineage graph stays up-to-date regardless of how jobs or pipelines were triggered.

## Entity Types

### Jobs
- Most common use case
- Supports dependency detection
- Can be materialized (root) or observed (downstream)

### Notebooks
- Standalone notebooks (not embedded in jobs)
- Requires `notebook_base_path` to specify which notebooks to import
- Always materializable assets

### Delta Live Tables (DLT)
- Materializable assets - trigger DLT pipeline updates from Dagster
- Uses the Databricks API to start pipeline updates and waits for completion
- Represents entire pipeline as single asset
- Each materialization triggers a full pipeline update and blocks until complete
- Observation sensor tracks updates that occur outside Dagster

### Model Serving Endpoints
- ML model deployments
- Always observable (monitors deployment status)
- Useful for ML workflow tracking

## Filtering

### By Tags
```yaml
filter_by_tags: production,analytics
```
Only imports entities with ANY of these tags.

### By Name Pattern (Include)
```yaml
filter_by_name_pattern: ^prod_.*
```
Only imports entities whose names match the regex.

### By Name Pattern (Exclude)
```yaml
exclude_name_pattern: test|dev|staging
```
Excludes entities whose names match the regex.

## Requirements

- `databricks-sdk>=0.18.0`
- `dagster>=1.6.0`

## Environment Variables

Set your Databricks token as an environment variable:

```bash
export DATABRICKS_TOKEN="dapi..."
```

Then reference in config:
```yaml
access_token: "{{ env('DATABRICKS_TOKEN') }}"
```

## Lineage

For implicit dependencies (within jobs, notebooks), you have two options:

1. **User-drawn lineage**: Use the visual editor to draw connections between assets
2. **Custom lineage**: Add `custom_lineage.json` to define dependencies programmatically

The component does NOT parse SQL or notebook code for dependencies - this keeps it simple and fast.

## Migration Path

Start with observation:
1. Import all jobs as observable assets
2. View lineage and understand your workflows
3. Progressively "upgrade" specific jobs to Dagster orchestration by:
   - Removing upstream Databricks dependencies
   - Letting the component detect them as root jobs
   - Now Dagster can materialize them

This allows gradual migration without breaking existing workflows.
