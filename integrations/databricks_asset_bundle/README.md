# Databricks Asset Bundle Component

Create Dagster assets from Databricks Asset Bundle YAML configuration files. Each task in your bundle becomes a materializable Dagster asset with automatic dependency tracking.

## Features

- **Bundle-as-Code**: Define assets using Databricks Asset Bundle YAML
- **Automatic Asset Generation**: Each task becomes a Dagster asset
- **Dependency Tracking**: Task `depends_on` relationships become asset dependencies
- **GitOps Workflow**: Version control your bundle configs
- **Variable Resolution**: Uses Databricks CLI to resolve template variables
- **Task Types**: Supports notebooks, Spark Python, Python wheels, Spark JARs, job tasks, and condition tasks
- **Cluster Configuration**: New clusters, existing clusters, or serverless compute

## How It Works

1. **Define Bundle**: Create a `databricks.yml` file with your jobs and tasks
2. **Component Reads Config**: Component parses the bundle YAML
3. **Assets Generated**: Each task becomes a materializable Dagster asset
4. **Execute Tasks**: Materializing an asset runs that specific Databricks task
5. **Dependency Resolution**: Dagster orchestrates tasks based on `depends_on` relationships

## Configuration

### Basic Example
```yaml
type: dagster_component_templates.DatabricksAssetBundleComponent
attributes:
  databricks_config_path: databricks.yml
  workspace_host: https://dbc-abc123.cloud.databricks.com
  workspace_token: "{{ env('DATABRICKS_TOKEN') }}"
  group_name: my_bundle
```

### Advanced Example
```yaml
type: dagster_component_templates.DatabricksAssetBundleComponent
attributes:
  databricks_config_path: bundles/production/databricks.yml
  workspace_host: https://dbc-abc123.cloud.databricks.com
  workspace_token: "{{ env('DATABRICKS_TOKEN') }}"
  asset_key_prefix: prod_etl
  group_name: production_bundle
```

## Databricks Bundle Configuration

Your `databricks.yml` file should follow the standard Databricks Asset Bundle format:

```yaml
resources:
  jobs:
    my_etl_job:
      name: "My ETL Job"
      tasks:
        - task_key: extract_data
          notebook_task:
            notebook_path: /Workspace/notebooks/extract
            base_parameters:
              date: "2024-01-01"
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2
          libraries:
            - pypi:
                package: pandas==2.0.0

        - task_key: transform_data
          spark_python_task:
            python_file: /Workspace/scripts/transform.py
            parameters: ["--input", "s3://bucket/data"]
          depends_on:
            - task_key: extract_data
          existing_cluster_id: "1234-567890-abc123"

        - task_key: load_data
          python_wheel_task:
            package_name: my_package
            entry_point: load_main
            parameters: ["--env", "production"]
          depends_on:
            - task_key: transform_data
```

## Supported Task Types

### Notebook Task
```yaml
notebook_task:
  notebook_path: /Workspace/notebooks/my_notebook
  base_parameters:
    param1: value1
```

### Spark Python Task
```yaml
spark_python_task:
  python_file: /Workspace/scripts/my_script.py
  parameters: ["--arg1", "value1"]
```

### Python Wheel Task
```yaml
python_wheel_task:
  package_name: my_package
  entry_point: main_function
  parameters: ["--config", "prod"]
```

### Spark JAR Task
```yaml
spark_jar_task:
  main_class_name: com.example.MainClass
  parameters: ["arg1", "arg2"]
```

### Run Job Task
```yaml
run_job_task:
  job_id: 123456
  job_parameters:
    param1: value1
```

### Condition Task
```yaml
condition_task:
  left: "{{ tasks.extract_data.status }}"
  op: "EQUAL_TO"
  right: "SUCCESS"
```

## Cluster Configuration

### New Cluster
```yaml
new_cluster:
  spark_version: "13.3.x-scala2.12"
  node_type_id: "i3.xlarge"
  num_workers: 2
  spark_conf:
    spark.speculation: true
```

### Existing Cluster
```yaml
existing_cluster_id: "1234-567890-abc123"
```

### Serverless (if supported)
```yaml
# Serverless configuration handled by Databricks
```

## Asset Dependencies

Task dependencies in the bundle automatically become asset dependencies:

```yaml
# Bundle Config
tasks:
  - task_key: task_a
    # ...

  - task_key: task_b
    depends_on:
      - task_key: task_a
```

This creates:
```
task_a (asset)
  ↓
task_b (asset, depends on task_a)
```

## Variable Resolution

The component uses the Databricks CLI to resolve template variables:

```yaml
# In databricks.yml
resources:
  jobs:
    my_job:
      name: "${workspace.current_user.userName}_job"
```

The CLI resolves `${workspace.current_user.userName}` to the actual username.

## Asset Key Naming

Asset keys are generated from task keys:
- Original: `extract-data.v1`
- Sanitized: `extract_data_v1`
- With prefix: `prod_etl_extract_data_v1` (if `asset_key_prefix: prod_etl`)

## Use Cases

- **GitOps ETL**: Version control your entire ETL pipeline definition
- **Multi-Environment**: Use different bundle configs for dev/staging/prod
- **Granular Execution**: Materialize individual tasks, not entire workflows
- **Dagster Orchestration**: Let Dagster control when tasks run based on dependencies
- **Unified Lineage**: Combine bundle task dependencies with data lineage

## Comparison with DatabricksWorkspaceComponent

| Feature | WorkspaceComponent | AssetBundleComponent |
|---------|-------------------|----------------------|
| **Source** | Workspace API | Local bundle YAML |
| **Discovery** | Queries deployed jobs | Reads databricks.yml |
| **Pattern** | Import existing resources | Define assets as code |
| **Use Case** | Observe workspace | GitOps workflow |
| **Execution** | Trigger existing jobs | Execute bundle tasks |

## Requirements

- Databricks SDK (`databricks-sdk>=0.18.0`)
- Databricks CLI (optional, for variable resolution)
- Databricks workspace with appropriate permissions
- Personal access token for authentication

## Permissions

Required Databricks permissions:
- `jobs:create` - Submit job runs
- `clusters:read` - View cluster configurations
- `workspace:read` - Access workspace files (notebooks, scripts)

## Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `databricks_config_path` | string | Yes | Path to databricks.yml file |
| `workspace_host` | string | Yes | Databricks workspace URL |
| `workspace_token` | string | Yes | Personal access token |
| `asset_key_prefix` | string | No | Prefix for all asset keys |
| `group_name` | string | No | Asset group name (default: `databricks_bundle`) |

## Limitations

- Requires Databricks CLI for full variable resolution (optional)
- Only materializable tasks are supported (no observable-only tasks)
- Condition tasks require Databricks runtime support

## Example Workflow

1. **Create Bundle**:
```bash
# databricks.yml
resources:
  jobs:
    etl_pipeline:
      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: /notebooks/ingest
```

2. **Configure Component**:
```yaml
# component.yaml
type: dagster_component_templates.DatabricksAssetBundleComponent
attributes:
  databricks_config_path: databricks.yml
  workspace_host: https://my-workspace.cloud.databricks.com
  workspace_token: "{{ env('DATABRICKS_TOKEN') }}"
```

3. **Materialize Assets**:
```bash
dagster asset materialize --select ingest
```

The component submits the task to Databricks and polls until completion!
