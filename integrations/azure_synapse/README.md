# Azure Synapse Analytics Component

Import Azure Synapse Analytics entities as Dagster assets for comprehensive data warehouse and analytics orchestration.

## Features

- **Pipelines**: Trigger Synapse pipeline runs (unified with ADF)
- **SQL Pools**: Pause/resume dedicated SQL pools for cost management
- **Spark Jobs**: Submit Spark job definitions
- **Notebooks**: Execute Synapse notebooks
- **Observation Sensor**: Track pipeline runs and completion

## Configuration

### Basic Example
```yaml
type: dagster_component_templates.AzureSynapseComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  workspace_name: my-synapse-workspace
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
  import_pipelines: true
```

### Advanced Example
```yaml
type: dagster_component_templates.AzureSynapseComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  workspace_name: my-synapse-workspace
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"

  # Import all entity types
  import_pipelines: true
  import_sql_pools: true
  import_spark_jobs: true
  import_notebooks: true

  # Filtering
  filter_by_name_pattern: ^prod_.*
  exclude_name_pattern: test|dev
  filter_by_tags: env,team

  # Sensor configuration
  generate_sensor: true
  poll_interval_seconds: 60

  group_name: synapse_workspace
  description: Complete Synapse Analytics integration
```

## Entity Types

### Pipelines (Materializable)
- Trigger Synapse pipeline runs (same engine as Azure Data Factory)
- Wait for pipeline completion
- Support for data movement, transformation, and orchestration activities
- Observation sensor tracks automatic runs

### SQL Pools (Materializable)
- Resume dedicated SQL pools from paused state
- Automatic cost management (pause idle pools)
- Wait for pool to reach online state
- Monitor pool status and provisioning state

### Spark Jobs (Materializable)
- Submit Spark job definitions
- Support for PySpark, Scala, .NET
- Execute batch processing and ML workloads
- Track job submission status

### Notebooks (Materializable)
- Execute Synapse notebooks
- Support for %%pyspark, %%spark, %%csharp, %%sql magic commands
- Typically executed via pipeline notebook activities
- Retrieve notebook metadata

## Authentication

Three authentication options:

1. **DefaultAzureCredential** (recommended): Omit tenant_id, client_id, client_secret
2. **Service Principal**: Provide all three credential parameters
3. **Environment Variables**: Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

## Permissions

Required Azure RBAC roles and permissions:

### Synapse Administrator Role
- Synapse/workspaces/pipelines/read
- Synapse/workspaces/pipelines/createRun/action
- Synapse/workspaces/sqlPools/read
- Synapse/workspaces/sqlPools/pause/action
- Synapse/workspaces/sqlPools/resume/action
- Synapse/workspaces/sparkJobDefinitions/read
- Synapse/workspaces/notebooks/read
- Synapse/workspaces/pipelineRuns/read

### Alternative: Synapse Contributor
Provides broader permissions for resource management.

## Use Cases

### Data Warehouse Orchestration
- Trigger ELT pipelines for data warehouse loads
- Coordinate SQL pool resume before processing
- Pause pools after ETL completion
- Monitor pipeline performance

### Spark Analytics
- Submit Spark jobs for big data processing
- Execute ML training workflows
- Process large-scale transformations
- Coordinate batch analytics

### Cost Optimization
- Automatically pause idle SQL pools
- Resume pools only when needed
- Schedule pool operations
- Track compute usage

### Unified Analytics
- Combine SQL and Spark workloads
- Execute notebooks in pipelines
- Coordinate data lake and warehouse processing
- Integrate with Azure ML

## Best Practices

1. Use pause/resume for dedicated SQL pools to manage costs
2. Configure auto-pause for Spark pools (idle timeout)
3. Use pipelines to coordinate multi-step workflows
4. Apply name patterns to filter production resources
5. Use DefaultAzureCredential with Managed Identity
6. Monitor pipeline runs for failures and retries
7. Combine SQL pools and Spark pools for hybrid workloads

## Resources

- [Azure Synapse Analytics Documentation](https://learn.microsoft.com/en-us/azure/synapse-analytics/)
- [Python SDK Reference](https://learn.microsoft.com/en-us/python/api/overview/azure/synapse)
- [Pause/Resume SQL Pools](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/how-to-pause-resume-pipelines)
- [Spark Job Definitions](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-job-definitions)
