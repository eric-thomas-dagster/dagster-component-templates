# Azure Data Factory Component

Import Azure Data Factory entities as Dagster assets with comprehensive orchestration and observation capabilities.

## Features

- **Pipelines**: Trigger pipeline runs on demand (includes SSIS package execution)
- **Triggers**: Start and stop triggers programmatically
- **Data Flows**: Observe data flow definitions
- **Integration Runtimes**: Monitor IR status and health (includes Azure-SSIS IR)
- **SSIS Support**: Execute SSIS packages via Execute SSIS Package activity
- **Observation Sensor**: Track pipeline runs and trigger runs

## Configuration

### Basic Example
```yaml
type: dagster_component_templates.AzureDataFactoryComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  factory_name: my-data-factory
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
  import_pipelines: true
```

### Advanced Example
```yaml
type: dagster_component_templates.AzureDataFactoryComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  factory_name: my-data-factory
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"

  # Import all entity types
  import_pipelines: true
  import_triggers: true
  import_data_flows: true
  import_integration_runtimes: true

  # Filtering
  filter_by_name_pattern: ^prod_.*
  exclude_name_pattern: test|dev
  filter_by_tags: env,team

  # Sensor configuration
  generate_sensor: true
  poll_interval_seconds: 60

  group_name: adf_workspace
  description: Complete Azure Data Factory integration
```

## Entity Types

### Pipelines (Materializable)
- Trigger pipeline runs on demand
- Wait for pipeline completion
- Returns run ID, status, duration, and error details
- Observation sensor tracks automatic runs

### Triggers (Materializable)
- Start triggers programmatically
- Stop triggers when needed
- Monitor trigger runtime state
- Supports schedule, tumbling window, and event-based triggers

### Data Flows (Observable)
- Observe data flow definitions
- Monitor data flow types (Mapping, Wrangling)
- Track data flow properties
- Data flows execute via pipeline activities

### Integration Runtimes (Observable)
- Monitor IR status and state
- Track Azure IR, Self-Hosted IR, and Azure-SSIS IR
- Observe IR health and availability
- Monitor compute resources

## SSIS Support

Azure Data Factory provides native support for SQL Server Integration Services (SSIS) through **Azure-SSIS Integration Runtime**:

### How SSIS Works in ADF

1. **Azure-SSIS Integration Runtime**: A fully managed cluster of Azure VMs dedicated to running SSIS packages
2. **Execute SSIS Package Activity**: Pipeline activity that executes SSIS packages stored in SSISDB or file system
3. **Package Deployment**: Supports both Project Deployment Model (SSISDB) and Package Deployment Model (file system, Azure Files, MSDB)

### Orchestrating SSIS Packages

- **Import Integration Runtimes**: Set `import_integration_runtimes: true` to monitor Azure-SSIS IR status
- **Import Pipelines**: Pipelines with "Execute SSIS Package" activities are automatically included
- **Trigger Execution**: Triggering an ADF pipeline will execute any SSIS packages defined in that pipeline

### SSIS Migration Benefits

- **Lift-and-Shift**: Move existing SSIS packages to Azure without code changes
- **Cost Management**: Start/stop Azure-SSIS IR via pipelines to save costs
- **Integration**: Combine SSIS packages with native ADF activities (Copy, Data Flow, etc.)
- **Scheduling**: Use ADF triggers for flexible SSIS package scheduling

### Example: SSIS Package Execution

```yaml
type: dagster_component_templates.AzureDataFactoryComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  factory_name: my-data-factory
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
  import_pipelines: true  # Includes SSIS package execution pipelines
  import_integration_runtimes: true  # Monitor Azure-SSIS IR
  filter_by_name_pattern: ^ssis_.*  # Filter to SSIS-related pipelines
  group_name: ssis_prod
```

### SSIS Best Practices

1. **Cost Optimization**: Use Web Activity to start/stop Azure-SSIS IR before/after package execution
2. **Monitoring**: Enable Azure-SSIS IR status monitoring with `import_integration_runtimes`
3. **Package Storage**: Use SSISDB for better package management and logging
4. **Parallel Execution**: Leverage ADF pipeline parallelism for multiple SSIS packages
5. **Hybrid Scenarios**: Combine SSIS packages with modern ADF data flows

## Authentication

Three authentication options:

1. **DefaultAzureCredential** (recommended): Omit tenant_id, client_id, client_secret
   - Uses Azure CLI, Managed Identity, or environment variables
   - Best for local development and Azure-hosted apps

2. **Service Principal**: Provide tenant_id, client_id, client_secret
   - Explicit credential management
   - Recommended for production

3. **Environment Variables**: Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
   - Used by DefaultAzureCredential
   - Flexible credential management

## Permissions

Required Azure RBAC roles and permissions:

### Data Factory Contributor Role
- Microsoft.DataFactory/factories/pipelines/read
- Microsoft.DataFactory/factories/pipelines/createRun/action
- Microsoft.DataFactory/factories/triggers/read
- Microsoft.DataFactory/factories/triggers/start/action
- Microsoft.DataFactory/factories/triggers/stop/action
- Microsoft.DataFactory/factories/pipelineruns/read
- Microsoft.DataFactory/factories/pipelineruns/queryByFactory/action
- Microsoft.DataFactory/factories/triggerruns/queryByFactory/action
- Microsoft.DataFactory/factories/dataflows/read
- Microsoft.DataFactory/factories/integrationruntimes/read
- Microsoft.DataFactory/factories/integrationruntimes/getStatus/read

### Alternative: Data Factory Data Access User
Provides read and run permissions without write access.

## Limitations

- Pipeline runs are limited to 60 minutes wait time
- Pipeline run history retained for 45 days (configure Azure Monitor for longer retention)
- Trigger assets only start triggers (doesn't stop them on completion)
- Data flows are not directly triggerable (execute via pipeline activities)
- Integration runtime status polling may have delays

## Use Cases

### Pipeline Orchestration
- Trigger pipelines on demand outside normal schedule
- Coordinate pipeline execution with upstream data
- Create dependencies on pipeline results
- Monitor pipeline performance and costs

### Trigger Management
- Start/stop triggers based on external events
- Enable triggers for specific time windows
- Coordinate trigger activation with data availability
- Manage trigger lifecycle programmatically

### Data Flow Monitoring
- Track data flow definitions and changes
- Monitor data transformation logic
- Observe data flow types and properties
- Centralize data flow observability

### Integration Runtime Health
- Monitor IR availability and status
- Track compute resource allocation
- Observe self-hosted IR connectivity
- Alert on IR failures

## Best Practices

1. Use `filter_by_name_pattern` to scope imports to production entities
2. Apply exclusion patterns to filter test/dev resources
3. Use tags for fine-grained resource filtering
4. Set appropriate sensor poll intervals (default: 60s)
5. Use DefaultAzureCredential for security best practices
6. Configure Azure Monitor for long-term run history
7. Use Managed Identity when running on Azure compute
8. Separate production and non-production data factories

## Monitoring and Observability

The observation sensor tracks:

- **Pipeline Runs**: Status (Succeeded, Failed, Cancelled), duration, timestamps
- **Trigger Runs**: Trigger execution events, status, timestamps
- **Failed Runs**: Error messages and failure details

Sensor emits `AssetMaterialization` events for completed pipeline runs, enabling:
- Downstream dependencies on ADF pipeline results
- Historical tracking of pipeline executions
- Alerting on pipeline failures
- Performance analytics

## Integration with Dagster

### Upstream Dependencies
Create dependencies on data sources:
```python
@asset(deps=["my_source_data"])
def my_adf_pipeline():
    # Triggers when source data materializes
    pass
```

### Downstream Dependencies
Use ADF pipeline results in downstream assets:
```python
@asset(deps=["adf_pipeline_my_etl"])
def my_downstream_analysis():
    # Runs after ADF pipeline completes
    pass
```

### Dynamic Configuration
Pass runtime parameters to pipelines:
```python
# Future enhancement - parameter passing not yet implemented
```

## Troubleshooting

### Authentication Errors
- Verify service principal has correct permissions
- Check tenant ID, client ID, and client secret
- Ensure subscription ID and resource group are correct
- Test Azure CLI authentication: `az login`

### Pipeline Run Failures
- Check pipeline definition in Azure Portal
- Review activity errors in pipeline run details
- Verify linked services are configured correctly
- Check integration runtime connectivity

### Sensor Not Detecting Runs
- Verify `generate_sensor: true` in configuration
- Check sensor poll interval (may need to wait)
- Ensure pipelines match name filters
- Review sensor logs for errors

### Timeout Issues
- Increase pipeline timeout in component code (default: 60 minutes)
- Use asynchronous execution for long-running pipelines
- Consider using observation sensor instead of synchronous waits

## Resources

- [Azure Data Factory Documentation](https://learn.microsoft.com/en-us/azure/data-factory/)
- [Python SDK Reference](https://learn.microsoft.com/en-us/python/api/overview/azure/data-factory)
- [Pipeline Execution and Triggers](https://learn.microsoft.com/en-us/azure/data-factory/concepts-pipeline-execution-triggers)
- [Programmatic Monitoring](https://learn.microsoft.com/en-us/azure/data-factory/monitor-programmatically)
