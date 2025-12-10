# Azure Stream Analytics Component

Import Azure Stream Analytics entities as Dagster assets for orchestrating real-time analytics workloads.

## Features

- **Streaming Jobs**: Start real-time analytics jobs
- **Observation Sensor**: Monitor job status and health

## Configuration

### Basic Example
```yaml
type: dagster_component_templates.AzureStreamAnalyticsComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"
  import_streaming_jobs: true
```

### Advanced Example
```yaml
type: dagster_component_templates.AzureStreamAnalyticsComponent
attributes:
  subscription_id: "12345678-1234-1234-1234-123456789012"
  resource_group_name: my-resource-group
  tenant_id: "{{ env('AZURE_TENANT_ID') }}"
  client_id: "{{ env('AZURE_CLIENT_ID') }}"
  client_secret: "{{ env('AZURE_CLIENT_SECRET') }}"

  # Import streaming jobs
  import_streaming_jobs: true

  # Filtering
  filter_by_name_pattern: ^prod_.*
  exclude_name_pattern: test|dev
  filter_by_tags: env,team

  # Sensor configuration
  generate_sensor: true
  poll_interval_seconds: 60

  group_name: asa_workspace
```

## Entity Types

### Streaming Jobs (Materializable)
- Start real-time analytics jobs
- Wait for job to reach running state
- Monitor job state (Running, Stopped, Failed, Degraded)
- Track last output event time
- Support for Standard and Premium SKUs

## Authentication

Three authentication options:

1. **DefaultAzureCredential** (recommended): Omit credentials
2. **Service Principal**: Provide tenant_id, client_id, client_secret
3. **Environment Variables**: Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

## Permissions

Required Azure RBAC permissions:

### Stream Analytics Operations
- Microsoft.StreamAnalytics/streamingjobs/read
- Microsoft.StreamAnalytics/streamingjobs/start/action

## Use Cases

### Real-Time Analytics
- IoT device data processing
- Clickstream analysis
- Fraud detection
- Social media sentiment analysis

### Event Processing
- Log aggregation and analysis
- Metrics collection and monitoring
- Alert generation
- Data enrichment and transformation

### Data Integration
- Real-time data pipelines
- Event-driven architectures
- Stream-to-batch processing
- Multi-source data correlation

## Best Practices

1. Use Standard SKU for development, Premium for production
2. Configure auto-scaling for variable workloads
3. Monitor streaming units and throughput
4. Use diagnostic logs for troubleshooting
5. Apply tags for resource organization
6. Test queries thoroughly before deployment

## Resources

- [Azure Stream Analytics Documentation](https://learn.microsoft.com/en-us/azure/stream-analytics/)
- [Python SDK Reference](https://learn.microsoft.com/en-us/python/api/overview/azure/stream-analytics)
- [Query Language Reference](https://learn.microsoft.com/en-us/stream-analytics-query/stream-analytics-query-language-reference)
