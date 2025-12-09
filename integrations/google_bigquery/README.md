# Google BigQuery Component

Import BigQuery entities as Dagster assets with comprehensive orchestration and observation capabilities.

## Features

- **Scheduled Queries**: Trigger query execution on demand
- **Stored Procedures**: Call procedures from Dagster
- **Materialized Views**: Refresh materialized views
- **Transfer Jobs**: Trigger BigQuery Data Transfer Service jobs
- **Tables**: Monitor table metadata (observable)
- **Routines**: Observe UDFs and stored procedures
- **Observation Sensor**: Track scheduled query runs

## Configuration

### Basic Example
```yaml
type: dagster_component_templates.GoogleBigQueryComponent
attributes:
  project_id: my-gcp-project
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  dataset_id: analytics
  import_scheduled_queries: true
```

### Advanced Example
```yaml
type: dagster_component_templates.GoogleBigQueryComponent
attributes:
  project_id: my-gcp-project
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  location: US
  
  import_scheduled_queries: true
  import_stored_procedures: true
  import_materialized_views: true
  import_transfer_jobs: true
  import_tables: true
  import_routines: true
  
  filter_by_name_pattern: ^prod_.*
  filter_by_labels: env,team
  generate_sensor: true
```

## Entity Types

### Scheduled Queries (Materializable)
- Trigger query execution on demand
- Uses BigQuery Data Transfer Service
- Returns run ID and state
- Observation sensor tracks automatic runs

### Stored Procedures (Materializable)
- Call parameterless procedures
- Execute via `CALL` statement
- Returns job ID
- Integrates procedures with Dagster orchestration

### Materialized Views (Materializable)
- Refresh materialized views
- Returns row count, bytes, last modified
- Query access triggers refresh
- Monitor view staleness

### Transfer Jobs (Materializable)
- Trigger Data Transfer Service jobs
- Supports all data sources (GCS, YouTube Analytics, etc.)
- Returns run ID and state
- Coordinate external data ingestion

### Tables (Observable)
- Monitor table metadata
- Track row counts, bytes, last modified
- Limited to 50 tables per dataset
- Downstream dependency tracking

### Routines (Observable)
- Observe UDFs and stored procedures
- Monitor routine definitions
- Track routine types (SCALAR_FUNCTION, TABLE_FUNCTION, PROCEDURE)
- Centralize routine observability

## Authentication

Three options:
1. **Application Default Credentials** (recommended): Omit `credentials_path`
2. **Service Account JSON**: Provide `credentials_path`
3. **Environment Variable**: Set `GOOGLE_APPLICATION_CREDENTIALS`

## Permissions

Required IAM permissions:
- `bigquery.jobs.create` - Run queries
- `bigquery.tables.get`, `bigquery.tables.list` - List tables/views
- `bigquery.routines.get`, `bigquery.routines.list` - List routines
- `bigquery.transfers.get`, `bigquery.transfers.update` - Manage transfers
- `bigquerydatatransfer.runs.list` - List transfer runs

## Limitations

- Stored procedures must be parameterless
- Materialized views don't have direct refresh (uses query access)
- Table import limited to 50 per dataset (to avoid too many assets)
- Scheduled queries require Data Transfer Service API enabled
- Transfer jobs require appropriate data source permissions

## Use Cases

### Scheduled Query Orchestration
- Trigger queries on demand outside normal schedule
- Coordinate query execution with upstream data
- Create dependencies on query results
- Monitor query performance and costs

### Stored Procedure Execution
- Execute maintenance procedures
- Run data validation routines
- Trigger batch processing
- Coordinate with external systems

### Materialized View Management
- Force view refreshes for critical updates
- Coordinate refreshes with data changes
- Monitor view staleness
- Optimize query performance

### Transfer Job Coordination
- Trigger data imports from external sources
- Coordinate GCS to BigQuery loads
- Integrate third-party data sources
- Monitor transfer job health

### Table Monitoring
- Track table growth and size
- Monitor data freshness
- Create downstream dependencies
- Alert on stale data

## Best Practices

1. Use `dataset_id` to scope imports to specific datasets
2. Apply name patterns to filter production entities
3. Use labels for fine-grained filtering
4. Limit table imports (can create many assets)
5. Use Application Default Credentials for security
6. Set appropriate sensor poll intervals
