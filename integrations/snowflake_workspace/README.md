# Snowflake Workspace Component

Import Snowflake workspace entities as Dagster assets with comprehensive orchestration and observation capabilities.

## Features

- **Tasks**: Execute Snowflake tasks on demand from Dagster
- **Stored Procedures**: Call stored procedures from Dagster
- **Dynamic Tables**: Trigger manual refreshes for dynamic tables
- **Materialized Views**: Refresh traditional materialized views (SUSPEND/RESUME)
- **Streams**: Monitor CDC streams for data availability
- **Snowpipes**: Trigger Snowpipe refresh to load pending files
- **Stages**: Monitor file landing zones (internal/external stages)
- **External Tables**: Refresh external table metadata from cloud sources
- **Alerts**: Monitor Snowflake native alert history and status
- **OpenFlow Flows**: Monitor data integration flows via telemetry
- **Filtering**: Filter by name patterns or exclude patterns
- **Observation Sensor**: Automatically track task runs, dynamic table refreshes, and Snowpipe loads

## Configuration

### Basic Example

```yaml
type: dagster_component_templates.SnowflakeWorkspaceComponent
attributes:
  account: xy12345.us-east-1
  user: dagster_user
  password: "{{ env('SNOWFLAKE_PASSWORD') }}"
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema: PUBLIC
  import_tasks: true
```

### Advanced Example with All Entity Types

```yaml
type: dagster_component_templates.SnowflakeWorkspaceComponent
attributes:
  # Connection
  account: xy12345.us-east-1
  user: dagster_user
  password: "{{ env('SNOWFLAKE_PASSWORD') }}"
  warehouse: COMPUTE_WH
  database: ANALYTICS
  schema: PUBLIC
  role: ACCOUNTADMIN

  # Entity types
  import_tasks: true
  import_stored_procedures: true
  import_dynamic_tables: true
  import_materialized_views: true
  import_streams: true
  import_snowpipes: true
  import_stages: true
  import_external_tables: true
  import_alerts: true
  import_openflow_flows: true

  # Filtering
  filter_by_name_pattern: ^PROD_.*
  exclude_name_pattern: TEST|DEV|STAGING
  task_filter_by_state: STARTED

  # Sensor configuration
  generate_sensor: true
  poll_interval_seconds: 60

  # Organization
  group_name: snowflake_prod
  description: Production Snowflake workspace
```

## How It Works

### Tasks

- **Materializable**: Execute tasks on demand using `EXECUTE TASK`
- Dagster can trigger task execution directly
- Observation sensor tracks scheduled task runs
- Metadata includes query ID, state, and timing information

### Stored Procedures

- **Materializable**: Call stored procedures using `CALL`
- Execute parameterless stored procedures from Dagster
- Returns procedure results as metadata
- Useful for ETL processes, data validation, maintenance operations

### Dynamic Tables

- **Materializable**: Trigger manual refreshes using `ALTER DYNAMIC TABLE ... REFRESH`
- Dagster can force refreshes outside of the normal schedule
- Observation sensor monitors automatic refreshes
- Metadata includes refresh status, scheduling state, and timing

### Streams

- **Observable**: Monitor change data capture (CDC) streams
- Check if streams have data using `SYSTEM$STREAM_HAS_DATA`
- Useful for downstream processing coordination
- Tracks data availability without consuming the stream

### Observation Sensor

When `generate_sensor: true`, a sensor is created that:
1. Polls Snowflake for completed task runs, dynamic table refreshes, and Snowpipe loads
2. Emits `AssetMaterialization` events for successful completions
3. Tracks runs that happen outside Dagster (scheduled tasks, automatic refreshes, auto-ingest)
4. Monitors tasks, dynamic tables, and Snowpipes

This ensures your lineage graph stays up-to-date regardless of how tasks, refreshes, or ingestion were triggered.

## Entity Types

### Tasks
- Scheduled SQL or stored procedure executions
- Can be executed on demand via Dagster
- Tracks execution history and state
- Filter by state (STARTED, SUSPENDED)

### Stored Procedures
- Callable SQL routines
- Execute from Dagster with `CALL`
- Returns procedure results
- Note: Currently supports parameterless procedures

### Dynamic Tables
- Materialized views with automatic refresh
- Can trigger manual refreshes from Dagster
- Monitors refresh status and timing
- Useful for forcing updates outside normal schedule

### Streams
- Change data capture (CDC) on tables
- Observable assets (no materialization)
- Monitors data availability
- Useful for coordinating downstream processing

### Snowpipes
- Continuous data ingestion from stages
- Materializable - trigger manual refresh to load pending files
- Monitors load history and file ingestion
- Observation sensor tracks automatic loads
- Useful for forcing immediate ingestion

### Stages
- Internal and external file storage locations
- Observable assets - monitor files in stages
- Tracks file count and total size
- Useful for monitoring data landing zones
- Coordinates with Snowpipe for ingestion

### Materialized Views
- Traditional materialized views (older than dynamic tables)
- Materializable - refresh via SUSPEND/RESUME cycle
- Returns row count and bytes after refresh
- Useful for legacy MV workflows
- Manual refresh control from Dagster

### External Tables
- Tables backed by external data sources (S3, Azure, GCS)
- Materializable - refresh metadata to detect new files
- Uses ALTER EXTERNAL TABLE REFRESH
- Monitors row count and size
- Integrates external data with Snowflake lineage

### Alerts
- Snowflake native alerts on conditions
- Observable - monitor alert history
- Tracks alert state and execution
- Uses INFORMATION_SCHEMA.ALERT_HISTORY
- Monitors alert triggering patterns

### OpenFlow Flows
- Apache NiFi-based data integration flows
- Observable - monitor via SNOWFLAKE.TELEMETRY.EVENTS
- Tracks process groups, processors, and connections
- Monitors flow metrics and performance
- Discovers flows from recent telemetry activity

## Filtering

### By Name Pattern (Include)
```yaml
filter_by_name_pattern: ^PROD_.*
```
Only imports entities whose names match the regex.

### By Name Pattern (Exclude)
```yaml
exclude_name_pattern: TEST|DEV|STAGING
```
Excludes entities whose names match the regex.

### Task State Filter
```yaml
task_filter_by_state: STARTED
```
Only imports tasks in the specified state (STARTED, SUSPENDED).

## Requirements

- `snowflake-connector-python>=3.0.0`
- `dagster>=1.6.0`

## Environment Variables

Set your Snowflake password as an environment variable:

```bash
export SNOWFLAKE_PASSWORD="your-password"
```

Then reference in config:
```yaml
password: "{{ env('SNOWFLAKE_PASSWORD') }}"
```

## Permissions

The Snowflake user needs the following privileges:

### For Tasks:
- `USAGE` on warehouse, database, and schema
- `EXECUTE TASK` privilege
- `SELECT` on `INFORMATION_SCHEMA.TASKS`
- Access to `INFORMATION_SCHEMA.TASK_HISTORY` table function

### For Stored Procedures:
- `USAGE` on warehouse, database, and schema
- `USAGE` on the stored procedure
- `SELECT` on `INFORMATION_SCHEMA.PROCEDURES`

### For Dynamic Tables:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `INFORMATION_SCHEMA.DYNAMIC_TABLES`
- `ALTER` privilege on dynamic tables (to trigger refresh)

### For Streams:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `INFORMATION_SCHEMA.STREAMS`
- Access to `SYSTEM$STREAM_HAS_DATA` function

### For Snowpipes:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `INFORMATION_SCHEMA.PIPES`
- `OPERATE` on pipes (to trigger refresh)
- Access to `INFORMATION_SCHEMA.COPY_HISTORY` table function
- Access to `SYSTEM$PIPE_STATUS` function

### For Stages:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `INFORMATION_SCHEMA.STAGES`
- `READ` on stages (to list files)
- For external stages, appropriate cloud storage permissions

### For Materialized Views:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `INFORMATION_SCHEMA.VIEWS`
- `OPERATE` on materialized views (to suspend/resume)

### For External Tables:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `INFORMATION_SCHEMA.TABLES`
- `ALTER` on external tables (to refresh)
- Appropriate cloud storage permissions for external data

### For Alerts:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `INFORMATION_SCHEMA.ALERTS`
- Access to `INFORMATION_SCHEMA.ALERT_HISTORY` table function

### For OpenFlow Flows:
- `USAGE` on warehouse, database, and schema
- `SELECT` on `SNOWFLAKE.TELEMETRY.EVENTS`
- OpenFlow must be configured to send telemetry to event table

## Use Cases

### Task Orchestration
Use Dagster to:
- Trigger Snowflake tasks on demand (outside their normal schedule)
- Coordinate task execution with other data pipelines
- Monitor task execution history and failures
- Create dependencies between Snowflake tasks and other assets

### Stored Procedure Execution
Use Dagster to:
- Execute maintenance procedures (VACUUM, ANALYZE, etc.)
- Run data validation procedures
- Trigger ETL stored procedures
- Coordinate procedure execution with external systems

### Dynamic Table Refresh
Use Dagster to:
- Force dynamic table refreshes for critical updates
- Coordinate refreshes with upstream data changes
- Monitor refresh status and history
- Create dependencies on dynamic table freshness

### Stream Monitoring
Use Dagster to:
- Monitor CDC streams for data availability
- Coordinate downstream processing based on stream data
- Track stream consumption patterns
- Alert on stream data anomalies

### Snowpipe Orchestration
Use Dagster to:
- Force immediate ingestion of pending files
- Coordinate Snowpipe loads with downstream pipelines
- Monitor ingestion rates and file processing
- Track load history and error rates
- Create dependencies on ingested data

### Stage Monitoring
Use Dagster to:
- Monitor file landing zones for new data
- Track file counts and sizes over time
- Coordinate file arrival with pipeline execution
- Alert on missing or stale files
- Integrate with Snowpipe ingestion workflows

### Materialized View Refresh
Use Dagster to:
- Manually refresh legacy materialized views
- Coordinate MV refreshes with upstream data changes
- Monitor MV size and row counts
- Migrate from scheduled refreshes to event-driven
- Control refresh timing independently of Snowflake schedules

### External Table Management
Use Dagster to:
- Refresh external table metadata when new files land
- Coordinate external data with internal pipelines
- Monitor external data source availability
- Track external table growth and partitioning
- Integrate cloud storage with Snowflake workflows

### Alert Monitoring
Use Dagster to:
- Track Snowflake alert firing patterns
- Monitor alert execution and errors
- Coordinate alerts with downstream actions
- Centralize alert observability with other assets
- Create dependencies on alert conditions

### OpenFlow Integration
Use Dagster to:
- Monitor OpenFlow data integration flows
- Track flow performance and metrics
- Observe processor status and queues
- Detect stuck data or bottlenecks
- Integrate OpenFlow telemetry with Dagster lineage

## Lineage

For dependencies between Snowflake entities and other assets, you have two options:

1. **User-drawn lineage**: Use the visual editor to draw connections between assets
2. **Custom lineage**: Add `custom_lineage.json` to define dependencies programmatically

The component does NOT parse SQL or task definitions for dependencies - this keeps it simple and fast.

## Best Practices

1. **Use filters**: Use name patterns to import only production entities
2. **Separate environments**: Create separate components for dev/staging/prod
3. **Role-based access**: Use a dedicated Snowflake role with minimal required privileges
4. **Task state filter**: Use `task_filter_by_state: STARTED` to only import active tasks
5. **Sensor interval**: Adjust `poll_interval_seconds` based on your task execution frequency

## Limitations

- Stored procedures must be parameterless (or have default parameters)
- Only monitors successful task runs, dynamic table refreshes, and Snowpipe loads
- Stream observation doesn't consume stream data
- Requires Snowflake Enterprise Edition for dynamic tables and streams
- Task execution via `EXECUTE TASK` requires `EXECUTE TASK` privilege
- Snowpipe refresh triggers load but doesn't wait for completion
- Stage file listing may be slow for stages with many files
- Materialized view refresh uses SUSPEND/RESUME (not direct refresh)
- External table refresh only updates metadata (doesn't reload data)
- OpenFlow flows can only be observed (no programmatic start/stop via SQL)
- OpenFlow discovery requires recent telemetry activity (last 7 days)

## Migration Path

Start with observation:
1. Import all tasks as observable assets (using the sensor)
2. View lineage and understand your workflows
3. Progressively enable on-demand execution for specific tasks
4. Coordinate Snowflake tasks with Dagster-orchestrated workflows

This allows gradual adoption without breaking existing Snowflake schedules.
