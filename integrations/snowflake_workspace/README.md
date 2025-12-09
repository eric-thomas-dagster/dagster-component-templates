# Snowflake Workspace Component

Import Snowflake workspace entities (tasks, stored procedures, dynamic tables, streams, Snowpipes, stages) as Dagster assets with automatic observation and orchestration.

## Features

- **Tasks**: Import Snowflake tasks as materializable assets (execute on demand from Dagster)
- **Stored Procedures**: Import stored procedures as materializable assets (call from Dagster)
- **Dynamic Tables**: Import dynamic tables as materializable assets (trigger manual refreshes)
- **Streams**: Import streams as observable assets (monitor CDC)
- **Snowpipes**: Import Snowpipe continuous ingestion pipes as materializable assets (trigger refresh)
- **Stages**: Import internal/external stages as observable assets (monitor files)
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
  import_streams: true
  import_snowpipes: true
  import_stages: true

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

## Future Enhancements

### Snowflake OpenFlow Support
OpenFlow is Snowflake's integration with Apache NiFi for visual data flow design. Future versions of this component may include:
- Import OpenFlow data flows as Dagster assets
- Monitor flow execution status and metrics
- Trigger flow runs from Dagster
- Track data volumes and processing times

OpenFlow support will require access to NiFi's REST API or Snowflake's OpenFlow management APIs. If you're interested in this feature, please open an issue on GitHub.

## Migration Path

Start with observation:
1. Import all tasks as observable assets (using the sensor)
2. View lineage and understand your workflows
3. Progressively enable on-demand execution for specific tasks
4. Coordinate Snowflake tasks with Dagster-orchestrated workflows

This allows gradual adoption without breaking existing Snowflake schedules.
