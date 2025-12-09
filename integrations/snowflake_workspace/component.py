"""Snowflake Workspace Component.

Import Snowflake tasks, stored procedures, dynamic tables, and streams
as Dagster assets with automatic observation and orchestration.
"""

import re
from typing import Optional, List, Dict, Any
from datetime import datetime

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    observable_source_asset,
    sensor,
    SensorEvaluationContext,
    AssetMaterialization,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


class SnowflakeWorkspaceComponent(Component, Model, Resolvable):
    """Component for importing Snowflake workspace entities as Dagster assets.

    Supports importing:
    - Tasks (scheduled SQL/stored procedure executions)
    - Stored Procedures (callable routines)
    - Dynamic Tables (materialized views with automatic refresh)
    - Materialized Views (traditional MVs with manual refresh)
    - Streams (change data capture)
    - Snowpipe (continuous data ingestion pipes)
    - Stages (internal/external file stages)
    - External Tables (monitor external data sources)
    - Alerts (Snowflake native alerts on conditions)
    - OpenFlow Flows (data integration flows via Apache NiFi)

    Example:
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
          import_dynamic_tables: true
        ```
    """

    account: str = Field(
        description="Snowflake account identifier (e.g., xy12345.us-east-1)"
    )

    user: str = Field(
        description="Snowflake username"
    )

    password: str = Field(
        description="Snowflake password"
    )

    warehouse: str = Field(
        description="Snowflake warehouse to use for queries"
    )

    database: str = Field(
        description="Snowflake database to connect to"
    )

    schema: str = Field(
        default="PUBLIC",
        description="Snowflake schema to use"
    )

    role: Optional[str] = Field(
        default=None,
        description="Snowflake role to use (optional)"
    )

    import_tasks: bool = Field(
        default=True,
        description="Import Snowflake tasks as materializable assets"
    )

    import_stored_procedures: bool = Field(
        default=False,
        description="Import stored procedures as materializable assets"
    )

    import_dynamic_tables: bool = Field(
        default=False,
        description="Import dynamic tables as observable assets"
    )

    import_streams: bool = Field(
        default=False,
        description="Import streams as observable assets"
    )

    import_snowpipes: bool = Field(
        default=False,
        description="Import Snowpipe continuous ingestion pipes as materializable assets"
    )

    import_stages: bool = Field(
        default=False,
        description="Import internal and external stages as observable assets"
    )

    import_materialized_views: bool = Field(
        default=False,
        description="Import materialized views as materializable assets (trigger refresh)"
    )

    import_external_tables: bool = Field(
        default=False,
        description="Import external tables as materializable assets (trigger refresh)"
    )

    import_alerts: bool = Field(
        default=False,
        description="Import Snowflake alerts as observable assets (monitor alert status)"
    )

    import_openflow_flows: bool = Field(
        default=False,
        description="Import OpenFlow data integration flows as observable assets (monitor via telemetry)"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    task_filter_by_state: Optional[str] = Field(
        default=None,
        description="Filter tasks by state (STARTED, SUSPENDED). If not specified, imports all tasks."
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="How often (in seconds) the sensor should check for completed runs"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Create a sensor to observe task runs and dynamic table refreshes"
    )

    group_name: Optional[str] = Field(
        default="snowflake",
        description="Group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the Snowflake workspace component"
    )

    def _create_connection(self) -> SnowflakeConnection:
        """Create and return a Snowflake connection."""
        conn_params = {
            'account': self.account,
            'user': self.user,
            'password': self.password,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema,
        }

        if self.role:
            conn_params['role'] = self.role

        return snowflake.connector.connect(**conn_params)

    def _should_include_entity(self, name: str) -> bool:
        """Check if an entity should be included based on filters."""
        # Check name exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name, re.IGNORECASE):
                return False

        # Check name inclusion pattern
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name, re.IGNORECASE):
                return False

        return True

    def _execute_query(self, conn: SnowflakeConnection, query: str) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description] if cursor.description else []
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            return results
        finally:
            cursor.close()

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from Snowflake workspace entities."""
        conn = self._create_connection()

        assets_list = []
        sensors_list = []

        # Track task, dynamic table, and snowpipe metadata for sensor
        task_metadata = {}
        dynamic_table_metadata = {}
        snowpipe_metadata = {}

        try:
            # Import Tasks
            if self.import_tasks:
                try:
                    # Query to get tasks from information schema
                    query = f"""
                    SELECT
                        name,
                        database_name,
                        schema_name,
                        owner,
                        state,
                        schedule,
                        created_on
                    FROM {self.database}.INFORMATION_SCHEMA.TASKS
                    WHERE schema_name = '{self.schema}'
                    """

                    if self.task_filter_by_state:
                        query += f" AND state = '{self.task_filter_by_state}'"

                    tasks = self._execute_query(conn, query)

                    for task in tasks:
                        task_name = task['NAME']

                        if not self._should_include_entity(task_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"task_{re.sub(r'[^a-zA-Z0-9_]', '_', task_name.lower())}"

                        # Store metadata for sensor
                        task_metadata[asset_key] = {
                            'task_name': task_name,
                            'database': task['DATABASE_NAME'],
                            'schema': task['SCHEMA_NAME'],
                            'state': task['STATE'],
                        }

                        # Tasks are materializable - we can execute them via EXECUTE TASK
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake task: {task_name}",
                            metadata={
                                "snowflake_task_name": task_name,
                                "snowflake_database": task['DATABASE_NAME'],
                                "snowflake_schema": task['SCHEMA_NAME'],
                                "snowflake_state": task['STATE'],
                                "snowflake_schedule": task.get('SCHEDULE'),
                                "entity_type": "task",
                            }
                        )
                        def _task_asset(context: AssetExecutionContext, task_name=task_name, db=task['DATABASE_NAME'], schema=task['SCHEMA_NAME']):
                            """Materialize by executing Snowflake task."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Execute the task
                                execute_query = f"EXECUTE TASK {db}.{schema}.{task_name}"
                                cursor.execute(execute_query)

                                context.log.info(f"Executed Snowflake task: {task_name}")

                                # Get task execution history
                                history_query = f"""
                                SELECT
                                    query_id,
                                    state,
                                    scheduled_time,
                                    query_start_time,
                                    completed_time,
                                    error_message
                                FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                                    TASK_NAME => '{task_name}',
                                    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
                                ))
                                ORDER BY scheduled_time DESC
                                LIMIT 1
                                """

                                cursor.execute(history_query)
                                history = cursor.fetchone()

                                metadata = {
                                    "task_name": task_name,
                                    "database": db,
                                    "schema": schema,
                                }

                                if history:
                                    columns = [col[0] for col in cursor.description]
                                    history_dict = dict(zip(columns, history))
                                    metadata.update({
                                        "query_id": history_dict.get('QUERY_ID'),
                                        "state": history_dict.get('STATE'),
                                        "scheduled_time": str(history_dict.get('SCHEDULED_TIME')) if history_dict.get('SCHEDULED_TIME') else None,
                                    })

                                return metadata

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_task_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake tasks: {e}")

            # Import Stored Procedures
            if self.import_stored_procedures:
                try:
                    query = f"""
                    SELECT
                        procedure_name,
                        procedure_schema,
                        procedure_catalog,
                        argument_signature,
                        created
                    FROM {self.database}.INFORMATION_SCHEMA.PROCEDURES
                    WHERE procedure_schema = '{self.schema}'
                    """

                    procedures = self._execute_query(conn, query)

                    for proc in procedures:
                        proc_name = proc['PROCEDURE_NAME']

                        if not self._should_include_entity(proc_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"proc_{re.sub(r'[^a-zA-Z0-9_]', '_', proc_name.lower())}"

                        # Stored procedures are materializable
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake stored procedure: {proc_name}",
                            metadata={
                                "snowflake_procedure_name": proc_name,
                                "snowflake_database": proc['PROCEDURE_CATALOG'],
                                "snowflake_schema": proc['PROCEDURE_SCHEMA'],
                                "snowflake_signature": proc.get('ARGUMENT_SIGNATURE'),
                                "entity_type": "stored_procedure",
                            }
                        )
                        def _procedure_asset(context: AssetExecutionContext, proc_name=proc_name, db=proc['PROCEDURE_CATALOG'], schema=proc['PROCEDURE_SCHEMA']):
                            """Materialize by calling stored procedure."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Call the stored procedure
                                call_query = f"CALL {db}.{schema}.{proc_name}()"
                                cursor.execute(call_query)

                                result = cursor.fetchone()

                                context.log.info(f"Called Snowflake stored procedure: {proc_name}")

                                metadata = {
                                    "procedure_name": proc_name,
                                    "database": db,
                                    "schema": schema,
                                    "result": str(result) if result else None,
                                }

                                return metadata

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_procedure_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake stored procedures: {e}")

            # Import Dynamic Tables
            if self.import_dynamic_tables:
                try:
                    # Query to get dynamic tables
                    query = f"""
                    SELECT
                        name,
                        database_name,
                        schema_name,
                        owner,
                        target_lag,
                        refresh_mode,
                        created_on
                    FROM {self.database}.INFORMATION_SCHEMA.DYNAMIC_TABLES
                    WHERE schema_name = '{self.schema}'
                    """

                    dynamic_tables = self._execute_query(conn, query)

                    for dt in dynamic_tables:
                        dt_name = dt['NAME']

                        if not self._should_include_entity(dt_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"dynamic_table_{re.sub(r'[^a-zA-Z0-9_]', '_', dt_name.lower())}"

                        # Store metadata for sensor
                        dynamic_table_metadata[asset_key] = {
                            'table_name': dt_name,
                            'database': dt['DATABASE_NAME'],
                            'schema': dt['SCHEMA_NAME'],
                        }

                        # Dynamic tables can be manually refreshed
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake dynamic table: {dt_name}",
                            metadata={
                                "snowflake_table_name": dt_name,
                                "snowflake_database": dt['DATABASE_NAME'],
                                "snowflake_schema": dt['SCHEMA_NAME'],
                                "snowflake_target_lag": dt.get('TARGET_LAG'),
                                "snowflake_refresh_mode": dt.get('REFRESH_MODE'),
                                "entity_type": "dynamic_table",
                            }
                        )
                        def _dynamic_table_asset(context: AssetExecutionContext, dt_name=dt_name, db=dt['DATABASE_NAME'], schema=dt['SCHEMA_NAME']):
                            """Materialize by triggering dynamic table refresh."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Trigger refresh
                                refresh_query = f"ALTER DYNAMIC TABLE {db}.{schema}.{dt_name} REFRESH"
                                cursor.execute(refresh_query)

                                context.log.info(f"Triggered refresh for dynamic table: {dt_name}")

                                # Get table info
                                info_query = f"""
                                SELECT
                                    name,
                                    refresh_mode,
                                    target_lag,
                                    scheduling_state,
                                    last_refresh_status
                                FROM {db}.INFORMATION_SCHEMA.DYNAMIC_TABLES
                                WHERE schema_name = '{schema}' AND name = '{dt_name}'
                                """

                                cursor.execute(info_query)
                                info = cursor.fetchone()

                                metadata = {
                                    "table_name": dt_name,
                                    "database": db,
                                    "schema": schema,
                                }

                                if info:
                                    columns = [col[0] for col in cursor.description]
                                    info_dict = dict(zip(columns, info))
                                    metadata.update({
                                        "refresh_mode": info_dict.get('REFRESH_MODE'),
                                        "scheduling_state": info_dict.get('SCHEDULING_STATE'),
                                        "last_refresh_status": info_dict.get('LAST_REFRESH_STATUS'),
                                    })

                                return metadata

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_dynamic_table_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake dynamic tables: {e}")

            # Import Streams
            if self.import_streams:
                try:
                    query = f"""
                    SELECT
                        name,
                        database_name,
                        schema_name,
                        owner,
                        table_name,
                        type,
                        created_on
                    FROM {self.database}.INFORMATION_SCHEMA.STREAMS
                    WHERE schema_name = '{self.schema}'
                    """

                    streams = self._execute_query(conn, query)

                    for stream in streams:
                        stream_name = stream['NAME']

                        if not self._should_include_entity(stream_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"stream_{re.sub(r'[^a-zA-Z0-9_]', '_', stream_name.lower())}"

                        # Streams are observable (CDC)
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake stream: {stream_name}",
                            metadata={
                                "snowflake_stream_name": stream_name,
                                "snowflake_database": stream['DATABASE_NAME'],
                                "snowflake_schema": stream['SCHEMA_NAME'],
                                "snowflake_table": stream.get('TABLE_NAME'),
                                "snowflake_type": stream.get('TYPE'),
                                "entity_type": "stream",
                            }
                        )
                        def _stream_asset(context: AssetExecutionContext, stream_name=stream_name, db=stream['DATABASE_NAME'], schema=stream['SCHEMA_NAME']):
                            """Observable stream asset."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Check if stream has data
                                query = f"SELECT SYSTEM$STREAM_HAS_DATA('{db}.{schema}.{stream_name}')"
                                cursor.execute(query)
                                has_data = cursor.fetchone()[0]

                                context.log.info(f"Stream {stream_name} has data: {has_data}")

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_stream_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake streams: {e}")

            # Import Snowpipes
            if self.import_snowpipes:
                try:
                    query = f"""
                    SELECT
                        name,
                        database_name,
                        schema_name,
                        owner,
                        notification_channel,
                        created_on
                    FROM {self.database}.INFORMATION_SCHEMA.PIPES
                    WHERE schema_name = '{self.schema}'
                    """

                    pipes = self._execute_query(conn, query)

                    for pipe in pipes:
                        pipe_name = pipe['NAME']

                        if not self._should_include_entity(pipe_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"snowpipe_{re.sub(r'[^a-zA-Z0-9_]', '_', pipe_name.lower())}"

                        # Store metadata for sensor
                        snowpipe_metadata[asset_key] = {
                            'pipe_name': pipe_name,
                            'database': pipe['DATABASE_NAME'],
                            'schema': pipe['SCHEMA_NAME'],
                        }

                        # Snowpipes are materializable - can trigger refresh
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake pipe: {pipe_name}",
                            metadata={
                                "snowflake_pipe_name": pipe_name,
                                "snowflake_database": pipe['DATABASE_NAME'],
                                "snowflake_schema": pipe['SCHEMA_NAME'],
                                "snowflake_notification_channel": pipe.get('NOTIFICATION_CHANNEL'),
                                "entity_type": "snowpipe",
                            }
                        )
                        def _snowpipe_asset(context: AssetExecutionContext, pipe_name=pipe_name, db=pipe['DATABASE_NAME'], schema=pipe['SCHEMA_NAME']):
                            """Materialize by refreshing Snowpipe (loading pending files)."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Refresh the pipe to process pending files
                                refresh_query = f"ALTER PIPE {db}.{schema}.{pipe_name} REFRESH"
                                cursor.execute(refresh_query)

                                context.log.info(f"Refreshed Snowpipe: {pipe_name}")

                                # Get pipe status
                                status_query = f"SELECT SYSTEM$PIPE_STATUS('{db}.{schema}.{pipe_name}')"
                                cursor.execute(status_query)
                                status = cursor.fetchone()

                                # Get load history
                                history_query = f"""
                                SELECT
                                    file_name,
                                    stage_location,
                                    last_load_time,
                                    row_count,
                                    row_parsed,
                                    file_size,
                                    first_error_message
                                FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                                    TABLE_NAME => '{pipe_name}',
                                    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())
                                ))
                                ORDER BY last_load_time DESC
                                LIMIT 5
                                """

                                try:
                                    cursor.execute(history_query)
                                    recent_loads = cursor.fetchall()
                                    load_count = len(recent_loads) if recent_loads else 0
                                except:
                                    load_count = 0

                                metadata = {
                                    "pipe_name": pipe_name,
                                    "database": db,
                                    "schema": schema,
                                    "pipe_status": str(status[0]) if status else None,
                                    "recent_loads": load_count,
                                }

                                return metadata

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_snowpipe_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake pipes: {e}")

            # Import Stages
            if self.import_stages:
                try:
                    query = f"""
                    SELECT
                        stage_name,
                        stage_schema,
                        stage_catalog,
                        stage_url,
                        stage_type,
                        stage_owner,
                        created
                    FROM {self.database}.INFORMATION_SCHEMA.STAGES
                    WHERE stage_schema = '{self.schema}'
                    """

                    stages = self._execute_query(conn, query)

                    for stage in stages:
                        stage_name = stage['STAGE_NAME']

                        if not self._should_include_entity(stage_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"stage_{re.sub(r'[^a-zA-Z0-9_]', '_', stage_name.lower())}"

                        # Stages are observable (monitor files)
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake stage: {stage_name}",
                            metadata={
                                "snowflake_stage_name": stage_name,
                                "snowflake_database": stage['STAGE_CATALOG'],
                                "snowflake_schema": stage['STAGE_SCHEMA'],
                                "snowflake_url": stage.get('STAGE_URL'),
                                "snowflake_type": stage.get('STAGE_TYPE'),
                                "entity_type": "stage",
                            }
                        )
                        def _stage_asset(context: AssetExecutionContext, stage_name=stage_name, db=stage['STAGE_CATALOG'], schema=stage['STAGE_SCHEMA']):
                            """Observable stage asset - monitor files."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # List files in stage
                                list_query = f"LIST @{db}.{schema}.{stage_name}"
                                cursor.execute(list_query)

                                files = cursor.fetchall()
                                file_count = len(files) if files else 0

                                total_size = 0
                                if files:
                                    # Sum up file sizes (size is typically in column index 2)
                                    for file_row in files:
                                        if len(file_row) > 2:
                                            total_size += file_row[2]

                                context.log.info(f"Stage {stage_name} has {file_count} files, total size: {total_size} bytes")

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_stage_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake stages: {e}")

            # Import Materialized Views
            if self.import_materialized_views:
                try:
                    query = f"""
                    SELECT
                        name,
                        database_name,
                        schema_name,
                        owner,
                        created_on,
                        cluster_by,
                        is_secure
                    FROM {self.database}.INFORMATION_SCHEMA.VIEWS
                    WHERE schema_name = '{self.schema}'
                    AND table_type = 'MATERIALIZED VIEW'
                    """

                    mv_list = self._execute_query(conn, query)

                    for mv in mv_list:
                        mv_name = mv['NAME']

                        if not self._should_include_entity(mv_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"mv_{re.sub(r'[^a-zA-Z0-9_]', '_', mv_name.lower())}"

                        # Materialized views are materializable
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake materialized view: {mv_name}",
                            metadata={
                                "snowflake_view_name": mv_name,
                                "snowflake_database": mv['DATABASE_NAME'],
                                "snowflake_schema": mv['SCHEMA_NAME'],
                                "snowflake_cluster_by": mv.get('CLUSTER_BY'),
                                "entity_type": "materialized_view",
                            }
                        )
                        def _mv_asset(context: AssetExecutionContext, mv_name=mv_name, db=mv['DATABASE_NAME'], schema=mv['SCHEMA_NAME']):
                            """Materialize by refreshing materialized view."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Refresh the materialized view
                                refresh_query = f"ALTER MATERIALIZED VIEW {db}.{schema}.{mv_name} SUSPEND"
                                cursor.execute(refresh_query)

                                resume_query = f"ALTER MATERIALIZED VIEW {db}.{schema}.{mv_name} RESUME"
                                cursor.execute(resume_query)

                                context.log.info(f"Refreshed materialized view: {mv_name}")

                                # Get view info
                                info_query = f"""
                                SELECT
                                    table_name,
                                    row_count,
                                    bytes
                                FROM {db}.INFORMATION_SCHEMA.TABLES
                                WHERE table_schema = '{schema}' AND table_name = '{mv_name}'
                                """

                                cursor.execute(info_query)
                                info = cursor.fetchone()

                                metadata = {
                                    "view_name": mv_name,
                                    "database": db,
                                    "schema": schema,
                                }

                                if info:
                                    columns = [col[0] for col in cursor.description]
                                    info_dict = dict(zip(columns, info))
                                    metadata.update({
                                        "row_count": info_dict.get('ROW_COUNT'),
                                        "bytes": info_dict.get('BYTES'),
                                    })

                                return metadata

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_mv_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake materialized views: {e}")

            # Import External Tables
            if self.import_external_tables:
                try:
                    query = f"""
                    SELECT
                        table_name,
                        table_schema,
                        table_catalog,
                        table_owner,
                        created,
                        last_altered
                    FROM {self.database}.INFORMATION_SCHEMA.TABLES
                    WHERE table_schema = '{self.schema}'
                    AND table_type = 'EXTERNAL TABLE'
                    """

                    ext_tables = self._execute_query(conn, query)

                    for ext_table in ext_tables:
                        table_name = ext_table['TABLE_NAME']

                        if not self._should_include_entity(table_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"external_table_{re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())}"

                        # External tables can be refreshed
                        @asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake external table: {table_name}",
                            metadata={
                                "snowflake_table_name": table_name,
                                "snowflake_database": ext_table['TABLE_CATALOG'],
                                "snowflake_schema": ext_table['TABLE_SCHEMA'],
                                "entity_type": "external_table",
                            }
                        )
                        def _external_table_asset(context: AssetExecutionContext, table_name=table_name, db=ext_table['TABLE_CATALOG'], schema=ext_table['TABLE_SCHEMA']):
                            """Materialize by refreshing external table metadata."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Refresh external table metadata
                                refresh_query = f"ALTER EXTERNAL TABLE {db}.{schema}.{table_name} REFRESH"
                                cursor.execute(refresh_query)

                                context.log.info(f"Refreshed external table: {table_name}")

                                # Get table info
                                info_query = f"""
                                SELECT
                                    table_name,
                                    row_count,
                                    bytes
                                FROM {db}.INFORMATION_SCHEMA.TABLES
                                WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                                """

                                cursor.execute(info_query)
                                info = cursor.fetchone()

                                metadata = {
                                    "table_name": table_name,
                                    "database": db,
                                    "schema": schema,
                                }

                                if info:
                                    columns = [col[0] for col in cursor.description]
                                    info_dict = dict(zip(columns, info))
                                    metadata.update({
                                        "row_count": info_dict.get('ROW_COUNT'),
                                        "bytes": info_dict.get('BYTES'),
                                    })

                                return metadata

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_external_table_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake external tables: {e}")

            # Import Alerts
            if self.import_alerts:
                try:
                    query = f"""
                    SELECT
                        name,
                        database_name,
                        schema_name,
                        owner,
                        condition,
                        action,
                        created_on
                    FROM {self.database}.INFORMATION_SCHEMA.ALERTS
                    WHERE schema_name = '{self.schema}'
                    """

                    alerts = self._execute_query(conn, query)

                    for alert in alerts:
                        alert_name = alert['NAME']

                        if not self._should_include_entity(alert_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"alert_{re.sub(r'[^a-zA-Z0-9_]', '_', alert_name.lower())}"

                        # Alerts are observable
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"Snowflake alert: {alert_name}",
                            metadata={
                                "snowflake_alert_name": alert_name,
                                "snowflake_database": alert['DATABASE_NAME'],
                                "snowflake_schema": alert['SCHEMA_NAME'],
                                "snowflake_condition": alert.get('CONDITION'),
                                "entity_type": "alert",
                            }
                        )
                        def _alert_asset(context: AssetExecutionContext, alert_name=alert_name, db=alert['DATABASE_NAME'], schema=alert['SCHEMA_NAME']):
                            """Observable alert asset - monitor alert status."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Get alert history
                                history_query = f"""
                                SELECT
                                    name,
                                    state,
                                    scheduled_time,
                                    completed_time,
                                    error
                                FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY(
                                    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
                                ))
                                WHERE name = '{alert_name}'
                                ORDER BY scheduled_time DESC
                                LIMIT 1
                                """

                                cursor.execute(history_query)
                                history = cursor.fetchone()

                                if history:
                                    columns = [col[0] for col in cursor.description]
                                    history_dict = dict(zip(columns, history))

                                    context.log.info(f"Alert {alert_name} last state: {history_dict.get('STATE')}")

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_alert_asset)

                except Exception as e:
                    context.log.error(f"Error importing Snowflake alerts: {e}")

            # Import OpenFlow Flows
            if self.import_openflow_flows:
                try:
                    # Query OpenFlow telemetry to discover flows
                    # Get unique process group names from recent telemetry
                    query = f"""
                    SELECT DISTINCT
                        RECORD['process_group_name']::STRING AS flow_name,
                        RECORD['runtime_id']::STRING AS runtime_id
                    FROM SNOWFLAKE.TELEMETRY.EVENTS
                    WHERE RECORD_TYPE = 'openflow_metric'
                    AND RECORD['metric_name']::STRING = 'process_group_input_bytes'
                    AND TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                    ORDER BY flow_name
                    """

                    flows = self._execute_query(conn, query)

                    for flow in flows:
                        flow_name = flow['FLOW_NAME']
                        runtime_id = flow.get('RUNTIME_ID')

                        if not flow_name or not self._should_include_entity(flow_name):
                            continue

                        # Sanitize name for asset key
                        asset_key = f"openflow_{re.sub(r'[^a-zA-Z0-9_]', '_', flow_name.lower())}"

                        # OpenFlow flows are observable
                        @observable_source_asset(
                            name=asset_key,
                            group_name=self.group_name,
                            description=f"OpenFlow data integration flow: {flow_name}",
                            metadata={
                                "openflow_flow_name": flow_name,
                                "openflow_runtime_id": runtime_id,
                                "entity_type": "openflow_flow",
                            }
                        )
                        def _openflow_asset(context: AssetExecutionContext, flow_name=flow_name, runtime_id=runtime_id):
                            """Observable OpenFlow flow - monitor via telemetry."""
                            conn = self._create_connection()
                            cursor = conn.cursor()

                            try:
                                # Get recent flow metrics
                                metrics_query = f"""
                                SELECT
                                    TIMESTAMP,
                                    RECORD['metric_name']::STRING AS metric_name,
                                    RECORD['metric_value']::NUMBER AS metric_value,
                                    RECORD['component_name']::STRING AS component_name
                                FROM SNOWFLAKE.TELEMETRY.EVENTS
                                WHERE RECORD_TYPE = 'openflow_metric'
                                AND RECORD['process_group_name']::STRING = '{flow_name}'
                                AND TIMESTAMP >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
                                ORDER BY TIMESTAMP DESC
                                LIMIT 100
                                """

                                cursor.execute(metrics_query)
                                metrics = cursor.fetchall()

                                if metrics:
                                    context.log.info(f"OpenFlow flow {flow_name} has {len(metrics)} recent metrics")
                                else:
                                    context.log.info(f"OpenFlow flow {flow_name} has no recent activity")

                            finally:
                                cursor.close()
                                conn.close()

                        assets_list.append(_openflow_asset)

                except Exception as e:
                    context.log.error(f"Error importing OpenFlow flows: {e}")

        finally:
            conn.close()

        # Create observation sensor if requested
        if self.generate_sensor and (task_metadata or dynamic_table_metadata or snowpipe_metadata):
            @sensor(
                name=f"{self.group_name}_observation_sensor",
                minimum_interval_seconds=self.poll_interval_seconds
            )
            def snowflake_observation_sensor(context: SensorEvaluationContext):
                """Sensor to observe Snowflake task runs, dynamic table refreshes, and Snowpipe loads."""
                conn = self._create_connection()
                cursor = conn.cursor()

                try:
                    # Check for completed task runs
                    for asset_key, metadata in task_metadata.items():
                        task_name = metadata['task_name']
                        db = metadata['database']
                        schema_name = metadata['schema']

                        try:
                            # Get recent task history
                            history_query = f"""
                            SELECT
                                query_id,
                                state,
                                scheduled_time,
                                query_start_time,
                                completed_time
                            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                                TASK_NAME => '{task_name}',
                                SCHEDULED_TIME_RANGE_START => DATEADD('minute', -{self.poll_interval_seconds / 60}, CURRENT_TIMESTAMP())
                            ))
                            WHERE state = 'SUCCEEDED'
                            ORDER BY scheduled_time DESC
                            LIMIT 5
                            """

                            cursor.execute(history_query)

                            for run in cursor:
                                columns = [col[0] for col in cursor.description]
                                run_dict = dict(zip(columns, run))

                                yield AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "query_id": run_dict.get('QUERY_ID'),
                                        "state": run_dict.get('STATE'),
                                        "scheduled_time": str(run_dict.get('SCHEDULED_TIME')) if run_dict.get('SCHEDULED_TIME') else None,
                                        "completed_time": str(run_dict.get('COMPLETED_TIME')) if run_dict.get('COMPLETED_TIME') else None,
                                        "source": "snowflake_observation_sensor",
                                        "entity_type": "task",
                                    }
                                )
                        except Exception as e:
                            context.log.error(f"Error checking runs for task {task_name}: {e}")

                    # Check for dynamic table refreshes
                    for asset_key, metadata in dynamic_table_metadata.items():
                        table_name = metadata['table_name']
                        db = metadata['database']
                        schema_name = metadata['schema']

                        try:
                            # Get dynamic table refresh info
                            info_query = f"""
                            SELECT
                                name,
                                scheduling_state,
                                last_refresh_status,
                                last_successful_refresh_time
                            FROM {db}.INFORMATION_SCHEMA.DYNAMIC_TABLES
                            WHERE schema_name = '{schema_name}' AND name = '{table_name}'
                            """

                            cursor.execute(info_query)
                            result = cursor.fetchone()

                            if result:
                                columns = [col[0] for col in cursor.description]
                                info_dict = dict(zip(columns, result))

                                # Only emit if last refresh was successful
                                if info_dict.get('LAST_REFRESH_STATUS') == 'SUCCESS':
                                    yield AssetMaterialization(
                                        asset_key=asset_key,
                                        metadata={
                                            "table_name": table_name,
                                            "scheduling_state": info_dict.get('SCHEDULING_STATE'),
                                            "last_refresh_status": info_dict.get('LAST_REFRESH_STATUS'),
                                            "last_successful_refresh_time": str(info_dict.get('LAST_SUCCESSFUL_REFRESH_TIME')) if info_dict.get('LAST_SUCCESSFUL_REFRESH_TIME') else None,
                                            "source": "snowflake_observation_sensor",
                                            "entity_type": "dynamic_table",
                                        }
                                    )
                        except Exception as e:
                            context.log.error(f"Error checking refreshes for dynamic table {table_name}: {e}")

                    # Check for Snowpipe loads
                    for asset_key, metadata in snowpipe_metadata.items():
                        pipe_name = metadata['pipe_name']
                        db = metadata['database']
                        schema_name = metadata['schema']

                        try:
                            # Get recent copy history for the pipe
                            history_query = f"""
                            SELECT
                                file_name,
                                stage_location,
                                last_load_time,
                                row_count,
                                row_parsed,
                                file_size,
                                status,
                                first_error_message
                            FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                                TABLE_NAME => '{pipe_name}',
                                START_TIME => DATEADD('minute', -{self.poll_interval_seconds / 60}, CURRENT_TIMESTAMP())
                            ))
                            WHERE status = 'LOADED'
                            ORDER BY last_load_time DESC
                            LIMIT 10
                            """

                            cursor.execute(history_query)

                            for load in cursor:
                                columns = [col[0] for col in cursor.description]
                                load_dict = dict(zip(columns, load))

                                yield AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "pipe_name": pipe_name,
                                        "file_name": load_dict.get('FILE_NAME'),
                                        "last_load_time": str(load_dict.get('LAST_LOAD_TIME')) if load_dict.get('LAST_LOAD_TIME') else None,
                                        "row_count": load_dict.get('ROW_COUNT'),
                                        "file_size": load_dict.get('FILE_SIZE'),
                                        "source": "snowflake_observation_sensor",
                                        "entity_type": "snowpipe",
                                    }
                                )
                        except Exception as e:
                            context.log.error(f"Error checking loads for Snowpipe {pipe_name}: {e}")

                finally:
                    cursor.close()
                    conn.close()

            sensors_list.append(snowflake_observation_sensor)

        return Definitions(
            assets=assets_list,
            sensors=sensors_list if sensors_list else None,
        )
