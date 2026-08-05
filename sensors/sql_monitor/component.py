"""SQL Monitor Sensor Component.

Monitors a SQL database table for new or updated rows and triggers jobs when
changes are detected. Tracks state via a watermark column (e.g., updated_at,
created_at, or an auto-increment ID). Passes row information via run_config
to downstream assets.
"""

import json
from typing import Optional, Union

from dagster import (
    AssetKey,
    AssetObservation,
    Component,
    ComponentLoadContext,
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
    Resolvable,
    Model,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class SQLMonitorSensorComponent(Component, Model, Resolvable):
    """Component for monitoring a SQL database table for new or updated rows.

    This sensor queries a database table for rows where a watermark column
    (e.g., updated_at, created_at, or an integer ID) is greater than the last
    seen value. The watermark is stored in the sensor cursor.

    Supports any database with a SQLAlchemy-compatible driver:
    PostgreSQL, MySQL, SQL Server, SQLite, Snowflake, BigQuery, Redshift, etc.

    Example:
        ```yaml
        type: dagster_component_templates.SQLMonitorSensorComponent
        attributes:
          sensor_name: orders_table_sensor
          connection_string_env_var: DATABASE_URL
          table_name: orders
          watermark_column: updated_at
          job_name: process_new_orders_job
          minimum_interval_seconds: 60
        ```
    """

    sensor_name: str = Field(description="Unique name for this sensor")

    connection_string_env_var: str = Field(
        description="Name of the environment variable containing the SQLAlchemy connection string "
                    "(e.g., 'postgresql://user:pass@host:5432/db')"
    )

    table_name: str = Field(
        description="Name of the table to monitor. Use 'schema.table' for non-default schemas."
    )

    watermark_column: Union[str, int] = Field(
        description="Column used to detect new/updated rows (e.g., 'updated_at', 'created_at', 'id')"
    )

    job_name: str = Field(description="Name of the job to trigger when new rows are detected")

    id_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Primary key column used as the run_key for deduplication. "
                    "Defaults to the watermark_column if not set."
    )

    batch_size: int = Field(
        default=100,
        description="Maximum number of rows to return per sensor evaluation"
    )

    minimum_interval_seconds: int = Field(
        default=60,
        description="Minimum time (in seconds) between sensor evaluations"
    )

    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster resource key providing a pre-configured client. "
            "When set, context.resources.<resource_key>.poll(table_name, last_id) "
            "is called instead of building a SQLAlchemy engine from "
            "connection_string_env_var. The resource must return a list of dicts, "
            "each shaped {id, ts, source, payload} (payload being the row). "
            "Use for demo-mode seams or shared connection pooling."
        ),
    )

    op_name: Optional[str] = Field(
        default="config",
        description=(
            "Op name to key the run_config under. Emitted as "
            "`{'ops': {op_name: {'config': {...}}}}`. Defaults to 'config' for "
            "backward compat, but if the target job's op is not named 'config' "
            "(e.g. an @asset or @multi_asset with a custom name), set this to "
            "the actual op name — otherwise Dagster rejects the run config with "
            "'Received unexpected config entry \"config\" at path root:ops'."
        ),
    )

    source_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional asset key (e.g. 'orders_source') to receive an "
            "AssetObservation on every tick that yields new rows. Metadata "
            "includes table, new_rows, last_event_id, last_watermark — lets an "
            "observable-source asset in the graph show a live timeline of row "
            "arrivals in the Dagster UI."
        ),
    )

    default_status: str = Field(
        default="running",
        description="Default status of the sensor (running or stopped)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        sensor_name = self.sensor_name
        connection_string_env_var = self.connection_string_env_var
        table_name = self.table_name
        watermark_column = self.watermark_column
        job_name = self.job_name
        id_column = self.id_column or watermark_column
        batch_size = self.batch_size
        minimum_interval_seconds = self.minimum_interval_seconds
        default_status_str = self.default_status
        resource_key = self.resource_key
        op_name = self.op_name or "config"
        source_asset_key = self.source_asset_key

        default_status = (
            DefaultSensorStatus.RUNNING
            if default_status_str == "running"
            else DefaultSensorStatus.STOPPED
        )

        required_resource_keys = {resource_key} if resource_key else set()

        def _observation(new_rows: int, last_id: Optional[str], last_watermark) -> list:
            if not source_asset_key or new_rows == 0:
                return []
            return [
                AssetObservation(
                    asset_key=AssetKey.from_user_string(source_asset_key),
                    metadata={
                        "table": table_name,
                        "new_rows": new_rows,
                        "last_event_id": last_id or "",
                        "last_watermark": str(last_watermark) if last_watermark is not None else "",
                    },
                )
            ]

        @sensor(
            name=sensor_name,
            minimum_interval_seconds=minimum_interval_seconds,
            default_status=default_status,
            job_name=job_name,
            required_resource_keys=required_resource_keys,
        )
        def sql_sensor(context: SensorEvaluationContext):
            """Sensor that monitors a SQL table for new or updated rows."""
            import os

            # ── Resource-backed path (demo-mode seam or shared client) ──────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    events = client.poll(table_name, context.cursor)
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.poll failed: {e}")
                    return SensorResult(skip_reason=f"resource poll failed: {e}")

                run_requests = []
                last_id = context.cursor
                last_ts = None
                for event in events or []:
                    eid = str(event.get("id"))
                    ets = event.get("ts")
                    src = event.get("source", table_name)
                    payload = event.get("payload") or {}
                    serialized = payload if isinstance(payload, str) else json.dumps(
                        {k: str(v) for k, v in dict(payload).items()}
                    )
                    run_requests.append(
                        RunRequest(
                            run_key=f"{table_name}-{eid}",
                            run_config={
                                "ops": {
                                    op_name: {
                                        "config": {
                                            "table_name": table_name,
                                            "row_id": eid,
                                            "watermark_value": str(ets) if ets is not None else "",
                                            "source": src,
                                            "row": serialized,
                                        }
                                    }
                                }
                            },
                        )
                    )
                    last_id = eid
                    last_ts = ets

                if run_requests:
                    context.log.info(
                        f"Found {len(run_requests)} new row(s) in '{table_name}' (via resource '{resource_key}')"
                    )
                    return SensorResult(
                        run_requests=run_requests,
                        cursor=last_id,
                        asset_events=_observation(len(run_requests), last_id, last_ts),
                    )
                return SensorResult(skip_reason=f"No new rows in '{table_name}' (via resource '{resource_key}')")

            # ── SQLAlchemy native path ──────────────────────────────────────
            try:
                from sqlalchemy import create_engine, text
            except ImportError:
                return SensorResult(
                    skip_reason="sqlalchemy is not installed. Run: pip install sqlalchemy"
                )

            conn_str = os.environ.get(connection_string_env_var)
            if not conn_str:
                return SensorResult(
                    skip_reason=f"Environment variable '{connection_string_env_var}' is not set"
                )

            # cursor stores the last-seen watermark value as a JSON string
            last_watermark = context.cursor  # None on first run

            try:
                engine = create_engine(conn_str)
            except Exception as e:
                context.log.error(f"Failed to create database engine: {e}")
                return SensorResult(skip_reason=f"Failed to create database engine: {e}")

            run_requests = []
            new_watermark = last_watermark
            last_row_id: Optional[str] = None

            try:
                with engine.connect() as conn:
                    if last_watermark is None:
                        # First run — get current max watermark and stop (don't process historical data)
                        result = conn.execute(
                            text(f"SELECT MAX({watermark_column}) FROM {table_name}")
                        )
                        max_val = result.scalar()
                        if max_val is not None:
                            new_watermark = str(max_val)
                        context.log.info(
                            f"First run: watermark initialized to {new_watermark}. "
                            "New rows after this point will be processed."
                        )
                        return SensorResult(
                            skip_reason="Watermark initialized — will process new rows on next evaluation",
                            cursor=new_watermark,
                        )

                    # Query for rows newer than the last watermark
                    query = text(
                        f"SELECT * FROM {table_name} "
                        f"WHERE {watermark_column} > :watermark "
                        f"ORDER BY {watermark_column} ASC "
                        f"LIMIT :batch_size"
                    )
                    result = conn.execute(
                        query,
                        {"watermark": last_watermark, "batch_size": batch_size},
                    )
                    rows = result.mappings().all()

                    for row in rows:
                        row_dict = dict(row)
                        row_id = str(row_dict.get(id_column, ""))
                        watermark_val = row_dict.get(watermark_column)

                        # Serialize all values to strings for run_config transport
                        serializable_row = {k: str(v) for k, v in row_dict.items()}

                        run_requests.append(
                            RunRequest(
                                run_key=f"{table_name}-{id_column}-{row_id}",
                                run_config={
                                    "ops": {
                                        op_name: {
                                            "config": {
                                                "table_name": table_name,
                                                "watermark_column": watermark_column,
                                                "watermark_value": str(watermark_val),
                                                "row_id_column": id_column,
                                                "row_id": row_id,
                                                "row": json.dumps(serializable_row),
                                                "columns": json.dumps(list(row_dict.keys())),
                                            }
                                        }
                                    }
                                },
                            )
                        )

                        if new_watermark is None or str(watermark_val) > new_watermark:
                            new_watermark = str(watermark_val)
                        last_row_id = row_id

            except Exception as e:
                context.log.error(f"Error querying table '{table_name}': {e}")
                return SensorResult(skip_reason=f"Error querying database: {e}")

            if run_requests:
                context.log.info(
                    f"Found {len(run_requests)} new/updated row(s) in '{table_name}' "
                    f"(watermark: {last_watermark} → {new_watermark})"
                )
                return SensorResult(
                    run_requests=run_requests,
                    cursor=new_watermark,
                    asset_events=_observation(len(run_requests), last_row_id, new_watermark),
                )

            return SensorResult(
                skip_reason=f"No new rows in '{table_name}' since watermark {last_watermark}",
                cursor=new_watermark,
            )

        return Definitions(sensors=[sql_sensor])
