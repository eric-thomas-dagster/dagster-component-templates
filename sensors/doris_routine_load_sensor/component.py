"""Apache Doris Routine Load status sensor.

Doris's **Routine Load** is the continuous Kafka-→-Doris ingestion path
(server-side managed; Doris's analog to Snowflake Snowpipe AUTO_INGEST).
Customers running real-time pipelines often have N Routine Load jobs
keeping tables hot from upstream Kafka topics.

This sensor polls ``SHOW ALL ROUTINE LOAD FOR <db>`` (or the modern
``SHOW ROUTINE LOAD`` against a specific job name) and emits Dagster
materializations / observations + alerting RunRequests on **state
changes** — RUNNING → PAUSED / CANCELLED / STOPPED. Healthy steady
state (still RUNNING) is a no-op tick.

Pair with ``external_doris_table`` declaring the destination table —
the sensor's events flow onto that asset_key, so the catalog reflects
Routine Load health alongside the table.

Job states (per Doris docs):
  - RUNNING   → healthy
  - NEED_SCHEDULE → transient (job warming up)
  - PAUSED    → admin-paused OR auto-paused on errors (degraded)
  - CANCELLED → admin-cancelled (terminal)
  - STOPPED   → terminal stop
"""
from typing import Dict, List, Optional

import dagster as dg
from dagster import (
    AssetMaterialization,
    AssetObservation,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field

DORIS_ROUTINE_LOAD_HEALTHY = {"RUNNING", "NEED_SCHEDULE"}
DORIS_ROUTINE_LOAD_UNHEALTHY = {"PAUSED", "CANCELLED", "STOPPED"}


class DorisRoutineLoadSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Watch Apache Doris Routine Load jobs for state changes.

    Example:

        ```yaml
        type: dagster_community_components.DorisRoutineLoadSensorComponent
        attributes:
          sensor_name: doris_orders_rl_health
          database: analytics
          job_name: orders_rl                  # or omit to watch all jobs in db
          host_env_var: DORIS_FE_HOST
          query_port: 9030
          username_env_var: DORIS_USER
          password_env_var: DORIS_PASSWORD
          job_name_target: alert_oncall_job
          asset_key: doris/analytics/events_realtime
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    database: str = Field(description="Doris database to watch.")
    job_name: Optional[str] = Field(
        default=None,
        description=(
            "Specific Routine Load job name. If unset, watches all jobs "
            "in the database and fires an event per state change observed."
        ),
    )
    job_name_target: str = Field(
        description="Dagster job to trigger when an unhealthy state is detected.",
    )
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional asset key for the destination table. Sensor emits "
            "AssetObservation (default — Routine Load is an external "
            "ingestion path; observation is the right shape) on state changes."
        ),
    )

    # Connection.
    host_env_var: str = Field(default="DORIS_FE_HOST", description="Env var with Doris FE host.")
    query_port: int = Field(default=9030, description="Doris MySQL-protocol query port.")
    username_env_var: str = Field(default="DORIS_USER", description="Env var with username.")
    password_env_var: str = Field(default="DORIS_PASSWORD", description="Env var with password.")

    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls.")
    default_status: str = Field(default="running", description="'running' or 'stopped'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name_target,
        )
        def doris_routine_load_sensor(context: SensorEvaluationContext):
            import os, json
            try:
                from sqlalchemy import create_engine, text
            except ImportError:
                return SensorResult(skip_reason="sqlalchemy + pymysql not installed")
            import urllib.parse

            host = os.environ.get(_self.host_env_var, "")
            user = os.environ.get(_self.username_env_var, "")
            pw = os.environ.get(_self.password_env_var, "")
            if not host or not user:
                return SensorResult(skip_reason=f"{_self.host_env_var} / {_self.username_env_var} not set")

            pw_q = urllib.parse.quote_plus(pw)
            conn_str = (
                f"mysql+pymysql://{user}:{pw_q}@{host}:{_self.query_port}/{_self.database}"
            )
            engine = create_engine(conn_str)

            sql = (
                f"SHOW ROUTINE LOAD FOR {_self.database}.{_self.job_name}"
                if _self.job_name else
                f"SHOW ALL ROUTINE LOAD FROM {_self.database}"
            )
            try:
                with engine.begin() as conn:
                    result = conn.execute(text(sql)).mappings().all()
            except Exception as e:
                return SensorResult(skip_reason=f"Doris SHOW ROUTINE LOAD failed: {e}")

            # Cursor: JSON dict job_name → State, so we only fire on state changes.
            try:
                prev_state: Dict[str, str] = json.loads(context.cursor) if context.cursor else {}
            except Exception:
                prev_state = {}

            new_state: Dict[str, str] = dict(prev_state)
            asset_events: List = []
            run_requests: List = []

            for row in result:
                # Doris column names: Id, Name, CreateTime, PauseTime, EndTime,
                # DbName, TableName, State, DataSourceType, CurrentTaskNum,
                # JobProperties, DataSourceProperties, CustomProperties,
                # Statistic, Progress, Lag, ReasonOfStateChanged, ErrorLogUrls, OtherMsg
                name = row.get("Name") or row.get("name")
                state = (row.get("State") or row.get("state") or "UNKNOWN").upper()
                if not name:
                    continue
                new_state[name] = state
                if prev_state.get(name) == state:
                    continue  # no change

                metadata = {
                    "doris_routine_load_job": name,
                    "doris_routine_load_state": state,
                    "doris_database": _self.database,
                    "doris_table": row.get("TableName") or row.get("tablename"),
                    "doris_progress": str(row.get("Progress", "") or ""),
                    "doris_reason_of_state_changed": str(row.get("ReasonOfStateChanged", "") or ""),
                    "doris_error_log_urls": str(row.get("ErrorLogUrls", "") or ""),
                }
                if _self.asset_key:
                    ak = dg.AssetKey(_self.asset_key.split("/"))
                    if state in DORIS_ROUTINE_LOAD_HEALTHY:
                        # Healthy resumption — emit Observation (asset state OK).
                        asset_events.append(AssetObservation(
                            asset_key=ak,
                            description=f"Routine Load {name} → {state}",
                            metadata=metadata,
                        ))
                    else:
                        # Unhealthy — emit Materialization (visible in failure feed).
                        asset_events.append(AssetMaterialization(
                            asset_key=ak,
                            description=f"Routine Load {name} → {state} (degraded)",
                            metadata=metadata,
                        ))

                # Fire a RunRequest on unhealthy → run an oncall job.
                if state in DORIS_ROUTINE_LOAD_UNHEALTHY:
                    run_requests.append(RunRequest(
                        run_key=f"{name}-{state}",
                        tags={
                            "doris/routine_load_job": name,
                            "doris/routine_load_state": state,
                            "doris/database": _self.database,
                        },
                    ))

            if not asset_events and not run_requests:
                return SensorResult(
                    skip_reason="No Routine Load state changes this tick.",
                    cursor=json.dumps(new_state),
                )

            return SensorResult(
                asset_events=asset_events or None,
                run_requests=run_requests or None,
                cursor=json.dumps(new_state),
            )

        return dg.Definitions(sensors=[doris_routine_load_sensor])
