"""Snowflake task completion sensor.

Polls ``INFORMATION_SCHEMA.TASK_HISTORY`` for a named task and emits
``AssetMaterialization`` for the matching asset_key on each new SUCCESS
row. Optionally fires a ``RunRequest`` for a downstream Dagster job.

Snowflake owns the task's cron schedule; Dagster surfaces those
autonomous runs in the catalog. Pair with
``snowflake_task_execute_asset`` (use the same ``asset_key``) so both
on-demand materializations AND scheduler-fired runs land on the same
catalog asset.
"""
from typing import Any, Dict, Optional

import dagster as dg
from dagster import (
    AssetMaterialization,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class SnowflakeTaskCompletionSensorComponent(Component, Model, Resolvable):
    """Emit AssetMaterialization on each new Snowflake task SUCCESS.

    Example:
        ```yaml
        type: dagster_community_components.SnowflakeTaskCompletionSensorComponent
        attributes:
          sensor_name: refresh_orders_done
          task_name: DAILY_REFRESH_ORDERS
          database: DAGSTER_DEMO
          schema: RAW
          asset_key: snowflake/tasks/refresh_orders   # pair with execute asset
          account_env_var: SNOWFLAKE_ACCOUNT
          user_env_var: SNOWFLAKE_USER
          password_env_var: SNOWFLAKE_PASSWORD
          warehouse_env_var: SNOWFLAKE_WAREHOUSE
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    task_name: str = Field(description="Snowflake task to watch.")
    database: str = Field(description="Snowflake database holding the task.")
    schema_name: str = Field(description="Snowflake schema holding the task.", alias="schema")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster asset key. When set, the sensor yields "
            "AssetMaterialization(asset_key=...) on each new SUCCESS row "
            "in TASK_HISTORY. Use '/' separators for nested keys."
        ),
    )
    job_name: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster job to trigger via RunRequest on each new SUCCESS. "
            "When unset, the sensor is observe-only (AssetMaterialization "
            "without RunRequest)."
        ),
    )

    account_env_var: str = Field(description="Env var with Snowflake account.")
    user_env_var: str = Field(description="Env var with Snowflake username.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake password.")
    pat_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake PAT.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var with RSA private key path.")
    private_key_passphrase_env_var: Optional[str] = Field(default=None, description="Env var with private-key passphrase.")
    warehouse_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake warehouse.")
    role_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake role.")

    minimum_interval_seconds: int = Field(default=60, description="Seconds between polls.")
    lookback_minutes: int = Field(
        default=10,
        description="How far back to query TASK_HISTORY each tick. Should be ≥ poll interval.",
    )
    default_status: str = Field(default="running", description="'running' or 'stopped'.")

    model_config = {"populate_by_name": True}

    def _connection_kwargs(self) -> Dict[str, Any]:
        import os
        kwargs: Dict[str, Any] = {
            "account": os.environ[self.account_env_var],
            "user": os.environ[self.user_env_var],
            "database": self.database,
            "schema": self.schema_name,
        }
        if self.warehouse_env_var:
            kwargs["warehouse"] = os.environ.get(self.warehouse_env_var, "")
        if self.role_env_var:
            kwargs["role"] = os.environ.get(self.role_env_var, "")
        if self.pat_env_var and os.environ.get(self.pat_env_var):
            kwargs["password"] = os.environ[self.pat_env_var]
        elif self.private_key_path_env_var and os.environ.get(self.private_key_path_env_var):
            kwargs["private_key_file"] = os.environ[self.private_key_path_env_var]
            if self.private_key_passphrase_env_var:
                kwargs["private_key_file_pwd"] = os.environ.get(self.private_key_passphrase_env_var, "")
            kwargs["authenticator"] = "snowflake_jwt"
        elif self.password_env_var:
            kwargs["password"] = os.environ.get(self.password_env_var, "")
        return kwargs

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _self = self
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
        )
        def snowflake_task_completion_sensor(context: SensorEvaluationContext):
            try:
                import snowflake.connector
            except ImportError:
                return SensorResult(skip_reason="snowflake-connector-python not installed")

            try:
                conn = snowflake.connector.connect(**_self._connection_kwargs())
            except Exception as e:
                return SensorResult(skip_reason=f"Snowflake connect failed: {e}")

            cursor = conn.cursor()
            try:
                cursor.execute(f"""
                    SELECT query_id, state, scheduled_time, completed_time,
                           error_code, error_message
                    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                        TASK_NAME => '{_self.task_name}',
                        SCHEDULED_TIME_RANGE_START =>
                            DATEADD('minute', -{_self.lookback_minutes}, CURRENT_TIMESTAMP())
                    ))
                    WHERE state = 'SUCCEEDED'
                    ORDER BY scheduled_time DESC
                """)
                rows = cursor.fetchall()
            except Exception as e:
                cursor.close()
                conn.close()
                return SensorResult(skip_reason=f"Snowflake TASK_HISTORY query failed: {e}")
            finally:
                pass

            cursor_state = context.cursor or ""
            new_rows = []
            for row in rows:
                qid = row[0]
                if qid == cursor_state:
                    break  # we've already processed this and everything older
                new_rows.append(row)

            cursor.close()
            conn.close()

            if not new_rows:
                return SensorResult(skip_reason=f"No new SUCCEEDED rows for {_self.task_name}")

            asset_events = []
            run_requests = []
            for row in reversed(new_rows):  # emit oldest-first for chronological order
                qid, state, sched_t, comp_t, err_code, err_msg = row
                if _self.asset_key:
                    asset_events.append(AssetMaterialization(
                        asset_key=dg.AssetKey(_self.asset_key.split("/")),
                        description=f"Snowflake task {_self.task_name} → {state}",
                        metadata={
                            "task_name": _self.task_name,
                            "database": _self.database,
                            "schema": _self.schema_name,
                            "query_id": qid,
                            "state": state,
                            "scheduled_time": str(sched_t) if sched_t else None,
                            "completed_time": str(comp_t) if comp_t else None,
                        },
                    ))
                if _self.job_name:
                    run_requests.append(RunRequest(
                        run_key=qid,
                        run_config={"ops": {"config": {
                            "snowflake_task_name": _self.task_name,
                            "snowflake_query_id": qid,
                        }}},
                    ))

            # Latest query_id becomes the new cursor.
            newest_qid = new_rows[0][0]
            return SensorResult(
                asset_events=asset_events or None,
                run_requests=run_requests or None,
                cursor=newest_qid,
            )

        return Definitions(sensors=[snowflake_task_completion_sensor])
