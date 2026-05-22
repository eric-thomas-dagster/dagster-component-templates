"""Snowflake OpenFlow status sensor.

Polls ``SNOWFLAKE.TELEMETRY.EVENTS`` for openflow_metric rows belonging
to a named flow (process_group_name) and emits ``AssetMaterialization``
when the flow's most recent activity timestamp advances beyond the
sensor cursor. Optionally fires a ``RunRequest`` for downstream Dagster
work.

OpenFlow owns the schedule; Dagster surfaces its activity in the catalog.
Pair with ``external_snowflake_openflow_flow`` (same ``asset_key``) for
the catalog node + materialization history pattern.
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


class SnowflakeOpenflowStatusSensorComponent(Component, Model, Resolvable):
    """Emit AssetMaterialization on each new OpenFlow telemetry activity for a named flow.

    Example:
        ```yaml
        type: dagster_community_components.SnowflakeOpenflowStatusSensorComponent
        attributes:
          sensor_name: customer_sync_done
          flow_name: customer_sync
          asset_key: snowflake/openflow/customer_sync
          account_env_var: SNOWFLAKE_ACCOUNT
          user_env_var: SNOWFLAKE_USER
          password_env_var: SNOWFLAKE_PASSWORD
          warehouse_env_var: SNOWFLAKE_WAREHOUSE
        ```
    """

    sensor_name: str = Field(description="Unique sensor name.")
    flow_name: str = Field(description="OpenFlow process-group name.")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional Dagster asset key. When set, the sensor yields "
            "AssetMaterialization(asset_key=...) when the flow's most recent "
            "activity timestamp advances past the cursor."
        ),
    )
    job_name: Optional[str] = Field(
        default=None,
        description="Optional Dagster job to trigger via RunRequest on each activity advance.",
    )

    account_env_var: str = Field(description="Env var with Snowflake account.")
    user_env_var: str = Field(description="Env var with Snowflake username.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake password.")
    pat_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake PAT.")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Env var with RSA private key path.")
    private_key_passphrase_env_var: Optional[str] = Field(default=None, description="Env var with private-key passphrase.")
    warehouse_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake warehouse.")
    role_env_var: Optional[str] = Field(default=None, description="Env var with Snowflake role.")

    minimum_interval_seconds: int = Field(default=120, description="Seconds between Telemetry polls.")
    lookback_minutes: int = Field(default=15, description="How far back to query TELEMETRY each tick.")
    default_status: str = Field(default="running", description="'running' or 'stopped'.")

    def _connection_kwargs(self) -> Dict[str, Any]:
        import os
        kwargs: Dict[str, Any] = {
            "account": os.environ[self.account_env_var],
            "user": os.environ[self.user_env_var],
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
        def snowflake_openflow_status_sensor(context: SensorEvaluationContext):
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
                    SELECT MAX(TIMESTAMP) AS latest_activity,
                           COUNT(*) AS event_count,
                           MAX(RECORD['runtime_id']::STRING) AS runtime_id
                    FROM SNOWFLAKE.TELEMETRY.EVENTS
                    WHERE RECORD_TYPE = 'openflow_metric'
                      AND RECORD['process_group_name']::STRING = '{_self.flow_name}'
                      AND TIMESTAMP >=
                          DATEADD('minute', -{_self.lookback_minutes}, CURRENT_TIMESTAMP())
                """)
                row = cursor.fetchone()
            except Exception as e:
                cursor.close()
                conn.close()
                return SensorResult(skip_reason=f"OpenFlow TELEMETRY query failed: {e}")

            cursor.close()
            conn.close()

            if not row or row[0] is None:
                return SensorResult(skip_reason=f"No telemetry for OpenFlow flow {_self.flow_name}")

            latest_activity, event_count, runtime_id = row
            cursor_key = str(latest_activity)
            prev_cursor = context.cursor or ""

            if cursor_key == prev_cursor:
                return SensorResult(skip_reason=f"No new telemetry since {prev_cursor}")

            asset_events = []
            if _self.asset_key:
                asset_events.append(AssetMaterialization(
                    asset_key=dg.AssetKey(_self.asset_key.split("/")),
                    description=f"OpenFlow flow {_self.flow_name} activity",
                    metadata={
                        "openflow_flow_name": _self.flow_name,
                        "openflow_runtime_id": runtime_id,
                        "latest_activity": cursor_key,
                        "event_count_in_window": int(event_count or 0),
                        "lookback_minutes": _self.lookback_minutes,
                    },
                ))

            run_requests = []
            if _self.job_name:
                run_requests.append(RunRequest(
                    run_key=cursor_key,
                    run_config={"ops": {"config": {
                        "openflow_flow_name": _self.flow_name,
                        "openflow_runtime_id": runtime_id,
                        "openflow_latest_activity": cursor_key,
                    }}},
                ))

            return SensorResult(
                asset_events=asset_events or None,
                run_requests=run_requests or None,
                cursor=cursor_key,
            )

        return Definitions(sensors=[snowflake_openflow_status_sensor])
