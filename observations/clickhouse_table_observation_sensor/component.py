"""ClickHouse Table Observation Sensor Component.

Periodically connects to ClickHouse and emits AssetObservation events
for an external ClickHouse table. Uses ClickHouseResource when available.
"""
from typing import Optional
import dagster as dg
from dagster import SensorEvaluationContext, SensorResult
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class ClickHouseTableObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external ClickHouse table.

    Queries system tables for row count, disk size, active parts, and engine type.
    Use a resource_key pointing to a ClickHouseResource for shared connection config,
    or provide env vars directly.

    Example (env vars):
        ```yaml
        type: dagster_component_templates.ClickHouseTableObservationSensorComponent
        attributes:
          sensor_name: clickhouse_events_observation
          asset_key: clickhouse/analytics/events
          database: analytics
          table: events
          host_env_var: CLICKHOUSE_HOST
          password_env_var: CLICKHOUSE_PASSWORD
        ```

    Example (resource):
        ```yaml
        type: dagster_component_templates.ClickHouseTableObservationSensorComponent
        attributes:
          sensor_name: clickhouse_events_observation
          asset_key: clickhouse/analytics/events
          database: analytics
          table: events
          resource_key: clickhouse
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the external ClickHouse table to observe")
    database: str = Field(description="ClickHouse database name")
    table: str = Field(description="ClickHouse table name")
    # Connection via env vars (used when resource_key is not set)
    host_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port")
    username_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse password")
    # Connection via resource
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of a ClickHouseResource. When set, env var fields are ignored.",
    )
    check_interval_seconds: int = Field(default=300, description="Seconds between observations")
    default_status: str = Field(default="running", description="running or stopped")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key} if self.resource_key else set()
        asset_key = dg.AssetKey(self.asset_key.split("/"))
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            default_status=default_status,
            required_resource_keys=required_resource_keys,
            monitored_assets=dg.AssetSelection.keys(asset_key),
        )
        def clickhouse_observation_sensor(context: SensorEvaluationContext):
            import os

            try:
                import clickhouse_connect
            except ImportError:
                return SensorResult(skip_reason="clickhouse-connect not installed. Run: pip install clickhouse-connect")

            # Get client — via resource or env vars
            if _self.resource_key:
                resource = getattr(context.resources, _self.resource_key)
                client = resource.get_client()
            else:
                host = os.environ.get(_self.host_env_var or "", "")
                username = os.environ.get(_self.username_env_var or "", "default") if _self.username_env_var else "default"
                password = os.environ.get(_self.password_env_var or "", "") if _self.password_env_var else ""
                client = clickhouse_connect.get_client(
                    host=host, port=_self.port, username=username, password=password,
                    secure=(_self.port == 8443),
                )

            db, tbl = _self.database, _self.table
            try:
                row_count = client.command(f"SELECT count() FROM {db}.{tbl}")
                size_bytes = client.command(
                    f"SELECT sum(bytes_on_disk) FROM system.parts "
                    f"WHERE database = '{db}' AND table = '{tbl}' AND active"
                )
                last_modified = client.command(
                    f"SELECT max(modification_time) FROM system.parts "
                    f"WHERE database = '{db}' AND table = '{tbl}' AND active"
                )
                parts_count = client.command(
                    f"SELECT count() FROM system.parts "
                    f"WHERE database = '{db}' AND table = '{tbl}' AND active"
                )
                engine = client.command(
                    f"SELECT engine FROM system.tables WHERE database = '{db}' AND name = '{tbl}'"
                )
            except Exception as e:
                return SensorResult(skip_reason=f"ClickHouse query error: {e}")

            observation = dg.AssetObservation(
                asset_key=asset_key,
                metadata={
                    "row_count": dg.MetadataValue.int(int(row_count or 0)),
                    "size_bytes": dg.MetadataValue.int(int(size_bytes or 0)),
                    "active_parts": dg.MetadataValue.int(int(parts_count or 0)),
                    "engine": dg.MetadataValue.text(str(engine or "")),
                    "last_modified": dg.MetadataValue.text(str(last_modified or "")),
                    "database": dg.MetadataValue.text(db),
                    "table": dg.MetadataValue.text(tbl),
                },
            )
            return SensorResult(asset_events=[observation])

        return dg.Definitions(sensors=[clickhouse_observation_sensor])
