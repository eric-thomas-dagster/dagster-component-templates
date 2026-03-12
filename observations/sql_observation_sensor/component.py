"""SQL Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class SqlObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalSqlAsset to observe")
    table_name: str = Field(description="Table name to observe")
    connection_string_env_var: str = Field(description="Env var with SQLAlchemy connection string")
    watermark_column: Optional[str] = Field(default=None, description="Column for latest value metric (e.g. updated_at)")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(default=None, description="Optional Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            monitored_assets=dg.AssetSelection.keys(
                dg.AssetKey(_self.asset_key.split("/"))
            ),
        )
        def _sql_obs(context: SensorEvaluationContext):
            try:
                import sqlalchemy as sa
            except ImportError:
                return SensorResult(skip_reason="sqlalchemy not installed")

            import os
            try:
                if resource_key:
                    engine = getattr(context.resources, resource_key).get_engine()
                else:
                    conn_str = os.environ.get(_self.connection_string_env_var, "")
                    engine = sa.create_engine(conn_str)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                with engine.connect() as conn:
                    row_count = conn.execute(
                        sa.text(f"SELECT COUNT(*) FROM {_self.table_name}")
                    ).scalar()
                    latest_watermark = None
                    if _self.watermark_column:
                        latest_watermark = conn.execute(
                            sa.text(f"SELECT MAX({_self.watermark_column}) FROM {_self.table_name}")
                        ).scalar()
            except Exception as e:
                return SensorResult(skip_reason=f"Query failed: {e}")

            metadata = {
                "row_count": row_count,
                "table_name": _self.table_name,
            }
            if latest_watermark is not None:
                metadata["latest_watermark"] = str(latest_watermark)

            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_sql_obs])
