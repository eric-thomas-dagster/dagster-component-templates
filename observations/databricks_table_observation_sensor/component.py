"""Databricks Table Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class DatabricksTableObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external Databricks Delta table."""
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalDatabricksTableAsset to observe")
    workspace_url: str = Field(description="Databricks workspace URL")
    catalog: Optional[str] = Field(default=None, description="Unity Catalog name")
    schema_name: str = Field(description="Schema/database name")
    table_name: str = Field(description="Table name")
    token_env_var: str = Field(description="Env var with Databricks personal access token")
    http_path: str = Field(description="SQL warehouse HTTP path (from connection details)")
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
        def _dbx_obs(context: SensorEvaluationContext):
            import os
            try:
                from databricks import sql as dbsql
            except ImportError:
                return SensorResult(skip_reason="databricks-sql-connector not installed")

            token = os.environ.get(_self.token_env_var, "")
            full_name = (
                f"{_self.catalog}.{_self.schema_name}.{_self.table_name}"
                if _self.catalog else f"{_self.schema_name}.{_self.table_name}"
            )
            try:
                if resource_key:
                    conn = getattr(context.resources, resource_key).get_connection()
                else:
                    conn = dbsql.connect(
                        server_hostname=_self.workspace_url.replace("https://", ""),
                        http_path=_self.http_path,
                        access_token=token,
                    )
                with conn.cursor() as cur:
                    cur.execute(f"DESCRIBE DETAIL {full_name}")
                    detail = dict(zip([d[0] for d in cur.description], cur.fetchone()))
                    cur.execute(f"SELECT COUNT(*) FROM {full_name}")
                    row_count = cur.fetchone()[0]
                conn.close()
            except Exception as e:
                return SensorResult(skip_reason=f"Query failed: {e}")

            metadata = {
                "row_count": row_count,
                "size_in_bytes": detail.get("sizeInBytes", 0),
                "num_files": detail.get("numFiles", 0),
                "last_modified": str(detail.get("lastModified", "")),
                "table_name": full_name,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_dbx_obs])
