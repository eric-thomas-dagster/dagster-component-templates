"""ADLS Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class AdlsObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalAdlsAsset to observe")
    account_name: str = Field(description="Azure storage account name")
    container_name: str = Field(description="ADLS container / filesystem name")
    path_prefix: str = Field(default="", description="Path prefix within the container")
    connection_string_env_var: Optional[str] = Field(default=None, description="Env var with connection string (alternative to DefaultAzureCredential)")
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
        def _adls_obs(context: SensorEvaluationContext):
            try:
                from azure.storage.filedatalake import DataLakeServiceClient
            except ImportError:
                return SensorResult(skip_reason="azure-storage-file-datalake not installed")

            try:
                if resource_key:
                    client = getattr(context.resources, resource_key)
                elif _self.connection_string_env_var:
                    import os
                    conn_str = os.environ.get(_self.connection_string_env_var, "")
                    client = DataLakeServiceClient.from_connection_string(conn_str)
                else:
                    from azure.identity import DefaultAzureCredential
                    client = DataLakeServiceClient(
                        account_url=f"https://{_self.account_name}.dfs.core.windows.net",
                        credential=DefaultAzureCredential(),
                    )
                fs = client.get_file_system_client(_self.container_name)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            count = 0; total = 0; latest = None
            try:
                prefix = _self.path_prefix or None
                for path in fs.get_paths(path=prefix, recursive=True):
                    if not path.is_directory:
                        count += 1
                        total += path.content_length or 0
                        lm = path.last_modified
                        if lm and (latest is None or lm > latest):
                            latest = lm
            except Exception as e:
                return SensorResult(skip_reason=f"List failed: {e}")

            metadata = {
                "file_count": count,
                "total_size_bytes": total,
                "latest_modified_iso": latest.isoformat() if latest else "",
                "account_name": _self.account_name,
                "container_name": _self.container_name,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_adls_obs])
