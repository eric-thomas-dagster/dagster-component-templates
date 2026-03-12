"""SFTP Path Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class SftpPathObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external SFTP path."""
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalSftpPathAsset to observe")
    host: str = Field(description="SFTP host")
    port: int = Field(default=22, description="SFTP port")
    remote_path: str = Field(description="Remote directory path")
    username_env_var: str = Field(description="Env var with SFTP username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with SFTP password")
    private_key_env_var: Optional[str] = Field(default=None, description="Env var with path to SSH private key")
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
        def _sftp_obs(context: SensorEvaluationContext):
            import os
            try:
                import paramiko
            except ImportError:
                return SensorResult(skip_reason="paramiko not installed")

            username = os.environ.get(_self.username_env_var, "")
            password = os.environ.get(_self.password_env_var) if _self.password_env_var else None
            key_path = os.environ.get(_self.private_key_env_var) if _self.private_key_env_var else None

            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                kwargs = {"username": username, "port": _self.port, "timeout": 15}
                if password: kwargs["password"] = password
                if key_path: kwargs["key_filename"] = key_path
                ssh.connect(_self.host, **kwargs)
                sftp = ssh.open_sftp()
                attrs = sftp.listdir_attr(_self.remote_path)
                file_count = sum(1 for a in attrs if not (a.st_mode & 0o040000))
                total_size = sum(a.st_size or 0 for a in attrs if not (a.st_mode & 0o040000))
                latest = max((a.st_mtime or 0 for a in attrs), default=0)
                sftp.close()
                ssh.close()
            except Exception as e:
                return SensorResult(skip_reason=f"SFTP failed: {e}")

            from datetime import datetime, timezone
            metadata = {
                "file_count": file_count,
                "total_size_bytes": total_size,
                "latest_mtime_iso": datetime.fromtimestamp(latest, tz=timezone.utc).isoformat() if latest else "",
                "host": _self.host,
                "remote_path": _self.remote_path,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_sftp_obs])
