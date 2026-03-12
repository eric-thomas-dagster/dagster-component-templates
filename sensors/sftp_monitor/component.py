"""SFTP Monitor Sensor Component.

Monitors an SFTP directory for new files and triggers a job
when new files are detected. Extremely common in regulated industries
(finance, healthcare, insurance) for file-based integrations.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field


class SftpMonitorSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Monitor an SFTP directory for new files.

    Example:
        ```yaml
        type: dagster_component_templates.SftpMonitorSensorComponent
        attributes:
          sensor_name: sftp_drop_zone_monitor
          host: sftp.partner.com
          username_env_var: SFTP_USERNAME
          password_env_var: SFTP_PASSWORD
          remote_path: /incoming/orders
          job_name: process_sftp_file_job
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    host: str = Field(description="SFTP host")
    port: int = Field(default=22, description="SFTP port")
    username_env_var: str = Field(description="Env var with SFTP username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with SFTP password")
    private_key_env_var: Optional[str] = Field(default=None, description="Env var with path to SSH private key file")
    remote_path: str = Field(description="Remote directory path to monitor")
    file_pattern: str = Field(default="*", description="Glob-style filename filter (e.g. '*.csv')")
    job_name: str = Field(description="Job to trigger when new files are detected")
    minimum_interval_seconds: int = Field(default=300, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")
    resource_key: Optional[str] = Field(default=None, description="Optional Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.minimum_interval_seconds,
            default_status=default_status,
            job_name=_self.job_name,
            required_resource_keys=required_resource_keys,
        )
        def sftp_sensor(context: SensorEvaluationContext):
            import os, fnmatch
            try:
                import paramiko
            except ImportError:
                return SensorResult(skip_reason="paramiko not installed. Run: pip install paramiko")

            username = os.environ.get(_self.username_env_var, "")
            password = os.environ.get(_self.password_env_var) if _self.password_env_var else None
            key_path = os.environ.get(_self.private_key_env_var) if _self.private_key_env_var else None

            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                connect_kwargs = {"username": username, "port": _self.port, "timeout": 30}
                if password:
                    connect_kwargs["password"] = password
                if key_path:
                    connect_kwargs["key_filename"] = key_path
                ssh.connect(_self.host, **connect_kwargs)
                sftp = ssh.open_sftp()
            except Exception as e:
                return SensorResult(skip_reason=f"SFTP connect failed: {e}")

            cursor = context.cursor or ""
            run_requests = []
            latest = cursor

            try:
                file_attrs = sftp.listdir_attr(_self.remote_path)
                for attr in file_attrs:
                    name = attr.filename
                    if not fnmatch.fnmatch(name, _self.file_pattern):
                        continue
                    mtime = attr.st_mtime or 0
                    size = attr.st_size or 0
                    run_key = f"{name}-{mtime}"
                    if run_key <= cursor:
                        continue
                    run_requests.append(RunRequest(
                        run_key=run_key,
                        run_config={"ops": {"config": {
                            "host": _self.host,
                            "remote_path": _self.remote_path,
                            "file_name": name,
                            "full_path": f"{_self.remote_path}/{name}",
                            "size_bytes": size,
                            "modified_timestamp": mtime,
                        }}},
                    ))
                    if run_key > latest:
                        latest = run_key
            except Exception as e:
                return SensorResult(skip_reason=f"List dir failed: {e}")
            finally:
                try:
                    sftp.close()
                    ssh.close()
                except Exception:
                    pass

            if run_requests:
                return SensorResult(run_requests=run_requests, cursor=latest)
            return SensorResult(skip_reason=f"No new files in {_self.remote_path}")

        return dg.Definitions(sensors=[sftp_sensor])
