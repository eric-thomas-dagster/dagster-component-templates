"""SFTP Monitor Sensor Component.

Monitors an SFTP directory for new files and triggers a job
when new files are detected. Extremely common in regulated industries
(finance, healthcare, insurance) for file-based integrations.

resource_key, when set, genuinely drives the connection via a registered
SFTPResourceComponent's own get_client() method (a paramiko SFTPClient) --
previously this field was declared and wired into required_resource_keys
but never actually read, so setting it silently had no effect and the
sensor always built its own ad-hoc paramiko connection from
username_env_var/password_env_var/private_key_env_var regardless.
"""
from typing import Optional
import dagster as dg
from dagster import RunRequest, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field, model_validator


class SftpMonitorSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Monitor an SFTP directory for new files.

    Example:
        ```yaml
        type: dagster_community_components.SftpMonitorSensorComponent
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
    host: Optional[str] = Field(default=None, description="SFTP host. Required unless resource_key is set.")
    port: int = Field(default=22, description="SFTP port. Ignored when resource_key is set.")
    username_env_var: Optional[str] = Field(default=None, description="Env var with SFTP username. Required unless resource_key is set.")
    password_env_var: Optional[str] = Field(default=None, description="Env var with SFTP password. Ignored when resource_key is set.")
    private_key_env_var: Optional[str] = Field(default=None, description="Env var with path to SSH private key file. Ignored when resource_key is set.")
    remote_path: str = Field(description="Remote directory path to monitor")
    file_pattern: str = Field(default="*", description="Glob-style filename filter (e.g. '*.csv')")
    job_name: str = Field(description="Job to trigger when new files are detected")
    minimum_interval_seconds: int = Field(default=300, description="Seconds between polls")
    default_status: str = Field(default="running", description="running or stopped")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Optional resource key registered by an SFTPResourceComponent. When set, the "
            "connection comes from that resource's own get_client() method instead of the "
            "inline host/username_env_var/password_env_var/private_key_env_var below."
        ),
    )

    @model_validator(mode="after")
    def _check_credentials(self):
        if not self.resource_key and not (self.host and self.username_env_var):
            raise ValueError(
                "SftpMonitorSensorComponent: supply resource_key (a registered "
                "SFTPResourceComponent) OR both host and username_env_var."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Sftp Monitor Sensor', editable=True)

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
        def sftp_sensor(context: SensorEvaluationContext, **_resources):
            import fnmatch
            ssh = None

            if resource_key:
                _res = getattr(context.resources, resource_key, None)
                if _res is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    sftp = _res.get_client()
                except Exception as e:
                    return SensorResult(skip_reason=f"SFTP connect failed (via resource '{resource_key}'): {e}")
            else:
                import os
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
                # Only close a connection we opened ourselves -- a resource-owned
                # client manages its own lifecycle (and may be reused elsewhere).
                if not resource_key:
                    try:
                        sftp.close()
                        ssh.close()
                    except Exception:
                        pass

            if run_requests:
                return SensorResult(run_requests=run_requests, cursor=latest)
            return SensorResult(skip_reason=f"No new files in {_self.remote_path}")

        return dg.Definitions(sensors=[sftp_sensor])
