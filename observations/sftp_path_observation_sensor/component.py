"""SFTP Path Observation Sensor Component.

Polls an SFTP path on a schedule and emits an AssetObservation tagged with a
DataVersion. Downstream AutomationCondition.newly_updated() / .eager() fires
when the DataVersion changes.

Two operating modes:

- resource_key set: `.observe(source)` on the resource where source is
  `"host:remote_path"`. Returns `{"data_version": str, **metadata}`.
- resource_key unset: uses paramiko, lists the remote directory, counts files,
  tracks latest mtime, derives DataVersion from `f"{file_count}-{latest_iso}"`.
"""
import os
from datetime import datetime, timezone
from typing import Any, Optional

import dagster as dg
from dagster import AssetKey, AssetMaterialization, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class SftpPathObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external SFTP path."""
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalSftpPathAsset to observe")
    host: str = Field(description="SFTP host")
    port: int = Field(default=22, description="SFTP port")
    remote_path: str = Field(description="Remote directory path")
    username_env_var: str = Field(default="", description="Env var with SFTP username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with SFTP password")
    private_key_env_var: Optional[str] = Field(default=None, description="Env var with path to SSH private key")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'host:remote_path') that returns `{'data_version': str, **metadata}`."
        ),
    )

    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), emit AssetMaterialization on the target "
            "asset key. External assets show healthy/green in the Dagster UI "
            "and downstream AutomationCondition.eager() fires naturally on "
            "parent updates. When False, emit AssetObservation — free of "
            "Dagster+ credit charges, but the target asset renders as "
            "observed-external (dashed border, gray) and downstream "
            "conditions that gate on ~any_deps_missing() (including "
            "eager()) will not fire. Both event types carry the same "
            "dagster/data_version tag."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(
                dg.AssetKey.from_user_string(_self.asset_key)
            ),
        )
        def _sftp_obs(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation
            # ── Resource-backed path ────────────────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    source = f"{_self.host}:{_self.remote_path}"
                    observed: dict[str, Any] = dict(client.observe(source))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return SensorResult(asset_events=[_event_cls(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native paramiko path ────────────────────────────────────────
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
                if password:
                    kwargs["password"] = password
                if key_path:
                    kwargs["key_filename"] = key_path
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

            latest_iso = datetime.fromtimestamp(latest, tz=timezone.utc).isoformat() if latest else ""
            data_version = f"{file_count}-{latest_iso}"
            metadata = {
                "file_count": file_count,
                "total_size_bytes": total_size,
                "latest_mtime_iso": latest_iso,
                "host": _self.host,
                "remote_path": _self.remote_path,
            }
            return SensorResult(asset_events=[_event_cls(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_sftp_obs])
