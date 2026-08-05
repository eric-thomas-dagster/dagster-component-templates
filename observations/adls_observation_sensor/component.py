"""ADLS Observation Sensor Component.

Polls an ADLS Gen2 container/prefix on a schedule and emits an AssetObservation
tagged with a DataVersion. Downstream AutomationCondition.newly_updated() /
.eager() fires when the DataVersion changes.

Two operating modes:

- resource_key set: sensor calls `.observe(source)` on the resource, where
  source is `"container/path_prefix"`. Resource returns
  `{"data_version": str, **metadata}`.

- resource_key unset: sensor connects via azure-storage-file-datalake, walks
  the prefix, counts files, tracks latest mtime, and derives a DataVersion
  from `f"{file_count}-{latest_iso}"`.
"""
import os
from typing import Any, Optional

import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class AdlsObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalAdlsAsset to observe")
    account_name: str = Field(description="Azure storage account name")
    container_name: str = Field(description="ADLS container / filesystem name")
    path_prefix: str = Field(default="", description="Path prefix within the container")
    connection_string_env_var: Optional[str] = Field(default=None, description="Env var with connection string (alternative to DefaultAzureCredential)")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'container/path_prefix') that returns `{'data_version': str, **metadata}`."
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
        def _adls_obs(context: SensorEvaluationContext, **_resources):
            # ── Resource-backed path ────────────────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    source = f"{_self.container_name}/{_self.path_prefix}" if _self.path_prefix else _self.container_name
                    observed: dict[str, Any] = dict(client.observe(source))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return SensorResult(asset_events=[AssetObservation(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native azure-storage-file-datalake path ─────────────────────
            try:
                from azure.storage.filedatalake import DataLakeServiceClient
            except ImportError:
                return SensorResult(skip_reason="azure-storage-file-datalake not installed")

            try:
                if _self.connection_string_env_var:
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

            count = 0
            total = 0
            latest = None
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

            latest_iso = latest.isoformat() if latest else ""
            data_version = f"{count}-{latest_iso}"
            metadata = {
                "file_count": count,
                "total_size_bytes": total,
                "latest_modified_iso": latest_iso,
                "account_name": _self.account_name,
                "container_name": _self.container_name,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_adls_obs])
