"""External SharePoint Library Asset Component."""
from typing import Any, Dict, List, Optional
import dagster as dg
from pydantic import Field

def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class ExternalSharePointLibraryAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a SharePoint document library as an observable external asset."""
    asset_key: str = Field(description="Dagster asset key")
    site_url: str = Field(description="SharePoint site URL")
    library_name: str = Field(description="Document library name")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    create_observation_sensor: bool = Field(
        default=False,
        description=(
            "Also create the polling sensor that keeps this external asset's health/data-version "
            "current -- same trio as the sql/s3/sftp/kafka ExternalXAsset siblings. When False "
            "(default), this component only declares the AssetSpec."
        ),
    )
    sensor_name: Optional[str] = Field(
        default=None,
        description="Unique sensor name. Defaults to '{asset_key}__observation_sensor'. Only used when create_observation_sensor=True.",
    )
    check_interval_seconds: int = Field(
        default=300,
        description="Seconds between health checks. Only used when create_observation_sensor=True.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'site_url/library_name') that returns `{'data_version': str, **metadata}`. "
            "Only used when create_observation_sensor=True; unset uses Microsoft Graph "
            "directly (requires tenant_id_env_var/client_id_env_var/client_secret_env_var)."
        ),
    )
    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), the sensor emits AssetMaterialization (asset shows healthy/green, "
            "downstream AutomationCondition.eager() fires on parent updates). When False, emits "
            "AssetObservation instead (no Dagster+ credit charge, but eager()-style conditions won't "
            "fire). Only used when create_observation_sensor=True."
        ),
    )
    tenant_id_env_var: Optional[str] = Field(
        default=None,
        description="Env var with Azure tenant ID. Only used when create_observation_sensor=True and resource_key is unset.",
    )
    client_id_env_var: Optional[str] = Field(
        default=None,
        description="Env var with Azure app client ID. Only used when create_observation_sensor=True and resource_key is unset.",
    )
    client_secret_env_var: Optional[str] = Field(
        default=None,
        description="Env var with Azure app client secret. Only used when create_observation_sensor=True and resource_key is unset.",
    )
    folder_path: str = Field(
        default="",
        description="Optional subfolder path within the library to observe. Only used when create_observation_sensor=True and resource_key is unset.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi', or None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date for time-based partition types (e.g. '2024-01-01').",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static / multi partition types.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec; overrides flat fields when set.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=('partition_values',), tags=())

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        spec = dg.AssetSpec(
            key=dg.AssetKey.from_user_string(self.asset_key),
            group_name=self.group_name,
            description=self.description or f"SharePoint {self.site_url}/{self.library_name}",
            kinds={"sharepoint", "microsoft", "files"},
            metadata={
                "site_url": self.site_url,
                "library_name": self.library_name,
                "dagster.observability_type": "external",
            },
            partitions_def=partitions_def,
        )

        if not self.create_observation_sensor:
            return dg.Definitions(assets=[spec])

        import re as _re
        _self = self
        sensor_name = self.sensor_name or f"{_re.sub(r'[^A-Za-z0-9_]+', '_', self.asset_key).strip('_')}__observation_sensor"
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.assets(dg.AssetKey.from_user_string(self.asset_key)),
        )
        def _sharepoint_obs(context: dg.SensorEvaluationContext, **_resources):
            import os
            from dagster._core.definitions.data_version import DATA_VERSION_TAG
            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation

            # ── Resource-backed path (preferred) ────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    source = f"{_self.site_url}/{_self.library_name}"
                    observed: dict = dict(client.observe(source))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return dg.SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return dg.SensorResult(asset_events=[_event_cls(
                    asset_key=dg.AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native Microsoft Graph path ──────────────────────────────────
            try:
                import requests
            except ImportError:
                return dg.SensorResult(skip_reason="requests not installed")

            if not (_self.tenant_id_env_var and _self.client_id_env_var and _self.client_secret_env_var):
                return dg.SensorResult(
                    skip_reason=(
                        "tenant_id_env_var/client_id_env_var/client_secret_env_var are required "
                        "for the native Microsoft Graph path (or set resource_key instead)."
                    )
                )
            tenant_id = os.environ.get(_self.tenant_id_env_var, "")
            client_id = os.environ.get(_self.client_id_env_var, "")
            client_secret = os.environ.get(_self.client_secret_env_var, "")

            try:
                token_resp = requests.post(
                    f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
                    data={"grant_type": "client_credentials", "client_id": client_id,
                          "client_secret": client_secret,
                          "scope": "https://graph.microsoft.com/.default"},
                    timeout=30,
                )
                token_resp.raise_for_status()
                access_token = token_resp.json()["access_token"]
            except Exception as e:
                return dg.SensorResult(skip_reason=f"Auth failed: {e}")

            headers = {"Authorization": f"Bearer {access_token}"}

            try:
                hostname = _self.site_url.split("/")[2]
                site_path = "/" + "/".join(_self.site_url.split("/")[3:])
                site_resp = requests.get(
                    f"https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}",
                    headers=headers, timeout=30,
                )
                site_resp.raise_for_status()
                site_id = site_resp.json()["id"]
            except Exception as e:
                return dg.SensorResult(skip_reason=f"Could not resolve site ID: {e}")

            try:
                drive_resp = requests.get(
                    f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
                    headers=headers, timeout=30,
                )
                drive_resp.raise_for_status()
                drives = drive_resp.json().get("value", [])
                drive = next((d for d in drives if d["name"] == _self.library_name), None)
                if not drive:
                    return dg.SensorResult(skip_reason=f"Library '{_self.library_name}' not found")
                drive_id = drive["id"]

                folder = _self.folder_path.strip("/")
                path_segment = f":/{folder}:" if folder else ""
                items_resp = requests.get(
                    f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root{path_segment}/children",
                    headers=headers, params={"$top": 200}, timeout=30,
                )
                items_resp.raise_for_status()
                items = items_resp.json().get("value", [])
            except Exception as e:
                return dg.SensorResult(skip_reason=f"List files failed: {e}")

            file_count = 0
            total_size = 0
            latest_modified = ""
            for item in items:
                if "folder" in item:
                    continue
                file_count += 1
                total_size += item.get("size", 0)
                modified = item.get("lastModifiedDateTime", "")
                if modified > latest_modified:
                    latest_modified = modified

            data_version = f"{file_count}-{latest_modified}"
            metadata = {
                "file_count": file_count,
                "total_size_bytes": total_size,
                "latest_modified_iso": latest_modified,
                "site_url": _self.site_url,
                "library_name": _self.library_name,
            }
            return dg.SensorResult(asset_events=[_event_cls(
                asset_key=dg.AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(assets=[spec], sensors=[_sharepoint_obs])
