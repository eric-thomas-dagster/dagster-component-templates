"""External SFTP Path Asset Component.

Set `create_observation_sensor: true` to also get the polling sensor that
`sftp_path_observation_sensor` provides standalone -- one component, one
YAML, asset + sensor wired together automatically. The standalone sensor
component is unaffected and still exists for cases where the sensor needs
to observe an asset_key defined elsewhere.
"""
import re
from typing import Any, Dict, List, Optional
import dagster as dg
from pydantic import Field

def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
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
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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


class ExternalSftpPathAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare an SFTP directory as an observable external asset."""
    host: str = Field(description="SFTP host")
    asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster asset key for a single path. Mutually exclusive with `paths:` -- use this "
            "(with `remote_path:`) for one directory, or `paths:` to declare several from one "
            "component instance sharing host and one observation sensor."
        ),
    )
    remote_path: Optional[str] = Field(
        default=None,
        description="Remote directory path for a single path. Mutually exclusive with `paths:`.",
    )
    paths: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Declare several remote directories as external assets from one component instance, "
            "instead of one ExternalSftpPathAsset YAML block (and, with "
            "create_observation_sensor=true, one polling sensor and one SSH connection) per path. "
            "Each entry: `key` (Dagster asset key, required), `remote_path` (required), plus "
            "optional `deps`, `owners`, `metadata`, `group_name`, `description`. All entries "
            "share this component's host/port/credentials and (if enabled) ONE sensor that polls "
            "every path over a single SSH connection per tick -- set `sensor_name` explicitly "
            "when using `paths:` with create_observation_sensor=true. Mutually exclusive with the "
            "top-level `asset_key`/`remote_path` fields."
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    create_observation_sensor: bool = Field(
        default=False,
        description=(
            "Also create the polling sensor that keeps this external asset's health/data-version "
            "current (same logic as the standalone sftp_path_observation_sensor component). "
            "When False (default), this component only declares the AssetSpec -- pair it with a "
            "separate sftp_path_observation_sensor component yourself if you want observation."
        ),
    )
    sensor_name: Optional[str] = Field(
        default=None,
        description=(
            "Unique sensor name. Defaults to '{asset_key}__observation_sensor' in single-path mode. "
            "Required when using `paths:` (no single path to derive a default from). Only used "
            "when create_observation_sensor=True."
        ),
    )
    port: int = Field(default=22, description="SFTP port. Only used when create_observation_sensor=True.")
    username_env_var: str = Field(
        default="",
        description="Env var with SFTP username. Only used when create_observation_sensor=True.",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var with SFTP password. Only used when create_observation_sensor=True.",
    )
    private_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with path to SSH private key. Only used when create_observation_sensor=True.",
    )
    check_interval_seconds: int = Field(
        default=300,
        description="Seconds between health checks. Only used when create_observation_sensor=True.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'host:remote_path') that returns `{'data_version': str, **metadata}`. "
            "Only used when create_observation_sensor=True; unset uses paramiko directly."
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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi', or None for unpartitioned.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
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

    def _resolve_path_entries(self) -> List[Dict[str, Any]]:
        has_single = bool(self.asset_key or self.remote_path)
        has_multi = bool(self.paths)
        if has_single and has_multi:
            raise ValueError("Set either asset_key/remote_path (single path) or paths: (several paths), not both.")
        if not has_single and not has_multi:
            raise ValueError("Set either asset_key/remote_path (single path) or paths: (several paths).")

        if has_single:
            if not self.asset_key or not self.remote_path:
                raise ValueError("Both asset_key and remote_path are required in single-path mode.")
            return [{
                "key": self.asset_key,
                "remote_path": self.remote_path,
                "deps": None,
                "owners": None,
                "metadata": None,
                "group_name": self.group_name,
                "description": self.description,
            }]

        entries = []
        seen_keys = set()
        for i, raw in enumerate(self.paths):
            if not isinstance(raw, dict):
                raise ValueError(f"paths[{i}] must be a mapping, got {type(raw).__name__}")
            key = raw.get("key")
            remote_path = raw.get("remote_path")
            if not key:
                raise ValueError(f"paths[{i}] is missing required field 'key'")
            if not remote_path:
                raise ValueError(f"paths[{i}] ({key!r}) is missing required field 'remote_path'")
            if key in seen_keys:
                raise ValueError(f"paths: duplicate key {key!r}")
            seen_keys.add(key)
            entries.append({
                "key": key,
                "remote_path": remote_path,
                "deps": raw.get("deps"),
                "owners": raw.get("owners"),
                "metadata": raw.get("metadata"),
                "group_name": raw.get("group_name", self.group_name),
                "description": raw.get("description"),
            })
        return entries

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        entries = self._resolve_path_entries()
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)

        specs = []
        for e in entries:
            metadata = {
                "host": self.host,
                "remote_path": e["remote_path"],
                "dagster/uri": f"sftp://{self.host}{e['remote_path']}",
                "dagster.observability_type": "external",
            }
            if e["metadata"]:
                metadata.update(e["metadata"])
            specs.append(dg.AssetSpec(
                key=dg.AssetKey.from_user_string(e["key"]),
                group_name=e["group_name"],
                description=e["description"] or f"SFTP {self.host}{e['remote_path']}",
                kinds={"sftp", "files"},
                deps=[dg.AssetKey.from_user_string(d) for d in e["deps"]] if e["deps"] else None,
                owners=e["owners"] or None,
                metadata=metadata,
                partitions_def=partitions_def,
            ))

        if not self.create_observation_sensor:
            return dg.Definitions(assets=specs)

        is_multi = self.paths is not None
        if is_multi and not self.sensor_name:
            raise ValueError(
                "sensor_name is required when using paths: with create_observation_sensor=true "
                "(there's no single path to derive a default from)."
            )
        sensor_name = self.sensor_name or f"{re.sub(r'[^A-Za-z0-9_]+', '_', entries[0]['key']).strip('_')}__observation_sensor"
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        _entries = entries
        _self = self

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.assets(*[dg.AssetKey.from_user_string(e["key"]) for e in _entries]),
        )
        def _sftp_obs(context: dg.SensorEvaluationContext, **_resources):
            import os
            from datetime import datetime, timezone
            from dagster._core.definitions.data_version import DATA_VERSION_TAG
            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation
            events = []

            # ── Resource-backed path (preferred): one client.observe() call per directory ──
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                for e in _entries:
                    try:
                        source = f"{_self.host}:{e['remote_path']}"
                        observed: dict[str, Any] = dict(client.observe(source))
                    except Exception as ex:
                        context.log.error(f"resource '{resource_key}'.observe({e['remote_path']!r}) failed: {ex}")
                        continue
                    data_version = str(observed.pop("data_version", ""))
                    events.append(_event_cls(
                        asset_key=dg.AssetKey.from_user_string(e["key"]),
                        metadata=observed,
                        tags={DATA_VERSION_TAG: data_version} if data_version else None,
                    ))
                if not events:
                    return dg.SensorResult(skip_reason="no paths observed this tick")
                return dg.SensorResult(asset_events=events)

            try:
                import paramiko
            except ImportError:
                return dg.SensorResult(skip_reason="paramiko not installed")

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
            except Exception as ex:
                return dg.SensorResult(skip_reason=f"SSH connect failed: {ex}")

            try:
                sftp = ssh.open_sftp()
                try:
                    for e in _entries:
                        remote_path = e["remote_path"]
                        try:
                            attrs = sftp.listdir_attr(remote_path)
                        except Exception as ex:
                            context.log.error(f"SFTP listdir for {remote_path!r} failed: {ex}")
                            continue
                        file_count = sum(1 for a in attrs if not (a.st_mode & 0o040000))
                        total_size = sum(a.st_size or 0 for a in attrs if not (a.st_mode & 0o040000))
                        latest = max((a.st_mtime or 0 for a in attrs), default=0)
                        latest_iso = datetime.fromtimestamp(latest, tz=timezone.utc).isoformat() if latest else ""
                        data_version = f"{file_count}-{latest_iso}"
                        obs_metadata: dict[str, Any] = {
                            "file_count": file_count,
                            "total_size_bytes": total_size,
                            "latest_mtime_iso": latest_iso,
                            "host": _self.host,
                            "remote_path": remote_path,
                        }
                        events.append(_event_cls(
                            asset_key=dg.AssetKey.from_user_string(e["key"]),
                            metadata=obs_metadata,
                            tags={DATA_VERSION_TAG: data_version},
                        ))
                finally:
                    sftp.close()
            finally:
                ssh.close()

            if not events:
                return dg.SensorResult(skip_reason="no paths observed this tick")
            return dg.SensorResult(asset_events=events)

        return dg.Definitions(assets=specs, sensors=[_sftp_obs])
