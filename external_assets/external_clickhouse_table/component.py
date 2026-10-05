"""External ClickHouse Table Component.

Declares a ClickHouse table as an observable external asset in Dagster.
Includes a ClickHouseResource for reuse across components.

Set `create_observation_sensor: true` to also get the polling sensor that
`clickhouse_table_observation_sensor` provides standalone -- one component,
one YAML, asset + sensor wired together automatically. The standalone sensor
component is unaffected and still exists for cases where the sensor needs to
observe an asset_key defined elsewhere.
"""
from typing import Any, Dict, List, Optional
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class ClickHouseResource(ConfigurableResource):
    """Resource for connecting to ClickHouse.

    Example (dagster.yaml or Definitions):
        ```python
        ClickHouseResource(
            host=EnvVar("CLICKHOUSE_HOST"),
            password=EnvVar("CLICKHOUSE_PASSWORD"),
        )
        ```
    """

    host: str = Field(description="ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port (8443 for HTTPS, 8123 for HTTP)")
    username: str = Field(default="default", description="ClickHouse username")
    password: str = Field(default="", description="ClickHouse password")
    secure: bool = Field(default=True, description="Use HTTPS (recommended)")

    def get_client(self):
        """Return a clickhouse_connect client."""
        import clickhouse_connect
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            secure=self.secure,
        )

    def query_value(self, sql: str):
        """Execute a scalar query and return the result."""
        client = self.get_client()
        return client.command(sql)


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


class ExternalClickHouseTableComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declare a ClickHouse table as an observable external asset.

    Example:
        ```yaml
        type: dagster_component_templates.ExternalClickHouseTableComponent
        attributes:
          asset_key: clickhouse/analytics/events
          database: analytics
          table: events
          host_env_var: CLICKHOUSE_HOST
          password_env_var: CLICKHOUSE_PASSWORD
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'clickhouse/analytics/events')")
    database: str = Field(description="ClickHouse database name")
    table: str = Field(description="ClickHouse table name")
    host_env_var: str = Field(description="Env var with ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port")
    username_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse password")
    description: Optional[str] = Field(default=None, description="Human-readable description")
    group_name: Optional[str] = Field(default="clickhouse", description="Dagster asset group name")
    owners: Optional[list] = Field(default=None, description="List of owner emails or team names")

    create_observation_sensor: bool = Field(
        default=False,
        description=(
            "Also create the polling sensor that keeps this external asset's health/data-version "
            "current (same logic as the standalone clickhouse_table_observation_sensor component). "
            "When False (default), this component only declares the AssetSpec -- pair it with a "
            "separate clickhouse_table_observation_sensor component yourself if you want observation."
        ),
    )
    sensor_name: Optional[str] = Field(
        default=None,
        description="Unique sensor name. Defaults to '{table}__observation_sensor'. Only used when create_observation_sensor=True.",
    )
    check_interval_seconds: int = Field(
        default=300,
        description="Seconds between observations. Only used when create_observation_sensor=True.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Key of a ClickHouseResource exposing `.observe(source) -> dict` (source is "
            "'database.table') that returns `{'data_version': str, **metadata}`. Only used when "
            "create_observation_sensor=True; unset uses clickhouse-connect directly via "
            "host_env_var/port/username_env_var/password_env_var."
        ),
    )
    default_status: str = Field(
        default="running",
        description="running or stopped. Only used when create_observation_sensor=True.",
    )
    emit_materialization: bool = Field(
        default=True,
        description=(
            "When True (default), emit AssetMaterialization on the target asset key. External assets "
            "show healthy/green in the Dagster UI and downstream AutomationCondition.eager() fires "
            "naturally on parent updates. When False, emit AssetObservation -- free of Dagster+ credit "
            "charges, but the target asset renders as observed-external (dashed border, gray) and "
            "downstream conditions that gate on ~any_deps_missing() (including eager()) will not fire. "
            "Both event types carry the same dagster/data_version tag. Only used when "
            "create_observation_sensor=True."
        ),
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily'|'weekly'|'monthly'|'hourly'|'static'|'dynamic'|'multi', or None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date for time-based partition types (e.g. '2024-01-01').",
    )
    partition_values: Optional[str] = Field(
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
            description=self.description or f"ClickHouse table {self.database}.{self.table}",
            group_name=self.group_name,
            owners=self.owners or [],
            kinds={"clickhouse", "sql"},
            metadata={
                "dagster/storage_kind": "clickhouse",
                "dagster/observability_type": "external",
                "database": self.database,
                "table": self.table,
                "full_table_name": f"{self.database}.{self.table}",
            },
            partitions_def=partitions_def,
        )

        if not self.create_observation_sensor:
            return dg.Definitions(assets=[spec])

        from dagster._core.definitions.sensor_definition import DefaultSensorStatus

        _self = self
        sensor_name = self.sensor_name or f"{self.table}__observation_sensor"
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()
        asset_key = dg.AssetKey.from_user_string(self.asset_key)
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.check_interval_seconds,
            default_status=default_status,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.keys(asset_key),
        )
        def _ch_obs(context: dg.SensorEvaluationContext):
            from dagster._core.definitions.data_version import DATA_VERSION_TAG
            _event_cls = dg.AssetMaterialization if _self.emit_materialization else dg.AssetObservation

            # ── Resource-backed path ────────────────────────────────────────
            if resource_key:
                resource = getattr(context.resources, resource_key, None)
                if resource is None:
                    return dg.SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    source = f"{_self.database}.{_self.table}"
                    observed: dict[str, Any] = dict(resource.observe(source))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return dg.SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return dg.SensorResult(asset_events=[_event_cls(
                    asset_key=asset_key,
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native clickhouse-connect path ──────────────────────────────
            import os

            try:
                import clickhouse_connect
            except ImportError:
                return dg.SensorResult(skip_reason="clickhouse-connect not installed. Run: pip install clickhouse-connect")

            host = os.environ.get(_self.host_env_var or "", "")
            username = os.environ.get(_self.username_env_var or "", "default") if _self.username_env_var else "default"
            password = os.environ.get(_self.password_env_var or "", "") if _self.password_env_var else ""
            client = clickhouse_connect.get_client(
                host=host, port=_self.port, username=username, password=password,
                secure=(_self.port == 8443),
            )

            db, tbl = _self.database, _self.table
            try:
                row_count = client.command(f"SELECT count() FROM {db}.{tbl}")
                size_bytes = client.command(
                    f"SELECT sum(bytes_on_disk) FROM system.parts "
                    f"WHERE database = '{db}' AND table = '{tbl}' AND active"
                )
                last_modified = client.command(
                    f"SELECT max(modification_time) FROM system.parts "
                    f"WHERE database = '{db}' AND table = '{tbl}' AND active"
                )
                parts_count = client.command(
                    f"SELECT count() FROM system.parts "
                    f"WHERE database = '{db}' AND table = '{tbl}' AND active"
                )
                engine = client.command(
                    f"SELECT engine FROM system.tables WHERE database = '{db}' AND name = '{tbl}'"
                )
            except Exception as e:
                return dg.SensorResult(skip_reason=f"ClickHouse query error: {e}")

            data_version = f"{int(row_count or 0)}-{last_modified or ''}"
            observation = _event_cls(
                asset_key=asset_key,
                metadata={
                    "row_count": dg.MetadataValue.int(int(row_count or 0)),
                    "size_bytes": dg.MetadataValue.int(int(size_bytes or 0)),
                    "active_parts": dg.MetadataValue.int(int(parts_count or 0)),
                    "engine": dg.MetadataValue.text(str(engine or "")),
                    "last_modified": dg.MetadataValue.text(str(last_modified or "")),
                    "database": dg.MetadataValue.text(db),
                    "table": dg.MetadataValue.text(tbl),
                },
                tags={DATA_VERSION_TAG: data_version},
            )
            return dg.SensorResult(asset_events=[observation])

        return dg.Definitions(assets=[spec], sensors=[_ch_obs])
