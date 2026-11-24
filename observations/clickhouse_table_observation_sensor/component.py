"""ClickHouse Table Observation Sensor Component.

Periodically connects to ClickHouse and emits an AssetObservation (or
AssetMaterialization) for one or more external ClickHouse tables. Uses
ClickHouseResource when available.

A cursor (`{asset_key_str: last_seen_data_version}`, JSON-encoded) tracks the
last observed DataVersion per table, so a tick where nothing changed emits
NOTHING instead of re-materializing every table on every poll -- without
this, a downstream freshness policy on the observed asset is meaningless (the
asset always looks fresh, whether or not anything new actually landed).
"""
from typing import Any, Dict, List, Optional
import json
import dagster as dg
from dagster import AssetMaterialization, AssetObservation, SensorEvaluationContext, SensorResult
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from dagster._core.definitions.sensor_definition import DefaultSensorStatus
from pydantic import Field, model_validator


# BEGIN ClickHouse system-table observation helper (inlined per component; keep copies identical
# with external_clickhouse_table's own copy -- see tests/test_resource_key.py)
def _observe_via_client(client, database: str, table: str) -> dict:
    """Query system tables for row count, disk size, active parts, and engine via a
    clickhouse-connect-shaped client (anything exposing `.command()`). Returns
    `{'data_version': str, **metadata}`, the same shape the native env-var path has always
    produced -- shared so a ClickHouseResourceComponent-backed client (get_client() only, no
    custom .observe()) can be observed the same way the native path is."""
    row_count = client.command(f"SELECT count() FROM {database}.{table}")
    size_bytes = client.command(
        f"SELECT sum(bytes_on_disk) FROM system.parts "
        f"WHERE database = '{database}' AND table = '{table}' AND active"
    )
    last_modified = client.command(
        f"SELECT max(modification_time) FROM system.parts "
        f"WHERE database = '{database}' AND table = '{table}' AND active"
    )
    parts_count = client.command(
        f"SELECT count() FROM system.parts "
        f"WHERE database = '{database}' AND table = '{table}' AND active"
    )
    engine = client.command(
        f"SELECT engine FROM system.tables WHERE database = '{database}' AND name = '{table}'"
    )
    return {
        "data_version": f"{int(row_count or 0)}-{last_modified or ''}",
        "row_count": dg.MetadataValue.int(int(row_count or 0)),
        "size_bytes": dg.MetadataValue.int(int(size_bytes or 0)),
        "active_parts": dg.MetadataValue.int(int(parts_count or 0)),
        "engine": dg.MetadataValue.text(str(engine or "")),
        "last_modified": dg.MetadataValue.text(str(last_modified or "")),
        "database": dg.MetadataValue.text(database),
        "table": dg.MetadataValue.text(table),
    }
# END ClickHouse system-table observation helper


class ClickHouseTableObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for one or more external ClickHouse tables.

    Queries system tables for row count, disk size, active parts, and engine type.
    Use a resource_key pointing to a ClickHouseResource for shared connection config,
    or provide env vars directly.

    A cursor tracks each table's last-seen DataVersion so a tick only emits a
    materialization for a table when its DataVersion actually changed -- not on
    every poll regardless.

    Example (env vars, single table):
        ```yaml
        type: dagster_community_components.ClickHouseTableObservationSensorComponent
        attributes:
          sensor_name: clickhouse_events_observation
          asset_key: clickhouse/analytics/events
          database: analytics
          table: events
          host_env_var: CLICKHOUSE_HOST
          password_env_var: CLICKHOUSE_PASSWORD
        ```

    Example (resource, single table):
        ```yaml
        type: dagster_community_components.ClickHouseTableObservationSensorComponent
        attributes:
          sensor_name: clickhouse_events_observation
          asset_key: clickhouse/analytics/events
          database: analytics
          table: events
          resource_key: clickhouse
        ```

    Example (`targets:`, several tables from one component instance):
        ```yaml
        type: dagster_community_components.ClickHouseTableObservationSensorComponent
        attributes:
          sensor_name: clickhouse_observation
          resource_key: clickhouse
          targets:
            - asset_key: clickhouse/analytics/events
              database: analytics
              table: events
            - asset_key: clickhouse/analytics/sessions
              database: analytics
              table: sessions
        ```
    """

    sensor_name: str = Field(description="Unique sensor name")
    asset_key: Optional[str] = Field(
        default=None,
        description="Asset key of the external ClickHouse table to observe, for a single table. Mutually exclusive with `targets:`.",
    )
    database: Optional[str] = Field(default=None, description="ClickHouse database name, for a single table. Mutually exclusive with `targets:`.")
    table: Optional[str] = Field(default=None, description="ClickHouse table name, for a single table. Mutually exclusive with `targets:`.")
    targets: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Declare several ClickHouse tables from one component instance, instead of one "
            "ClickHouseTableObservationSensorComponent YAML block (and one polling sensor, one "
            "connection/resource call per tick) per table. Each entry: `asset_key` (required), "
            "`database` (required), `table` (required). All entries share this component's "
            "resource_key/host_env_var/.../check_interval_seconds/emit_materialization and ONE "
            "sensor that polls every table per tick. Mutually exclusive with the top-level "
            "`asset_key`/`database`/`table` fields."
        ),
    )
    # Connection via env vars (used when resource_key is not set)
    host_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse host")
    port: int = Field(default=8443, description="ClickHouse port")
    username_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with ClickHouse password")
    # Connection via resource
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Key of a registered resource. When set, env var fields are ignored. Two resource "
            "shapes are supported: (1) a custom resource exposing `.observe(source) -> dict` "
            "(source is 'database.table'), which returns `{'data_version': str, **metadata}` "
            "verbatim; (2) a registered ClickHouseResourceComponent (resources/clickhouse_resource) "
            "exposing only `.get_client()` -- its clickhouse-connect client is used to run the same "
            "system-table queries as the native path below, so one shared credential can back both "
            "this sensor and other ClickHouse components without configuring it twice."
        ),
    )
    check_interval_seconds: int = Field(default=300, description="Seconds between observations")
    default_status: str = Field(default="running", description="running or stopped")

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

    @model_validator(mode="after")
    def _validate_targets(self):
        has_single = bool(self.asset_key or self.database or self.table)
        has_multi = bool(self.targets)
        if has_single and has_multi:
            raise ValueError("Set either asset_key/database/table (single table) or targets: (several tables), not both.")
        if not has_single and not has_multi:
            raise ValueError("Set either asset_key/database/table (single table) or targets: (several tables).")
        if has_single and not (self.asset_key and self.database and self.table):
            raise ValueError("asset_key, database, and table are all required in single-table mode.")
        return self

    def _resolve_targets(self) -> List[Dict[str, Any]]:
        if self.targets:
            entries = []
            seen = set()
            for i, raw in enumerate(self.targets):
                if not isinstance(raw, dict):
                    raise ValueError(f"targets[{i}] must be a mapping, got {type(raw).__name__}")
                asset_key = raw.get("asset_key")
                database = raw.get("database")
                table = raw.get("table")
                if not asset_key:
                    raise ValueError(f"targets[{i}] is missing required field 'asset_key'")
                if not database or not table:
                    raise ValueError(f"targets[{i}] ({asset_key!r}) requires both 'database' and 'table'")
                if asset_key in seen:
                    raise ValueError(f"targets: duplicate asset_key {asset_key!r}")
                seen.add(asset_key)
                entries.append({"asset_key": asset_key, "database": database, "table": table})
            return entries
        return [{"asset_key": self.asset_key, "database": self.database, "table": self.table}]

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Click House Table Observation Sensor', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key} if self.resource_key else set()
        entries = self._resolve_targets()
        asset_keys = [dg.AssetKey.from_user_string(e["asset_key"]) for e in entries]
        default_status = (
            DefaultSensorStatus.RUNNING if self.default_status == "running"
            else DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            default_status=default_status,
            required_resource_keys=required_resource_keys,
            asset_selection=dg.AssetSelection.assets(*asset_keys),
        )
        def clickhouse_observation_sensor(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation

            # Cursor: {asset_key_str: last_seen_data_version}. Without this, every
            # tick re-emits a materialization for every table even when nothing
            # changed, which makes any downstream freshness policy meaningless --
            # the asset always looks fresh, whether or not anything new landed.
            try:
                last_versions: Dict[str, str] = json.loads(context.cursor) if context.cursor else {}
            except (TypeError, ValueError):
                last_versions = {}
            new_versions: Dict[str, str] = dict(last_versions)
            events = []

            # ── Resource-backed path ────────────────────────────────────────
            if _self.resource_key:
                resource = getattr(context.resources, _self.resource_key, None)
                if resource is None:
                    return SensorResult(skip_reason=f"resource '{_self.resource_key}' not found on context")
                for e in entries:
                    key_str = e["asset_key"]
                    try:
                        if hasattr(resource, "observe"):
                            source = f"{e['database']}.{e['table']}"
                            observed: dict[str, Any] = dict(resource.observe(source))
                        else:
                            # ClickHouseResourceComponent-style resource (get_client() only, no
                            # custom .observe()): run the same system-table queries the native
                            # path below runs, via the shared client.
                            observed = _observe_via_client(resource.get_client(), e["database"], e["table"])
                    except Exception as ex:
                        context.log.error(f"resource '{_self.resource_key}' observation of {key_str!r} failed: {ex}")
                        continue
                    data_version = str(observed.pop("data_version", ""))
                    new_versions[key_str] = data_version
                    if data_version and data_version == last_versions.get(key_str):
                        continue  # unchanged since last tick -- don't re-materialize
                    events.append(_event_cls(
                        asset_key=dg.AssetKey.from_user_string(key_str),
                        metadata=observed,
                        tags={DATA_VERSION_TAG: data_version} if data_version else None,
                    ))
                cursor = json.dumps(new_versions)
                if not events:
                    return SensorResult(cursor=cursor, skip_reason="no target changed since last tick")
                return SensorResult(asset_events=events, cursor=cursor)

            # ── Native clickhouse-connect path ──────────────────────────────
            import os

            try:
                import clickhouse_connect
            except ImportError:
                return SensorResult(skip_reason="clickhouse-connect not installed. Run: pip install clickhouse-connect")

            host = os.environ.get(_self.host_env_var or "", "")
            username = os.environ.get(_self.username_env_var or "", "default") if _self.username_env_var else "default"
            password = os.environ.get(_self.password_env_var or "", "") if _self.password_env_var else ""
            try:
                client = clickhouse_connect.get_client(
                    host=host, port=_self.port, username=username, password=password,
                    secure=(_self.port == 8443),
                )
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            for e in entries:
                key_str = e["asset_key"]
                try:
                    observed = _observe_via_client(client, e["database"], e["table"])
                except Exception as ex:
                    context.log.error(f"ClickHouse query error for {key_str!r}: {ex}")
                    continue
                data_version = str(observed.pop("data_version", ""))
                new_versions[key_str] = data_version
                if data_version == last_versions.get(key_str):
                    continue  # unchanged since last tick -- don't re-materialize
                events.append(_event_cls(
                    asset_key=dg.AssetKey.from_user_string(key_str),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version},
                ))

            cursor = json.dumps(new_versions)
            if not events:
                return SensorResult(cursor=cursor, skip_reason="no target changed since last tick")
            return SensorResult(asset_events=events, cursor=cursor)

        return dg.Definitions(sensors=[clickhouse_observation_sensor])
