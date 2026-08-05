"""Databricks Table Observation Sensor Component.

Polls a Databricks Delta table via a SQL warehouse and emits an
AssetObservation tagged with a DataVersion (`f"{row_count}-{last_modified}"`).

When resource_key is set, `.observe(source)` is called (source is
`"catalog.schema.table"` or `"schema.table"` when no catalog).
"""
import os
from typing import Any, Optional

import dagster as dg
from dagster import AssetKey, AssetMaterialization, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class DatabricksTableObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external Databricks Delta table."""
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalDatabricksTableAsset to observe")
    workspace_url: str = Field(description="Databricks workspace URL")
    catalog: Optional[str] = Field(default=None, description="Unity Catalog name")
    schema_name: str = Field(description="Schema/database name")
    table_name: str = Field(description="Table name")
    token_env_var: str = Field(default="", description="Env var with Databricks personal access token")
    http_path: str = Field(default="", description="SQL warehouse HTTP path (from connection details)")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'catalog.schema.table' or 'schema.table') that returns "
            "`{'data_version': str, **metadata}`."
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
        def _dbx_obs(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation
            full_name = (
                f"{_self.catalog}.{_self.schema_name}.{_self.table_name}"
                if _self.catalog else f"{_self.schema_name}.{_self.table_name}"
            )
            # ── Resource-backed path ────────────────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    observed: dict[str, Any] = dict(client.observe(full_name))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return SensorResult(asset_events=[_event_cls(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native databricks-sql-connector path ────────────────────────
            try:
                from databricks import sql as dbsql
            except ImportError:
                return SensorResult(skip_reason="databricks-sql-connector not installed")

            token = os.environ.get(_self.token_env_var, "")
            try:
                conn = dbsql.connect(
                    server_hostname=_self.workspace_url.replace("https://", ""),
                    http_path=_self.http_path,
                    access_token=token,
                )
                with conn.cursor() as cur:
                    cur.execute(f"DESCRIBE DETAIL {full_name}")
                    detail = dict(zip([d[0] for d in cur.description], cur.fetchone()))
                    cur.execute(f"SELECT COUNT(*) FROM {full_name}")
                    row_count = cur.fetchone()[0]
                conn.close()
            except Exception as e:
                return SensorResult(skip_reason=f"Query failed: {e}")

            last_modified = str(detail.get("lastModified", ""))
            data_version = f"{row_count}-{last_modified}"
            metadata = {
                "row_count": row_count,
                "size_in_bytes": detail.get("sizeInBytes", 0),
                "num_files": detail.get("numFiles", 0),
                "last_modified": last_modified,
                "table_name": full_name,
            }
            return SensorResult(asset_events=[_event_cls(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_dbx_obs])
