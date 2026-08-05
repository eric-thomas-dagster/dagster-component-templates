"""SQL Observation Sensor Component.

Polls a SQL table on a schedule and emits an AssetObservation tagged with a
DataVersion. Downstream AutomationCondition.newly_updated() / .eager() fires
when the DataVersion changes — i.e. when the table has new activity.

Two operating modes:

- resource_key set (declarative / demo-mode / shared client): sensor calls
  `context.resources.<resource_key>.observe(table_name)`. The resource returns
  a dict of the form `{"data_version": "...", **metadata}`. This is the
  preferred contract — no SQLAlchemy connection managed by the sensor.

- resource_key unset (real database): sensor connects via SQLAlchemy using
  `connection_string_env_var`, runs `SELECT COUNT(*)` and optional
  `SELECT MAX({watermark_column})`, and derives a DataVersion from the
  latest watermark value (falling back to row_count when no watermark_column
  is set).
"""
import os
from typing import Any, Optional, Union

import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class SqlObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalSqlAsset to observe")
    table_name: str = Field(description="Table name to observe")
    connection_string_env_var: str = Field(
        description="Env var with SQLAlchemy connection string (ignored when resource_key is set)",
        default="",
    )
    watermark_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column for latest value metric (e.g. updated_at). Used to derive the DataVersion in the native path.",
    )
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(table_name) -> dict` that returns "
            "`{'data_version': str, **metadata}`. When set, the sensor uses this "
            "instead of managing a SQLAlchemy engine — enables demo-mode and shared "
            "connection pooling."
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
        def _sql_obs(context: SensorEvaluationContext, **_resources):
            # ── Resource-backed path (preferred) ────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    observed: dict[str, Any] = dict(client.observe(_self.table_name))
                except Exception as e:
                    context.log.error(f"resource '{resource_key}'.observe failed: {e}")
                    return SensorResult(skip_reason=f"resource observe failed: {e}")
                data_version = str(observed.pop("data_version", ""))
                return SensorResult(asset_events=[AssetObservation(
                    asset_key=AssetKey.from_user_string(_self.asset_key),
                    metadata=observed,
                    tags={DATA_VERSION_TAG: data_version} if data_version else None,
                )])

            # ── Native SQLAlchemy path ──────────────────────────────────────
            try:
                import sqlalchemy as sa
            except ImportError:
                return SensorResult(skip_reason="sqlalchemy not installed")

            try:
                conn_str = os.environ.get(_self.connection_string_env_var, "")
                if not conn_str:
                    return SensorResult(skip_reason=f"env var '{_self.connection_string_env_var}' not set")
                engine = sa.create_engine(conn_str)
            except Exception as e:
                return SensorResult(skip_reason=f"Connect failed: {e}")

            try:
                with engine.connect() as conn:
                    row_count = conn.execute(
                        sa.text(f"SELECT COUNT(*) FROM {_self.table_name}")
                    ).scalar()
                    latest_watermark = None
                    if _self.watermark_column:
                        latest_watermark = conn.execute(
                            sa.text(f"SELECT MAX({_self.watermark_column}) FROM {_self.table_name}")
                        ).scalar()
            except Exception as e:
                return SensorResult(skip_reason=f"Query failed: {e}")

            metadata: dict[str, Any] = {
                "row_count": row_count,
                "table_name": _self.table_name,
            }
            if latest_watermark is not None:
                metadata["latest_watermark"] = str(latest_watermark)

            # Prefer watermark for data_version; fall back to row_count.
            if latest_watermark is not None:
                data_version = str(latest_watermark)
            else:
                data_version = str(row_count if row_count is not None else 0)

            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_sql_obs])
