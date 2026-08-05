"""BigQuery Table Observation Sensor Component.

Polls a BigQuery table on a schedule and emits an AssetObservation tagged
with a DataVersion. Downstream AutomationCondition.newly_updated() / .eager()
fires when the DataVersion changes.

Two operating modes:

- resource_key set: `.observe(source)` on the resource where source is
  `"project.dataset.table"`. Returns `{"data_version": str, **metadata}`.
- resource_key unset: uses google-cloud-bigquery to fetch table metadata
  (row_count, modified time), derives DataVersion from
  `f"{row_count}-{modified_iso}"`.
"""
from typing import Any, Optional

import dagster as dg
from dagster import AssetKey, AssetMaterialization, AssetObservation, MetadataValue, SensorEvaluationContext, SensorResult, sensor
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from pydantic import Field


class BigQueryTableObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external BigQuery table."""
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalBigQueryTableAsset to observe")
    project_id: str = Field(description="GCP project ID")
    dataset_id: str = Field(description="BigQuery dataset ID")
    table_id: str = Field(description="BigQuery table ID")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(
        default=None,
        description=(
            "Dagster resource key exposing `.observe(source) -> dict` (source is "
            "'project.dataset.table') that returns `{'data_version': str, **metadata}`."
        ),
    )
    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Run an extra `SELECT * LIMIT preview_rows` against the table and "
            "include the result as a markdown preview on the AssetObservation, "
            "so builder UIs can show table contents without their own warehouse access."
        ),
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview SELECT when include_preview_metadata=True.",
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
        def _bq_obs(context: SensorEvaluationContext, **_resources):
            _event_cls = AssetMaterialization if _self.emit_materialization else AssetObservation
            # ── Resource-backed path ────────────────────────────────────────
            if resource_key:
                client = getattr(context.resources, resource_key, None)
                if client is None:
                    return SensorResult(skip_reason=f"resource '{resource_key}' not found on context")
                try:
                    source = f"{_self.project_id}.{_self.dataset_id}.{_self.table_id}"
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

            # ── Native google-cloud-bigquery path ───────────────────────────
            try:
                from google.cloud import bigquery
            except ImportError:
                return SensorResult(skip_reason="google-cloud-bigquery not installed")

            try:
                client = bigquery.Client(project=_self.project_id)
                table_ref = client.get_table(f"{_self.project_id}.{_self.dataset_id}.{_self.table_id}")
            except Exception as e:
                return SensorResult(skip_reason=f"Connect or get_table failed: {e}")

            modified_iso = table_ref.modified.isoformat() if table_ref.modified else ""
            data_version = f"{table_ref.num_rows}-{modified_iso}"
            metadata: dict[str, Any] = {
                "row_count": table_ref.num_rows,
                "size_bytes": table_ref.num_bytes,
                "modified_time_iso": modified_iso,
                "created_time_iso": table_ref.created.isoformat() if table_ref.created else "",
                "project_id": _self.project_id,
                "dataset_id": _self.dataset_id,
                "table_id": _self.table_id,
            }
            if _self.include_preview_metadata:
                try:
                    fqn = f"`{_self.project_id}.{_self.dataset_id}.{_self.table_id}`"
                    df = client.query(f"SELECT * FROM {fqn} LIMIT {_self.preview_rows}").to_dataframe()
                    if len(df) > 0:
                        metadata["preview"] = MetadataValue.md(df.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"Preview query failed: {e}")
            return SensorResult(asset_events=[_event_cls(
                asset_key=AssetKey.from_user_string(_self.asset_key),
                metadata=metadata,
                tags={DATA_VERSION_TAG: data_version},
            )])

        return dg.Definitions(sensors=[_bq_obs])
