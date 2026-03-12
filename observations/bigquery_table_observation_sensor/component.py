"""BigQuery Table Observation Sensor Component."""
from typing import Optional
import dagster as dg
from dagster import AssetKey, AssetObservation, SensorEvaluationContext, SensorResult, sensor
from pydantic import Field

class BigQueryTableObservationSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit health observations for an external BigQuery table."""
    sensor_name: str = Field(description="Unique sensor name")
    asset_key: str = Field(description="Asset key of the ExternalBigQueryTableAsset to observe")
    project_id: str = Field(description="GCP project ID")
    dataset_id: str = Field(description="BigQuery dataset ID")
    table_id: str = Field(description="BigQuery table ID")
    check_interval_seconds: int = Field(default=300, description="Seconds between health checks")
    resource_key: Optional[str] = Field(default=None, description="Optional Dagster resource key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        resource_key = self.resource_key
        required_resource_keys = {resource_key} if resource_key else set()

        @sensor(
            name=_self.sensor_name,
            minimum_interval_seconds=_self.check_interval_seconds,
            required_resource_keys=required_resource_keys,
            monitored_assets=dg.AssetSelection.keys(
                dg.AssetKey(_self.asset_key.split("/"))
            ),
        )
        def _bq_obs(context: SensorEvaluationContext):
            try:
                from google.cloud import bigquery
            except ImportError:
                return SensorResult(skip_reason="google-cloud-bigquery not installed")

            try:
                if resource_key:
                    client = getattr(context.resources, resource_key)
                else:
                    client = bigquery.Client(project=_self.project_id)
                table_ref = client.get_table(f"{_self.project_id}.{_self.dataset_id}.{_self.table_id}")
            except Exception as e:
                return SensorResult(skip_reason=f"Connect or get_table failed: {e}")

            metadata = {
                "row_count": table_ref.num_rows,
                "size_bytes": table_ref.num_bytes,
                "modified_time_iso": table_ref.modified.isoformat() if table_ref.modified else "",
                "created_time_iso": table_ref.created.isoformat() if table_ref.created else "",
                "project_id": _self.project_id,
                "dataset_id": _self.dataset_id,
                "table_id": _self.table_id,
            }
            return SensorResult(asset_events=[AssetObservation(
                asset_key=AssetKey(_self.asset_key.split("/")), metadata=metadata)])

        return dg.Definitions(sensors=[_bq_obs])
