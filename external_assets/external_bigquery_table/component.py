"""External BigQuery Table Asset Component."""
from typing import Optional
import dagster as dg
from pydantic import Field

class ExternalBigQueryTableAsset(dg.Component, dg.Model, dg.Resolvable):
    """Declare a BigQuery table as an observable external asset."""
    asset_key: str = Field(description="Dagster asset key")
    project_id: str = Field(description="GCP project ID")
    dataset_id: str = Field(description="BigQuery dataset ID")
    table_id: str = Field(description="BigQuery table ID")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Human-readable description")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        spec = dg.AssetSpec(
            key=self.asset_key,
            group_name=self.group_name,
            description=self.description or f"BigQuery {self.project_id}.{self.dataset_id}.{self.table_id}",
            kinds={"bigquery", "gcp", "sql", "table"},
            metadata={
                "project_id": self.project_id,
                "dataset_id": self.dataset_id,
                "table_id": self.table_id,
                "dagster/uri": f"bq://{self.project_id}/{self.dataset_id}/{self.table_id}",
                "dagster.observability_type": "external",
            },
        )
        return dg.Definitions(assets=[spec])
