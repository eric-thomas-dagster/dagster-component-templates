"""BigQuery IO Manager component — reads and writes Pandas DataFrames via BigQuery."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class BigQueryIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a BigQueryPandasIOManager so assets are automatically stored in and loaded from BigQuery."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    project: str = Field(description="GCP project ID")
    dataset: str = Field(description="BigQuery dataset to store assets in")
    location: Optional[str] = Field(default=None, description="GCP region, e.g. 'US', 'EU', 'us-central1'")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Environment variable holding a JSON service account key (optional; uses ADC if not set)")
    temporary_gcs_bucket: Optional[str] = Field(default=None, description="GCS bucket for temporary files during load jobs")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_gcp_pandas import BigQueryPandasIOManager
        kwargs = dict(
            project=self.project,
            dataset=self.dataset,
            location=self.location,
        )
        if self.gcp_credentials_env_var:
            import json
            creds_str = os.environ.get(self.gcp_credentials_env_var, "")
            if creds_str:
                kwargs["gcp_credentials"] = creds_str
        if self.temporary_gcs_bucket:
            kwargs["temporary_gcs_bucket"] = self.temporary_gcs_bucket
        io_manager = BigQueryPandasIOManager(**kwargs)
        return dg.Definitions(resources={self.resource_key: io_manager})
