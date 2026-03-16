"""BigQuery Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class BigQueryResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-gcp BigQueryResource for use by other components."""

    resource_key: str = Field(default="bigquery_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    project: str = Field(description="GCP project ID")
    location: Optional[str] = Field(default=None, description="BigQuery location, e.g. 'US' or 'EU'")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Env var containing GCP service account JSON string")
    dataset: Optional[str] = Field(default=None, description="Default dataset")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_gcp import BigQueryResource
        gcp_credentials = os.environ.get(self.gcp_credentials_env_var) if self.gcp_credentials_env_var else None
        resource = BigQueryResource(
            project=self.project,
            location=self.location,
            gcp_credentials=gcp_credentials,
            dataset=self.dataset,
        )
        return dg.Definitions(resources={self.resource_key: resource})
