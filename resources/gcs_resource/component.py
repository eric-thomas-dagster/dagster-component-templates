"""GCS Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class GCSResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-gcp GCSResource for use by other components."""

    resource_key: str = Field(default="gcs_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    project: str = Field(description="GCP project ID")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Env var containing GCP service account JSON string")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_gcp import GCSResource
        gcp_credentials = os.environ.get(self.gcp_credentials_env_var) if self.gcp_credentials_env_var else None
        resource = GCSResource(project=self.project, gcp_credentials=gcp_credentials)
        return dg.Definitions(resources={self.resource_key: resource})
