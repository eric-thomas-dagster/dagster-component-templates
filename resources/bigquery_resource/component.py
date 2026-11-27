"""BigQuery Resource component."""
from typing import Optional
import dagster as dg
from pydantic import Field


def _build_bigquery_resource_with_direct_client_cls():
    """Build the BigQueryResourceWithDirectClient class lazily (needs dagster_gcp installed)."""
    from dagster_gcp import BigQueryResource

    class BigQueryResourceWithDirectClient(BigQueryResource):
        """Same as the official `BigQueryResource`, plus `get_direct_client()`.

        The official `get_client()` is a context manager (`with resource.get_client() as
        client: ...`), unlike `databricks_resource`'s and `gcs_resource`'s `get_client()`,
        which both hand back a ready-to-use client directly. `get_direct_client()` closes
        that gap for callers (including generated/converted code) that want one consistent
        `get_client()`-style call across all three integration resources.

        Caveat: when `gcp_credentials` is set, the official context manager writes a
        temporary credentials file for its duration and tears it down on exit.
        `get_direct_client()` enters that context manager and intentionally does NOT exit
        it, so the temporary file -- and the returned client's ability to authenticate --
        stays valid for the rest of the process. That file is only cleaned up on process
        exit. Prefer the official `get_client()`'s `with` form when you don't need this.
        """

        def get_direct_client(self):
            ctx = self.get_client()
            return ctx.__enter__()

    return BigQueryResourceWithDirectClient


class BigQueryResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-gcp BigQueryResource for use by other components."""

    resource_key: str = Field(default="bigquery_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    project: str = Field(description="GCP project ID")
    location: Optional[str] = Field(default=None, description="BigQuery location, e.g. 'US' or 'EU'")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Env var containing GCP service account JSON string")
    dataset: Optional[str] = Field(default=None, description="Default dataset")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Big Query Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        BigQueryResourceWithDirectClient = _build_bigquery_resource_with_direct_client_cls()
        gcp_credentials = dg.EnvVar(self.gcp_credentials_env_var) if self.gcp_credentials_env_var else None
        resource = BigQueryResourceWithDirectClient(
            project=self.project,
            location=self.location,
            gcp_credentials=gcp_credentials,
        )
        return dg.Definitions(resources={self.resource_key: resource})
