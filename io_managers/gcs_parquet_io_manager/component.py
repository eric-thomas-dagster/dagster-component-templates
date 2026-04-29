"""GCS Parquet IO Manager component.

YAML/Component wrapper around `GCSParquetIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import GCSParquetIOManager


class GCSParquetIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IO manager that stores assets as Parquet files on Google Cloud Storage.

    Features:
      - Partitioned assets land at ``<prefix>/<asset_key>/<partition_key>.parquet``
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested GCS paths, preventing collisions between assets of the same name
      - Output metadata records the GCS URI, row count, and partition key
      - Authenticates via ADC by default, or a JSON service-account key

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    bucket: str = Field(description="GCS bucket to store Parquet files in")
    prefix: str = Field(
        default="dagster/assets",
        description="GCS object prefix for asset files, e.g. 'dagster/assets'",
    )
    project: Optional[str] = Field(default=None, description="GCP project ID (uses default if empty)")
    gcp_credentials_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding a JSON service-account key. Leave empty to use Application Default Credentials.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = GCSParquetIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            project=self.project,
            gcp_credentials_json=dg.EnvVar(self.gcp_credentials_env_var) if self.gcp_credentials_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
