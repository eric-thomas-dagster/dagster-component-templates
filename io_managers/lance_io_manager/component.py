"""Lance IO Manager component.

YAML/Component wrapper around `LanceIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import LanceIOManager


class LanceIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a LanceDB IO manager for ML-optimised columnar storage.

    Features:
      - Partitioned assets land in tables named ``<asset_key>__<partition_key>``
      - Multi-component asset keys (e.g. ``["features","embeddings","text"]``) are
        flattened with ``__`` so each combination becomes its own table
      - Output metadata records the table path, row count, and partition key
      - Backed by a local directory or any object-storage URI (S3, GCS,
        S3-compatible MinIO via ``s3_endpoint_url``)

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    base_path: str = Field(
        default="./lance_data",
        description="Directory (or S3/GCS URI) where LanceDB tables are stored",
    )
    table_name_prefix: Optional[str] = Field(
        default=None,
        description="Optional prefix prepended to every table name, e.g. 'prod_'",
    )
    s3_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS access key when using S3-based storage",
    )
    s3_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable for AWS secret key when using S3-based storage",
    )
    s3_endpoint_url: Optional[str] = Field(
        default=None,
        description="Custom S3 endpoint URL, e.g. 'http://localhost:9000' for MinIO",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = LanceIOManager(
            base_path=self.base_path,
            table_name_prefix=self.table_name_prefix,
            s3_access_key=dg.EnvVar(self.s3_access_key_env_var) if self.s3_access_key_env_var else None,
            s3_secret_key=dg.EnvVar(self.s3_secret_key_env_var) if self.s3_secret_key_env_var else None,
            s3_endpoint_url=self.s3_endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
