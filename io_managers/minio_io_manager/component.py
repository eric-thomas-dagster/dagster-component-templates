from typing import Union
"""MinIO IO Manager component.

YAML/Component wrapper around `MinIODeltaIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field

from .io_manager import MinIODeltaIOManager


class MinIOIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Delta Lake IO manager pointed at a MinIO endpoint for S3-compatible lakehouse storage.

    Features:
      - Partitioned assets are written with Delta predicate-based overwrite
        so per-partition rewrites don't clobber sibling partitions
      - Multi-component asset keys (e.g. ``["raw","stripe","customers"]``) become
        nested table paths, preventing collisions between assets of the same name
      - Output metadata records the Delta table URI, row count, and partition key
      - S3-compatible (MinIO, LocalStack, Cloudflare R2, etc.) via ``endpoint_url``

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    bucket: str = Field(default="lake", description="S3/MinIO bucket to store Delta tables in")
    prefix: str = Field(
        default="dagster/assets",
        description="Key prefix within the bucket for asset tables",
    )
    endpoint_url: str = Field(
        default="http://localhost:9000",
        description="MinIO endpoint URL, e.g. 'http://localhost:9000'",
    )
    access_key_env_var: str = Field(
        default="MINIO_ACCESS_KEY",
        description="Environment variable holding the MinIO access key",
    )
    secret_key_env_var: str = Field(
        default="MINIO_SECRET_KEY",
        description="Environment variable holding the MinIO secret key",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition Delta overwrites",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = MinIODeltaIOManager(
            bucket=self.bucket,
            prefix=self.prefix,
            endpoint_url=self.endpoint_url,
            access_key=dg.EnvVar(self.access_key_env_var) if self.access_key_env_var else None,
            secret_key=dg.EnvVar(self.secret_key_env_var) if self.secret_key_env_var else None,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
