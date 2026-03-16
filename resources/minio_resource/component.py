"""MinIO Resource component — S3-compatible object storage."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


class MinIOResource(dg.ConfigurableResource):
    """MinIO S3-compatible object storage resource."""
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = False
    default_bucket: str = "lake"

    def get_client(self):
        from minio import Minio
        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )


@dataclass
class MinIOResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a MinIO S3-compatible object storage resource for use by other components."""

    resource_key: str = Field(default="minio_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    endpoint: str = Field(default="localhost:9000", description="MinIO server endpoint, e.g. 'localhost:9000' or 'minio.company.com:9000'")
    access_key_env_var: str = Field(default="MINIO_ACCESS_KEY", description="Environment variable holding the MinIO access key")
    secret_key_env_var: str = Field(default="MINIO_SECRET_KEY", description="Environment variable holding the MinIO secret key")
    secure: bool = Field(default=False, description="Use HTTPS (TLS) for the connection")
    default_bucket: str = Field(default="lake", description="Default bucket name for object storage operations")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = MinIOResource(
            endpoint=self.endpoint,
            access_key=os.environ.get(self.access_key_env_var, ""),
            secret_key=os.environ.get(self.secret_key_env_var, ""),
            secure=self.secure,
            default_bucket=self.default_bucket,
        )
        return dg.Definitions(resources={self.resource_key: resource})
