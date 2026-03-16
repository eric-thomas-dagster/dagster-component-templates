"""AWS S3 Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


@dataclass
class S3ResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-aws S3Resource for use by other components."""

    resource_key: str = Field(default="s3_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS access key ID")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS secret access key")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL for MinIO or LocalStack, e.g. 'http://localhost:9000'")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_aws.s3 import S3Resource
        resource = S3Resource(
            region_name=self.region_name or "",
            aws_access_key_id=os.environ.get(self.aws_access_key_id_env_var) if self.aws_access_key_id_env_var else None,
            aws_secret_access_key=os.environ.get(self.aws_secret_access_key_env_var) if self.aws_secret_access_key_env_var else None,
            endpoint_url=self.endpoint_url or "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
