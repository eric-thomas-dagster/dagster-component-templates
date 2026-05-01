"""AWS S3 Resource component."""
from typing import Optional
import dagster as dg
from pydantic import Field


class S3ResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-aws S3Resource for use by other components."""

    resource_key: str = Field(default="s3_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS access key ID")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS secret access key")
    endpoint_url: Optional[str] = Field(default=None, description="Custom endpoint URL for MinIO or LocalStack, e.g. 'http://localhost:9000'")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_aws.s3 import S3Resource
        kwargs: dict = {}
        if self.region_name:
            kwargs["region_name"] = self.region_name
        if self.aws_access_key_id_env_var:
            kwargs["aws_access_key_id"] = dg.EnvVar(self.aws_access_key_id_env_var)
        if self.aws_secret_access_key_env_var:
            kwargs["aws_secret_access_key"] = dg.EnvVar(self.aws_secret_access_key_env_var)
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        resource = S3Resource(**kwargs)
        return dg.Definitions(resources={self.resource_key: resource})
