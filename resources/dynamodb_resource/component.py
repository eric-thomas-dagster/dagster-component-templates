"""DynamoDB Resource component."""
from typing import Optional

import boto3
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class DynamoDBResource(ConfigurableResource):
    region_name: str
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    endpoint_url: str = ""

    def _client_kwargs(self) -> dict:
        kwargs: dict = {"region_name": self.region_name}
        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        return kwargs

    def get_resource(self):
        return boto3.resource("dynamodb", **self._client_kwargs())

    def get_client(self):
        return boto3.client("dynamodb", **self._client_kwargs())


class DynamoDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DynamoDB resource for use by other components."""

    resource_key: str = Field(default="dynamodb_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: str = Field(description="AWS region, e.g. 'us-east-1'")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS access key ID. Leave empty to use boto3's default credential chain (IAM role, ~/.aws, etc.).")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the AWS secret access key.")
    endpoint_url: Optional[str] = Field(default=None, description="Custom DynamoDB endpoint URL for LocalStack or DynamoDB Local, e.g. 'http://localhost:8000'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        aws_access_key_id = (
            dg.EnvVar(self.aws_access_key_id_env_var) if self.aws_access_key_id_env_var else ""
        )
        aws_secret_access_key = (
            dg.EnvVar(self.aws_secret_access_key_env_var) if self.aws_secret_access_key_env_var else ""
        )
        resource = DynamoDBResource(
            region_name=self.region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=self.endpoint_url or "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
