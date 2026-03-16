import os
from dataclasses import dataclass

import boto3
import dagster as dg
from dagster import ConfigurableResource


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


@dataclass
class DynamoDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a DynamoDB resource."""

    resource_key: str = "dynamodb_resource"
    region_name: str = ""
    aws_access_key_id_env_var: str = ""
    aws_secret_access_key_env_var: str = ""
    endpoint_url: str = ""

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        aws_access_key_id = (
            os.environ.get(self.aws_access_key_id_env_var, "")
            if self.aws_access_key_id_env_var
            else ""
        )
        aws_secret_access_key = (
            os.environ.get(self.aws_secret_access_key_env_var, "")
            if self.aws_secret_access_key_env_var
            else ""
        )
        resource = DynamoDBResource(
            region_name=self.region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=self.endpoint_url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
