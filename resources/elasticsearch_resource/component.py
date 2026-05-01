"""Elasticsearch Resource component."""
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from elasticsearch import Elasticsearch
from pydantic import Field


class ElasticsearchResource(ConfigurableResource):
    hosts: str
    api_key: str = ""
    username: str = ""
    password: str = ""
    verify_certs: bool = True

    def get_client(self) -> Elasticsearch:
        kwargs = {
            "hosts": [self.hosts],
            "verify_certs": self.verify_certs,
        }
        if self.api_key:
            kwargs["api_key"] = self.api_key
        elif self.username:
            kwargs["basic_auth"] = (self.username, self.password)
        return Elasticsearch(**kwargs)


class ElasticsearchResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Elasticsearch resource for use by other components."""

    resource_key: str = Field(default="elasticsearch_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    hosts: str = Field(description="Elasticsearch URL, e.g. 'https://localhost:9200'")
    api_key_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Elasticsearch API key (preferred). Mutually exclusive with username/password.")
    username: Optional[str] = Field(default="", description="Elasticsearch username for basic auth")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Elasticsearch password for basic auth")
    verify_certs: bool = Field(default=True, description="Verify TLS certificates. Set to False for self-signed dev clusters only.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        api_key = dg.EnvVar(self.api_key_env_var) if self.api_key_env_var else ""
        password = dg.EnvVar(self.password_env_var) if self.password_env_var else ""
        resource = ElasticsearchResource(
            hosts=self.hosts,
            api_key=api_key,
            username=self.username or "",
            password=password,
            verify_certs=self.verify_certs,
        )
        return dg.Definitions(resources={self.resource_key: resource})
