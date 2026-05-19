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
    api_key: Optional[str] = Field(default=None, description="Elasticsearch API key (literal). Set this OR api_key_env_var. Mutually exclusive with username/password.")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var holding the Elasticsearch API key. Set this OR api_key.")
    username: Optional[str] = Field(default="", description="Elasticsearch username for basic auth")
    password: Optional[str] = Field(default=None, description="Elasticsearch password (literal). Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the Elasticsearch password. Set this OR password.")
    verify_certs: bool = Field(default=True, description="Verify TLS certificates. Set to False for self-signed dev clusters only.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.api_key:
            api_key = self.api_key
        elif self.api_key_env_var:
            api_key = dg.EnvVar(self.api_key_env_var)
        else:
            api_key = ""
        if self.password:
            password = self.password
        elif self.password_env_var:
            password = dg.EnvVar(self.password_env_var)
        else:
            password = ""
        resource = ElasticsearchResource(
            hosts=self.hosts,
            api_key=api_key,
            username=self.username or "",
            password=password,
            verify_certs=self.verify_certs,
        )
        return dg.Definitions(resources={self.resource_key: resource})
