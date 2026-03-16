import os
from dataclasses import dataclass
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from elasticsearch import Elasticsearch


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


@dataclass
class ElasticsearchResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides an Elasticsearch resource."""

    resource_key: str = "elasticsearch_resource"
    hosts: str = ""
    api_key_env_var: str = ""
    username: str = ""
    password_env_var: str = ""
    verify_certs: bool = True

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        api_key = os.environ.get(self.api_key_env_var, "") if self.api_key_env_var else ""
        password = os.environ.get(self.password_env_var, "") if self.password_env_var else ""
        resource = ElasticsearchResource(
            hosts=self.hosts,
            api_key=api_key,
            username=self.username,
            password=password,
            verify_certs=self.verify_certs,
        )
        return dg.Definitions(resources={self.resource_key: resource})
