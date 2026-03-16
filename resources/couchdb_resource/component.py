import os
from dataclasses import dataclass

import couchdb
import dagster as dg
from dagster import ConfigurableResource


class CouchDBResource(ConfigurableResource):
    url: str
    username: str = ""
    password: str = ""

    def get_server(self) -> couchdb.Server:
        server = couchdb.Server(self.url)
        if self.username:
            server.resource.credentials = (self.username, self.password)
        return server


@dataclass
class CouchDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a CouchDB resource."""

    resource_key: str = "couchdb_resource"
    url: str = ""
    username: str = ""
    password_env_var: str = ""

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = os.environ.get(self.password_env_var, "") if self.password_env_var else ""
        resource = CouchDBResource(
            url=self.url,
            username=self.username,
            password=password,
        )
        return dg.Definitions(resources={self.resource_key: resource})
