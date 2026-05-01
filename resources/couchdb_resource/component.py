"""CouchDB Resource component."""
from typing import Optional

import couchdb
import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class CouchDBResource(ConfigurableResource):
    url: str
    username: str = ""
    password: str = ""

    def get_server(self) -> couchdb.Server:
        server = couchdb.Server(self.url)
        if self.username:
            server.resource.credentials = (self.username, self.password)
        return server


class CouchDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a CouchDB resource for use by other components."""

    resource_key: str = Field(default="couchdb_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    url: str = Field(description="CouchDB server URL, e.g. 'http://localhost:5984'")
    username: Optional[str] = Field(default="", description="CouchDB username (leave empty for anonymous access)")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the CouchDB password")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = dg.EnvVar(self.password_env_var) if self.password_env_var else ""
        resource = CouchDBResource(
            url=self.url,
            username=self.username or "",
            password=password,
        )
        return dg.Definitions(resources={self.resource_key: resource})
