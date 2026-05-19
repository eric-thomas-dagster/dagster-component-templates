"""MongoDB Resource component."""

from typing import Optional

import dagster as dg
import pymongo
from dagster import ConfigurableResource
from pydantic import Field


class MongoDBResource(ConfigurableResource):
    connection_string: str
    database: str = ""
    tls: bool = False

    def get_client(self) -> pymongo.MongoClient:
        return pymongo.MongoClient(self.connection_string, tls=self.tls)


class MongoDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a MongoDB resource for use by other components."""

    resource_key: str = Field(default="mongodb_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    connection_string: Optional[str] = Field(default=None, description="MongoDB connection string (literal). Set this OR connection_string_env_var.")
    connection_string_env_var: Optional[str] = Field(default=None, description="Env var with the MongoDB connection string. Set this OR connection_string.")
    database: str = Field(default="", description="Default database name (optional)")
    tls: bool = Field(default=False, description="Enable TLS. Required for MongoDB Atlas; leave False for local/self-hosted clusters that don't terminate TLS.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.connection_string:
            cs = self.connection_string
        elif self.connection_string_env_var:
            cs = dg.EnvVar(self.connection_string_env_var)
        else:
            raise ValueError("Set either 'connection_string' or 'connection_string_env_var'")
        resource = MongoDBResource(
            connection_string=cs,
            database=self.database,
            tls=self.tls,
        )
        return dg.Definitions(resources={self.resource_key: resource})
