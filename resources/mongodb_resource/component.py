"""MongoDB Resource component."""

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
    connection_string_env_var: str = Field(description="Environment variable holding the MongoDB connection string, e.g. 'mongodb+srv://user:pass@cluster.example.net/db'")
    database: str = Field(default="", description="Default database name (optional)")
    tls: bool = Field(default=False, description="Enable TLS. Required for MongoDB Atlas; leave False for local/self-hosted clusters that don't terminate TLS.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        connection_string = dg.EnvVar(self.connection_string_env_var)
        resource = MongoDBResource(
            connection_string=connection_string,
            database=self.database,
            tls=self.tls,
        )
        return dg.Definitions(resources={self.resource_key: resource})
