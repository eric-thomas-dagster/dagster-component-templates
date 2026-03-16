import os
from dataclasses import dataclass, field
from typing import Optional

import dagster as dg
import pymongo
from dagster import ConfigurableResource


class MongoDBResource(ConfigurableResource):
    connection_string: str
    database: str = ""
    tls: bool = True

    def get_client(self) -> pymongo.MongoClient:
        return pymongo.MongoClient(self.connection_string, tls=self.tls)


@dataclass
class MongoDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a MongoDB resource."""

    resource_key: str = "mongodb_resource"
    connection_string_env_var: str = ""
    database: str = ""
    tls: bool = True

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        connection_string = os.environ.get(self.connection_string_env_var, "")
        resource = MongoDBResource(
            connection_string=connection_string,
            database=self.database,
            tls=self.tls,
        )
        return dg.Definitions(resources={self.resource_key: resource})
