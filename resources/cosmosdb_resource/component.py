import os
from dataclasses import dataclass

import dagster as dg
from azure.cosmos import CosmosClient
from dagster import ConfigurableResource


class CosmosDBResource(ConfigurableResource):
    endpoint: str
    key: str
    database_name: str = ""

    def get_client(self) -> CosmosClient:
        return CosmosClient(self.endpoint, credential=self.key)

    def get_database(self):
        client = self.get_client()
        return client.get_database_client(self.database_name)


@dataclass
class CosmosDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides an Azure Cosmos DB resource."""

    resource_key: str = "cosmosdb_resource"
    endpoint: str = ""
    key_env_var: str = ""
    database_name: str = ""

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        key = os.environ.get(self.key_env_var, "") if self.key_env_var else ""
        resource = CosmosDBResource(
            endpoint=self.endpoint,
            key=key,
            database_name=self.database_name,
        )
        return dg.Definitions(resources={self.resource_key: resource})
