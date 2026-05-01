"""Azure Cosmos DB Resource component."""

import dagster as dg
from azure.cosmos import CosmosClient
from dagster import ConfigurableResource
from pydantic import Field


class CosmosDBResource(ConfigurableResource):
    endpoint: str
    key: str
    database_name: str = ""

    def get_client(self) -> CosmosClient:
        return CosmosClient(self.endpoint, credential=self.key)

    def get_database(self):
        client = self.get_client()
        return client.get_database_client(self.database_name)


class CosmosDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an Azure Cosmos DB resource for use by other components."""

    resource_key: str = Field(default="cosmosdb_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    endpoint: str = Field(description="Cosmos DB account endpoint, e.g. 'https://my-account.documents.azure.com:443/'")
    key_env_var: str = Field(description="Environment variable holding the Cosmos DB account key")
    database_name: str = Field(default="", description="Default database name (optional — can also be passed per-call)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        key = dg.EnvVar(self.key_env_var) if self.key_env_var else ""
        resource = CosmosDBResource(
            endpoint=self.endpoint,
            key=key,
            database_name=self.database_name,
        )
        return dg.Definitions(resources={self.resource_key: resource})
