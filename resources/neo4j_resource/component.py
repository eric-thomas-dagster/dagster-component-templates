"""Neo4j Resource component."""
from dataclasses import dataclass

import dagster as dg
from dagster import ConfigurableResource
from neo4j import Driver, GraphDatabase
from pydantic import Field


class Neo4jResource(ConfigurableResource):
    uri: str
    username: str = "neo4j"
    password: str = ""
    database: str = "neo4j"

    def get_driver(self) -> Driver:
        return GraphDatabase.driver(self.uri, auth=(self.username, self.password))


@dataclass
class Neo4jResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Neo4j resource for use by other components."""

    resource_key: str = Field(default="neo4j_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    uri: str = Field(description="Neo4j connection URI, e.g. 'neo4j+s://abc123.databases.neo4j.io' or 'bolt://localhost:7687'")
    username: str = Field(default="neo4j", description="Neo4j username")
    password_env_var: str = Field(description="Environment variable holding the Neo4j password")
    database: str = Field(default="neo4j", description="Default database name")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = dg.EnvVar(self.password_env_var) if self.password_env_var else ""
        resource = Neo4jResource(
            uri=self.uri,
            username=self.username,
            password=password,
            database=self.database,
        )
        return dg.Definitions(resources={self.resource_key: resource})
