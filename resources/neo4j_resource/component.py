import os
from dataclasses import dataclass

import dagster as dg
from dagster import ConfigurableResource
from neo4j import Driver, GraphDatabase


class Neo4jResource(ConfigurableResource):
    uri: str
    username: str = "neo4j"
    password: str = ""
    database: str = "neo4j"

    def get_driver(self) -> Driver:
        return GraphDatabase.driver(self.uri, auth=(self.username, self.password))


@dataclass
class Neo4jResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a Neo4j resource."""

    resource_key: str = "neo4j_resource"
    uri: str = ""
    username: str = "neo4j"
    password_env_var: str = ""
    database: str = "neo4j"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = os.environ.get(self.password_env_var, "") if self.password_env_var else ""
        resource = Neo4jResource(
            uri=self.uri,
            username=self.username,
            password=password,
            database=self.database,
        )
        return dg.Definitions(resources={self.resource_key: resource})
