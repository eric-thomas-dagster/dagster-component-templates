import os
from dataclasses import dataclass

import dagster as dg
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from dagster import ConfigurableResource


class CassandraResource(ConfigurableResource):
    hosts: str
    port: int = 9042
    keyspace: str = ""
    username: str = ""
    password: str = ""
    ssl: bool = False

    def get_session(self) -> Session:
        host_list = [h.strip() for h in self.hosts.split(",")]
        auth_provider = None
        if self.username:
            auth_provider = PlainTextAuthProvider(
                username=self.username, password=self.password
            )
        cluster = Cluster(
            contact_points=host_list,
            port=self.port,
            auth_provider=auth_provider,
            ssl_context=self.ssl or None,
        )
        return cluster.connect(self.keyspace if self.keyspace else None)


@dataclass
class CassandraResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Dagster component that provides a Cassandra resource."""

    resource_key: str = "cassandra_resource"
    hosts: str = ""
    port: int = 9042
    keyspace: str = ""
    username: str = ""
    password_env_var: str = ""
    ssl: bool = False

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = os.environ.get(self.password_env_var, "") if self.password_env_var else ""
        resource = CassandraResource(
            hosts=self.hosts,
            port=self.port,
            keyspace=self.keyspace,
            username=self.username,
            password=password,
            ssl=self.ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
