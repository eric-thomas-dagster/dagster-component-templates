"""Cassandra Resource component."""
from typing import Optional

import dagster as dg
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from dagster import ConfigurableResource
from pydantic import Field


class CassandraResource(ConfigurableResource):
    hosts: str
    port: int = 9042
    keyspace: str = ""
    username: str = ""
    password: str = ""
    ssl: bool = False

    def get_session(self) -> Session:
        import ssl as _ssl
        host_list = [h.strip() for h in self.hosts.split(",")]
        auth_provider = None
        if self.username:
            auth_provider = PlainTextAuthProvider(
                username=self.username, password=self.password
            )
        ssl_context = _ssl.create_default_context() if self.ssl else None
        cluster = Cluster(
            contact_points=host_list,
            port=self.port,
            auth_provider=auth_provider,
            ssl_context=ssl_context,
        )
        return cluster.connect(self.keyspace if self.keyspace else None)


class CassandraResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Cassandra resource for use by other components."""

    resource_key: str = Field(default="cassandra_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    hosts: str = Field(description="Comma-separated Cassandra contact points, e.g. 'host1,host2,host3'")
    port: int = Field(default=9042, description="Cassandra native transport port")
    keyspace: Optional[str] = Field(default="", description="Default keyspace to connect to")
    username: Optional[str] = Field(default="", description="Cassandra username (leave empty for unauthenticated access)")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Cassandra password")
    ssl: bool = Field(default=False, description="Enable TLS using the system default SSL context")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        password = dg.EnvVar(self.password_env_var) if self.password_env_var else ""
        resource = CassandraResource(
            hosts=self.hosts,
            port=self.port,
            keyspace=self.keyspace or "",
            username=self.username or "",
            password=password,
            ssl=self.ssl,
        )
        return dg.Definitions(resources={self.resource_key: resource})
