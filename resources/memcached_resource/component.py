"""Memcached Resource component."""
from typing import Optional

import dagster as dg
from dagster import ConfigurableResource
from pydantic import Field


class MemcachedResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 11211
    timeout_seconds: float = 3.0

    def get_client(self):
        from pymemcache.client.base import Client
        return Client(
            (self.host, self.port),
            connect_timeout=self.timeout_seconds,
            timeout=self.timeout_seconds,
        )


class MemcachedResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Memcached resource for use by other components.

    Example:

        ```yaml
        type: dagster_community_components.MemcachedResourceComponent
        attributes:
          resource_key: memcached_resource
          host: memcached.internal
          port: 11211
        ```

    Pairs with:
      - ``memcached_cache_flush`` — flush all / by prefix
    """

    resource_key: str = Field(default="memcached_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(default="localhost", description="Memcached host")
    port: int = Field(default=11211, description="Memcached port")
    timeout_seconds: float = Field(default=3.0, description="Connect + read/write timeout")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = MemcachedResource(
            host=self.host,
            port=self.port,
            timeout_seconds=self.timeout_seconds,
        )
        return dg.Definitions(resources={self.resource_key: resource})
