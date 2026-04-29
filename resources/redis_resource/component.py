"""Redis Resource component."""
from dataclasses import dataclass
from typing import Optional

import dagster as dg
import redis
from dagster import ConfigurableResource
from pydantic import Field


class RedisResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str = ""
    ssl: bool = False
    url: str = ""

    def get_client(self) -> redis.Redis:
        if self.url:
            return redis.from_url(self.url)
        return redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password or None,
            ssl=self.ssl,
        )


@dataclass
class RedisResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Redis resource for use by other components."""

    resource_key: str = Field(default="redis_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(default="localhost", description="Redis host (ignored if url_env_var is set)")
    port: int = Field(default=6379, description="Redis port (ignored if url_env_var is set)")
    db: int = Field(default=0, description="Redis logical database index")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Redis password")
    ssl: bool = Field(default=False, description="Enable TLS (use rediss:// in url for managed services)")
    url_env_var: Optional[str] = Field(default=None, description="Environment variable holding a full redis:// or rediss:// URL. When set, takes precedence over host/port/password.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        url = dg.EnvVar(self.url_env_var) if self.url_env_var else ""
        password = dg.EnvVar(self.password_env_var) if self.password_env_var else ""
        resource = RedisResource(
            host=self.host,
            port=self.port,
            db=self.db,
            password=password,
            ssl=self.ssl,
            url=url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
