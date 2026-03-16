import os
from dataclasses import dataclass

import dagster as dg
import redis
from dagster import ConfigurableResource


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
    """Dagster component that provides a Redis resource."""

    resource_key: str = "redis_resource"
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password_env_var: str = ""
    ssl: bool = False
    url_env_var: str = ""

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        url = os.environ.get(self.url_env_var, "") if self.url_env_var else ""
        password = os.environ.get(self.password_env_var, "") if self.password_env_var else ""
        resource = RedisResource(
            host=self.host,
            port=self.port,
            db=self.db,
            password=password,
            ssl=self.ssl,
            url=url,
        )
        return dg.Definitions(resources={self.resource_key: resource})
