"""MongoDBIOManagerComponent.

YAML/Component wrapper around `MongoDBIOManager`.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import MongoDBIOManager


class MongoDBIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Persist DataFrames to MongoDB collections (one per asset, document-store)."""

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key. Use 'io_manager' to make this the default.",
    )
    connection_uri_env_var: str = Field(default="MONGODB_URI", description="Env var holding the MongoDB URI.")
    database: str = Field(description="MongoDB database name.")
    if_exists: str = Field(default="replace", description="'replace', 'append', or 'fail'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = MongoDBIOManager(
            connection_uri=dg.EnvVar(self.connection_uri_env_var).get_value() or "",
            database=self.database,
            if_exists=self.if_exists,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
