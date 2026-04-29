"""Trino IO Manager component.

YAML/Component wrapper around `TrinoIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional

import dagster as dg
from pydantic import Field

from .io_manager import TrinoIOManager


class TrinoIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Trino IO manager that reads and writes Pandas DataFrames via Trino SQL.

    Features:
      - Partition-aware writes via transactional DELETE+INSERT scoped by ``partition_column``
      - Multi-component asset keys map to ``catalog.schema.table`` (first component is the schema)
      - Output metadata records the qualified table name, row count, and partition key
      - Idempotent partition replacement: re-running a partition replaces only that partition's rows

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(default="iceberg", description="Trino catalog, e.g. 'iceberg', 'hive', 'delta'")
    default_schema: str = Field(default="default", description="Schema used when asset key has only one component")
    password_env_var: Optional[str] = Field(
        default=None,
        description="Environment variable holding the Trino password (omit for no auth)",
    )
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = TrinoIOManager(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            default_schema=self.default_schema,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
