from typing import Union
"""Postgres IO Manager component.

YAML/Component wrapper around `PostgresIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field

from .io_manager import PostgresIOManager


class PostgresIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a PostgreSQL IO manager so assets are automatically stored in and loaded from Postgres tables.

    Features:
      - Partition-aware writes via transactional DELETE+INSERT scoped by
        ``partition_column`` — backfills don't clobber sibling partitions
      - Multi-component asset keys map to ``schema.table`` (first component is the schema)
      - Output metadata records the qualified table name, row count, and partition key
      - Idempotent partition replacement: re-running a partition replaces only that partition's rows

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(description="PostgreSQL database name")
    user: str = Field(description="PostgreSQL username")
    password_env_var: str = Field(description="Environment variable holding the PostgreSQL password")
    default_schema: str = Field(
        default="public",
        description="Schema used when asset key has only one component",
    )
    if_exists: str = Field(
        default="replace",
        description="Behavior for unpartitioned writes when table exists: 'replace', 'append', or 'fail'",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = PostgresIOManager(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
            default_schema=self.default_schema,
            if_exists=self.if_exists,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
