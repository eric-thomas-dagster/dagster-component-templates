"""Databricks IO Manager component.

YAML/Component wrapper around `DatabricksIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
from typing import Optional, Union

import dagster as dg
from pydantic import Field

from .io_manager import DatabricksIOManager


class DatabricksIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Databricks IO manager that reads and writes Pandas DataFrames as Unity Catalog Delta tables.

    Features:
      - Partition-aware writes via DELETE+INSERT scoped by ``partition_column``
        (Delta's per-statement ACID semantics make each DELETE/INSERT atomic)
      - Multi-component asset keys map to ``catalog.schema.table`` (first component is the schema)
      - Output metadata records the qualified table name, row count, and partition key
      - Optional ``staging_location`` for Parquet + ``COPY INTO`` ingestion (preferred for large datasets)
      - Idempotent partition replacement: re-running a partition replaces only that partition's rows

    To make this the default IO manager for the project, leave
    ``resource_key`` as ``io_manager``.
    """

    resource_key: str = Field(
        default="io_manager",
        description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.",
    )
    server_hostname: str = Field(description="Databricks workspace hostname, e.g. 'adb-1234567890.12.azuredatabricks.net'")
    http_path: str = Field(description="HTTP path to the SQL warehouse or cluster, e.g. '/sql/1.0/warehouses/abc123'")
    access_token_env_var: str = Field(description="Environment variable holding the Databricks personal access token")
    catalog: str = Field(default="main", description="Unity Catalog catalog name")
    default_schema: str = Field(default="default", description="Schema used when asset key has only one component")
    staging_location: Optional[str] = Field(
        default=None,
        description="Cloud storage URI for staging data, e.g. 's3://bucket/staging' or 'abfss://container@account.dfs.core.windows.net/staging'",
    )
    partition_column: Union[str, int] = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        io_manager = DatabricksIOManager(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=dg.EnvVar(self.access_token_env_var) if self.access_token_env_var else None,
            catalog=self.catalog,
            default_schema=self.default_schema,
            staging_location=self.staging_location,
            partition_column=self.partition_column,
        )
        return dg.Definitions(resources={self.resource_key: io_manager})
