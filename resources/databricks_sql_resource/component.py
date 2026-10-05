"""Databricks SQL Resource component.

This is NOT the same thing as `databricks_resource` (which wraps
`dagster_databricks.DatabricksClientResource` -- job/cluster submission and
management, with no SQL query capability at all). This component is for
querying a Databricks SQL Warehouse directly: it wraps the official
`databricks-sql-connector` package, whose connection object is DB-API
2.0 / PEP 249 compliant (confirmed against Databricks' own docs) -- so it
plugs directly into `get_connection()`-based dispatch (the same contract
`_ingest_warehouse_query` already uses for Postgres/Snowflake/DuckDB/MySQL),
no new dispatch logic required.

Use this as the `resource_key` in any component's `source: {kind:
warehouse_query, resource_key: databricks_sql_resource, sql: ...}` field.
"""
from typing import Optional

import dagster as dg
from pydantic import Field


class DatabricksSqlResource(dg.ConfigurableResource):
    """Provides a databricks-sql-connector connection to a SQL Warehouse."""

    server_hostname: str
    http_path: str
    access_token: str
    catalog: Optional[str] = None
    schema_name: Optional[str] = None

    def get_connection(self):
        from databricks import sql
        kwargs = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "access_token": self.access_token,
        }
        if self.catalog:
            kwargs["catalog"] = self.catalog
        if self.schema_name:
            kwargs["schema"] = self.schema_name
        return sql.connect(**kwargs)


class DatabricksSqlResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Databricks SQL Warehouse connection for use by other components.

    Example:

        ```yaml
        type: dagster_component_templates.DatabricksSqlResourceComponent
        attributes:
          resource_key: databricks_sql_resource
          server_hostname: dbc-a1b2345c-d6e7.cloud.databricks.com
          http_path: /sql/1.0/warehouses/a1b234c567d8e9fa
          access_token_env_var: DATABRICKS_SQL_TOKEN
        ```
    """

    resource_key: str = Field(
        default="databricks_sql_resource",
        description="Key used to register this resource. Other components reference it via resource_key.",
    )
    server_hostname: str = Field(
        description="Databricks workspace hostname, e.g. 'dbc-a1b2345c-d6e7.cloud.databricks.com' (no scheme/path)."
    )
    http_path: str = Field(
        description="SQL Warehouse HTTP path, e.g. '/sql/1.0/warehouses/a1b234c567d8e9fa' (from the warehouse's Connection Details tab)."
    )
    access_token_env_var: str = Field(
        description="Env var holding a Databricks personal access token (or an OAuth token). Token-based auth only -- see the component docstring for scope."
    )
    catalog: Optional[str] = Field(default=None, description="Default Unity Catalog catalog for the connection.")
    schema_name: Optional[str] = Field(default=None, description="Default schema for the connection.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os
        resource = DatabricksSqlResource(
            server_hostname=self.server_hostname,
            http_path=self.http_path,
            access_token=os.environ.get(self.access_token_env_var, ""),
            catalog=self.catalog,
            schema_name=self.schema_name,
        )
        return dg.Definitions(resources={self.resource_key: resource})
