"""SQL URL Resource component."""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class SQLUrlResource(dg.ConfigurableResource):
    """Connect to any SQLAlchemy-supported database via a single connection URL
    (e.g. 'postgresql://user:pass@host:5432/dbname') instead of separate
    host/database/user/password fields -- the shape Airflow's generic SQL
    hooks/connections typically use. Dialect-agnostic: works with any URL
    scheme whose SQLAlchemy driver is installed (postgresql, mysql, mssql,
    sqlite, snowflake, bigquery, redshift via a postgresql+psycopg2 URL,
    duckdb, ...), unlike this repo's per-dialect resources (mssql_resource,
    doris_resource, db2_resource, ...) which each build their own connection
    string from host/database/user fields.
    """

    connection_url: str

    def get_engine(self):
        """Escape hatch: the raw SQLAlchemy engine, for callers that want more
        than execute() offers (transactions, pandas.read_sql, reflection, ...)."""
        from sqlalchemy import create_engine
        return create_engine(self.connection_url)

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Run a query and return rows as a list of dicts. A SELECT's rows are
        returned; DDL/DML (no rows) returns an empty list after committing."""
        from sqlalchemy import text

        engine = self.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            if result.returns_rows:
                rows = [dict(row._mapping) for row in result]
            else:
                rows = []
            conn.commit()
        return rows


class SQLUrlResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SQLUrlResource (connect via a single SQLAlchemy connection
    URL, dialect-agnostic) for use by other components -- the reusable
    "SQL by URL" shape Airflow's generic SQL hooks/connections use, callable
    from any op or asset a user writes rather than locked inside one asset.
    """

    resource_key: str = Field(default="sql_url_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    connection_url: Optional[str] = Field(default=None, description="Full SQLAlchemy connection URL (literal), e.g. 'postgresql://user:pass@host:5432/dbname'. Set this OR connection_url_env_var.")
    connection_url_env_var: Optional[str] = Field(default=None, description="Env var holding the full connection URL. Set this OR connection_url.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='SQL URL Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.connection_url:
            url = self.connection_url
        elif self.connection_url_env_var:
            url = dg.EnvVar(self.connection_url_env_var)
        else:
            raise ValueError("Set either 'connection_url' or 'connection_url_env_var'.")
        resource = SQLUrlResource(connection_url=url)
        return dg.Definitions(resources={self.resource_key: resource})
