"""SQL URL Resource component."""
from typing import Any, Dict, List, Optional, Union

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

    def execute(
        self,
        query: str,
        params: Optional[Union[Dict[str, Any], List[Any], tuple]] = None,
        autocommit: bool = True,
        max_rows: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Run a query and return rows as a list of dicts. A SELECT's rows are
        returned; DDL/DML (no rows) returns an empty list after committing.

        `params` accepts a dict (named placeholders, e.g. `:name`) or a list/tuple
        (positional placeholders, e.g. `?`/`%s` -- whatever the underlying DBAPI
        driver's own paramstyle is; passed through as-is, not translated, matching
        `sql_asset`'s convention). When `params` is omitted entirely, the query is
        sent via the DBAPI driver directly (`Connection.exec_driver_sql`), bypassing
        SQLAlchemy's `:name` bind-parameter parsing -- so a literal colon in a
        string, e.g. `SELECT ':ab'`, is never mistaken for an unfilled bind
        parameter. A list/tuple `params` also goes through `exec_driver_sql` (raw
        DBAPI positional binding), so it has the same literal-colon safety. Only a
        dict `params` uses SQLAlchemy's `text(query)` for named substitution; a
        query that both uses real `:name` params AND contains an unrelated literal
        colon in the same string can still collide there (SQLAlchemy/DBAPI's
        `:name` syntax can't otherwise distinguish a real placeholder from one
        inside a literal) -- a known limitation of named-parameter SQL, not
        something this resource works around.

        `autocommit=False`: since this method opens and closes one throwaway
        connection per call, there is no persistent session to commit later --
        skipping the commit means any DML is rolled back when the connection
        closes (not persisted). This is only useful to run a statement and
        observe its effect within that single call without persisting it; to
        actually commit multiple statements together (or not at all, as a group),
        use `execute_many()` instead, which holds one transaction open across all
        of them.

        `max_rows` caps how many rows are materialized into the returned list,
        to avoid pulling an unbounded result set fully into memory.
        """
        from sqlalchemy import text

        engine = self.get_engine()
        with engine.connect() as conn:
            if isinstance(params, dict):
                result = conn.execute(text(query), params)
            elif params:
                result = conn.exec_driver_sql(query, tuple(params) if isinstance(params, list) else params)
            else:
                result = conn.exec_driver_sql(query)
            if result.returns_rows:
                rows = []
                for i, row in enumerate(result):
                    if max_rows is not None and i >= max_rows:
                        break
                    rows.append(dict(row._mapping))
            else:
                rows = []
            if autocommit:
                conn.commit()
        return rows

    def execute_many(
        self,
        statements: List[str],
        params_list: Optional[List[Optional[Union[Dict[str, Any], List[Any], tuple]]]] = None,
    ) -> List[List[Dict[str, Any]]]:
        """Run several statements inside ONE transaction: all commit together, or
        all roll back together if any statement raises. Returns each statement's
        rows (empty list for a non-row-returning statement), in the same order as
        `statements`. `params_list`, if given, must be the same length as
        `statements` -- `None` for a given position means that statement takes no
        params."""
        from sqlalchemy import text

        if params_list is not None and len(params_list) != len(statements):
            raise ValueError("params_list must be the same length as statements.")

        engine = self.get_engine()
        all_rows: List[List[Dict[str, Any]]] = []
        with engine.begin() as conn:
            for i, stmt in enumerate(statements):
                params = params_list[i] if params_list is not None else None
                if isinstance(params, dict):
                    result = conn.execute(text(stmt), params)
                elif params:
                    result = conn.exec_driver_sql(stmt, tuple(params) if isinstance(params, list) else params)
                else:
                    result = conn.exec_driver_sql(stmt)
                if result.returns_rows:
                    all_rows.append([dict(row._mapping) for row in result])
                else:
                    all_rows.append([])
        return all_rows


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
