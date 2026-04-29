"""Trino IO Manager.

Stores Dagster assets as Trino tables (in any catalog/schema combination
that supports CREATE/INSERT — Iceberg, Hive, Delta, etc.). Supports
partitioned assets (per-partition DELETE+INSERT inside a transaction)
and multi-component asset keys (mapped to ``schema.table``).

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize_ident(component: str) -> str:
    cleaned = "".join(c if c.isalnum() else "_" for c in component)
    return cleaned.lower().strip("_") or "t"


def _sql_literal(v) -> str:
    """Render a Python value as a Trino SQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, float) and v != v:  # NaN
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _trino_type_for(dtype) -> str:
    s = str(dtype)
    if s.startswith("float"):
        return "DOUBLE"
    if s.startswith("int") or s.startswith("uint"):
        return "BIGINT"
    if s.startswith("bool"):
        return "BOOLEAN"
    return "VARCHAR"


class TrinoIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames via Trino SQL."""

    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(default="iceberg", description="Trino catalog, e.g. 'iceberg', 'hive', 'delta'")
    default_schema: str = Field(default="default", description="Schema used when asset key has only one component")
    password: Optional[str] = Field(default=None, description="Trino password (resolved from env via dg.EnvVar)")
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def _get_connection(self):
        import trino.dbapi as trino_dbapi
        import trino.auth as trino_auth
        return trino_dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.default_schema,
            auth=trino_auth.BasicAuthentication(self.user, self.password) if self.password else None,
        )

    def _table_name(self, context) -> tuple[str, str]:
        if context.has_asset_key:
            path = [_sanitize_ident(p) for p in context.asset_key.path]
        else:
            path = [_sanitize_ident(p) for p in (context.step_key, context.name)]
        if len(path) >= 2:
            return path[0], "_".join(path[1:])
        return self.default_schema, path[0]

    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        if obj is None:
            return
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"TrinoIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )

        schema, table = self._table_name(context)
        qualified = f"{self.catalog}.{schema}.{table}"

        cols_with_types = list(obj.columns)
        col_defs = ", ".join(f'"{c}" {_trino_type_for(obj[c].dtype)}' for c in cols_with_types)
        if context.has_partition_key:
            col_defs = f'{col_defs}, "{self.partition_column}" VARCHAR'

        conn = self._get_connection()
        try:
            cur = conn.cursor()
            # Trino DDL is auto-committed per statement; multi-statement
            # transactional semantics are achieved via START TRANSACTION / COMMIT.
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{schema}")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified} ({col_defs})")

            # Per-partition: DELETE + INSERT inside an explicit transaction.
            # For unpartitioned: full TRUNCATE-equivalent (DELETE) + INSERT.
            cur.execute("START TRANSACTION")
            try:
                if context.has_partition_key:
                    partition_value = str(context.partition_key)
                    cur.execute(
                        f"DELETE FROM {qualified} "
                        f"WHERE \"{self.partition_column}\" = {_sql_literal(partition_value)}"
                    )
                else:
                    cur.execute(f"DELETE FROM {qualified}")

                if len(obj) > 0:
                    col_list = ", ".join(f'"{c}"' for c in cols_with_types)
                    if context.has_partition_key:
                        col_list = f'{col_list}, "{self.partition_column}"'
                    rows_sql = []
                    pk_literal = _sql_literal(str(context.partition_key)) if context.has_partition_key else None
                    for _, row in obj.iterrows():
                        vals = ", ".join(_sql_literal(v) for v in row)
                        if context.has_partition_key:
                            vals = f"{vals}, {pk_literal}"
                        rows_sql.append(f"({vals})")
                    cur.execute(f"INSERT INTO {qualified} ({col_list}) VALUES {', '.join(rows_sql)}")
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
        finally:
            conn.close()

        context.add_output_metadata(
            {
                "table": dg.MetadataValue.text(qualified),
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        upstream = context.upstream_output
        schema, table = self._table_name(upstream)
        qualified = f"{self.catalog}.{schema}.{table}"
        conn = self._get_connection()
        try:
            if upstream.has_partition_key:
                partition_value = str(upstream.partition_key)
                query = (
                    f"SELECT * FROM {qualified} "
                    f"WHERE \"{self.partition_column}\" = {_sql_literal(partition_value)}"
                )
            else:
                query = f"SELECT * FROM {qualified}"
            return pd.read_sql(query, conn)
        finally:
            conn.close()
