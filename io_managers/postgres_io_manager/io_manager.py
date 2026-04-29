"""Postgres IO Manager.

Stores Dagster assets as PostgreSQL tables. Supports partitioned assets
(per-partition DELETE+INSERT inside a transaction) and multi-component
asset keys (mapped to ``schema.table`` names).

Implemented as a `ConfigurableIOManager` subclass per Dagster's modern
Pythonic-config pattern. Importable directly for use without the Component
wrapper.
"""
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


def _sanitize_ident(component: str) -> str:
    """Sanitize an identifier component for safe interpolation into SQL.

    Lowercases, replaces non-alphanumeric chars with underscores. Postgres
    quoting around the final identifier still applies.
    """
    cleaned = "".join(c if c.isalnum() else "_" for c in component)
    return cleaned.lower().strip("_") or "t"


class PostgresIOManager(dg.ConfigurableIOManager):
    """ConfigurableIOManager that reads/writes pandas DataFrames as Postgres tables."""

    host: str = Field(default="localhost", description="PostgreSQL host")
    port: int = Field(default=5432, description="PostgreSQL port")
    database: str = Field(description="PostgreSQL database name")
    user: str = Field(description="PostgreSQL username")
    password: Optional[str] = Field(default=None, description="PostgreSQL password (resolved from env via dg.EnvVar)")
    default_schema: str = Field(default="public", description="Schema used when asset key has only one component")
    if_exists: str = Field(default="replace", description="Behavior for unpartitioned writes when the table already exists: 'replace', 'append', or 'fail'")
    partition_column: str = Field(
        default="partition_key",
        description="Column name used to scope per-partition DELETE+INSERT writes",
    )

    def _get_engine(self):
        import sqlalchemy as sa
        password = self.password or ""
        url = f"postgresql+psycopg2://{self.user}:{password}@{self.host}:{self.port}/{self.database}"
        return sa.create_engine(url)

    def _table_name(self, context) -> tuple[str, str]:
        """Return ``(schema, table)`` for an asset.

        Multi-component asset keys (``len(path) >= 2``) map the first component
        to the schema and join the rest with ``_`` for the table. Single-component
        keys land in ``default_schema``.
        """
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
                f"PostgresIOManager only handles pandas DataFrames; got {type(obj).__name__}. "
                f"Use a different IO manager for non-DataFrame outputs."
            )
        import sqlalchemy as sa

        schema, table = self._table_name(context)
        engine = self._get_engine()

        with engine.begin() as conn:  # transaction
            # Ensure schema exists.
            conn.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

            if context.has_partition_key:
                partition_value = str(context.partition_key)
                df = obj.copy()
                df[self.partition_column] = partition_value
                # First write may need to create the table; use append after that.
                exists = conn.execute(
                    sa.text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :s AND table_name = :t"
                    ),
                    {"s": schema, "t": table},
                ).first()
                if exists:
                    conn.execute(
                        sa.text(
                            f'DELETE FROM "{schema}"."{table}" '
                            f'WHERE "{self.partition_column}" = :pk'
                        ),
                        {"pk": partition_value},
                    )
                    df.to_sql(table, conn, schema=schema, if_exists="append", index=False)
                else:
                    df.to_sql(table, conn, schema=schema, if_exists="replace", index=False)
            else:
                obj.to_sql(table, conn, schema=schema, if_exists=self.if_exists, index=False)

        context.add_output_metadata(
            {
                "table": dg.MetadataValue.text(f"{schema}.{table}"),
                "row_count": dg.MetadataValue.int(len(obj)),
                "partition_key": dg.MetadataValue.text(
                    str(context.partition_key) if context.has_partition_key else "(unpartitioned)"
                ),
            }
        )

    def load_input(self, context: dg.InputContext) -> pd.DataFrame:
        import sqlalchemy as sa

        upstream = context.upstream_output
        schema, table = self._table_name(upstream)
        engine = self._get_engine()
        with engine.connect() as conn:
            if upstream.has_partition_key:
                partition_value = str(upstream.partition_key)
                return pd.read_sql(
                    sa.text(
                        f'SELECT * FROM "{schema}"."{table}" '
                        f'WHERE "{self.partition_column}" = :pk'
                    ),
                    conn,
                    params={"pk": partition_value},
                )
            return pd.read_sql(sa.text(f'SELECT * FROM "{schema}"."{table}"'), conn)
