from typing import Union
"""Postgres IO Manager component.

YAML/Component wrapper around `PostgresIOManager`. Use `resource_key:
io_manager` to make this the default IO manager for the project.
"""
import dagster as dg
from pydantic import Field
from typing import Optional
import pandas as pd


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
