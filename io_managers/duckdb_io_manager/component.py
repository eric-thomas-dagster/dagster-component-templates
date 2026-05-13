"""DuckDB IO Manager component — local in-process by default, optional Quack
(`quack://`) client mode for routing concurrent writes through a remote
DuckDB server.
"""
from typing import Optional, Any

import dagster as dg
from pydantic import Field


class DuckDBIOManagerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a DuckDBPandasIOManager so assets are stored in / loaded from a
    DuckDB database. Set `quack_remote:` to route reads/writes through a
    remote Quack server instead of an in-process file (solves the
    concurrent-write limitation of single-file DuckDB)."""

    resource_key: str = Field(default="io_manager", description="Dagster resource key for this IO manager. Use 'io_manager' to make it the default.")
    database: str = Field(default="dagster.duckdb", description="Path to the LOCAL DuckDB database file. When `quack_remote:` is set, this is just the local proxy — actual storage lives on the remote.")
    schema_name: Optional[str] = Field(default="main", description="Schema within DuckDB for asset tables.")

    # --- Optional Quack client mode -----------------------------------------
    quack_remote: Optional[str] = Field(
        default=None,
        description=(
            "If set, routes all reads/writes through a remote Quack server. "
            "Format: `quack:host` or `quack:host:port`. Each connection opened "
            "by the IO manager installs+loads the `quack` extension, creates "
            "a SECRET with `quack_token`, ATTACHes the remote as "
            "`<quack_attach_as>`, and uses it as the default schema target "
            "(asset tables land in `<quack_attach_as>.<schema_name>.<table>`)."
        ),
    )
    quack_token: Optional[str] = Field(
        default=None,
        description="Auth token for the remote Quack server. Prefer env-var reference: `\"{{ env('QUACK_TOKEN') }}\"`.",
    )
    quack_attach_as: str = Field(
        default="remote",
        description="Name under which the remote Quack database is ATTACHed.",
    )
    quack_extension_repo: str = Field(
        default="core_nightly",
        description="Repo to install the `quack` extension from (`core_nightly` until the extension stabilizes).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.quack_remote:
            # Default: vanilla DuckDBPandasIOManager (in-process file).
            from dagster_duckdb_pandas import DuckDBPandasIOManager
            io_manager = DuckDBPandasIOManager(
                database=self.database,
                schema=self.schema_name,
            )
            return dg.Definitions(resources={self.resource_key: io_manager})

        # Quack client mode: custom IOManager that opens a local DuckDB,
        # installs+loads `quack`, sets the SECRET, ATTACHes the remote, and
        # routes reads/writes through `<attach_as>.<schema>.<table>`.
        database = self.database
        schema_name = self.schema_name or "main"
        quack_remote = self.quack_remote
        quack_token = self.quack_token
        quack_attach_as = self.quack_attach_as
        quack_extension_repo = self.quack_extension_repo

        class _QuackDuckDBPandasIOManager(dg.IOManager):
            """IOManager that uses DuckDB in Quack client mode for asset storage."""

            def _open(self) -> Any:
                import duckdb
                conn = duckdb.connect(database=database, read_only=False)
                conn.execute(f"INSTALL quack FROM {quack_extension_repo};")
                conn.execute("LOAD quack;")
                if quack_token:
                    conn.execute(
                        "CREATE OR REPLACE SECRET quack_default (TYPE quack, TOKEN ?);",
                        [quack_token],
                    )
                conn.execute(f"ATTACH '{quack_remote}' AS {quack_attach_as};")
                conn.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {quack_attach_as}.{schema_name};"
                )
                return conn

            def _table_for(self, context) -> str:
                # Asset key path → schema/table. Last component is table; everything
                # else is schema. Falls back to the configured `schema_name` if no
                # prefix is set on the asset.
                parts = context.asset_key.path
                if len(parts) > 1:
                    return f"{quack_attach_as}.{parts[-2]}.{parts[-1]}"
                return f"{quack_attach_as}.{schema_name}.{parts[-1]}"

            def handle_output(self, context, obj) -> None:
                import pandas as pd
                if not isinstance(obj, pd.DataFrame):
                    raise TypeError(
                        f"DuckDBIOManagerComponent (quack mode) only handles "
                        f"pandas DataFrames, got {type(obj).__name__}"
                    )
                table = self._table_for(context)
                conn = self._open()
                try:
                    conn.register("_dagster_output_df", obj)
                    conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM _dagster_output_df;")
                    context.log.info(
                        f"Wrote {len(obj)} rows to {table} (via Quack remote {quack_remote})"
                    )
                finally:
                    conn.close()

            def load_input(self, context) -> Any:
                table = self._table_for(context.upstream_output) if context.upstream_output else self._table_for(context)
                conn = self._open()
                try:
                    df = conn.execute(f"SELECT * FROM {table};").fetchdf()
                    context.log.info(
                        f"Read {len(df)} rows from {table} (via Quack remote {quack_remote})"
                    )
                    return df
                finally:
                    conn.close()

        return dg.Definitions(resources={self.resource_key: _QuackDuckDBPandasIOManager()})
