"""DuckDB Resource component — local in-process by default, optional Quack
(`quack://`) client mode for connecting to a remote DuckDB instance.
"""
from contextlib import contextmanager
from typing import Optional

import dagster as dg
from pydantic import Field


class DuckDBResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-duckdb DuckDBResource for use by other components.

    Defaults to in-process DuckDB. Set `quack_remote:` to enable client-mode
    against a remote Quack server (DuckDB's HTTP-based client/server protocol,
    introduced May 2026 — solves the concurrent-write limitation of in-process
    DuckDB by routing writes through a single server instance).
    """

    resource_key: str = Field(default="duckdb_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    database: str = Field(default=":memory:", description="DuckDB database path for the LOCAL session. Use ':memory:' for ephemeral, or a file path for persistence. When `quack_remote:` is set, this is just the local proxy — actual writes go to the remote.")

    # --- Optional Quack client mode (DuckDB v1.5.2+) -----------------------
    quack_remote: Optional[str] = Field(
        default=None,
        description=(
            "If set, enables Quack client mode. Format: `quack:host` or `quack:host:port` "
            "(default port 9494). The component installs+loads the `quack` extension on each "
            "connection, creates a SECRET with the auth token, and ATTACHes the remote as "
            "`<quack_attach_as>`. Reference remote tables as `<quack_attach_as>.<table>` in "
            "your SQL (or `USE <quack_attach_as>;` to default unqualified references to remote)."
        ),
    )
    quack_token: Optional[str] = Field(
        default=None,
        description="Auth token for the remote Quack server (matches the server's `quack_serve(token=...)`). Prefer env-var reference: `\"{{ env('QUACK_TOKEN') }}\"`.",
    )
    quack_attach_as: str = Field(
        default="remote",
        description="Name under which the remote Quack database is ATTACHed in the local session.",
    )
    quack_extension_repo: str = Field(
        default="core_nightly",
        description="Repo to install the `quack` extension from. As of the May-2026 release, Quack lives in `core_nightly`; switch to `core` once the extension stabilizes.",
    )
    quack_use_remote_by_default: bool = Field(
        default=False,
        description="If True, runs `USE <quack_attach_as>;` after attach — unqualified table names then resolve to the remote (closer to drop-in replacement for a local DuckDB).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_duckdb import DuckDBResource

        if not self.quack_remote:
            # Default: vanilla in-process DuckDB.
            resource = DuckDBResource(database=self.database)
            return dg.Definitions(resources={self.resource_key: resource})

        # Quack client mode: subclass DuckDBResource so its get_connection()
        # context-manager runs the extension setup on each fresh connection.
        quack_remote = self.quack_remote
        quack_token = self.quack_token
        quack_attach_as = self.quack_attach_as
        quack_extension_repo = self.quack_extension_repo
        quack_use_remote_by_default = self.quack_use_remote_by_default

        class _QuackDuckDBResource(DuckDBResource):
            @contextmanager
            def get_connection(self):
                with super().get_connection() as conn:
                    conn.execute(f"INSTALL quack FROM {quack_extension_repo};")
                    conn.execute("LOAD quack;")
                    if quack_token:
                        conn.execute(
                            "CREATE OR REPLACE SECRET quack_default (TYPE quack, TOKEN ?);",
                            [quack_token],
                        )
                    conn.execute(f"ATTACH '{quack_remote}' AS {quack_attach_as};")
                    if quack_use_remote_by_default:
                        conn.execute(f"USE {quack_attach_as};")
                    yield conn

        resource = _QuackDuckDBResource(database=self.database)
        return dg.Definitions(resources={self.resource_key: resource})
