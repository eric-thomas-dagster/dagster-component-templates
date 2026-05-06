"""Microsoft SQL Server / Azure SQL Database Resource component.

Mirrors postgres_resource and mysql_resource — provides a connection string
and a connection factory other components can share via the Dagster resource
system. Works against on-prem SQL Server and Azure SQL Database (serverless
or provisioned). Uses pymssql by default for pure-Python install simplicity;
can be swapped to pyodbc by setting `driver: pyodbc`.
"""
import urllib.parse
from typing import Optional

import dagster as dg
from pydantic import Field


class MssqlResource(dg.ConfigurableResource):
    """Provides a SQL Server / Azure SQL connection string and connection factory."""

    host: str
    port: int = 1433
    database: str
    username: str
    password: str
    driver: str = "pymssql"
    encrypt: bool = True
    trust_server_certificate: bool = False

    @property
    def connection_string(self) -> str:
        """SQLAlchemy URL — works against both on-prem SQL Server and Azure SQL."""
        pw = urllib.parse.quote_plus(self.password)
        if self.driver == "pyodbc":
            params = (
                "driver=ODBC+Driver+18+for+SQL+Server"
                f"&encrypt={'yes' if self.encrypt else 'no'}"
                f"&TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}"
            )
            return (
                f"mssql+pyodbc://{self.username}:{pw}@{self.host}:{self.port}/"
                f"{self.database}?{params}"
            )
        # pymssql (default — pure Python, no ODBC driver required)
        return (
            f"mssql+pymssql://{self.username}:{pw}@{self.host}:{self.port}/"
            f"{self.database}"
        )

    def get_engine(self):
        """Return a SQLAlchemy engine using the configured connection string."""
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)

    def get_connection(self):
        """Return a raw DB-API connection (pymssql or pyodbc, by driver)."""
        if self.driver == "pyodbc":
            import pyodbc
            return pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={self.host},{self.port};DATABASE={self.database};"
                f"UID={self.username};PWD={self.password};"
                f"Encrypt={'yes' if self.encrypt else 'no'};"
                f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}"
            )
        import pymssql
        return pymssql.connect(
            server=self.host, port=self.port,
            database=self.database, user=self.username, password=self.password,
        )


class MssqlResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SQL Server / Azure SQL resource for use by other components."""

    resource_key: str = Field(
        default="mssql_resource",
        description="Resource key. Other components reference it via this name.",
    )
    host: str = Field(description="SQL Server hostname (e.g. 'myserver.database.windows.net')")
    port: int = Field(default=1433, description="SQL Server port")
    database: str = Field(description="Database name")
    username: str = Field(description="Login username")
    password_env_var: str = Field(description="Env var holding the password")
    driver: str = Field(
        default="pymssql",
        description="'pymssql' (pure Python, default) or 'pyodbc' (requires ODBC Driver 18 installed)",
    )
    encrypt: bool = Field(
        default=True,
        description="Enable connection encryption. Required for Azure SQL.",
    )
    trust_server_certificate: bool = Field(
        default=False,
        description="When True, skip server-cert verification. Use only for self-signed local SQL Server.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = MssqlResource(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=dg.EnvVar(self.password_env_var),
            driver=self.driver,
            encrypt=self.encrypt,
            trust_server_certificate=self.trust_server_certificate,
        )
        return dg.Definitions(resources={self.resource_key: resource})
