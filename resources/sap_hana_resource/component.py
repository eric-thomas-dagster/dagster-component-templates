"""SAP HANA Resource.

Registers a SAP HANA database resource for use by other components.
SAP HANA on Azure (preview) AND on-premise / multi-cloud all use the
same SQLAlchemy dialect (`sqlalchemy-hana` package).

Use this resource OR pass `hana://` URLs directly to:
- `dataframe_to_table` (sink) — write DataFrame to HANA table
- `dataframe_from_sql` (source) — query HANA → DataFrame

URL format:
  hana://<user>:<urlencoded-password>@<host>:<port>?encrypt=true

Common HANA on Azure use cases:
- Pull data from SAP S/4HANA / BW into Dagster pipelines for analytics
- Push aggregated results back into HANA Calculation Views
- Sync HANA tables to a data lake for ML
"""

from typing import Optional
import dagster as dg
from pydantic import Field


class SapHanaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SAP HANA database resource.

    Provides a SQLAlchemy URL + a `hana_url()` helper to other ops/assets.
    """

    resource_key: str = Field(default="sap_hana", description="Dagster resource key")

    host: str = Field(description="HANA host (e.g. 'myhana.hanacloud.ondemand.com')")
    port: int = Field(default=443, description="HANA port (443 for HANA Cloud, 30015 for on-prem default)")
    database: Optional[str] = Field(
        default=None,
        description="Tenant DB name (multi-tenant systems). For HANA Cloud, leave empty.",
    )
    user: str = Field(description="HANA user")
    password_env_var: str = Field(description="Env var holding the user's password")
    encrypt: bool = Field(default=True, description="TLS — required for HANA Cloud")
    validate_certificate: bool = Field(default=True, description="Verify the server certificate")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        cfg = self

        class HanaResource(dg.ConfigurableResource):
            """SAP HANA resource — exposes a SQLAlchemy URL + helpers."""

            def url(self) -> str:
                """Return a SQLAlchemy URL for HANA."""
                import os, urllib.parse
                pwd = os.environ[cfg.password_env_var]
                pwd_enc = urllib.parse.quote(pwd, safe="")
                params = []
                if cfg.encrypt:
                    params.append("encrypt=true")
                if not cfg.validate_certificate:
                    params.append("sslValidateCertificate=false")
                if cfg.database:
                    params.append(f"databaseName={cfg.database}")
                qs = ("?" + "&".join(params)) if params else ""
                return f"hana://{cfg.user}:{pwd_enc}@{cfg.host}:{cfg.port}{qs}"

            def get_engine(self):
                """Return a SQLAlchemy engine. Requires sqlalchemy-hana installed."""
                import sqlalchemy
                return sqlalchemy.create_engine(self.url())

        return dg.Definitions(resources={self.resource_key: HanaResource()})
