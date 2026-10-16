"""Starburst resource — alias for trino_resource.

Starburst is the commercial distribution of the Trino engine (originally
PrestoSQL, then renamed to Trino in 2021). Same Java codebase, same
Trino wire protocol, same SQL dialect, same Python client (``trino-python-
client``). The differences from Trino are enterprise features (Starburst
Galaxy SaaS, Stargate cross-cluster joins, enhanced RBAC, enterprise
connectors) — all server-side.

For Dagster's purposes, this resource is a **pure subclass** of
``TrinoResource`` — same field surface, same connection logic, registered
under a different vendor for discoverability. Customers running Starburst
Galaxy / Starburst Enterprise can ``add starburst_resource`` and not have
to know the underlying engine is Trino.

Connection notes specific to Starburst Galaxy:
  - Hostname pattern: ``<cluster>.galaxy.starburst.io``
  - Default port: 443 (HTTPS)
  - Auth: OAuth2 or username/password — both flow through the same
    ``trino-python-client`` BasicAuthentication / JWT mechanisms.

For full docs on the underlying engine see the ``trino_resource``
component — fields + behavior are identical.
"""
import dagster as dg
from contextlib import contextmanager
from typing import Optional

from pydantic import Field

# Self-contained: the Trino engine classes are inlined (a component.py must not import a
# sibling component -- the CLI copies only this file). Kept private to avoid registering
# duplicate public classes; behavior is identical to resources/trino_resource.

class _TrinoResource(dg.ConfigurableResource):
    """Trino distributed SQL query engine resource."""
    host: str
    port: int = 8080
    user: str = "dagster"
    catalog: str = "iceberg"
    schema_name: Optional[str] = None
    password: Optional[str] = None

    @contextmanager
    def get_connection(self):
        import trino.dbapi as trino_dbapi
        from trino.auth import BasicAuthentication
        conn = trino_dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema_name,
            auth=BasicAuthentication(self.user, self.password) if self.password else None,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str):
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(query)
            return cur.fetchall()


class _TrinoResourceBase(dg.Component, dg.Model, dg.Resolvable):
    """Register a Trino distributed SQL query engine resource for use by other components."""

    resource_key: str = Field(default="trino_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    host: str = Field(default="localhost", description="Trino coordinator host")
    port: int = Field(default=8080, description="Trino coordinator port")
    user: str = Field(default="dagster", description="Trino user name")
    catalog: str = Field(default="iceberg", description="Default catalog, e.g. 'iceberg', 'hive', 'delta'")
    schema_name: Optional[str] = Field(default=None, description="Default schema within the catalog")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Trino password (for basic auth)")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = _TrinoResource(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema_name=self.schema_name,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: resource})




class StarburstResource(_TrinoResource):
    """Starburst — alias for TrinoResource. Same engine, same wire protocol."""
    pass


class StarburstResourceComponent(_TrinoResourceBase):
    """Register a Starburst resource — alias for ``trino_resource``.

    Same field surface as ``trino_resource``; differs only in:

      * Default ``resource_key`` is ``starburst_resource``
      * Default ``port`` is 443 (Starburst Galaxy default)
      * Vendor grouping (Starburst vs Trino) in the registry UI

    Example (Starburst Galaxy):

        ```yaml
        type: dagster_community_components.StarburstResourceComponent
        attributes:
          resource_key: starburst_resource
          host: my-cluster.galaxy.starburst.io
          port: 443
          user: my-user@example.com
          catalog: tpch
          schema_name: sf1
          password_env_var: STARBURST_PASSWORD
        ```
    """
    # Override defaults for Galaxy convention.
    resource_key: str = "starburst_resource"
    port: int = 443

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Build a StarburstResource instance (subclass of TrinoResource) so
        # downstream consumers can do `isinstance(r, StarburstResource)` if
        # they care to distinguish. Same engine; just a different label.
        resource = StarburstResource(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema_name=self.schema_name,
            password=dg.EnvVar(self.password_env_var) if self.password_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: resource})
