"""IBM Db2 Resource — Db2 LUW + Db2 on Cloud + Db2 for i (AS/400 / iSeries).

Mirrors mssql_resource / postgres_resource — provides a SQLAlchemy connection
string + connection factory other components can share via the Dagster resource
system. Works against:

  - Db2 LUW (Linux/UNIX/Windows) — the default; port 50000, ``ibm_db_sa``
    SQLAlchemy dialect, INFORMATION_SCHEMA catalog
  - Db2 Community Edition (``icr.io/db2_community/db2`` Docker image) — same
    as LUW
  - Db2 on Cloud (IBM Cloud DBaaS) — LUW under the hood; port 31xxx, SSL on
  - Db2 Warehouse — LUW under the hood; same shape as LUW
  - **Db2 for i (AS/400 / iSeries / IBM i)** — port 446 (DRDA) by default,
    same ``ibm_db_sa`` dialect but the system catalog lives at
    ``QSYS2.SYSTABLES`` / ``QSYS2.SYSCOLUMNS`` (not ``SYSCAT.*``). See
    ``system_type='iseries'`` below — the resource adjusts port defaults and
    surfaces an optional ``library_list`` field that maps to AS/400's
    ``CURRENT SCHEMA`` / ``CURRENT PATH`` concept.

For a deeper dive on the AS/400 surface (catalog query differences, library
list semantics, EBCDIC encoding gotchas), see ``examples/db2_iseries.md``.

The IBM ``ibm_db_sa`` SQLAlchemy dialect handles both LUW and i variants;
the difference is the connection parameters + the SQL surface customers
write against. This resource papers over the parameter differences.
"""
import urllib.parse
from typing import List, Optional

import dagster as dg
from pydantic import Field


# Default ports per system type. AS/400 uses DRDA (446); LUW uses
# the standard DB2 listener (50000). Cloud customers override
# explicitly because port varies per instance (typically 31xxx).
_DEFAULT_PORTS = {
    "luw": 50000,
    "iseries": 446,
    "cloud": 50000,
}


class Db2Resource(dg.ConfigurableResource):
    """Provides an IBM Db2 connection string + connection factory.

    ``system_type`` controls port defaults + AS/400-specific connection
    parameters. The SQLAlchemy dialect (``ibm_db_sa``) is the same for
    all three; only the connection-string params differ.

    AS/400 auto-fixes (when ``system_type='iseries'``):

      * If ``database`` is empty, the resource discovers the local RDB
        name automatically at connect time via
        ``SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1`` — saves customers
        from running ``WRKRDBDIRE`` on the i to find the ``*LOCAL`` entry.
      * ``CCSID=1208`` (UTF-8) is forced on the connection DSN so string
        columns round-trip cleanly instead of returning EBCDIC mojibake
        — overridable via ``ccsid`` field.
      * The library-list is wired to ``CURRENT SCHEMA`` (first entry) +
        ``CURRENT PATH`` (full list) via the DSN. Equivalent to ``CHGLIBL``
        on the i.

    Catalog auto-routing: paired with ``database_schema_inventory``, the
    inventory component can auto-detect i vs LUW from the resource's
    ``system_type`` and switch to the ``QSYS2.*`` catalog automatically
    — see ``database_schema_inventory.database_type='db2'`` (auto-routes
    to ``db2_iseries`` when system_type='iseries').
    """

    host: str
    port: int = 50000
    database: str = ""  # auto-discovered for iseries when empty
    username: str
    password: str
    ssl: bool = False
    security_mechanism: Optional[str] = None
    system_type: str = "luw"
    library_list: Optional[List[str]] = None
    ccsid: int = 1208  # UTF-8 by default — fixes EBCDIC mojibake on iseries

    def _discover_rdb_name(self) -> Optional[str]:
        """Connect to the i without a database param and ask for CURRENT_SERVER.

        ``SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1`` returns the local
        RDB name on Db2 for i. Used when ``database`` is empty and
        ``system_type='iseries'`` — saves the customer from running
        ``WRKRDBDIRE`` on the system.
        """
        try:
            import ibm_db
        except ImportError:
            return None
        # Probe DSN without a DATABASE clause; CURRENT_SERVER works at session start.
        probe_dsn = (
            f"HOSTNAME={self.host};PORT={self.port};"
            f"PROTOCOL=TCPIP;UID={self.username};PWD={self.password};"
        )
        if self.ssl:
            probe_dsn += "Security=SSL;"
        try:
            conn = ibm_db.connect(probe_dsn, "", "")
            stmt = ibm_db.exec_immediate(conn, "SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")
            row = ibm_db.fetch_assoc(stmt)
            ibm_db.close(conn)
            if row:
                # Key casing varies; first value works
                return str(next(iter(row.values()))).strip()
        except Exception:
            return None
        return None

    @property
    def effective_database(self) -> str:
        """Resolve ``database``, auto-discovering on iseries when unset."""
        if self.database:
            return self.database
        if self.system_type == "iseries":
            discovered = self._discover_rdb_name()
            if discovered:
                return discovered
        return self.database  # empty → caller will error appropriately

    @property
    def connection_string(self) -> str:
        """SQLAlchemy URL for ``ibm_db_sa``.

        On iseries: CCSID=1208 forced (override via ``ccsid``); library-
        list mapped to CURRENT SCHEMA + CURRENT PATH; RDB name
        auto-discovered when ``database`` is empty.
        """
        pw = urllib.parse.quote_plus(self.password)
        url = (
            f"db2+ibm_db://{self.username}:{pw}@{self.host}:{self.port}/"
            f"{self.effective_database}"
        )
        params: List[str] = []
        if self.ssl:
            params.append("Security=SSL")
        if self.security_mechanism:
            params.append(f"SecurityMechanism={self.security_mechanism}")
        if self.system_type == "iseries":
            # CCSID=1208 forces UTF-8 on string columns — fixes EBCDIC
            # mojibake without requiring CHGUSRPRF on the i.
            params.append(f"CCSID={self.ccsid}")
            if self.library_list:
                # First library wins as CURRENT SCHEMA; the rest land on
                # CURRENT PATH. AS/400 library-list model (search order
                # is left-to-right).
                params.append(f"CurrentSchema={self.library_list[0]}")
                if len(self.library_list) > 1:
                    params.append("LibraryList=" + ",".join(self.library_list))
        if params:
            url += "?" + "&".join(params)
        return url

    def get_engine(self):
        """Return a SQLAlchemy engine using the configured connection string."""
        from sqlalchemy import create_engine
        return create_engine(self.connection_string)

    def get_connection(self):
        """Return a raw ``ibm_db`` connection."""
        import ibm_db
        db = self.effective_database
        dsn = (
            f"DATABASE={db};HOSTNAME={self.host};PORT={self.port};"
            f"PROTOCOL=TCPIP;UID={self.username};PWD={self.password};"
        )
        if self.ssl:
            dsn += "Security=SSL;"
        if self.security_mechanism:
            dsn += f"SecurityMechanism={self.security_mechanism};"
        if self.system_type == "iseries":
            dsn += f"CCSID={self.ccsid};"
            if self.library_list:
                dsn += f"CurrentSchema={self.library_list[0]};"
                if len(self.library_list) > 1:
                    dsn += "LibraryList=" + ",".join(self.library_list) + ";"
        return ibm_db.connect(dsn, "", "")


class Db2ResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an IBM Db2 resource for use by other components.

    Pairs with the generic SQL components — ``dataframe_to_table``,
    ``sql_command_job``, ``warehouse_maintenance_job``, ``database_replication``,
    ``database_schema_inventory`` — by exporting its SQLAlchemy URL.

    Example (Docker Db2 Community Edition — Linux/Unix/Windows):

        ```yaml
        type: dagster_community_components.Db2ResourceComponent
        attributes:
          resource_key: db2_resource
          host: localhost
          port: 50000
          database: testdb
          username: db2inst1
          password_env_var: DB2_PASSWORD
          system_type: luw                  # default — can omit
        ```

    Example (Db2 on Cloud):

        ```yaml
        type: dagster_community_components.Db2ResourceComponent
        attributes:
          resource_key: db2_resource
          host: '<id>.databases.appdomain.cloud'
          port: 31198
          database: bludb
          username: bluadmin
          password_env_var: DB2_CLOUD_PASSWORD
          ssl: true
          system_type: cloud
        ```

    Example (Db2 for i / AS/400 / iSeries):

        ```yaml
        type: dagster_community_components.Db2ResourceComponent
        attributes:
          resource_key: db2_iseries_resource
          host: as400.example.com
          # port defaults to 446 when system_type=iseries — DRDA listener
          database: 'S101ABCD'              # *LOCAL system name (RDB) — see your IBM i
          username: QSECOFR
          password_env_var: AS400_PASSWORD
          ssl: true                         # almost always required on i 7.x+
          system_type: iseries
          library_list:                     # AS/400 search path
            - MYAPP
            - MYAPP_DATA
            - QSYS2
        ```

        Notes for AS/400:
          - ``host`` is the IBM i system hostname or IP.
          - ``database`` is the **Relational Database Directory (RDB) name**
            (often the *LOCAL system name). Find it with
            ``WRKRDBDIRE`` on the i — the entry marked ``*LOCAL``.
          - ``library_list`` maps to ``CURRENT SCHEMA`` (first entry) +
            ``CURRENT PATH`` (rest). Schema-inventory and migration
            components honor this when ``database_type='db2_iseries'``.
          - The catalog lives at ``QSYS2.SYSTABLES`` / ``QSYS2.SYSCOLUMNS``,
            NOT ``SYSCAT.*``. Use ``database_schema_inventory`` with
            ``database_type: db2_iseries`` for AS/400-aware queries.
    """

    resource_key: str = Field(
        default="db2_resource",
        description="Resource key. Other components reference it via this name.",
    )
    host: str = Field(description="Db2 hostname (LUW server / Cloud DBaaS endpoint / IBM i system name).")
    port: Optional[int] = Field(
        default=None,
        description=(
            "Db2 port. If unset, defaults based on system_type: 50000 (luw / cloud) "
            "or 446 (iseries — DRDA listener). Override for non-standard ports "
            "(Db2 on Cloud typically uses 31xxx)."
        ),
    )
    database: str = Field(
        default="",
        description=(
            "Database name. For LUW / Cloud / Warehouse this is the DB name "
            "(e.g. 'BLUDB', 'testdb'). For iSeries this is the **Relational "
            "Database Directory entry** (often the *LOCAL system name). "
            "**Leave empty on iSeries** and the resource auto-discovers it via "
            "``SELECT CURRENT_SERVER`` — no `WRKRDBDIRE` lookup required."
        ),
    )
    username: str = Field(description="Login username (AS/400: user profile name, e.g. QSECOFR or a service profile).")
    password: Optional[str] = Field(default=None, description="Db2 password (literal). Set this OR password_env_var.")
    password_env_var: Optional[str] = Field(default=None, description="Env var holding the password. Set this OR password.")
    ssl: bool = Field(
        default=False,
        description="Enable SSL connection. Required for Db2 on Cloud and almost always required for IBM i 7.x+.",
    )
    security_mechanism: Optional[str] = Field(
        default=None,
        description="Optional SecurityMechanism override (e.g. 'PLAIN'). Leave unset for default.",
    )
    system_type: str = Field(
        default="luw",
        description=(
            "Db2 system variant: 'luw' (default — Db2 LUW / Community Edition / "
            "Warehouse), 'cloud' (Db2 on Cloud DBaaS), or 'iseries' (Db2 for i / "
            "AS/400 / IBM i). Drives port defaults + library-list connection params. "
            "All three use the same ibm_db_sa SQLAlchemy dialect; the catalog SQL "
            "differs (see database_schema_inventory's database_type='db2_iseries')."
        ),
    )
    library_list: Optional[List[str]] = Field(
        default=None,
        description=(
            "AS/400 only: ordered library-list passed as CURRENT SCHEMA (first "
            "entry) + CURRENT PATH (rest). Equivalent to a CHGLIBL on the i. "
            "Ignored when system_type != 'iseries'."
        ),
    )
    ccsid: int = Field(
        default=1208,
        description=(
            "AS/400 only: CCSID for string-column encoding. Defaults to 1208 "
            "(UTF-8) — fixes EBCDIC mojibake without requiring CHGUSRPRF on "
            "the i. Override only if you have a specific reason to use a "
            "different CCSID. Ignored when system_type != 'iseries'."
        ),
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.system_type not in _DEFAULT_PORTS:
            raise ValueError(
                f"system_type must be one of {sorted(_DEFAULT_PORTS)}; got "
                f"{self.system_type!r}."
            )
        port = self.port if self.port is not None else _DEFAULT_PORTS[self.system_type]
        resource = Db2Resource(
            host=self.host,
            port=port,
            database=self.database,
            username=self.username,
            password=self.password if self.password else dg.EnvVar(self.password_env_var or ""),
            ssl=self.ssl,
            security_mechanism=self.security_mechanism,
            system_type=self.system_type,
            library_list=self.library_list,
        )
        return dg.Definitions(resources={self.resource_key: resource})
