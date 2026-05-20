"""SnowflakeStoredProcedureComponent — define a Snowflake STORED PROCEDURE as Dagster YAML.

Define a Snowflake STORED PROCEDURE (SQL or Snowpark Python). Materialization runs CREATE OR REPLACE PROCEDURE and (optionally) CALLs it.

Pairs with `snowflake_workspace` (which DISCOVERS existing entities) — use
this when you want to define new entities as code rather than maintain
them separately in Snowflake worksheets.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext, Component, ComponentLoadContext, Definitions,
    MetadataValue, Model, Resolvable, asset,
)
from pydantic import Field


class SnowflakeStoredProcedureComponent(Component, Model, Resolvable):
    """Define a Snowflake STORED PROCEDURE (SQL or Snowpark Python). Materialization runs CREATE OR REPLACE PROCEDURE and (optionally) CALLs it."""

    asset_name: str = Field(description="Output Dagster asset name")

    # ── Connection (shared with all snowflake_* components) ──
    account: str = Field(description="Snowflake account, e.g. xy12345-abc.")
    user: str = Field(description="Snowflake user (NAME for keypair, LOGIN_NAME for password/SSO/PAT).")
    warehouse: Optional[str] = Field(default=None)
    database: str = Field(description="Database containing the entity (or where it will be created).")
    schema_name: str = Field(description="Schema for the entity.")
    role: Optional[str] = Field(default=None)

    # ── Auth (pick ONE: password / authenticator+keypair / SSO / PAT) ──
    password: Optional[str] = Field(default=None, description="Snowflake password. Leave unset for SSO/keypair/PAT.")
    authenticator: Optional[str] = Field(default=None,
        description="Snowflake authenticator: 'SNOWFLAKE_JWT' (keypair), 'externalbrowser' (SSO), 'PROGRAMMATIC_ACCESS_TOKEN' (PAT), 'oauth'.")
    private_key_file: Optional[str] = Field(default=None, description="PEM private key file path (for SNOWFLAKE_JWT).")
    private_key_file_pwd: Optional[str] = Field(default=None, description="Passphrase for encrypted private_key_file.")
    token: Optional[str] = Field(default=None, description="OAuth/PAT token (for PROGRAMMATIC_ACCESS_TOKEN or oauth).")

    procedure_name: str = Field(description="Procedure name (unquoted identifier)")
    language: str = Field(default="SQL", description="'SQL', 'PYTHON', 'JAVASCRIPT', or 'SCALA'")
    parameters: Optional[List[Dict[str, str]]] = Field(default=None, description="[{name: 'days', type: 'INT'}, ...]")
    returns: str = Field(default="VARCHAR", description="Return type: 'VARCHAR', 'TABLE(col TYPE, ...)', etc.")
    runtime_version: Optional[str] = Field(default=None, description="For PYTHON: '3.10' / '3.11' etc.")
    packages: Optional[List[str]] = Field(default=None, description="For PYTHON: ['snowflake-snowpark-python', 'pandas']")
    handler: Optional[str] = Field(default=None, description="For PYTHON: handler function name, e.g. 'main'")
    body: str = Field(description="Procedure body. SQL: BEGIN/END block. Python: function body wrapped in $$...$$")
    on_materialize: str = Field(default="create_only", description="'call' = CREATE + CALL the proc; 'create_only' = just CREATE.")
    call_args: Optional[List[str]] = Field(default=None, description="If on_materialize='call', literal args to pass: ['90'] for SP_PURGE(90).")
    # ── Asset metadata ──
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    def _connect(self):
        import snowflake.connector
        ck = dict(
            account=self.account, user=self.user,
            database=self.database, schema=self.schema_name,
        )
        if self.warehouse: ck["warehouse"] = self.warehouse
        if self.role:      ck["role"] = self.role
        if self.authenticator:
            ck["authenticator"] = self.authenticator
            if self.private_key_file:
                ck["private_key_file"] = self.private_key_file
                if self.private_key_file_pwd:
                    ck["private_key_file_pwd"] = self.private_key_file_pwd
            elif self.token:
                ck["token"] = self.token
        elif self.password:
            ck["password"] = self.password
        else:
            raise ValueError(f"{type(self).__name__}: must set password OR authenticator+key/token.")
        return snowflake.connector.connect(**ck)
    def _build_ddl(self) -> str:
        params = ""
        if self.parameters:
            params = ", ".join(f"{p['name']} {p['type']}" for p in self.parameters)
        body = f"CREATE OR REPLACE PROCEDURE {self.database}.{self.schema_name}.{self.procedure_name}({params})\n"
        body += f"  RETURNS {self.returns}\n"
        body += f"  LANGUAGE {self.language}\n"
        if self.language.upper() == "PYTHON":
            if self.runtime_version:
                body += f"  RUNTIME_VERSION = '{self.runtime_version}'\n"
            if self.packages:
                body += f"  PACKAGES = ({', '.join(repr(p) for p in self.packages)})\n"
            if self.handler:
                body += f"  HANDLER = '{self.handler}'\n"
        body += "AS\n"
        body += self.body
        return body

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        kinds = list(self.kinds or []) or ["snowflake"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            name=self.asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _entity_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
            ddl = self._build_ddl()
            context.log.info(f"Executing DDL:\n{ddl}")
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(ddl)
                if self.on_materialize == "call":
                    args = ", ".join(self.call_args or [])
                    try:
                        cur.execute(f"CALL {self.database}.{self.schema_name}.{self.procedure_name}({args})")
                        row = cur.fetchone()
                        if row:
                            context.log.info(f"Procedure returned: {row[0]}")
                    except Exception as e:
                        context.log.warning(f"Could not CALL procedure: {e}")
                cur.close()
            finally:
                conn.close()
            return dg.MaterializeResult(metadata={
                "snowflake/ddl": MetadataValue.md(f"```sql\n{ddl}\n```"),
                "snowflake/database": MetadataValue.text(self.database),
                "snowflake/schema": MetadataValue.text(self.schema_name),
            })

        return Definitions(assets=[_entity_asset])

    @classmethod
    def get_description(cls) -> str:
        return "Define a Snowflake STORED PROCEDURE (SQL or Snowpark Python). Materialization runs CREATE OR REPLACE PROCEDURE and (optionally) CALLs it."
