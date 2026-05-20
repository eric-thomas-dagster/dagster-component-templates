"""SnowflakeTaskComponent — define a Snowflake TASK as Dagster YAML.

Define a Snowflake TASK as Dagster YAML. Materialization runs CREATE OR REPLACE TASK and (optionally) EXECUTE TASK.

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


class SnowflakeTaskComponent(Component, Model, Resolvable):
    """Define a Snowflake TASK as Dagster YAML. Materialization runs CREATE OR REPLACE TASK and (optionally) EXECUTE TASK."""

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

    task_name: str = Field(description="Snowflake task name (unquoted identifier)")
    schedule: Optional[str] = Field(default=None, description='"USING CRON 0 2 * * * UTC" or "60 minute". Mutually exclusive with after_tasks.')
    after_tasks: Optional[List[str]] = Field(default=None, description="Fully-qualified parent task names (creates an AFTER chain).")
    sql: str = Field(description="Task body — single SQL statement or CALL.")
    allow_overlapping_execution: bool = Field(default=False)
    on_materialize: str = Field(default="execute", description="'execute' = CREATE + RESUME + EXECUTE; 'create_only' = just CREATE; 'create_and_resume' = create + resume schedule.")
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
        body = f"CREATE OR REPLACE TASK {self.database}.{self.schema_name}.{self.task_name}\n"
        body += f"  WAREHOUSE = {self.warehouse}\n"
        if self.schedule:
            body += f"  SCHEDULE = '{self.schedule}'\n"
        if self.after_tasks:
            body += "  AFTER " + ", ".join(self.after_tasks) + "\n"
        if self.allow_overlapping_execution:
            body += "  ALLOW_OVERLAPPING_EXECUTION = TRUE\n"
        body += f"AS\n  {self.sql}"
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
                if self.on_materialize in ("create_and_resume", "execute"):
                    try:
                        cur.execute(f"ALTER TASK {self.database}.{self.schema_name}.{self.task_name} RESUME")
                    except Exception as e:
                        context.log.warning(f"Could not resume task (EXECUTE TASK privilege required): {e}")
                if self.on_materialize == "execute":
                    try:
                        cur.execute(f"EXECUTE TASK {self.database}.{self.schema_name}.{self.task_name}")
                    except Exception as e:
                        context.log.warning(f"Could not execute task: {e}")
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
        return "Define a Snowflake TASK as Dagster YAML. Materialization runs CREATE OR REPLACE TASK and (optionally) EXECUTE TASK."
