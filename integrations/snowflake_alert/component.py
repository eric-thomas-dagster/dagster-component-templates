"""SnowflakeAlertComponent — define a Snowflake ALERT as Dagster YAML.

Define a Snowflake ALERT (scheduled conditional action). Materialization runs CREATE OR REPLACE ALERT and resumes it.

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


class SnowflakeAlertComponent(Component, Model, Resolvable):
    """Define a Snowflake ALERT (scheduled conditional action). Materialization runs CREATE OR REPLACE ALERT and resumes it."""

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

    alert_name: str = Field(description="Alert name (unquoted identifier)")
    schedule: str = Field(default="60 minute", description='"60 minute" or "USING CRON 0 * * * * UTC"')
    condition_sql: str = Field(description="A SELECT that the alert evaluates. If it returns rows, the action fires.")
    action_sql: str = Field(description="The action to run when condition_sql returns rows.")
    on_materialize: str = Field(default="resume", description="'resume' = CREATE + ALTER ALERT RESUME; 'create_only'.")
    # ── Asset metadata ──
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    # ── Retry policy ──
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on materialization failure. Defines a RetryPolicy. Useful for transient network failures, Snowflake rate-limits, etc.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

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
        body = f"CREATE OR REPLACE ALERT {self.database}.{self.schema_name}.{self.alert_name}\n"
        body += f"  WAREHOUSE = {self.warehouse}\n"
        body += f"  SCHEDULE = '{self.schedule}'\n"
        body += f"  IF (EXISTS ({self.condition_sql}))\n"
        body += f"  THEN {self.action_sql}"
        return body

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Build retry policy (opt-in via retry_policy_max_retries).
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        kinds = list(self.kinds or []) or ["snowflake"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            retry_policy=_retry_policy,
            key=dg.AssetKey.from_user_string(self.asset_name),
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
                if self.on_materialize == "resume":
                    try:
                        cur.execute(f"ALTER ALERT {self.database}.{self.schema_name}.{self.alert_name} RESUME")
                    except Exception as e:
                        context.log.warning(f"Could not RESUME alert (EXECUTE ALERT privilege required): {e}")
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
        return "Define a Snowflake ALERT (scheduled conditional action). Materialization runs CREATE OR REPLACE ALERT and resumes it."
