"""SnowflakeTaskComponent — define a Snowflake TASK as Dagster YAML.

Define a Snowflake TASK as Dagster YAML. Materialization runs CREATE OR REPLACE TASK and (optionally) EXECUTE TASK.

Pairs with `snowflake_workspace` (which DISCOVERS existing entities) — use
this when you want to define new entities as code rather than maintain
them separately in Snowflake worksheets.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext, Component, ComponentLoadContext,
    DailyPartitionsDefinition, Definitions, DynamicPartitionsDefinition,
    HourlyPartitionsDefinition, MetadataValue, Model, MonthlyPartitionsDefinition,
    MultiPartitionsDefinition, Resolvable, StaticPartitionsDefinition,
    WeeklyPartitionsDefinition, asset,
)
from pydantic import Field


def _build_partitions_def(partition_type, partition_start, partition_values, dynamic_partition_name):
    """Shared partitions-def factory matching dataframe_to_snowflake's shape."""
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if not partition_type: return None
    if partition_type in ("daily","weekly","monthly","hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_values + partition_start.")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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

    # ── Partitions ──
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'dynamic', 'multi', or None. Partition key is substituted into the task SQL via `<<partition_key>>` placeholders.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    )

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
        # Build retry policy (opt-in via retry_policy_max_retries).
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
        )
        kinds = list(self.kinds or []) or ["snowflake"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            retry_policy=_retry_policy,
            partitions_def=partitions_def,
            name=self.asset_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=tags,
            group_name=self.group_name,
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
            kinds=set(kinds),
        )
        def _entity_asset(context: AssetExecutionContext) -> dg.MaterializeResult:
            # Substitute <<partition_key>> in the task SQL body when partitioned.
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                original_sql = self.sql
                object.__setattr__(self, "sql", original_sql.replace("<<partition_key>>", str(partition_key)))
                try:
                    ddl = self._build_ddl()
                finally:
                    object.__setattr__(self, "sql", original_sql)
            else:
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
