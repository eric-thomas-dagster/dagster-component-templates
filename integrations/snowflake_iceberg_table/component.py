"""SnowflakeIcebergTableComponent — define a Snowflake ICEBERG TABLE as Dagster YAML.

Iceberg tables in Snowflake are first-class: Snowflake reads and writes Apache
Iceberg open-table-format data in cloud object storage (S3 / GCS / Azure),
managed by either Snowflake (catalog='SNOWFLAKE', no external catalog needed)
or an external Iceberg REST catalog (Glue / Polaris / Unity / Tabular).

This component runs `CREATE OR REPLACE ICEBERG TABLE` and treats the result
as a Dagster asset.

Pairs with `snowflake_workspace` (which DISCOVERS existing entities) — use
this when you want to define new entities as code rather than maintain them
separately in Snowflake worksheets.

Reference: https://docs.snowflake.com/en/user-guide/tables-iceberg
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetExecutionContext, Component, ComponentLoadContext, Definitions,
    MetadataValue, Model, Resolvable, asset,
)
from pydantic import Field


class SnowflakeIcebergTableComponent(Component, Model, Resolvable):
    """Define a Snowflake ICEBERG TABLE as Dagster YAML. Materialization runs CREATE OR REPLACE ICEBERG TABLE."""

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
    token: Optional[str] = Field(default=None, description="OAuth/PAT token.")

    # ── Iceberg-specific config ──
    table_name: str = Field(description="Iceberg table name (unquoted identifier).")
    external_volume: str = Field(description="Name of the EXTERNAL VOLUME object that points at the cloud-storage location (S3 / GCS / Azure).")
    catalog: str = Field(default="SNOWFLAKE",
        description="'SNOWFLAKE' for Snowflake-managed Iceberg (default), or the name of a registered external Iceberg catalog (Glue / Polaris / Unity / Tabular).")
    base_location: str = Field(description="Object-storage prefix under the external_volume's root, e.g. 'iceberg/my_table/'.")
    sql: Optional[str] = Field(default=None,
        description="Optional SELECT body that defines the table data. If unset, the table is created empty (you populate it via INSERT INTO / external writes / Spark).")
    columns: Optional[List[str]] = Field(default=None,
        description="Optional explicit column declarations when sql is None, e.g. ['id INT', 'name STRING', 'created_at TIMESTAMP'].")
    cluster_by: Optional[List[str]] = Field(default=None, description="Columns to CLUSTER BY for performance.")

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
        description="Max retries on materialization failure. Defines a RetryPolicy. Useful for transient network failures or Snowflake rate-limits.",
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
        ck = dict(account=self.account, user=self.user,
                  database=self.database, schema=self.schema_name)
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
        head = f"CREATE OR REPLACE ICEBERG TABLE {self.database}.{self.schema_name}.{self.table_name}"
        cols = ""
        if self.columns and not self.sql:
            cols = " (" + ", ".join(self.columns) + ")"
        body = head + cols + "\n"
        body += f"  EXTERNAL_VOLUME = '{self.external_volume}'\n"
        body += f"  CATALOG = '{self.catalog}'\n"
        body += f"  BASE_LOCATION = '{self.base_location}'\n"
        if self.cluster_by:
            body += f"  CLUSTER BY ({', '.join(self.cluster_by)})\n"
        if self.sql:
            body += f"AS\n  {self.sql}"
        return body

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        kinds = list(self.kinds or []) or ["snowflake", "iceberg"]
        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        @asset(
            retry_policy=_retry_policy,
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
            row_count = 0
            try:
                cur = conn.cursor()
                cur.execute(ddl)
                # Best-effort row count of the new table.
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {self.database}.{self.schema_name}.{self.table_name}")
                    row = cur.fetchone()
                    if row: row_count = int(row[0])
                except Exception as e:
                    context.log.warning(f"Could not COUNT(*) new iceberg table: {e}")
                cur.close()
            finally:
                conn.close()
            return dg.MaterializeResult(metadata={
                "snowflake/ddl": MetadataValue.md(f"```sql\n{ddl}\n```"),
                "snowflake/database": MetadataValue.text(self.database),
                "snowflake/schema": MetadataValue.text(self.schema_name),
                "snowflake/iceberg_table": MetadataValue.text(self.table_name),
                "snowflake/external_volume": MetadataValue.text(self.external_volume),
                "snowflake/catalog": MetadataValue.text(self.catalog),
                "snowflake/base_location": MetadataValue.text(self.base_location),
                "snowflake/row_count": MetadataValue.int(row_count),
            })

        return Definitions(assets=[_entity_asset])

    @classmethod
    def get_description(cls) -> str:
        return "Define a Snowflake ICEBERG TABLE as Dagster YAML. Materialization runs CREATE OR REPLACE ICEBERG TABLE."
