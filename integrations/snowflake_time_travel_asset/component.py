"""SnowflakeTimeTravelAssetComponent — query an existing Snowflake table
as-of a past point in time (Time Travel).

Snowflake's Time Travel lets you SELECT against a table at:

  - an `OFFSET` (seconds back from now: `AT(OFFSET => -3600)` = 1 hour ago)
  - a `TIMESTAMP` (specific moment: `AT(TIMESTAMP => '2025-05-19 10:00:00')`)
  - `BEFORE STATEMENT => '<query_id>'` (just before a specific DML statement)

This component runs ONE of those three forms against a target table and
materializes the result as a Dagster asset. Useful for:

  - Auditing what a table looked like before an incident (point-in-time
    snapshot as a first-class Dagster asset, queryable downstream)
  - Reproducing pipeline output from a specific historical state
  - Catalog-side rollback story for analytics: "yesterday's marketing
    rollup, recomputed from yesterday's RAW.ORDERS"

The asset value is a pandas DataFrame.

Reference:
  https://docs.snowflake.com/en/user-guide/data-time-travel
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from dagster import (
    AssetExecutionContext, Component, ComponentLoadContext, Definitions,
    MetadataValue, Model, Resolvable, asset,
)
from pydantic import Field


class SnowflakeTimeTravelAssetComponent(Component, Model, Resolvable):
    """Query a Snowflake table AT a past point in time (Time Travel) and materialize the result as a Dagster asset."""

    asset_name: str = Field(description="Output Dagster asset name")

    # ── Connection ──
    account: str = Field(description="Snowflake account, e.g. xy12345-abc.")
    user: str = Field(description="Snowflake user (NAME for keypair, LOGIN_NAME for password/SSO/PAT).")
    warehouse: Optional[str] = Field(default=None)
    database: str = Field(description="Database containing the source table.")
    schema_name: str = Field(description="Schema containing the source table.")
    role: Optional[str] = Field(default=None)

    # ── Auth ──
    password: Optional[str] = Field(default=None)
    authenticator: Optional[str] = Field(default=None,
        description="Snowflake authenticator: 'SNOWFLAKE_JWT', 'externalbrowser', 'PROGRAMMATIC_ACCESS_TOKEN', 'oauth'.")
    private_key_file: Optional[str] = Field(default=None)
    private_key_file_pwd: Optional[str] = Field(default=None)
    token: Optional[str] = Field(default=None)

    # ── Time travel target ──
    source_table: str = Field(description="Table to time-travel against (unqualified; resolved against database.schema_name).")
    columns: Optional[List[Union[str, int]]] = Field(default=None,
        description="Columns to SELECT (defaults to *). E.g. ['ID', 'NAME', 'AMOUNT'].")
    where_clause: Optional[str] = Field(default=None,
        description="Optional WHERE predicate (without the WHERE keyword). E.g. 'AMOUNT > 100'.")
    row_limit: Optional[int] = Field(default=None,
        description="Optional LIMIT N on the result.")

    # Pick exactly ONE of these three time-travel modes:
    time_travel_offset_seconds: Optional[int] = Field(default=None,
        description="Seconds back from now (NEGATIVE int, e.g. -3600 = 1 hour ago). Maps to `AT(OFFSET => <n>)`.")
    time_travel_timestamp: Optional[str] = Field(default=None,
        description="Specific timestamp to time-travel to, e.g. '2025-05-19 10:00:00'. Maps to `AT(TIMESTAMP => '...'::TIMESTAMP)`.")
    time_travel_statement_id: Optional[str] = Field(default=None,
        description="Query ID to time-travel BEFORE. Maps to `BEFORE(STATEMENT => '<query_id>')`.")

    # ── Asset metadata ──
    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    asset_tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    # ── Retry policy ──
    retry_policy_max_retries: Optional[int] = Field(default=None,
        description="Max retries on query failure.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

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

    def _build_sql(self) -> str:
        # Validate exactly-one time-travel mode set.
        modes = [
            self.time_travel_offset_seconds is not None,
            self.time_travel_timestamp is not None,
            self.time_travel_statement_id is not None,
        ]
        if sum(1 for m in modes if m) != 1:
            raise ValueError(
                "Set exactly one of: time_travel_offset_seconds, "
                "time_travel_timestamp, time_travel_statement_id."
            )
        if self.time_travel_offset_seconds is not None:
            tt_clause = f"AT(OFFSET => {self.time_travel_offset_seconds})"
        elif self.time_travel_timestamp is not None:
            tt_clause = f"AT(TIMESTAMP => '{self.time_travel_timestamp}'::TIMESTAMP)"
        else:
            tt_clause = f"BEFORE(STATEMENT => '{self.time_travel_statement_id}')"
        cols = ", ".join(self.columns) if self.columns else "*"
        sql = f"SELECT {cols} FROM {self.database}.{self.schema_name}.{self.source_table} {tt_clause}"
        if self.where_clause:
            sql += f" WHERE {self.where_clause}"
        if self.row_limit:
            sql += f" LIMIT {self.row_limit}"
        return sql

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        kinds = list(self.kinds or []) or ["snowflake", "time_travel"]
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
        def _tt_asset(context: AssetExecutionContext):
            import pandas as pd
            sql = self._build_sql()
            context.log.info(f"Time-travel SELECT:\n{sql}")
            conn = self._connect()
            df: Any = None
            try:
                df = pd.read_sql(sql, conn)
            finally:
                conn.close()
            row_count = len(df) if df is not None else 0
            return dg.MaterializeResult(
                value=df,
                metadata={
                    "snowflake/sql": MetadataValue.md(f"```sql\n{sql}\n```"),
                    "snowflake/source_table": MetadataValue.text(
                        f"{self.database}.{self.schema_name}.{self.source_table}"
                    ),
                    "snowflake/row_count": MetadataValue.int(row_count),
                    "snowflake/preview": MetadataValue.md(
                        (df.head(10).to_markdown(index=False) if row_count else "(no rows)")
                    ) if df is not None else MetadataValue.text("(query returned no result)"),
                },
            )

        return Definitions(assets=[_tt_asset])

    @classmethod
    def get_description(cls) -> str:
        return "Query a Snowflake table AT a past point in time (Time Travel) and materialize the result as a Dagster asset."
