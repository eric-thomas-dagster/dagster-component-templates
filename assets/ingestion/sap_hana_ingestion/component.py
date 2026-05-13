"""SAP HANA Ingestion Component.

Pull data from SAP HANA (Cloud, on-premise, or HANA-on-Azure preview) into a
Dagster asset as a pandas DataFrame. Companion to `sap_hana_resource`, which
only registers the connection — this component runs an actual SQL query.

Pattern:
- Provide a SQL `query` OR a `table_name` (+ optional `schema_name`)
- Connection comes from env vars (host/port/user/password) OR a `connection_url_env_var`
- Returns a pandas DataFrame; downstream components (`summarize`, `sort`,
  `dataframe_to_*`) can transform/persist as needed.

Partitioning: set `partition_type: daily` + `partition_start: YYYY-MM-DD`
and reference `{partition_key}` inside the query — it's substituted at run
time. Useful for HANA Calculation Views with date filters.
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
):
    """Build a Dagster partitions_def from the flat fields."""
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None

    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [
            v.strip()
            for v in (str(partition_values) if partition_values else "").split(",")
            if v.strip()
        ]

    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date)."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _build_hana_url(
    host: str,
    port: int,
    user: str,
    password: str,
    database: Optional[str],
    encrypt: bool,
    validate_certificate: bool,
) -> str:
    import urllib.parse

    pwd_enc = urllib.parse.quote(password, safe="")
    params = []
    if encrypt:
        params.append("encrypt=true")
    if not validate_certificate:
        params.append("sslValidateCertificate=false")
    if database:
        params.append(f"databaseName={database}")
    qs = ("?" + "&".join(params)) if params else ""
    return f"hana://{user}:{pwd_enc}@{host}:{port}{qs}"


class SapHanaIngestionComponent(Component, Model, Resolvable):
    """Query SAP HANA and materialize the result as a Dagster asset.

    Connection options (use ONE):
    - `connection_url_env_var`: env var holding the full SQLAlchemy URL
      (e.g. `hana://user:pwd@host:443?encrypt=true`)
    - Individual fields: `host` + `port` + `user` + `password_env_var`

    Source options (use ONE):
    - `query`: arbitrary SQL (supports `{partition_key}` substitution)
    - `table_name` (+ optional `schema_name`): generates `SELECT * FROM ...`

    Example:

        ```yaml
        type: dagster_component_templates.SapHanaIngestionComponent
        attributes:
          asset_name: customer_metrics
          host: myhana.hanacloud.ondemand.com
          port: 443
          user: DAGSTER_RO
          password_env_var: HANA_PASSWORD
          query: |
            SELECT CUSTOMER_ID, REGION, REVENUE
            FROM SAPABAP1.CUSTOMER_METRICS
            WHERE BUSINESS_DATE = '{partition_key}'
          partition_type: daily
          partition_start: '2024-01-01'
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    # --- Connection: either a URL or individual fields ------------------------

    connection_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var holding a full SQLAlchemy HANA URL "
            "(e.g. `hana://user:pwd@host:443?encrypt=true`). "
            "If unset, uses the individual host/port/user fields."
        ),
    )

    host: Optional[str] = Field(
        default=None,
        description="HANA host (e.g. 'myhana.hanacloud.ondemand.com'). Required if connection_url_env_var unset.",
    )
    port: int = Field(default=443, description="HANA port (443 = Cloud, 30015 = on-prem default)")
    database: Optional[str] = Field(
        default=None, description="Tenant DB name (multi-tenant only). HANA Cloud: leave empty."
    )
    user: Optional[str] = Field(
        default=None,
        description="HANA user. Required if connection_url_env_var unset.",
    )
    password_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the user's password. Required if connection_url_env_var unset.",
    )
    encrypt: bool = Field(default=True, description="TLS — required for HANA Cloud")
    validate_certificate: bool = Field(default=True, description="Verify the server certificate")

    # --- Source --------------------------------------------------------------

    query: Optional[str] = Field(
        default=None,
        description=(
            "SQL query to execute. Supports `{partition_key}` template substitution when a "
            "partition is set. Use this OR `table_name`."
        ),
    )

    table_name: Optional[str] = Field(
        default=None,
        description="Table or view name. Generates `SELECT * FROM <schema>.<table>`. Use this OR `query`.",
    )

    schema_name: Optional[str] = Field(
        default=None,
        description="Schema for `table_name` (e.g. 'SAPABAP1', 'SYS_BIC'). Optional.",
    )

    # --- Standard fields ------------------------------------------------------

    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default="sap_hana", description="Asset group")
    owners: Optional[List[str]] = Field(
        default=None, description="Asset owners (team or email list)"
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None, description="Additional key/value tags"
    )
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds. Defaults to ['hana', 'sql']."
    )
    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)
    include_preview_metadata: bool = Field(default=True)
    preview_rows: int = Field(default=25, ge=1, le=500)
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys")

    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | None",
    )
    partition_start: Optional[str] = Field(
        default=None, description="ISO start date for time-based partitions"
    )
    partition_values: Optional[str] = Field(
        default=None, description="Comma-separated values for static partitions"
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None, description="Name for DynamicPartitionsDefinition"
    )

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def _resolve_url(self) -> str:
        if self.connection_url_env_var:
            url = os.environ.get(self.connection_url_env_var)
            if not url:
                raise ValueError(
                    f"connection_url_env_var={self.connection_url_env_var!r} is set but empty."
                )
            return url
        if not (self.host and self.user and self.password_env_var):
            raise ValueError(
                "Either connection_url_env_var OR (host + user + password_env_var) must be set."
            )
        password = os.environ.get(self.password_env_var)
        if not password:
            raise ValueError(f"password_env_var={self.password_env_var!r} is set but empty.")
        return _build_hana_url(
            self.host,
            self.port,
            self.user,
            password,
            self.database,
            self.encrypt,
            self.validate_certificate,
        )

    def _resolve_query(self, partition_key: Optional[str]) -> str:
        if self.query and self.table_name:
            raise ValueError("Set query OR table_name, not both.")
        if self.query:
            q = self.query
        elif self.table_name:
            target = (
                f"{self.schema_name}.{self.table_name}"
                if self.schema_name
                else self.table_name
            )
            q = f"SELECT * FROM {target}"
        else:
            raise ValueError("One of `query` or `table_name` is required.")
        if partition_key is not None and "{partition_key}" in q:
            q = q.replace("{partition_key}", partition_key)
        return q

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        description = self.description or f"SAP HANA ingestion ({asset_name})"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        component = self

        kinds = list(self.kinds or []) or ["hana", "sql"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            None,
            self.dynamic_partition_name,
        )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            name=asset_name,
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            freshness_policy=freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
        )
        def sap_hana_ingestion_asset(context: AssetExecutionContext):
            import sqlalchemy

            partition_key = (
                context.partition_key if context.has_partition_key else None
            )
            url = component._resolve_url()
            query = component._resolve_query(partition_key)

            context.log.info(
                f"SAP HANA query: {query[:200]}{'...' if len(query) > 200 else ''}"
            )
            engine = sqlalchemy.create_engine(url)
            try:
                df = pd.read_sql(query, engine)
            finally:
                engine.dispose()

            context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "column_count": MetadataValue.int(len(df.columns)),
                "columns": MetadataValue.json(list(df.columns)),
                "query": MetadataValue.md(f"```sql\n{query}\n```"),
            }
            if partition_key:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            if include_preview and len(df) > 0:
                try:
                    sample = (
                        df.sample(min(preview_rows, len(df)))
                        if len(df) > preview_rows * 10
                        else df.head(preview_rows)
                    )
                    metadata["preview"] = MetadataValue.md(sample.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[sap_hana_ingestion_asset])
