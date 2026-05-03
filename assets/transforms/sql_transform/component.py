"""SQL Transform Asset Component.

Warehouse-pushdown SQL transform: takes a SELECT statement plus a destination
table name, runs `CREATE TABLE <dest> AS <SELECT>` directly on the warehouse,
no data round-tripped through Python. Intended for teams who live in
Snowflake / BigQuery / Redshift and want a lightweight pushdown asset
without spinning up a full dbt project.

Connection is via a SQLAlchemy URL env var, so any warehouse with a
SQLAlchemy dialect works:
- Snowflake: `snowflake://user:pwd@account/db/schema?warehouse=...`
- BigQuery:  `bigquery://project-id/dataset` (requires sqlalchemy-bigquery)
- Redshift:  `redshift+psycopg2://user:pwd@host:5439/dbname`
- Postgres:  `postgresql://user:pwd@host:5432/dbname`

For multi-step warehouse transforms, prefer `dagster-dbt`. This component
is for the simple "one CTAS asset" case.
"""
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


class SqlTransformComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a SQL SELECT on the warehouse and materialize the result as a table.

    Example:
        ```yaml
        type: dagster_component_templates.SqlTransformComponent
        attributes:
          asset_name: orders_dedup
          connection_url_env_var: SNOWFLAKE_URL
          destination_table: ANALYTICS.PUBLIC.ORDERS_DEDUP
          sql: |
            SELECT *
            FROM {{ upstream }}
            QUALIFY ROW_NUMBER() OVER (
              PARTITION BY order_id ORDER BY updated_at DESC
            ) = 1
          template_vars:
            upstream: ANALYTICS.PUBLIC.ORDERS_RAW
          if_exists: replace
          upstream_asset_keys:
            - orders_raw
          group_name: warehouse_transforms
        ```

    The SQL field is a SELECT body. The component wraps it as either:
    - replace: DROP TABLE IF EXISTS <dest>; CREATE TABLE <dest> AS <sql>
    - append:  INSERT INTO <dest> <sql>

    `template_vars` does simple `{{ name }}` substitution before execution.
    No Jinja, no dbt — just str.replace. Keeps the dependency surface
    minimal and the substitution behavior obvious.
    """

    asset_name: str = Field(description="Dagster asset name")
    connection_url_env_var: str = Field(
        description=(
            "Env var holding a SQLAlchemy URL. Examples: SNOWFLAKE_URL, "
            "BIGQUERY_URL, REDSHIFT_URL, POSTGRES_URL."
        ),
    )
    destination_table: str = Field(
        description=(
            "Fully-qualified destination table. Format depends on the "
            "warehouse: 'DB.SCHEMA.TABLE' for Snowflake, 'project.dataset.table' "
            "(or backticked) for BigQuery, 'schema.table' for Redshift."
        ),
    )
    sql: str = Field(
        description=(
            "The SELECT body. Will be wrapped in `CREATE TABLE <dest> AS <sql>` "
            "or `INSERT INTO <dest> <sql>` based on if_exists."
        ),
    )
    template_vars: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Optional simple `{{ name }}` substitution map. Applied via "
            "str.replace before execution — no Jinja."
        ),
    )
    if_exists: Literal["replace", "append"] = Field(
        default="replace",
        description=(
            "'replace' issues DROP TABLE IF EXISTS + CREATE TABLE AS. "
            "'append' issues INSERT INTO."
        ),
    )
    upstream_asset_keys: Optional[List[str]] = Field(
        default=None,
        description=(
            "Upstream Dagster asset keys this transform depends on. Typically "
            "external_snowflake_table / external_bigquery_table assets that "
            "represent the source warehouse tables."
        ),
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description=(
            "Asset kinds for the catalog. Auto-inferred from the connection URL "
            "scheme if unset (e.g. 'snowflake', 'bigquery', 'redshift')."
        ),
    )
    group_name: Optional[str] = Field(
        default="warehouse_transforms",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the catalog.",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Key-value tags for the catalog.",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names or email addresses.",
    )
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )


    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Run a SELECT on the warehouse and materialize the result as a table (CTAS pushdown)."

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        # Capture for closure
        asset_name = self.asset_name
        url_env_var = self.connection_url_env_var
        destination_table = self.destination_table
        sql_template = self.sql
        template_vars = self.template_vars or {}
        if_exists = self.if_exists
        upstream_keys = [
            dg.AssetKey.from_user_string(k) for k in (self.upstream_asset_keys or [])
        ]

        # Infer kinds from the env var name as a hint (best-effort).
        kinds = set(self.kinds or [])
        if not kinds:
            ev = url_env_var.lower()
            for hint in ("snowflake", "bigquery", "redshift", "postgres", "mysql", "trino", "duckdb"):
                if hint in ev:
                    kinds.add(hint)
                    break
            kinds.add("sql")

        tags = dict(self.asset_tags or {})
        for k in kinds:
            tags[f"dagster/kind/{k}"] = ""

        # Retry policy
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            name=asset_name,
            group_name=self.group_name or "warehouse_transforms",
            description=self.description or f"SQL pushdown → {destination_table}",
            tags=tags,
            deps=upstream_keys,
            owners=self.owners or [],
            retry_policy=_retry_policy,
        )
        def _sql_transform_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            try:
                import sqlalchemy
            except ImportError as e:
                raise ImportError(
                    "sqlalchemy is required for sql_transform. Install with `pip install sqlalchemy` "
                    "(plus a warehouse-specific driver: snowflake-sqlalchemy, "
                    "sqlalchemy-bigquery, sqlalchemy-redshift, etc.)."
                ) from e

            url = os.environ.get(url_env_var)
            if not url:
                raise ValueError(
                    f"Env var {url_env_var} is not set. Expected a SQLAlchemy "
                    "connection URL."
                )

            # Apply template substitution
            rendered_sql = sql_template
            for k, v in template_vars.items():
                rendered_sql = rendered_sql.replace("{{ " + k + " }}", str(v))
                rendered_sql = rendered_sql.replace("{{" + k + "}}", str(v))

            # Build the DDL
            if if_exists == "replace":
                drop_dml = f"DROP TABLE IF EXISTS {destination_table}"
                ctas_dml = f"CREATE TABLE {destination_table} AS\n{rendered_sql}"
                statements = [drop_dml, ctas_dml]
                op = "CREATE TABLE AS (replace)"
            else:
                statements = [f"INSERT INTO {destination_table}\n{rendered_sql}"]
                op = "INSERT INTO (append)"

            engine = sqlalchemy.create_engine(url)
            try:
                with engine.begin() as conn:
                    for stmt in statements:
                        context.log.info(f"Executing:\n{stmt}\n")
                        conn.execute(sqlalchemy.text(stmt))
                    # Best-effort row count
                    try:
                        result = conn.execute(
                            sqlalchemy.text(f"SELECT COUNT(*) FROM {destination_table}")
                        )
                        rows_written = int(result.scalar() or 0)
                    except Exception:
                        rows_written = -1
            finally:
                engine.dispose()

            context.log.info(
                f"SQL transform complete — {op} on {destination_table} "
                f"({rows_written if rows_written >= 0 else '?'} rows)"
            )

            metadata: Dict[str, Any] = {
                "destination_table": dg.MetadataValue.text(destination_table),
                "operation": dg.MetadataValue.text(op),
                "sql": dg.MetadataValue.md(f"```sql\n{rendered_sql}\n```"),
            }
            if rows_written >= 0:
                metadata["dagster/row_count"] = dg.MetadataValue.int(rows_written)
            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_sql_transform_asset])
