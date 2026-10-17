"""DLT DataFrame Writer Component."""

from typing import Any, Dict, List, Literal, Optional, Union
from typing import Literal
import pandas as pd
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
    asset,
)
from pydantic import AliasChoices, Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
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
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class DLTDataFrameWriterComponent(Component, Model, Resolvable):
    """
    Component for writing pandas DataFrames to any database using dlt (data load tool).

    This component reads data from an upstream asset (as a DataFrame) and writes
    it to any dlt-supported destination. Perfect for persisting DataFrames to
    production databases with automatic schema evolution and credential management.

    Features:
    - Support for 14 database destinations (Snowflake, BigQuery, Postgres, etc.)
    - Automatic schema evolution and table management
    - Environment-aware routing (local/branch/production)
    - Three write modes: replace, append, merge
    - Structured configuration with conditional fields
    """

    asset_name: str = Field(description="Name of the asset to create")

    table: str = Field(
        description="Name of the table to write data to in the destination",
        validation_alias=AliasChoices("table", "table_name"),
    )

    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description="Write mode: 'replace' (drop and recreate), 'append' (add rows), 'merge' (upsert based on primary key)"
    )

    primary_key: Optional[str] = Field(
        default=None,
        description="Primary key column(s) for merge mode (comma-separated if multiple)"
    )

    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )

    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in ONE run: the upstream frame "
            "(loaded once, spanning the whole range) is filtered to every partition "
            "key in the range via partition_date_column/partition_static_column and "
            "written together. 'multi_run' launches one run per partition (optionally "
            "capped via backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. Requires a partition-range-aware IO manager "
            "for the upstream asset (e.g. a DB-table IO manager with partition_expr "
            "set) -- a generic filesystem IO manager cannot persist/load a single_run "
            "output spanning multiple partitions and raises a clear error at "
            "materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )

    upstream_asset_keys: Optional[Union[str, List[str]]] = Field(
        default=None,
        description='Comma-separated list of upstream asset keys to load DataFrames from (automatically set by custom lineage)'
    )

    # Destination configuration
    destination: Optional[str] = Field(
        default=None,
        description="Database destination for persisting data (leave empty for in-memory DuckDB)"
    )

    # Snowflake fields
    snowflake_account: Optional[str] = Field(default=None, description="Snowflake account identifier")
    snowflake_database: Optional[str] = Field(default=None, description="Snowflake database name")
    snowflake_schema: Optional[str] = Field(default="public", description="Snowflake schema name")
    snowflake_warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse name")
    snowflake_username: Optional[str] = Field(default=None, description="Snowflake username")
    snowflake_password: Optional[str] = Field(default=None, description="Snowflake password")
    snowflake_role: Optional[str] = Field(default=None, description="Snowflake role (optional)")

    # BigQuery fields
    bigquery_project_id: Optional[str] = Field(default=None, description="Google Cloud project ID")
    bigquery_dataset: Optional[str] = Field(default=None, description="BigQuery dataset name")
    bigquery_credentials_path: Optional[str] = Field(default=None, description="Path to service account JSON")
    bigquery_location: Optional[str] = Field(default="US", description="BigQuery dataset location")

    # Postgres fields
    postgres_host: Optional[str] = Field(default="localhost", description="PostgreSQL host")
    postgres_port: Optional[int] = Field(default=5432, description="PostgreSQL port")
    postgres_database: Optional[str] = Field(default=None, description="PostgreSQL database name")
    postgres_username: Optional[str] = Field(default=None, description="PostgreSQL username")
    postgres_password: Optional[str] = Field(default=None, description="PostgreSQL password")
    postgres_schema: Optional[str] = Field(default="public", description="PostgreSQL schema")

    # Redshift fields
    redshift_host: Optional[str] = Field(default=None, description="Redshift cluster endpoint")
    redshift_port: Optional[int] = Field(default=5439, description="Redshift port")
    redshift_database: Optional[str] = Field(default=None, description="Redshift database name")
    redshift_username: Optional[str] = Field(default=None, description="Redshift username")
    redshift_password: Optional[str] = Field(default=None, description="Redshift password")
    redshift_schema: Optional[str] = Field(default="public", description="Redshift schema")

    # DuckDB fields
    duckdb_database_path: Optional[str] = Field(default=None, description="Path to DuckDB file")

    # MotherDuck fields
    motherduck_database: Optional[str] = Field(default=None, description="MotherDuck database name")
    motherduck_token: Optional[str] = Field(default=None, description="MotherDuck authentication token")

    # Databricks fields
    databricks_server_hostname: Optional[str] = Field(default=None, description="Databricks workspace hostname")
    databricks_http_path: Optional[str] = Field(default=None, description="SQL warehouse HTTP path")
    databricks_access_token: Optional[str] = Field(default=None, description="Databricks personal access token")
    databricks_catalog: Optional[str] = Field(default=None, description="Unity Catalog name")
    databricks_schema: Optional[str] = Field(default="default", description="Databricks schema name")

    # ClickHouse fields
    clickhouse_host: Optional[str] = Field(default="localhost", description="ClickHouse server host")
    clickhouse_port: Optional[int] = Field(default=9000, description="ClickHouse server port")
    clickhouse_database: Optional[str] = Field(default="default", description="ClickHouse database name")
    clickhouse_username: Optional[str] = Field(default=None, description="ClickHouse username")
    clickhouse_password: Optional[str] = Field(default=None, description="ClickHouse password")

    # MSSQL fields
    mssql_host: Optional[str] = Field(default="localhost", description="SQL Server host")
    mssql_port: Optional[int] = Field(default=1433, description="SQL Server port")
    mssql_database: Optional[str] = Field(default=None, description="SQL Server database name")
    mssql_username: Optional[str] = Field(default=None, description="SQL Server username")
    mssql_password: Optional[str] = Field(default=None, description="SQL Server password")

    # Athena fields
    athena_query_result_bucket: Optional[str] = Field(default=None, description="S3 bucket for query results")
    athena_database: Optional[str] = Field(default="default", description="Athena database name")
    athena_aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    athena_aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")
    athena_region: Optional[str] = Field(default="us-east-1", description="AWS region")

    # MySQL fields
    mysql_host: Optional[str] = Field(default="localhost", description="MySQL server host")
    mysql_port: Optional[int] = Field(default=3306, description="MySQL server port")
    mysql_database: Optional[str] = Field(default=None, description="MySQL database name")
    mysql_username: Optional[str] = Field(default=None, description="MySQL username")
    mysql_password: Optional[str] = Field(default=None, description="MySQL password")

    # Filesystem fields
    filesystem_bucket_path: Optional[str] = Field(default=None, description="Local or S3 path")
    filesystem_format: Optional[str] = Field(default="parquet", description="Output file format")

    # Synapse fields
    synapse_host: Optional[str] = Field(default=None, description="Azure Synapse workspace hostname")
    synapse_database: Optional[str] = Field(default=None, description="Synapse database name")
    synapse_username: Optional[str] = Field(default=None, description="Synapse username")
    synapse_password: Optional[str] = Field(default=None, description="Synapse password")

    # Environment-aware routing
    use_environment_routing: bool = Field(
        default=False,
        description="Automatically select destination based on Dagster deployment environment"
    )
    destination_local: Optional[str] = Field(default=None, description="Destination for local development")
    destination_branch: Optional[str] = Field(default=None, description="Destination for branch deployments")
    destination_prod: Optional[str] = Field(default=None, description="Destination for production")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="multi", optional=True)

    def _get_effective_destination(self) -> Optional[str]:
        """Get destination based on environment routing if enabled."""
        import os

        if not self.use_environment_routing:
            return self.destination

        # Check Dagster Cloud environment variables
        is_branch = os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "").lower() == "true"
        deployment_name = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "")

        # Determine which destination to use
        if is_branch and self.destination_branch:
            return self.destination_branch
        elif deployment_name and not is_branch and self.destination_prod:
            # In Dagster Cloud but not a branch deployment = production
            return self.destination_prod
        elif not deployment_name and self.destination_local:
            # Not in Dagster Cloud = local development
            return self.destination_local
        else:
            # Fallback to main destination field
            return self.destination

    def _build_destination_config(self) -> dict:
        """Build dlt destination config from structured fields."""
        if not self.destination:
            return {}

        if self.destination == "snowflake":
            return {
                "credentials": {
                    "database": self.snowflake_database,
                    "username": self.snowflake_username,
                    "password": self.snowflake_password,
                    "host": self.snowflake_account,
                    "warehouse": self.snowflake_warehouse,
                    "role": self.snowflake_role if self.snowflake_role else None,
                }
            }
        elif self.destination == "bigquery":
            config = {
                "project_id": self.bigquery_project_id,
                "dataset": self.bigquery_dataset,
            }
            if self.bigquery_credentials_path:
                config["credentials"] = self.bigquery_credentials_path
            if self.bigquery_location:
                config["location"] = self.bigquery_location
            return config
        elif self.destination == "postgres":
            return {
                "credentials": {
                    "database": self.postgres_database,
                    "username": self.postgres_username,
                    "password": self.postgres_password,
                    "host": self.postgres_host,
                    "port": self.postgres_port,
                }
            }
        elif self.destination == "redshift":
            return {
                "credentials": {
                    "database": self.redshift_database,
                    "username": self.redshift_username,
                    "password": self.redshift_password,
                    "host": self.redshift_host,
                    "port": self.redshift_port,
                }
            }
        elif self.destination == "duckdb":
            return {
                "credentials": self.duckdb_database_path if self.duckdb_database_path else ":memory:"
            }
        elif self.destination == "motherduck":
            return {
                "credentials": {
                    "database": self.motherduck_database,
                    "token": self.motherduck_token,
                }
            }
        elif self.destination == "databricks":
            config = {
                "credentials": {
                    "server_hostname": self.databricks_server_hostname,
                    "http_path": self.databricks_http_path,
                    "access_token": self.databricks_access_token,
                }
            }
            if self.databricks_catalog:
                config["credentials"]["catalog"] = self.databricks_catalog
            if self.databricks_schema:
                config["credentials"]["schema"] = self.databricks_schema
            return config
        elif self.destination == "clickhouse":
            return {
                "credentials": {
                    "database": self.clickhouse_database,
                    "username": self.clickhouse_username,
                    "password": self.clickhouse_password,
                    "host": self.clickhouse_host,
                    "port": self.clickhouse_port,
                }
            }
        elif self.destination == "mssql":
            return {
                "credentials": {
                    "database": self.mssql_database,
                    "username": self.mssql_username,
                    "password": self.mssql_password,
                    "host": self.mssql_host,
                    "port": self.mssql_port,
                }
            }
        elif self.destination == "athena":
            return {
                "credentials": {
                    "query_result_bucket": self.athena_query_result_bucket,
                    "database": self.athena_database,
                    "aws_access_key_id": self.athena_aws_access_key_id,
                    "aws_secret_access_key": self.athena_aws_secret_access_key,
                    "region_name": self.athena_region,
                }
            }
        elif self.destination == "mysql":
            return {
                "credentials": {
                    "database": self.mysql_database,
                    "username": self.mysql_username,
                    "password": self.mysql_password,
                    "host": self.mysql_host,
                    "port": self.mysql_port,
                }
            }
        elif self.destination == "filesystem":
            config = {
                "bucket_url": self.filesystem_bucket_path if self.filesystem_bucket_path else "/tmp/dlt_data",
            }
            if self.filesystem_format:
                config["format"] = self.filesystem_format
            return config
        elif self.destination == "synapse":
            return {
                "credentials": {
                    "database": self.synapse_database,
                    "username": self.synapse_username,
                    "password": self.synapse_password,
                    "host": self.synapse_host,
                }
            }
        else:
            return {}

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the DLT DataFrame writer."""
        _column_lineage_md = _column_lineage_metadata(self)
        import dlt

        # Capture fields for closure
        asset_name = self.asset_name
        table_name = self.table
        write_disposition = self.write_disposition
        primary_key = self.primary_key
        destination = self.destination
        description = self.description or f"Write DataFrame to {destination or 'DuckDB'} table {table_name}"
        group_name = self.group_name or None
        upstream_asset_keys_str = self.upstream_asset_keys

        # Parse upstream asset keys if provided
        upstream_keys = []
        if upstream_asset_keys_str:
            # The shape normalizer hands us a list; older configs may still be a comma-separated str.
            _raw_keys = upstream_asset_keys_str.split(',') if isinstance(upstream_asset_keys_str, str) else upstream_asset_keys_str
            upstream_keys = [k.strip() for k in _raw_keys if k and k.strip()]

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "dlt_dataframe_writer"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(retry_policy=_retry_policy, backfill_policy=_backfill_policy_obj,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def dlt_writer_asset(context: AssetExecutionContext, **kwargs) -> None:
            """Write DataFrame to destination using dlt."""

            # Determine destination (with environment routing if enabled)
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            effective_destination = self._get_effective_destination()
            use_destination = effective_destination if effective_destination else "duckdb"
            destination_config = self._build_destination_config() if effective_destination else {}

            context.log.info(f"Using destination: {use_destination}")

            # Load upstream assets based on configuration
            upstream_assets = {}

            # If upstream_asset_keys is configured, try to load assets explicitly
            if upstream_keys and hasattr(context, 'load_asset_value'):
                # Real execution context - load assets explicitly
                context.log.info(f"Loading {len(upstream_keys)} upstream asset(s) via context.load_asset_value()")
                for key in upstream_keys:
                    try:
                        # Convert string key to AssetKey if needed
                        asset_key = AssetKey(key) if isinstance(key, str) else key
                        value = context.load_asset_value(asset_key)
                        upstream_assets[key] = value
                        context.log.info(f"  - Loaded '{key}': {type(value).__name__}")
                    except Exception as e:
                        context.log.error(f"  - Failed to load '{key}': {e}")
                        raise
            else:
                # Preview/mock context or no upstream_keys - fall back to kwargs
                upstream_assets = {k: v for k, v in kwargs.items()}

            if not upstream_assets:
                raise ValueError(
                    f"DLT DataFrame Writer '{asset_name}' requires at least one upstream asset "
                    "that produces a DataFrame. Connect an upstream asset using the custom lineage UI."
                )

            # Get the first (and should be only) upstream DataFrame
            df = list(upstream_assets.values())[0]

            # Filter to current partition if partitioned
            # A single_run/multi_run backfill range spans several partition keys:
            # keep rows matching ANY key in the range (one key on a normal run).
            if context.has_partition_key or context.has_partition_key_range:
                import numpy as _np
                _keys = [context.partition_key] if context.has_partition_key else list(context.partition_keys)
                _keep = _np.zeros(len(df), dtype=bool)
                for _pk in _keys:
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    _m = _np.ones(len(df), dtype=bool)
                    if partition_date_column and partition_date_column in df.columns and _date_key:
                        _col_dates = pd.to_datetime(df[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        _m &= (_col_dates == _date_key).to_numpy()
                    if partition_static_column and partition_static_column in df.columns and _static_key:
                        _m &= (df[partition_static_column].astype(str) == _static_key).to_numpy()
                    elif partition_static_column and partition_static_column in df.columns and not _is_multi:
                        _m &= (df[partition_static_column].astype(str) == str(_pk)).to_numpy()
                    _keep |= _m
                df = df[_keep]

            if not isinstance(df, pd.DataFrame):
                raise ValueError(
                    f"Expected DataFrame from upstream asset, got {type(df)}"
                )

            context.log.info(
                f"Writing {len(df)} rows to {use_destination} table '{table_name}' "
                f"with {write_disposition} mode"
            )

            # Create pipeline (in-memory DuckDB or specified destination)
            pipeline_kwargs = {
                "pipeline_name": f"{asset_name}_pipeline",
                "destination": use_destination,
                "dataset_name": asset_name
            }

            # Credentials are NOT a dlt.pipeline() argument (that raises TypeError); they belong on the
            # destination factory: dlt.destinations.<name>(credentials=...).
            if destination_config and "credentials" in destination_config:
                _dest_kwargs = {"credentials": destination_config["credentials"]}
                if use_destination == "bigquery" and "location" in destination_config:
                    _dest_kwargs["location"] = destination_config["location"]
                pipeline_kwargs["destination"] = getattr(dlt.destinations, use_destination)(**_dest_kwargs)

            pipeline = dlt.pipeline(**pipeline_kwargs)

            # Prepare run kwargs
            run_kwargs = {
                "table_name": table_name,
                "write_disposition": write_disposition,
            }

            # Add primary key if merge mode
            if write_disposition == "merge":
                if not primary_key:
                    raise ValueError(
                        f"primary_key is required when write_disposition is 'merge'"
                    )
                run_kwargs["primary_key"] = [k.strip() for k in primary_key.split(',')]

            # Run the pipeline with the DataFrame
            info = pipeline.run(df, **run_kwargs)

            context.log.info(
                f"Successfully wrote {len(df)} rows to {use_destination}.{table_name}"
            )
            context.log.info(f"Pipeline info: {info}")

        return Definitions(assets=[dlt_writer_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
