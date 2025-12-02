"""DLT DataFrame Writer Component."""

from typing import Optional, Literal
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
from pydantic import Field


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

    table_name: str = Field(
        description="Name of the table to write data to in the destination"
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

    upstream_asset_keys: Optional[str] = Field(
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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the DLT DataFrame writer."""
        import dlt

        # Capture fields for closure
        asset_name = self.asset_name
        table_name = self.table_name
        write_disposition = self.write_disposition
        primary_key = self.primary_key
        destination = self.destination
        description = self.description or f"Write DataFrame to {destination or 'DuckDB'} table {table_name}"
        group_name = self.group_name or None
        upstream_asset_keys_str = self.upstream_asset_keys

        # Parse upstream asset keys if provided
        upstream_keys = []
        if upstream_asset_keys_str:
            upstream_keys = [k.strip() for k in upstream_asset_keys_str.split(',')]

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def dlt_writer_asset(context: AssetExecutionContext, **kwargs) -> None:
            """Write DataFrame to destination using dlt."""

            # Determine destination (with environment routing if enabled)
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

            # Add credentials if destination is configured
            if destination_config:
                if "credentials" in destination_config:
                    pipeline_kwargs["credentials"] = destination_config["credentials"]
                # For BigQuery, project_id goes at root level
                if use_destination == "bigquery" and "project_id" in destination_config:
                    pipeline_kwargs["project_id"] = destination_config["project_id"]
                    if "location" in destination_config:
                        pipeline_kwargs["location"] = destination_config["location"]

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
