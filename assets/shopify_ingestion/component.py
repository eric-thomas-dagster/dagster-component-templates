"""Shopify Ingestion Component.

Ingest Shopify e-commerce data using dlt (data load tool).
Extracts customers, orders, and products.
"""

from typing import Optional, List
import pandas as pd
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field
import dlt


class ShopifyIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Shopify e-commerce data using dlt.

    Shopify is a leading e-commerce platform. This component extracts data from
    Shopify's Admin API and returns it as a pandas DataFrame for downstream analysis.

    Available data resources:
    - customers: Individual customers and their contact information
    - orders: Transactions and order details
    - products: Product catalog with prices and variants

    The component uses dlt's verified Shopify source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.ShopifyIngestionComponent
        attributes:
          asset_name: shopify_ecommerce_data
          shop_url: "https://my-shop.myshopify.com"
          private_app_password: "{{ env('SHOPIFY_ADMIN_API_TOKEN') }}"
          resources:
            - customers
            - orders
            - products
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    shop_url: str = Field(
        description="Shopify store URL (e.g., https://my-shop.myshopify.com)"
    )

    private_app_password: str = Field(
        description="Shopify Admin API access token"
    )

    resources: List[str] = Field(
        default=["customers", "orders", "products"],
        description="Shopify resources to extract (customers, orders, products)"
    )

    start_date: Optional[str] = Field(
        default="2000-01-01",
        description="Start date for incremental loading (YYYY-MM-DD)"
    )

    order_status: Optional[str] = Field(
        default="any",
        description="Filter orders by status (open, closed, cancelled, any)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="shopify",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    # Database destination fields
    destination: Optional[str] = Field(
        default=None,
        description="Database destination for persisting data (leave empty for DataFrame-only mode)"
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

    persist_and_return: bool = Field(
        default=False,
        description="When destination is set: persist to database AND return DataFrame"
    )

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
        else:
            return {}

def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        shop_url = self.shop_url
        private_app_password = self.private_app_password
        resources_list = self.resources
        start_date = self.start_date
        order_status = self.order_status
        description = self.description or f"Shopify e-commerce data ({', '.join(resources_list)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def shopify_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Shopify data using dlt."""
            from dlt.sources.shopify_dlt import shopify_source

            context.log.info(f"Starting Shopify ingestion for resources: {resources_list}")

            # Determine destination
            use_destination = destination if destination else "duckdb"
            destination_config = self._build_destination_config() if destination else {}
            context.log.info(f"Using destination: {use_destination}")

            # Create pipeline (in-memory DuckDB or specified destination)
            pipeline_kwargs = {
                "pipeline_name": f"{asset_name}_pipeline",
                "destination": use_destination,
                "dataset_name": asset_name if destination else f"{asset_name}_temp"
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

            # Create Shopify source
            source = shopify_source(
                shop_url=shop_url,
                private_app_password=private_app_password,
                start_date=start_date,
                order_status=order_status,
            )

            # Filter to requested resources
            if resources_list:
                selected_resources = []
                for resource_name in resources_list:
                    if hasattr(source, resource_name):
                        selected_resources.append(getattr(source, resource_name))
                    else:
                        context.log.warning(f"Resource {resource_name} not found in Shopify source")

                if not selected_resources:
                    raise ValueError(f"No valid resources found. Available: customers, orders, products")

                load_info = pipeline.run(selected_resources)
            else:
                load_info = pipeline.run(source)

            context.log.info(f"Shopify data loaded: {load_info}")

            # Handle based on destination mode
            if destination and not persist_and_return:
                # Persist only mode: data is in destination, return metadata only
                context.log.info(f"Data persisted to {destination}. Not returning DataFrame (persist_and_return=False)")

                # Get row counts from load_info
                total_rows = sum(package.get('row_counts', {}).get(resource_name, 0)
                               for package in load_info.load_packages
                               for resource_name in resources_list)

                metadata = {
                    "destination": destination,
                    "dataset_name": asset_name,
                    "row_count": total_rows,
                    "resources": resources_list,
                }
                context.add_output_metadata(metadata)

                # Return empty DataFrame with metadata
                return pd.DataFrame({"status": ["persisted"], "destination": [destination], "row_count": [total_rows]})

            # DataFrame return mode: extract data from destination
            dataset_name = asset_name if destination else f"{asset_name}_temp"
            all_data = []
            for resource_name in resources_list:
                try:
                    query = f"SELECT * FROM {dataset_name}.{resource_name}"
                    with pipeline.sql_client() as client:
                        with client.execute_query(query) as cursor:
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()
                            if rows:
                                df = pd.DataFrame(rows, columns=columns)
                                df['_resource_type'] = resource_name
                                all_data.append(df)
                                context.log.info(f"Extracted {len(df)} rows from {resource_name}")
                except Exception as e:
                    context.log.warning(f"Could not extract {resource_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted from Shopify")
                return pd.DataFrame()

            # Combine all resources into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Shopify ingestion complete: {len(combined_df)} total rows from {len(all_data)} resources"
            )

            # Add metadata
            metadata = {
                "row_count": len(combined_df),
                "resources_extracted": len(all_data),
                "resource_types": list(combined_df['_resource_type'].unique()) if '_resource_type' in combined_df.columns else [],
            }

            # Add destination info if persisting
            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return

            # Return with metadata
            if include_sample and len(combined_df) > 0:
                return Output(
                    value=combined_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(combined_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(combined_df.head(10))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return combined_df

        return Definitions(assets=[shopify_ingestion_asset])
