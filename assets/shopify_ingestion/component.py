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

    destination: Optional[str] = Field(
        default=None,
        description="Optional dlt destination (e.g., 'snowflake', 'bigquery', 'postgres', 'redshift'). If not set, uses in-memory DuckDB and returns DataFrame."
    )

    destination_config: Optional[str] = Field(
        default=None,
        description="Optional destination configuration as connection string or JSON. Required if destination is set."
    )

    persist_and_return: bool = Field(
        default=False,
        description="If True with destination set: persist to database AND return DataFrame. If False: only persist to database."
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
        destination_config = self.destination_config
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
            if destination and not destination_config:
                raise ValueError(f"destination_config is required when destination is set to '{destination}'")

            context.log.info(f"Using destination: {use_destination}")

            # Create pipeline (in-memory DuckDB or specified destination)
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=use_destination,
                dataset_name=asset_name if destination else f"{asset_name}_temp"
            )

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
