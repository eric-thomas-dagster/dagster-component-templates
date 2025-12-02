"""Airtable Ingestion Component.

Ingest Airtable database data using dlt (data load tool).
Extracts tables from Airtable bases.
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


class AirtableIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Airtable database data using dlt.

    Airtable is a cloud-based spreadsheet-database hybrid platform. This component
    extracts data from Airtable tables and returns it as a pandas DataFrame for
    downstream transformation and analysis.

    The component uses dlt's verified Airtable source to handle API pagination,
    rate limiting, and data extraction.

    Example:
        ```yaml
        type: dagster_component_templates.AirtableIngestionComponent
        attributes:
          asset_name: airtable_data
          api_key: "{{ env('AIRTABLE_API_KEY') }}"
          base_id: "appXXXXXXXXXXXXXX"
          table_names:
            - Customers
            - Orders
            - Products
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    api_key: str = Field(
        description="Airtable API key for authentication"
    )

    base_id: str = Field(
        description="Airtable base ID (starts with 'app')"
    )

    table_names: List[str] = Field(
        description="List of table names to extract from the base"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="airtable",
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
        api_key = self.api_key
        base_id = self.base_id
        table_names = self.table_names
        description = self.description or f"Airtable data ({', '.join(table_names)})"
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
        def airtable_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Airtable data using dlt."""
            from dlt.sources.airtable import airtable_source

            context.log.info(f"Starting Airtable ingestion for tables: {table_names}")
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

            # Create Airtable source
            source = airtable_source(
                base_id=base_id,
                table_names=table_names,
                access_token=api_key,
            )

            # Run pipeline
            load_info = pipeline.run(source)
            context.log.info(f"Airtable data loaded: {load_info}")

            # Handle based on destination mode
            if destination and not persist_and_return:
                # Persist only mode: data is in destination, return metadata only
                context.log.info(f"Data persisted to {destination}. Not returning DataFrame (persist_and_return=False)")

                # Get row counts from load_info if available
                try:
                    total_rows = sum(
                        package.get('row_counts', {}).get(resource_name, 0)
                        for package in load_info.load_packages
                        for resource_name in resources_list if 'resources_list' in locals()
                    )
                except:
                    total_rows = 0

                metadata = {
                    "destination": destination,
                    "dataset_name": asset_name,
                    "row_count": total_rows,
            }

            # Add destination info if persisting
            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return
                context.add_output_metadata(metadata)

                # Return empty DataFrame with metadata
                return pd.DataFrame({"status": ["persisted"], "destination": [destination], "row_count": [total_rows]})

            # DataFrame return mode: extract data from destination
            dataset_name = asset_name if destination else f"{asset_name}_temp"


            # Extract data from DuckDB to DataFrame
            all_data = []
            for table_name in table_names:
                try:
                    # Clean table name for query (replace spaces, special chars)
                    table_name_clean = table_name.replace(" ", "_").replace("-", "_").lower()
                    query = f"SELECT * FROM {dataset_name}.{table_name_clean}"
                    with pipeline.sql_client() as client:
                        with client.execute_query(query) as cursor:
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()
                            if rows:
                                df = pd.DataFrame(rows, columns=columns)
                                df['_resource_type'] = table_name
                                all_data.append(df)
                                context.log.info(f"Extracted {len(df)} rows from {table_name}")
                except Exception as e:
                    context.log.warning(f"Could not extract {table_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted from Airtable")
                return pd.DataFrame()

            # Combine all tables into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Airtable ingestion complete: {len(combined_df)} total rows from {len(all_data)} tables"
            )

            # Add metadata
            metadata = {
                "row_count": len(combined_df),
                "resources_extracted": len(all_data),
                "resource_types": list(combined_df['_resource_type'].unique()) if '_resource_type' in combined_df.columns else [],
            }

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

        return Definitions(assets=[airtable_ingestion_asset])
