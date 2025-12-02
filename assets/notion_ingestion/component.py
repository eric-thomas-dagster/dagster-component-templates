"""Notion Ingestion Component.

Ingest Notion workspace database data using dlt (data load tool).
Extracts database records and their properties.
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


class NotionIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Notion workspace databases using dlt.

    Notion is a collaborative workspace platform for notes, documents, and databases.
    This component extracts data from Notion databases and returns it as a pandas
    DataFrame for downstream transformation and analysis.

    The component extracts data from specified Notion databases, including all
    properties and values. Each database becomes a resource in the extracted data.

    The component uses dlt's verified Notion source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.NotionIngestionComponent
        attributes:
          asset_name: notion_workspace_data
          api_key: "{{ env('NOTION_API_KEY') }}"
          database_ids:
            - "abc123def456"
            - "xyz789uvw012"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    api_key: str = Field(
        description="Notion Integration Token for API authentication"
    )

    database_ids: List[str] = Field(
        description="List of Notion database IDs to extract data from"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="notion",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_key = self.api_key
        database_ids = self.database_ids
        description = self.description or f"Notion workspace databases ({len(database_ids)} databases)"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def notion_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Notion data using dlt."""
            from dlt.sources.notion import notion_databases

            context.log.info(f"Starting Notion ingestion for {len(database_ids)} databases")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create Notion source
            source = notion_databases(
                database_ids=database_ids,
                api_key=api_key,
            )

            # Run pipeline
            load_info = pipeline.run(source)

            context.log.info(f"Notion data loaded: {load_info}")

            # Extract data from DuckDB to DataFrame
            all_data = []

            # Get all tables in the dataset
            with pipeline.sql_client() as client:
                # Query to get all table names
                tables_query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{asset_name}_temp'
                """
                with client.execute_query(tables_query) as cursor:
                    tables = [row[0] for row in cursor.fetchall()]

                context.log.info(f"Found {len(tables)} tables: {tables}")

                # Extract data from each table
                for table_name in tables:
                    try:
                        query = f"SELECT * FROM {asset_name}_temp.{table_name}"
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
                context.log.warning("No data extracted from Notion")
                return pd.DataFrame()

            # Combine all resources into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Notion ingestion complete: {len(combined_df)} total rows from {len(all_data)} databases"
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

        return Definitions(assets=[notion_ingestion_asset])
