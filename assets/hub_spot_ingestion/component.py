"""HubSpot Ingestion Component.

Ingest HubSpot CRM data using dlt (data load tool).
Simplified version that extracts contacts, companies, and deals.
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


class HubSpotIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting HubSpot CRM data using dlt.

    Extracts contacts, companies, and deals from HubSpot's API.

    Example:
        ```yaml
        type: dagster_component_templates.HubSpotIngestionComponent
        attributes:
          asset_name: hubspot_crm_data
          api_key: "{{ env('HUBSPOT_API_KEY') }}"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    api_key: str = Field(
        description="HubSpot Private App API key or access token"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_key = self.api_key
        description = self.description or "HubSpot CRM data (contacts, companies, deals)"

        @asset(
            name=asset_name,
            description=description,
            group_name="hubspot",
        )
        def hubspot_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests HubSpot data using dlt."""
            from dlt.sources.hubspot import hubspot

            context.log.info("Starting HubSpot ingestion for contacts, companies, and deals")

            # Create in-memory DuckDB pipeline
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",
                dataset_name=f"{asset_name}_temp"
            )

            # Create HubSpot source
            source = hubspot(api_key=api_key)

            # Extract core resources
            resources_list = ["contacts", "companies", "deals"]
            selected_resources = []
            for resource_name in resources_list:
                if hasattr(source, resource_name):
                    selected_resources.append(getattr(source, resource_name))

            # Run pipeline
            load_info = pipeline.run(selected_resources)
            context.log.info(f"HubSpot data loaded: {load_info}")

            # Extract data from DuckDB to DataFrame
            all_data = []
            dataset_name = f"{asset_name}_temp"

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
                context.log.warning("No data extracted from HubSpot")
                return pd.DataFrame()

            # Combine all resources into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"HubSpot ingestion complete: {len(combined_df)} total rows from {len(all_data)} resources"
            )

            # Return with metadata
            return Output(
                value=combined_df,
                metadata={
                    "row_count": len(combined_df),
                    "resources_extracted": len(all_data),
                    "resource_types": list(combined_df['_resource_type'].unique()),
                    "preview": MetadataValue.md(combined_df.head(5).to_markdown())
                }
            )

        return Definitions(assets=[hubspot_ingestion_asset])
