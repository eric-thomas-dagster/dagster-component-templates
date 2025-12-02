"""HubSpot Ingestion Component.

Ingest HubSpot CRM and marketing data using dlt (data load tool).
Extracts contacts, companies, deals, tickets, and more.
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
    """Component for ingesting HubSpot CRM and marketing data using dlt.

    HubSpot is a popular CRM and marketing automation platform. This component
    extracts data from HubSpot's API and returns it as a pandas DataFrame for
    downstream transformation and analysis.

    Available data resources:
    - contacts: Visitors and potential customers
    - companies: Organizational information
    - deals: Deal records and pipeline tracking
    - tickets: Customer support requests
    - products: Product catalog and pricing
    - quotes: Price proposals
    - hubspot_events_for_objects: Web analytics events

    The component uses dlt's verified HubSpot source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.HubSpotIngestionComponent
        attributes:
          asset_name: hubspot_crm_data
          api_key: "{{ env('HUBSPOT_API_KEY') }}"
          resources:
            - contacts
            - companies
            - deals
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    api_key: str = Field(
        description="HubSpot Private App API key or access token"
    )

    resources: List[str] = Field(
        default=["contacts", "companies", "deals", "tickets"],
        description="HubSpot resources to extract (contacts, companies, deals, tickets, products, quotes, hubspot_events_for_objects)"
    )

    include_history: bool = Field(
        default=False,
        description="Include property change history for CRM objects"
    )

    include_custom_props: bool = Field(
        default=True,
        description="Extract custom CRM object properties"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="hubspot",
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
        resources_list = self.resources
        include_history = self.include_history
        include_custom_props = self.include_custom_props
        description = self.description or f"HubSpot data ({', '.join(resources_list)})"
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
        def hubspot_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests HubSpot data using dlt."""
            from dlt.sources.hubspot import hubspot

            context.log.info(f"Starting HubSpot ingestion for resources: {resources_list}")
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

            # Create HubSpot source
            source = hubspot(
                api_key=api_key,
                include_history=include_history,
            )

            # Filter to requested resources
            if resources_list:
                # Select specific resources
                selected_resources = []
                for resource_name in resources_list:
                    if hasattr(source, resource_name):
                        selected_resources.append(getattr(source, resource_name))
                    else:
                        context.log.warning(f"Resource {resource_name} not found in HubSpot source")

                if not selected_resources:
                    raise ValueError(f"No valid resources found. Available: contacts, companies, deals, tickets, products, quotes, hubspot_events_for_objects")

                # Run pipeline with selected resources
                load_info = pipeline.run(selected_resources)
            else:
                # Run with all resources
                load_info = pipeline.run(source)

            context.log.info(f"HubSpot data loaded: {load_info}")

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
            for resource_name in resources_list:
                try:
                    # Try to query the resource table
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

        return Definitions(assets=[hubspot_ingestion_asset])
