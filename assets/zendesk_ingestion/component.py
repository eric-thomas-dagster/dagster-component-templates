"""Zendesk Ingestion Component.

Ingest Zendesk customer support data using dlt (data load tool).
Extracts tickets, users, organizations, and groups.
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


class ZendeskIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Zendesk customer support data using dlt.

    Zendesk is a popular customer service and support platform. This component
    extracts data from Zendesk's API and returns it as a pandas DataFrame for
    downstream transformation and analysis.

    Available data resources:
    - tickets: Customer support tickets and their status
    - users: End users and agents in the system
    - organizations: Company and organization information
    - groups: Support team groups

    The component uses dlt's verified Zendesk source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.ZendeskIngestionComponent
        attributes:
          asset_name: zendesk_support_data
          subdomain: "my-company"
          email: "{{ env('ZENDESK_EMAIL') }}"
          api_token: "{{ env('ZENDESK_API_TOKEN') }}"
          resources:
            - tickets
            - users
            - organizations
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    subdomain: str = Field(
        description="Zendesk subdomain (e.g., 'my-company' for my-company.zendesk.com)"
    )

    email: str = Field(
        description="Email address for Zendesk authentication"
    )

    api_token: str = Field(
        description="Zendesk API token for authentication"
    )

    resources: List[str] = Field(
        default=["tickets", "users", "organizations", "groups"],
        description="Zendesk resources to extract (tickets, users, organizations, groups)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="zendesk",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        subdomain = self.subdomain
        email = self.email
        api_token = self.api_token
        resources_list = self.resources
        description = self.description or f"Zendesk support data ({', '.join(resources_list)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def zendesk_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Zendesk data using dlt."""
            from dlt.sources.zendesk import zendesk_support

            context.log.info(f"Starting Zendesk ingestion for resources: {resources_list}")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create Zendesk source with credentials
            source = zendesk_support(
                credentials={
                    "subdomain": subdomain,
                    "email": email,
                    "password": api_token,
                }
            )

            # Filter to requested resources
            if resources_list:
                selected_resources = []
                for resource_name in resources_list:
                    if hasattr(source, resource_name):
                        selected_resources.append(getattr(source, resource_name))
                    else:
                        context.log.warning(f"Resource {resource_name} not found in Zendesk source")

                if not selected_resources:
                    raise ValueError(f"No valid resources found. Available: tickets, users, organizations, groups")

                load_info = pipeline.run(selected_resources)
            else:
                load_info = pipeline.run(source)

            context.log.info(f"Zendesk data loaded: {load_info}")

            # Extract data from DuckDB to DataFrame
            all_data = []
            for resource_name in resources_list:
                try:
                    query = f"SELECT * FROM {asset_name}_temp.{resource_name}"
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
                context.log.warning("No data extracted from Zendesk")
                return pd.DataFrame()

            # Combine all resources into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Zendesk ingestion complete: {len(combined_df)} total rows from {len(all_data)} resources"
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

        return Definitions(assets=[zendesk_ingestion_asset])
