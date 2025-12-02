"""Salesforce Ingestion Component.

Ingest Salesforce CRM data using dlt (data load tool).
Extracts accounts, contacts, opportunities, leads, and more.
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


class SalesforceIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Salesforce CRM data using dlt.

    Salesforce is the leading enterprise CRM platform. This component extracts
    data from Salesforce's API and returns it as a pandas DataFrame for downstream
    transformation and analysis.

    Available data objects (20+):

    Replace Mode (full refresh):
    - User, UserRole: User accounts and permissions
    - Lead: Potential customers
    - Contact: Individual contact records
    - Campaign: Marketing campaigns
    - Product2, Pricebook2, PricebookEntry: Product catalog

    Merge Mode (incremental loading):
    - Account: Company/organization records
    - Opportunity, OpportunityLineItem, OpportunityContactRole: Sales pipeline
    - CampaignMember: Campaign participation
    - Task, Event: Activities and calendar events

    The component uses dlt's verified Salesforce source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.SalesforceIngestionComponent
        attributes:
          asset_name: salesforce_crm_data
          username: "{{ env('SALESFORCE_USERNAME') }}"
          password: "{{ env('SALESFORCE_PASSWORD') }}"
          security_token: "{{ env('SALESFORCE_SECURITY_TOKEN') }}"
          sf_objects:
            - Account
            - Opportunity
            - Contact
            - Lead
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    username: str = Field(
        description="Salesforce username"
    )

    password: str = Field(
        description="Salesforce password"
    )

    security_token: str = Field(
        description="Salesforce security token (from Settings > Personal Setup > Reset My Security Token)"
    )

    sf_objects: List[str] = Field(
        default=["Account", "Opportunity", "Contact", "Lead"],
        description="Salesforce objects to extract (Account, Opportunity, Contact, Lead, Campaign, Task, Event, etc.)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="salesforce",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        username = self.username
        password = self.password
        security_token = self.security_token
        sf_objects = self.sf_objects
        description = self.description or f"Salesforce data ({', '.join(sf_objects)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def salesforce_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Salesforce data using dlt."""
            from dlt.sources.salesforce import salesforce_source

            context.log.info(f"Starting Salesforce ingestion for objects: {sf_objects}")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create Salesforce source with credentials
            source = salesforce_source(
                user_name=username,
                password=password,
                security_token=security_token,
            )

            # Filter to requested objects
            if sf_objects:
                # Select specific resources
                selected_resources = []
                for object_name in sf_objects:
                    # Salesforce objects are typically in replace or merge mode
                    # Try to find the resource by name
                    if hasattr(source, object_name):
                        selected_resources.append(getattr(source, object_name))
                    elif hasattr(source, object_name.lower()):
                        selected_resources.append(getattr(source, object_name.lower()))
                    else:
                        context.log.warning(f"Object {object_name} not found in Salesforce source")

                if not selected_resources:
                    raise ValueError(f"No valid objects found. Check Salesforce object names.")

                # Run pipeline with selected resources
                load_info = pipeline.run(selected_resources)
            else:
                # Run with all resources
                load_info = pipeline.run(source)

            context.log.info(f"Salesforce data loaded: {load_info}")

            # Extract data from DuckDB to DataFrame
            all_data = []
            for object_name in sf_objects:
                try:
                    # Try both capitalized and lowercase table names
                    for table_name in [object_name, object_name.lower()]:
                        try:
                            query = f"SELECT * FROM {asset_name}_temp.{table_name}"
                            with pipeline.sql_client() as client:
                                with client.execute_query(query) as cursor:
                                    columns = [desc[0] for desc in cursor.description]
                                    rows = cursor.fetchall()
                                    if rows:
                                        df = pd.DataFrame(rows, columns=columns)
                                        df['_resource_type'] = object_name
                                        all_data.append(df)
                                        context.log.info(f"Extracted {len(df)} rows from {object_name}")
                                        break
                        except:
                            continue
                except Exception as e:
                    context.log.warning(f"Could not extract {object_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted from Salesforce")
                return pd.DataFrame()

            # Combine all objects into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Salesforce ingestion complete: {len(combined_df)} total rows from {len(all_data)} objects"
            )

            # Add metadata
            metadata = {
                "row_count": len(combined_df),
                "objects_extracted": len(all_data),
                "object_types": list(combined_df['_resource_type'].unique()) if '_resource_type' in combined_df.columns else [],
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

        return Definitions(assets=[salesforce_ingestion_asset])
