"""Jira Ingestion Component.

Ingest Jira issue tracking data using dlt (data load tool).
Extracts issues, users, projects, and boards.
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


class JiraIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Jira issue tracking data using dlt.

    Jira is a popular issue tracking and agile project management platform. This component
    extracts data from Jira's API and returns it as a pandas DataFrame for
    downstream transformation and analysis.

    Available data resources:
    - issues: Issue records with status, assignee, and metadata
    - users: User accounts and their details
    - projects: Project configurations and settings
    - boards: Agile boards and their configurations

    The component uses dlt's verified Jira source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.JiraIngestionComponent
        attributes:
          asset_name: jira_issue_data
          domain: "your-company.atlassian.net"
          email: "{{ env('JIRA_EMAIL') }}"
          api_token: "{{ env('JIRA_API_TOKEN') }}"
          resources:
            - issues
            - projects
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    domain: str = Field(
        description="Jira domain (e.g., your-company.atlassian.net)"
    )

    email: str = Field(
        description="Email address for Jira authentication"
    )

    api_token: str = Field(
        description="Jira API token for authentication"
    )

    resources: List[str] = Field(
        default=["issues", "projects"],
        description="Jira resources to extract (issues, users, projects, boards)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="jira",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        domain = self.domain
        email = self.email
        api_token = self.api_token
        resources_list = self.resources
        description = self.description or f"Jira issue tracking data ({', '.join(resources_list)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def jira_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Jira data using dlt."""
            from dlt.sources.jira import jira

            context.log.info(f"Starting Jira ingestion for resources: {resources_list}")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create Jira source
            source = jira(
                domain=domain,
                email=email,
                api_token=api_token,
            )

            # Filter to requested resources
            if resources_list:
                selected_resources = []
                for resource_name in resources_list:
                    if hasattr(source, resource_name):
                        selected_resources.append(getattr(source, resource_name))
                    else:
                        context.log.warning(f"Resource {resource_name} not found in Jira source")

                if not selected_resources:
                    raise ValueError(f"No valid resources found. Available: issues, users, projects, boards")

                load_info = pipeline.run(selected_resources)
            else:
                load_info = pipeline.run(source)

            context.log.info(f"Jira data loaded: {load_info}")

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
                context.log.warning("No data extracted from Jira")
                return pd.DataFrame()

            # Combine all resources into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Jira ingestion complete: {len(combined_df)} total rows from {len(all_data)} resources"
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

        return Definitions(assets=[jira_ingestion_asset])
