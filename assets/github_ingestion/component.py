"""GitHub Ingestion Component.

Ingest GitHub repository data using dlt (data load tool).
Extracts issues, pull requests, comments, and reactions.
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


class GitHubIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting GitHub repository data using dlt.

    GitHub is the leading platform for version control and collaboration. This component
    extracts data from GitHub's API and returns it as a pandas DataFrame for
    downstream transformation and analysis.

    Available data resources:
    - issues: Issue records with labels and assignees
    - pull_requests: Pull request data with review status
    - comments: Comments on issues and pull requests
    - reactions: Emoji reactions to content

    The component uses dlt's verified GitHub source to handle API pagination,
    rate limiting, and incremental loading automatically.

    Example:
        ```yaml
        type: dagster_component_templates.GitHubIngestionComponent
        attributes:
          asset_name: github_repo_data
          access_token: "{{ env('GITHUB_ACCESS_TOKEN') }}"
          owner: "my-org"
          repositories:
            - "repo-1"
            - "repo-2"
          resources:
            - issues
            - pull_requests
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    access_token: str = Field(
        description="GitHub Personal Access Token for API authentication"
    )

    owner: str = Field(
        description="Repository owner (username or organization)"
    )

    repositories: List[str] = Field(
        description="List of repository names to extract data from"
    )

    resources: List[str] = Field(
        default=["issues", "pull_requests"],
        description="GitHub resources to extract (issues, pull_requests, comments, reactions)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="github",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        access_token = self.access_token
        owner = self.owner
        repositories = self.repositories
        resources_list = self.resources
        description = self.description or f"GitHub repository data ({', '.join(resources_list)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def github_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests GitHub data using dlt."""
            from dlt.sources.github import github_reactions

            context.log.info(f"Starting GitHub ingestion for resources: {resources_list}")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create GitHub source
            source = github_reactions(
                owner=owner,
                repositories=repositories,
                access_token=access_token,
            )

            # Filter to requested resources
            if resources_list:
                selected_resources = []
                for resource_name in resources_list:
                    if hasattr(source, resource_name):
                        selected_resources.append(getattr(source, resource_name))
                    else:
                        context.log.warning(f"Resource {resource_name} not found in GitHub source")

                if not selected_resources:
                    raise ValueError(f"No valid resources found. Available: issues, pull_requests, comments, reactions")

                load_info = pipeline.run(selected_resources)
            else:
                load_info = pipeline.run(source)

            context.log.info(f"GitHub data loaded: {load_info}")

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
                context.log.warning("No data extracted from GitHub")
                return pd.DataFrame()

            # Combine all resources into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"GitHub ingestion complete: {len(combined_df)} total rows from {len(all_data)} resources"
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

        return Definitions(assets=[github_ingestion_asset])
