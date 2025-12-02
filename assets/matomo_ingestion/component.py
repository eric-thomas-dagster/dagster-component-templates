"""Matomo Ingestion Component.

Ingest Matomo web analytics data using dlt (data load tool).
Extracts various analytics reports and metrics.
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


class MatomoIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Matomo web analytics data using dlt.

    Matomo is a leading open-source web analytics platform that prioritizes user
    privacy. This component extracts analytics data from Matomo's API and returns
    it as a pandas DataFrame for downstream transformation and analysis.

    The component can extract various report types including:
    - VisitsSummary: Overview of visits and visitor metrics
    - Actions: Page views, downloads, and outlinks
    - Events: Custom event tracking
    - Goals: Conversion tracking
    - And many more analytics reports

    The component uses dlt's verified Matomo source to handle API pagination
    and data extraction automatically.

    Example:
        ```yaml
        type: dagster_component_templates.MatomoIngestionComponent
        attributes:
          asset_name: matomo_analytics_data
          api_url: "https://analytics.example.com"
          api_token: "{{ env('MATOMO_API_TOKEN') }}"
          site_id: "1"
          reports:
            - "VisitsSummary"
            - "Actions"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    api_url: str = Field(
        description="Matomo instance URL (e.g., https://analytics.example.com)"
    )

    api_token: str = Field(
        description="Matomo API token for authentication"
    )

    site_id: str = Field(
        description="Matomo site ID to extract data from"
    )

    reports: List[str] = Field(
        default=["VisitsSummary", "Actions"],
        description="List of Matomo report types to extract (e.g., VisitsSummary, Actions, Events, Goals)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="matomo",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_url = self.api_url
        api_token = self.api_token
        site_id = self.site_id
        reports = self.reports
        description = self.description or f"Matomo web analytics data ({', '.join(reports)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def matomo_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Matomo data using dlt."""
            from dlt.sources.matomo import matomo_reports

            context.log.info(f"Starting Matomo ingestion for reports: {reports}")

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            # Create Matomo source
            source = matomo_reports(
                api_url=api_url,
                api_token=api_token,
                site_id=site_id,
                queries=reports,
            )

            # Run pipeline
            load_info = pipeline.run(source)

            context.log.info(f"Matomo data loaded: {load_info}")

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
                context.log.warning("No data extracted from Matomo")
                return pd.DataFrame()

            # Combine all resources into single DataFrame
            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Matomo ingestion complete: {len(combined_df)} total rows from {len(all_data)} reports"
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

        return Definitions(assets=[matomo_ingestion_asset])
