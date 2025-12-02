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

    def _build_destination_config(self) -> dict:
        """Build dlt destination config from structured fields."""
        if not self.destination:
            return {}

        if self.destination == "snowflake":
            return {
                "credentials": {
                    "database": self.snowflake_database,
                    "username": self.snowflake_username,
                    "password": self.snowflake_password,
                    "host": self.snowflake_account,
                    "warehouse": self.snowflake_warehouse,
                    "role": self.snowflake_role if self.snowflake_role else None,
                }
            }
        elif self.destination == "bigquery":
            config = {
                "project_id": self.bigquery_project_id,
                "dataset": self.bigquery_dataset,
            }
            if self.bigquery_credentials_path:
                config["credentials"] = self.bigquery_credentials_path
            if self.bigquery_location:
                config["location"] = self.bigquery_location
            return config
        elif self.destination == "postgres":
            return {
                "credentials": {
                    "database": self.postgres_database,
                    "username": self.postgres_username,
                    "password": self.postgres_password,
                    "host": self.postgres_host,
                    "port": self.postgres_port,
                }
            }
        elif self.destination == "redshift":
            return {
                "credentials": {
                    "database": self.redshift_database,
                    "username": self.redshift_username,
                    "password": self.redshift_password,
                    "host": self.redshift_host,
                    "port": self.redshift_port,
                }
            }
        elif self.destination == "duckdb":
            return {
                "credentials": self.duckdb_database_path if self.duckdb_database_path else ":memory:"
            }
        elif self.destination == "motherduck":
            return {
                "credentials": {
                    "database": self.motherduck_database,
                    "token": self.motherduck_token,
                }
            }
        elif self.destination == "databricks":
            config = {
                "credentials": {
                    "server_hostname": self.databricks_server_hostname,
                    "http_path": self.databricks_http_path,
                    "access_token": self.databricks_access_token,
                }
            }
            if self.databricks_catalog:
                config["credentials"]["catalog"] = self.databricks_catalog
            if self.databricks_schema:
                config["credentials"]["schema"] = self.databricks_schema
            return config
        else:
            return {}

def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        api_url = self.api_url
        api_token = self.api_token
        site_id = self.site_id
        reports = self.reports
        description = self.description or f"Matomo web analytics data ({', '.join(reports)})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def matomo_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Matomo data using dlt."""
            from dlt.sources.matomo import matomo_reports

            context.log.info(f"Starting Matomo ingestion for reports: {reports}")
            # Determine destination

            use_destination = destination if destination else "duckdb"

            destination_config = self._build_destination_config() if destination else {}

            context.log.info(f"Using destination: {use_destination}")


            # Create pipeline (in-memory DuckDB or specified destination)

            pipeline_kwargs = {

                "pipeline_name": f"{asset_name}_pipeline",

                "destination": use_destination,

                "dataset_name": asset_name if destination else f"{asset_name}_temp"

            }


            # Add credentials if destination is configured

            if destination_config:

                if "credentials" in destination_config:

                    pipeline_kwargs["credentials"] = destination_config["credentials"]

                # For BigQuery, project_id goes at root level

                if use_destination == "bigquery" and "project_id" in destination_config:

                    pipeline_kwargs["project_id"] = destination_config["project_id"]

                    if "location" in destination_config:

                        pipeline_kwargs["location"] = destination_config["location"]


            pipeline = dlt.pipeline(**pipeline_kwargs)

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
                        query = f"SELECT * FROM {dataset_name}.{table_name}"
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
