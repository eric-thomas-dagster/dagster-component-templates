"""Google Analytics 4 Ingestion Component using dlt.

Ingest Google Analytics 4 data (sessions, users, events, conversions, and custom metrics)
using dlt's verified Google Analytics source. Returns DataFrames for flexible transformation.
"""

from typing import Optional
import pandas as pd
import json
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class GoogleAnalyticsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Google Analytics 4 data using dlt - returns DataFrames.

    This component uses dlt's verified Google Analytics source to extract GA4 data and
    returns it as a pandas DataFrame for downstream transformation and analysis.

    Common dimensions:
    - date, city, country, deviceCategory
    - sessionSource, sessionMedium, sessionCampaignName
    - pagePath, pageTitle, eventName

    Common metrics:
    - sessions, totalUsers, activeUsers
    - screenPageViews, eventCount
    - conversions, totalRevenue
    - averageSessionDuration, bounceRate

    The DataFrame can then be:
    - Transformed with Marketing Data Standardizer
    - Further processed with DataFrame Transformer
    - Written to any warehouse with DuckDB/Snowflake/BigQuery Writer

    Example:
        ```yaml
        type: dagster_component_templates.GoogleAnalyticsIngestionComponent
        attributes:
          asset_name: google_analytics_data
          property_id: "123456789"
          credentials_json: "${GOOGLE_ANALYTICS_CREDENTIALS}"
          dimensions: "date,sessionSource,sessionMedium,deviceCategory"
          metrics: "sessions,totalUsers,screenPageViews,conversions"
          start_date: "2024-01-01"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the Google Analytics data"
    )

    property_id: str = Field(
        description="Google Analytics 4 Property ID (numeric, e.g., '123456789'). Find in GA4 Admin > Property Settings."
    )

    # Authentication - Service Account (recommended)
    credentials_json: Optional[str] = Field(
        default=None,
        description="Service account credentials as JSON string. Use ${GOOGLE_ANALYTICS_CREDENTIALS} for env vars."
    )

    project_id: Optional[str] = Field(
        default=None,
        description="Google Cloud Project ID (for service account)"
    )

    client_email: Optional[str] = Field(
        default=None,
        description="Service account email (alternative to credentials_json)"
    )

    private_key: Optional[str] = Field(
        default=None,
        description="Service account private key (alternative to credentials_json). Use ${GOOGLE_ANALYTICS_PRIVATE_KEY} for env vars."
    )

    # OAuth credentials (alternative to service account)
    use_oauth: bool = Field(
        default=False,
        description="Use OAuth instead of service account authentication"
    )

    client_id: Optional[str] = Field(
        default=None,
        description="OAuth Client ID (if use_oauth=true)"
    )

    client_secret: Optional[str] = Field(
        default=None,
        description="OAuth Client Secret (if use_oauth=true)"
    )

    refresh_token: Optional[str] = Field(
        default=None,
        description="OAuth Refresh Token (if use_oauth=true)"
    )

    # Query configuration
    dimensions: str = Field(
        default="date,sessionSource,sessionMedium,deviceCategory",
        description="Comma-separated list of dimensions (e.g., 'date,city,sessionSource')"
    )

    metrics: str = Field(
        default="sessions,totalUsers,screenPageViews,conversions",
        description="Comma-separated list of metrics (e.g., 'sessions,totalUsers,conversions')"
    )

    start_date: str = Field(
        default="2024-01-01",
        description="Start date for data extraction (YYYY-MM-DD)"
    )

    end_date: Optional[str] = Field(
        default=None,
        description="End date for data extraction (YYYY-MM-DD). Defaults to today."
    )

    rows_per_page: int = Field(
        default=10000,
        description="Number of rows per API page (max 100,000)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="google_analytics",
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
        property_id = self.property_id
        credentials_json_str = self.credentials_json
        project_id = self.project_id
        client_email = self.client_email
        private_key = self.private_key
        use_oauth = self.use_oauth
        oauth_client_id = self.client_id
        oauth_client_secret = self.client_secret
        oauth_refresh_token = self.refresh_token
        dimensions_str = self.dimensions
        metrics_str = self.metrics
        start_date = self.start_date
        end_date = self.end_date
        rows_per_page = self.rows_per_page
        description = self.description or "Google Analytics 4 data ingestion via dlt"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def google_analytics_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Google Analytics 4 data using dlt and returns as DataFrame."""

            context.log.info(f"Starting Google Analytics 4 ingestion for property: {property_id}")

            # Parse dimensions and metrics
            dimensions_list = [d.strip() for d in dimensions_str.split(',')]
            metrics_list = [m.strip() for m in metrics_str.split(',')]

            context.log.info(f"Dimensions: {dimensions_list}")
            context.log.info(f"Metrics: {metrics_list}")

            # Import dlt Google Analytics source
            try:
                from dlt.sources.google_analytics import google_analytics
                import dlt
            except ImportError as e:
                context.log.error(f"Failed to import dlt Google Analytics source: {e}")
                context.log.info("Install with: pip install 'dlt[google_analytics]'")
                raise
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

            context.log.info("Created dlt pipeline for data extraction")

            # Prepare credentials
            credentials = {}

            if use_oauth:
                # OAuth authentication
                context.log.info("Using OAuth authentication")
                credentials = {
                    "client_id": oauth_client_id,
                    "client_secret": oauth_client_secret,
                    "refresh_token": oauth_refresh_token,
                    "project_id": project_id or "default",
                }
            else:
                # Service account authentication
                context.log.info("Using service account authentication")

                if credentials_json_str:
                    # Parse JSON credentials
                    try:
                        creds = json.loads(credentials_json_str)
                        credentials = {
                            "project_id": creds.get("project_id"),
                            "client_email": creds.get("client_email"),
                            "private_key": creds.get("private_key"),
                        }
                    except json.JSONDecodeError as e:
                        context.log.error(f"Invalid credentials JSON: {e}")
                        raise
                else:
                    # Use individual fields
                    credentials = {
                        "project_id": project_id,
                        "client_email": client_email,
                        "private_key": private_key,
                    }

            # Build query configuration
            queries = [{
                "resource_name": "ga4_data",
                "dimensions": dimensions_list,
                "metrics": metrics_list,
            }]

            # Create Google Analytics source
            try:
                source = google_analytics(
                    credentials=credentials,
                    property_id=property_id,
                    queries=queries,
                    start_date=start_date,
                    end_date=end_date,
                    rows_per_page=rows_per_page,
                )
            except Exception as e:
                context.log.error(f"Failed to create Google Analytics source: {e}")
                raise

            # Run pipeline
            context.log.info(f"Extracting Google Analytics data from {start_date} to {end_date or 'today'}...")
            load_info = pipeline.run(source)
            context.log.info("Data extraction complete")

            # Extract data from DuckDB to DataFrame
            all_data = []

            try:
                # Query the main GA4 data table
                query = f"SELECT * FROM {dataset_name}.ga4_data"
                with pipeline.sql_client() as client:
                    df = client.execute_df(query)

                if len(df) > 0:
                    all_data.append(df)
                    context.log.info(f"Loaded ga4_data: {len(df)} rows")
            except Exception as e:
                context.log.warning(f"Could not load ga4_data: {e}")

            # Combine all data into single DataFrame
            if not all_data:
                context.log.warning("No data extracted")
                return pd.DataFrame()

            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Extraction complete: {len(combined_df)} total rows, "
                f"{len(combined_df.columns)} columns"
            )

            # Add output metadata
            total_rows = len(combined_df)
            metadata = {
                "property_id": property_id,
                "dimensions": dimensions_list,
                "metrics": metrics_list,
                "start_date": start_date,
                "end_date": end_date or "today",
                "total_rows": total_rows,
                "columns": list(combined_df.columns),
            }

            # Add destination info if persisting
            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(combined_df) > 0:
                return Output(
                    value=combined_df,
                    metadata={
                        "row_count": len(combined_df),
                        "column_count": len(combined_df.columns),
                        "date_range": f"{start_date} to {end_date or 'today'}",
                        "sample": MetadataValue.md(combined_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(combined_df.head(10))
                    }
                )
            else:
                return combined_df

        return Definitions(assets=[google_analytics_ingestion_asset])
