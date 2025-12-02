"""Google Ads Ingestion Component using dlt.

Ingest Google Ads data (customers, campaigns, ad groups, ads, and performance metrics)
using dlt's verified Google Ads source. Returns DataFrames for flexible transformation.
Mimics capabilities of Supermetrics and Funnel.io.
"""

from typing import Optional
import pandas as pd
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


class GoogleAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Google Ads data using dlt - returns DataFrames.

    This component uses dlt's verified Google Ads source to extract data and
    returns it as a pandas DataFrame for downstream transformation and analysis.

    Resources extracted:
    - customers: Advertiser account information
    - campaigns: Campaign configurations, budgets, and targeting
    - ad_groups: Ad group settings within campaigns
    - ads: Individual ad creative and settings
    - keywords: Keyword targeting and bids (if available)
    - change_events: Historical changes to account settings

    Performance metrics included:
    - Impressions, Clicks, Cost
    - Conversions, Conversion Value
    - CTR, Average CPC, Average CPM
    - Quality Score (for keywords)

    The DataFrame can then be:
    - Transformed with Marketing Data Standardizer
    - Further processed with DataFrame Transformer
    - Written to any warehouse with DuckDB/Snowflake/BigQuery Writer

    Example:
        ```yaml
        type: dagster_component_templates.GoogleAdsIngestionComponent
        attributes:
          asset_name: google_ads_data
          customer_id: "1234567890"
          developer_token: "${GOOGLE_ADS_DEV_TOKEN}"
          client_id: "${GOOGLE_ADS_CLIENT_ID}"
          client_secret: "${GOOGLE_ADS_CLIENT_SECRET}"
          refresh_token: "${GOOGLE_ADS_REFRESH_TOKEN}"
          resources: "customers,campaigns"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the Google Ads data"
    )

    customer_id: str = Field(
        description="Google Ads Customer ID (10-digit number, format: XXX-XXX-XXXX or XXXXXXXXXX)"
    )

    developer_token: str = Field(
        description="Google Ads API Developer Token. Use ${GOOGLE_ADS_DEV_TOKEN} for env vars."
    )

    # OAuth credentials
    client_id: str = Field(
        description="Google OAuth Client ID. Use ${GOOGLE_ADS_CLIENT_ID} for env vars."
    )

    client_secret: str = Field(
        description="Google OAuth Client Secret. Use ${GOOGLE_ADS_CLIENT_SECRET} for env vars."
    )

    refresh_token: str = Field(
        description="Google OAuth Refresh Token. Use ${GOOGLE_ADS_REFRESH_TOKEN} for env vars."
    )

    # Optional: Service Account credentials (alternative to OAuth)
    use_service_account: bool = Field(
        default=False,
        description="Use service account instead of OAuth credentials"
    )

    project_id: Optional[str] = Field(
        default=None,
        description="Google Cloud Project ID (for service account auth)"
    )

    service_account_email: Optional[str] = Field(
        default=None,
        description="Service account email (for service account auth)"
    )

    private_key: Optional[str] = Field(
        default=None,
        description="Service account private key (for service account auth). Use ${GOOGLE_ADS_PRIVATE_KEY} for env vars."
    )

    impersonated_email: Optional[str] = Field(
        default=None,
        description="Email to impersonate for service account delegation"
    )

    resources: str = Field(
        default="customers,campaigns",
        description="Comma-separated list of resources to extract: customers, campaigns, ad_groups, ads, keywords, change_events"
    )

    start_date: Optional[str] = Field(
        default=None,
        description="Start date for performance metrics (YYYY-MM-DD). Defaults to 30 days ago."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="End date for performance metrics (YYYY-MM-DD). Defaults to today."
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="google_ads",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        customer_id = self.customer_id
        developer_token = self.developer_token
        client_id = self.client_id
        client_secret = self.client_secret
        refresh_token = self.refresh_token
        use_service_account = self.use_service_account
        project_id = self.project_id
        service_account_email = self.service_account_email
        private_key = self.private_key
        impersonated_email = self.impersonated_email
        resources_str = self.resources
        start_date = self.start_date
        end_date = self.end_date
        description = self.description or "Google Ads data ingestion via dlt"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def google_ads_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Google Ads data using dlt and returns as DataFrame."""

            context.log.info(f"Starting Google Ads ingestion for customer: {customer_id}")

            # Parse resources to load
            resources_list = [r.strip() for r in resources_str.split(',')]
            context.log.info(f"Resources to extract: {resources_list}")

            # Import dlt Google Ads source
            try:
                from dlt.sources.google_ads import google_ads
                import dlt
            except ImportError as e:
                context.log.error(f"Failed to import dlt Google Ads source: {e}")
                context.log.info("Install with: pip install 'dlt[google_ads]'")
                raise

            # Create in-memory pipeline for data extraction
            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination="duckdb",  # Use DuckDB in-memory
                dataset_name=f"{asset_name}_temp"
            )

            context.log.info("Created dlt pipeline for data extraction")

            # Prepare authentication config
            auth_config = {
                "dev_token": developer_token,
                "customer_id": customer_id,
            }

            if use_service_account:
                # Service account authentication
                context.log.info("Using service account authentication")
                auth_config.update({
                    "project_id": project_id,
                    "client_email": service_account_email,
                    "private_key": private_key,
                })
                if impersonated_email:
                    auth_config["impersonated_email"] = impersonated_email
            else:
                # OAuth authentication
                context.log.info("Using OAuth authentication")
                auth_config.update({
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                    "project_id": project_id or "default",
                })

            # Add date range if provided
            query_params = {}
            if start_date:
                query_params["start_date"] = start_date
                context.log.info(f"Start date: {start_date}")
            if end_date:
                query_params["end_date"] = end_date
                context.log.info(f"End date: {end_date}")

            # Create Google Ads source
            try:
                source = google_ads(**auth_config, **query_params)
            except Exception as e:
                context.log.error(f"Failed to create Google Ads source: {e}")
                raise

            # Select only requested resources
            load_data = source.with_resources(*resources_list)

            # Run pipeline
            context.log.info("Extracting Google Ads data...")
            load_info = pipeline.run(load_data)
            context.log.info(f"Loaded {len(resources_list)} resources")

            # Collect all data
            all_data = []
            resource_metadata = {}

            # Extract data from DuckDB to DataFrame
            for resource_name in resources_list:
                try:
                    # Query the loaded data
                    query = f"SELECT * FROM {asset_name}_temp.{resource_name}"
                    with pipeline.sql_client() as client:
                        df = client.execute_df(query)

                    if len(df) > 0:
                        df['_resource_type'] = resource_name
                        all_data.append(df)
                        resource_metadata[resource_name] = len(df)
                        context.log.info(f"  {resource_name}: {len(df)} rows")
                except Exception as e:
                    context.log.warning(f"Could not load {resource_name}: {e}")

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
                "customer_id": customer_id,
                "resources_loaded": list(resource_metadata.keys()),
                "total_rows": total_rows,
                "columns": list(combined_df.columns),
            }

            if start_date:
                metadata["start_date"] = start_date
            if end_date:
                metadata["end_date"] = end_date

            # Add per-resource row counts
            for resource, rows in resource_metadata.items():
                metadata[f"rows_{resource}"] = rows

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(combined_df) > 0:
                return Output(
                    value=combined_df,
                    metadata={
                        "row_count": len(combined_df),
                        "column_count": len(combined_df.columns),
                        "resources": list(resource_metadata.keys()),
                        "sample": MetadataValue.md(combined_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(combined_df.head(10))
                    }
                )
            else:
                return combined_df

        return Definitions(assets=[google_ads_ingestion_asset])
