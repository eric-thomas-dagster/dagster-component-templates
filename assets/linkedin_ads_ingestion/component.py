"""LinkedIn Ads Ingestion Component using dlt.

Ingest LinkedIn Ads data (campaigns, creatives, analytics, and performance metrics)
using dlt's REST API source with LinkedIn Marketing API. Returns DataFrames for flexible transformation.
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


class LinkedInAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting LinkedIn Ads data using dlt - returns DataFrames.

    This component uses dlt's REST API source to extract LinkedIn Ads data via the
    LinkedIn Marketing API and returns it as a pandas DataFrame for downstream
    transformation and analysis.

    Resources extracted:
    - accounts: Ad account information and settings
    - campaigns: Campaign configurations, budgets, and status
    - creatives: Ad creatives including images, videos, and copy
    - campaign_groups: Campaign group hierarchies
    - analytics: Performance metrics by campaign, creative, or account
    - conversions: Conversion tracking data

    Performance metrics included:
    - Impressions, Clicks, Cost
    - Conversions, Conversion Value
    - CTR, Average CPC, Average CPM
    - Engagement metrics (likes, comments, shares)
    - Video metrics (views, completion rate)

    The DataFrame can then be:
    - Transformed with Marketing Data Standardizer
    - Further processed with DataFrame Transformer
    - Written to any warehouse with DuckDB/Snowflake/BigQuery Writer

    Example:
        ```yaml
        type: dagster_component_templates.LinkedInAdsIngestionComponent
        attributes:
          asset_name: linkedin_ads_data
          access_token: "${LINKEDIN_ACCESS_TOKEN}"
          account_ids: "123456789,987654321"
          resources: "campaigns,analytics"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the LinkedIn Ads data"
    )

    access_token: str = Field(
        description="LinkedIn OAuth 2.0 Access Token with r_ads and r_ads_reporting permissions. Use ${LINKEDIN_ACCESS_TOKEN} for env vars."
    )

    account_ids: str = Field(
        description="Comma-separated list of LinkedIn Ad Account IDs (e.g., '123456789,987654321')"
    )

    resources: str = Field(
        default="campaigns,analytics",
        description="Comma-separated list of resources to extract: accounts, campaigns, creatives, campaign_groups, analytics, conversions"
    )

    start_date: Optional[str] = Field(
        default=None,
        description="Start date for analytics data (YYYY-MM-DD). Defaults to 30 days ago."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="End date for analytics data (YYYY-MM-DD). Defaults to today."
    )

    time_granularity: str = Field(
        default="DAILY",
        description="Analytics time granularity: DAILY, MONTHLY, or ALL (cumulative)"
    )

    pivot_by: Optional[str] = Field(
        default=None,
        description="Analytics pivot dimension: CAMPAIGN, CREATIVE, ACCOUNT, or COMPANY"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="linkedin_ads",
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
        access_token = self.access_token
        account_ids_str = self.account_ids
        resources_str = self.resources
        start_date = self.start_date
        end_date = self.end_date
        time_granularity = self.time_granularity
        pivot_by = self.pivot_by
        description = self.description or "LinkedIn Ads data ingestion via dlt"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def linkedin_ads_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests LinkedIn Ads data using dlt and returns as DataFrame."""

            context.log.info(f"Starting LinkedIn Ads ingestion for accounts: {account_ids_str}")

            # Parse account IDs and resources
            account_ids = [a.strip() for a in account_ids_str.split(',')]
            resources_list = [r.strip() for r in resources_str.split(',')]
            context.log.info(f"Resources to extract: {resources_list}")

            # Import dlt REST API source
            try:
                import dlt
                from dlt.sources.rest_api import rest_api_source
            except ImportError as e:
                context.log.error(f"Failed to import dlt REST API source: {e}")
                context.log.info("Install with: pip install 'dlt[rest_api]'")
                raise

            # Determine dates for analytics
            from datetime import datetime, timedelta
            if start_date:
                start_date_str = start_date
            else:
                start_date_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

            if end_date:
                end_date_str = end_date
            else:
                end_date_str = datetime.now().strftime("%Y-%m-%d")

            # Convert to Unix timestamp (milliseconds) for LinkedIn API
            start_timestamp = int(datetime.strptime(start_date_str, "%Y-%m-%d").timestamp() * 1000)
            end_timestamp = int(datetime.strptime(end_date_str, "%Y-%m-%d").timestamp() * 1000)

            context.log.info(f"Date range: {start_date_str} to {end_date_str}")

            # Create pipeline
            use_destination = destination if destination else "duckdb"
            dataset_name = asset_name if destination else f"{asset_name}_temp"

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=use_destination,
                dataset_name=dataset_name
            )

            context.log.info("Created dlt pipeline for data extraction")

            # Build REST API configuration for LinkedIn Marketing API
            # Base URL: https://api.linkedin.com/rest
            config = {
                "client": {
                    "base_url": "https://api.linkedin.com/rest",
                    "auth": {
                        "type": "bearer",
                        "token": access_token
                    },
                    "headers": {
                        "LinkedIn-Version": "202401"
                    }
                },
                "resources": []
            }

            # Add resource configurations based on requested resources
            if "accounts" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"accounts_{account_id}",
                        "endpoint": {
                            "path": f"adAccounts/{account_id}",
                            "paginator": None
                        }
                    })

            if "campaigns" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"campaigns_{account_id}",
                        "endpoint": {
                            "path": "adCampaignsV2",
                            "params": {
                                "q": "search",
                                "search.account.values[0]": f"urn:li:sponsoredAccount:{account_id}",
                                "count": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "start"
                            }
                        }
                    })

            if "creatives" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"creatives_{account_id}",
                        "endpoint": {
                            "path": "adCreativesV2",
                            "params": {
                                "q": "search",
                                "search.account.values[0]": f"urn:li:sponsoredAccount:{account_id}",
                                "count": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "start"
                            }
                        }
                    })

            if "campaign_groups" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"campaign_groups_{account_id}",
                        "endpoint": {
                            "path": "adCampaignGroupsV2",
                            "params": {
                                "q": "search",
                                "search.account.values[0]": f"urn:li:sponsoredAccount:{account_id}",
                                "count": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "start"
                            }
                        }
                    })

            if "analytics" in resources_list:
                for account_id in account_ids:
                    analytics_params = {
                        "q": "analytics",
                        "dateRange.start.day": datetime.fromtimestamp(start_timestamp/1000).day,
                        "dateRange.start.month": datetime.fromtimestamp(start_timestamp/1000).month,
                        "dateRange.start.year": datetime.fromtimestamp(start_timestamp/1000).year,
                        "dateRange.end.day": datetime.fromtimestamp(end_timestamp/1000).day,
                        "dateRange.end.month": datetime.fromtimestamp(end_timestamp/1000).month,
                        "dateRange.end.year": datetime.fromtimestamp(end_timestamp/1000).year,
                        "timeGranularity": time_granularity,
                        "accounts[0]": f"urn:li:sponsoredAccount:{account_id}"
                    }

                    if pivot_by:
                        analytics_params["pivot"] = pivot_by

                    config["resources"].append({
                        "name": f"analytics_{account_id}",
                        "endpoint": {
                            "path": "adAnalytics",
                            "params": analytics_params,
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "start"
                            }
                        }
                    })

            if "conversions" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"conversions_{account_id}",
                        "endpoint": {
                            "path": "conversions",
                            "params": {
                                "q": "account",
                                "account": f"urn:li:sponsoredAccount:{account_id}"
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "start"
                            }
                        }
                    })

            # Create REST API source
            context.log.info("Creating LinkedIn Ads REST API source...")
            source = rest_api_source(config)

            # Run pipeline
            context.log.info("Extracting LinkedIn Ads data...")
            load_info = pipeline.run(source)
            context.log.info(f"Loaded data from LinkedIn Marketing API")

            # Collect all data from all account-specific resources
            all_data = []
            resource_metadata = {}

            # Query each resource table
            with pipeline.sql_client() as client:
                # Get list of tables
                tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{dataset_name}'"
                try:
                    tables_df = client.execute_df(tables_query)
                    table_names = tables_df['table_name'].tolist()
                except:
                    # Fallback: try to query expected table names
                    table_names = []
                    for resource in resources_list:
                        for account_id in account_ids:
                            table_names.append(f"{resource}_{account_id}")

                context.log.info(f"Found tables: {table_names}")

                for table_name in table_names:
                    try:
                        query = f"SELECT * FROM {dataset_name}.{table_name}"
                        df = client.execute_df(query)

                        if len(df) > 0:
                            df['_resource_type'] = table_name
                            all_data.append(df)
                            resource_metadata[table_name] = len(df)
                            context.log.info(f"  {table_name}: {len(df)} rows")
                    except Exception as e:
                        context.log.warning(f"Could not load {table_name}: {e}")

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
            metadata = {
                "account_ids": account_ids,
                "resources_loaded": list(resource_metadata.keys()),
                "total_rows": len(combined_df),
                "columns": list(combined_df.columns),
            }

            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return

            if start_date:
                metadata["start_date"] = start_date_str
            if end_date:
                metadata["end_date"] = end_date_str

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

        return Definitions(assets=[linkedin_ads_ingestion_asset])
