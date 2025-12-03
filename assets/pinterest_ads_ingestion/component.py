"""Pinterest Ads Ingestion Component using dlt.

Ingest Pinterest Ads data (campaigns, ad groups, ads, pins, and performance metrics)
using dlt's REST API source with Pinterest Marketing API. Returns DataFrames for flexible transformation.
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


class PinterestAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Pinterest Ads data using dlt - returns DataFrames.

    This component uses dlt's REST API source to extract Pinterest Ads data via the
    Pinterest Marketing API and returns it as a pandas DataFrame for downstream
    transformation and analysis.

    Resources extracted:
    - ad_accounts: Ad account information and settings
    - campaigns: Campaign configurations and budgets
    - ad_groups: Ad group settings and targeting
    - ads: Individual ad creative and settings
    - pins: Pin creative content
    - keywords: Keyword targeting and bids
    - analytics: Performance metrics and insights

    Performance metrics included:
    - Impressions, Clicks, Cost
    - Saves, Pin clicks, Outbound clicks
    - CTR, Average CPC, Average CPM
    - Video metrics (starts, complete views)
    - Conversions and ROAS

    The DataFrame can then be:
    - Transformed with Marketing Data Standardizer
    - Further processed with DataFrame Transformer
    - Written to any warehouse with DuckDB/Snowflake/BigQuery Writer

    Example:
        ```yaml
        type: dagster_component_templates.PinterestAdsIngestionComponent
        attributes:
          asset_name: pinterest_ads_data
          access_token: "${PINTEREST_ACCESS_TOKEN}"
          ad_account_ids: "123456789,987654321"
          resources: "campaigns,analytics"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the Pinterest Ads data"
    )

    access_token: str = Field(
        description="Pinterest OAuth 2.0 Access Token with ads:read scope. Use ${PINTEREST_ACCESS_TOKEN} for env vars."
    )

    ad_account_ids: str = Field(
        description="Comma-separated list of Pinterest Ad Account IDs (e.g., '123456789,987654321')"
    )

    app_id: Optional[str] = Field(
        default=None,
        description="Pinterest App ID (optional, for token refresh)"
    )

    app_secret: Optional[str] = Field(
        default=None,
        description="Pinterest App Secret (optional, for token refresh). Use ${PINTEREST_APP_SECRET} for env vars."
    )

    resources: str = Field(
        default="campaigns,analytics",
        description="Comma-separated list of resources to extract: ad_accounts, campaigns, ad_groups, ads, pins, keywords, analytics"
    )

    start_date: Optional[str] = Field(
        default=None,
        description="Start date for analytics (YYYY-MM-DD). Defaults to 30 days ago."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="End date for analytics (YYYY-MM-DD). Defaults to today."
    )

    granularity: str = Field(
        default="DAY",
        description="Analytics granularity: DAY, HOUR, WEEK, or MONTH"
    )

    level: str = Field(
        default="CAMPAIGN",
        description="Analytics aggregation level: AD_ACCOUNT, CAMPAIGN, AD_GROUP, AD, KEYWORD, or PIN_PROMOTION"
    )

    columns: Optional[str] = Field(
        default=None,
        description="Comma-separated list of metric columns to retrieve (leave empty for default metrics)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="pinterest_ads",
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
        ad_account_ids_str = self.ad_account_ids
        app_id = self.app_id
        app_secret = self.app_secret
        resources_str = self.resources
        start_date = self.start_date
        end_date = self.end_date
        granularity = self.granularity
        level = self.level
        columns_str = self.columns
        description = self.description or "Pinterest Ads data ingestion via dlt"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def pinterest_ads_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests Pinterest Ads data using dlt and returns as DataFrame."""

            context.log.info(f"Starting Pinterest Ads ingestion for accounts: {ad_account_ids_str}")

            # Parse account IDs and resources
            ad_account_ids = [a.strip() for a in ad_account_ids_str.split(',')]
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

            # Build REST API configuration for Pinterest Marketing API
            # Base URL: https://api.pinterest.com/v5
            config = {
                "client": {
                    "base_url": "https://api.pinterest.com/v5",
                    "auth": {
                        "type": "bearer",
                        "token": access_token
                    }
                },
                "resources": []
            }

            # Add resource configurations based on requested resources
            if "ad_accounts" in resources_list:
                for ad_account_id in ad_account_ids:
                    config["resources"].append({
                        "name": f"ad_accounts_{ad_account_id}",
                        "endpoint": {
                            "path": f"ad_accounts/{ad_account_id}",
                            "paginator": None
                        }
                    })

            if "campaigns" in resources_list:
                for ad_account_id in ad_account_ids:
                    config["resources"].append({
                        "name": f"campaigns_{ad_account_id}",
                        "endpoint": {
                            "path": f"ad_accounts/{ad_account_id}/campaigns",
                            "params": {
                                "page_size": 250
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "bookmark",
                                "cursor_param": "bookmark"
                            }
                        }
                    })

            if "ad_groups" in resources_list:
                for ad_account_id in ad_account_ids:
                    config["resources"].append({
                        "name": f"ad_groups_{ad_account_id}",
                        "endpoint": {
                            "path": f"ad_accounts/{ad_account_id}/ad_groups",
                            "params": {
                                "page_size": 250
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "bookmark",
                                "cursor_param": "bookmark"
                            }
                        }
                    })

            if "ads" in resources_list:
                for ad_account_id in ad_account_ids:
                    config["resources"].append({
                        "name": f"ads_{ad_account_id}",
                        "endpoint": {
                            "path": f"ad_accounts/{ad_account_id}/ads",
                            "params": {
                                "page_size": 250
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "bookmark",
                                "cursor_param": "bookmark"
                            }
                        }
                    })

            if "pins" in resources_list:
                for ad_account_id in ad_account_ids:
                    config["resources"].append({
                        "name": f"pins_{ad_account_id}",
                        "endpoint": {
                            "path": f"ad_accounts/{ad_account_id}/product_groups",
                            "params": {
                                "page_size": 250
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "bookmark",
                                "cursor_param": "bookmark"
                            }
                        }
                    })

            if "keywords" in resources_list:
                for ad_account_id in ad_account_ids:
                    config["resources"].append({
                        "name": f"keywords_{ad_account_id}",
                        "endpoint": {
                            "path": f"ad_accounts/{ad_account_id}/keywords",
                            "params": {
                                "page_size": 250
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "bookmark",
                                "cursor_param": "bookmark"
                            }
                        }
                    })

            if "analytics" in resources_list:
                # Default columns if not specified
                default_columns = [
                    "SPEND_IN_DOLLAR", "IMPRESSION_1", "CLICKTHROUGH_1",
                    "CTR", "CPC_IN_DOLLAR", "CPM_IN_DOLLAR",
                    "TOTAL_ENGAGEMENT", "SAVE_1", "PIN_CLICK_1", "OUTBOUND_CLICK_1",
                    "TOTAL_CONVERSIONS", "TOTAL_CONVERSION_VALUE"
                ]

                columns_list = columns_str.split(',') if columns_str else default_columns

                for ad_account_id in ad_account_ids:
                    analytics_params = {
                        "start_date": start_date_str,
                        "end_date": end_date_str,
                        "granularity": granularity,
                        "level": level,
                        "columns": ",".join(columns_list)
                    }

                    config["resources"].append({
                        "name": f"analytics_{ad_account_id}",
                        "endpoint": {
                            "path": f"ad_accounts/{ad_account_id}/reports",
                            "params": analytics_params,
                            "method": "POST",
                            "paginator": None
                        }
                    })

            # Create REST API source
            context.log.info("Creating Pinterest Ads REST API source...")
            source = rest_api_source(config)

            # Run pipeline
            context.log.info("Extracting Pinterest Ads data...")
            load_info = pipeline.run(source)
            context.log.info(f"Loaded data from Pinterest Marketing API")

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
                        for ad_account_id in ad_account_ids:
                            table_names.append(f"{resource}_{ad_account_id}")

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
                "ad_account_ids": ad_account_ids,
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

        return Definitions(assets=[pinterest_ads_ingestion_asset])
