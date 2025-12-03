"""TikTok Ads Ingestion Component using dlt.

Ingest TikTok Ads data (campaigns, ad groups, ads, and performance metrics)
using dlt's REST API source with TikTok Marketing API. Returns DataFrames for flexible transformation.
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


class TikTokAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting TikTok Ads data using dlt - returns DataFrames.

    This component uses dlt's REST API source to extract TikTok Ads data via the
    TikTok Marketing API and returns it as a pandas DataFrame for downstream
    transformation and analysis.

    Resources extracted:
    - advertisers: Advertiser account information
    - campaigns: Campaign configurations and budgets
    - ad_groups: Ad group settings and targeting
    - ads: Individual ad creative and settings
    - videos: Video creative assets
    - images: Image creative assets
    - reports: Performance metrics and analytics

    Performance metrics included:
    - Impressions, Clicks, Cost (spend)
    - Conversions, Conversion Value
    - CTR, Average CPC, Average CPM
    - Video metrics (views, completion rate, watch time)
    - Engagement metrics (likes, comments, shares)

    The DataFrame can then be:
    - Transformed with Marketing Data Standardizer
    - Further processed with DataFrame Transformer
    - Written to any warehouse with DuckDB/Snowflake/BigQuery Writer

    Example:
        ```yaml
        type: dagster_component_templates.TikTokAdsIngestionComponent
        attributes:
          asset_name: tiktok_ads_data
          access_token: "${TIKTOK_ACCESS_TOKEN}"
          advertiser_ids: "1234567890,9876543210"
          resources: "campaigns,reports"
        ```
    """

    asset_name: str = Field(
        description="Name of the asset that will hold the TikTok Ads data"
    )

    access_token: str = Field(
        description="TikTok Marketing API Access Token. Use ${TIKTOK_ACCESS_TOKEN} for env vars."
    )

    advertiser_ids: str = Field(
        description="Comma-separated list of TikTok Advertiser IDs (e.g., '1234567890,9876543210')"
    )

    app_id: Optional[str] = Field(
        default=None,
        description="TikTok App ID (optional, for API versioning)"
    )

    secret: Optional[str] = Field(
        default=None,
        description="TikTok App Secret (optional, for token refresh). Use ${TIKTOK_SECRET} for env vars."
    )

    resources: str = Field(
        default="campaigns,reports",
        description="Comma-separated list of resources to extract: advertisers, campaigns, ad_groups, ads, videos, images, reports"
    )

    start_date: Optional[str] = Field(
        default=None,
        description="Start date for reports (YYYY-MM-DD). Defaults to 30 days ago."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="End date for reports (YYYY-MM-DD). Defaults to today."
    )

    report_type: str = Field(
        default="BASIC",
        description="Report type: BASIC (summary), AUDIENCE (demographics), or PLAYABLE_MATERIAL (creative)"
    )

    data_level: str = Field(
        default="AUCTION_CAMPAIGN",
        description="Data aggregation level: AUCTION_ADVERTISER, AUCTION_CAMPAIGN, AUCTION_ADGROUP, or AUCTION_AD"
    )

    metrics: Optional[str] = Field(
        default=None,
        description="Comma-separated list of metrics to retrieve (leave empty for default metrics)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="tiktok_ads",
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
        advertiser_ids_str = self.advertiser_ids
        app_id = self.app_id
        secret = self.secret
        resources_str = self.resources
        start_date = self.start_date
        end_date = self.end_date
        report_type = self.report_type
        data_level = self.data_level
        metrics_str = self.metrics
        description = self.description or "TikTok Ads data ingestion via dlt"
        group_name = self.group_name
        include_sample = self.include_sample_metadata
        destination = self.destination
        persist_and_return = self.persist_and_return

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def tiktok_ads_ingestion_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Asset that ingests TikTok Ads data using dlt and returns as DataFrame."""

            context.log.info(f"Starting TikTok Ads ingestion for advertisers: {advertiser_ids_str}")

            # Parse advertiser IDs and resources
            advertiser_ids = [a.strip() for a in advertiser_ids_str.split(',')]
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

            # Determine dates for reports
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

            # Build REST API configuration for TikTok Marketing API
            # Base URL: https://business-api.tiktok.com/open_api/v1.3
            config = {
                "client": {
                    "base_url": "https://business-api.tiktok.com/open_api/v1.3",
                    "auth": {
                        "type": "bearer",
                        "token": access_token
                    }
                },
                "resources": []
            }

            # Add resource configurations based on requested resources
            if "advertisers" in resources_list:
                for advertiser_id in advertiser_ids:
                    config["resources"].append({
                        "name": f"advertisers_{advertiser_id}",
                        "endpoint": {
                            "path": "advertiser/info/",
                            "params": {
                                "advertiser_ids": f"[{advertiser_id}]"
                            },
                            "paginator": None
                        }
                    })

            if "campaigns" in resources_list:
                for advertiser_id in advertiser_ids:
                    config["resources"].append({
                        "name": f"campaigns_{advertiser_id}",
                        "endpoint": {
                            "path": "campaign/get/",
                            "params": {
                                "advertiser_id": advertiser_id,
                                "page_size": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "page"
                            }
                        }
                    })

            if "ad_groups" in resources_list:
                for advertiser_id in advertiser_ids:
                    config["resources"].append({
                        "name": f"ad_groups_{advertiser_id}",
                        "endpoint": {
                            "path": "adgroup/get/",
                            "params": {
                                "advertiser_id": advertiser_id,
                                "page_size": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "page"
                            }
                        }
                    })

            if "ads" in resources_list:
                for advertiser_id in advertiser_ids:
                    config["resources"].append({
                        "name": f"ads_{advertiser_id}",
                        "endpoint": {
                            "path": "ad/get/",
                            "params": {
                                "advertiser_id": advertiser_id,
                                "page_size": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "page"
                            }
                        }
                    })

            if "videos" in resources_list:
                for advertiser_id in advertiser_ids:
                    config["resources"].append({
                        "name": f"videos_{advertiser_id}",
                        "endpoint": {
                            "path": "file/video/ad/search/",
                            "params": {
                                "advertiser_id": advertiser_id,
                                "page_size": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "page"
                            }
                        }
                    })

            if "images" in resources_list:
                for advertiser_id in advertiser_ids:
                    config["resources"].append({
                        "name": f"images_{advertiser_id}",
                        "endpoint": {
                            "path": "file/image/ad/get/",
                            "params": {
                                "advertiser_id": advertiser_id,
                                "page_size": 100
                            },
                            "paginator": {
                                "type": "offset",
                                "limit": 100,
                                "offset_param": "page"
                            }
                        }
                    })

            if "reports" in resources_list:
                # Default metrics if not specified
                default_metrics = [
                    "spend", "impressions", "clicks", "ctr", "cpc", "cpm",
                    "conversions", "cost_per_conversion", "conversion_rate",
                    "video_play_actions", "video_watched_2s", "video_watched_6s",
                    "average_video_play", "average_video_play_per_user"
                ]

                metrics_list = metrics_str.split(',') if metrics_str else default_metrics

                for advertiser_id in advertiser_ids:
                    report_params = {
                        "advertiser_id": advertiser_id,
                        "service_type": "AUCTION",
                        "report_type": report_type,
                        "data_level": data_level,
                        "dimensions": '["stat_time_day"]',
                        "start_date": start_date_str,
                        "end_date": end_date_str,
                        "metrics": str(metrics_list),
                        "page_size": 1000
                    }

                    config["resources"].append({
                        "name": f"reports_{advertiser_id}",
                        "endpoint": {
                            "path": "report/integrated/get/",
                            "params": report_params,
                            "paginator": {
                                "type": "offset",
                                "limit": 1000,
                                "offset_param": "page"
                            }
                        }
                    })

            # Create REST API source
            context.log.info("Creating TikTok Ads REST API source...")
            source = rest_api_source(config)

            # Run pipeline
            context.log.info("Extracting TikTok Ads data...")
            load_info = pipeline.run(source)
            context.log.info(f"Loaded data from TikTok Marketing API")

            # Collect all data from all advertiser-specific resources
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
                        for advertiser_id in advertiser_ids:
                            table_names.append(f"{resource}_{advertiser_id}")

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
                "advertiser_ids": advertiser_ids,
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

        return Definitions(assets=[tiktok_ads_ingestion_asset])
