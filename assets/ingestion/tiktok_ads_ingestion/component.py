"""TikTok Ads Ingestion Component.

Ingest TikTok Ads data (campaigns, ad groups, ads, and performance metrics)
using dlt's REST API source with the TikTok Marketing API.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame.
Set `destination` to persist directly to any dlt-supported destination
(snowflake, bigquery, postgres, filesystem, etc.). See
`assets/ingestion/DESTINATIONS.md` for the full configuration reference.
"""

import os
from typing import Dict, List, Optional

import pandas as pd
import dlt
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class TikTokAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting TikTok Ads data using dlt.

    Uses dlt's REST API source to extract TikTok Ads data via the TikTok
    Marketing API. dlt handles pagination, rate limiting, and incremental loading
    automatically.

    Available resources: `advertisers`, `campaigns`, `ad_groups`, `ads`, `videos`,
    `images`, `reports`.

    Example:

        ```yaml
        type: dagster_component_templates.TikTokAdsIngestionComponent
        attributes:
          asset_name: tiktok_ads_data
          access_token: "${TIKTOK_ACCESS_TOKEN}"
          advertiser_ids: "1234567890,9876543210"
          resources: "campaigns,reports"
        ```

    To persist into a destination instead of returning a DataFrame, set
    `destination` and (optionally) `dataset_name` / `persist_only` /
    `destination_credentials_url`. See `../DESTINATIONS.md`.
    """

    # --- Source-specific fields ------------------------------------------------

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

    # --- Destination fields (see ../DESTINATIONS.md) --------------------------

    destination: Optional[str] = Field(
        default=None,
        description=(
            "dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', "
            "'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', "
            "'mssql', 'motherduck'). Leave empty for in-memory DuckDB → DataFrame mode."
        ),
    )

    dataset_name: Optional[str] = Field(
        default=None,
        description="Target dataset/schema in the destination. Defaults to the asset name.",
    )

    persist_only: bool = Field(
        default=False,
        description=(
            "If True with destination set: emit a MaterializeResult and skip DataFrame return. "
            "If False: query the destination back into a DataFrame (only meaningful for SQL "
            "destinations — non-SQL destinations always emit MaterializeResult)."
        ),
    )

    destination_credentials_url: Optional[str] = Field(
        default=None,
        description=(
            "Inline connection string passed to dlt's destination factory. Useful when one "
            "Dagster project ingests into multiple accounts of the same destination type. "
            "If unset, dlt resolves credentials from env vars — see ../DESTINATIONS.md."
        ),
    )

    destination_credentials_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Alternative to destination_credentials_url: name of an env var holding the "
            "connection string. Resolved at run-time."
        ),
    )

    # --- Standard asset metadata -----------------------------------------------

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="tiktok_ads", description="Asset group for organization"
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Auto-inferred from destination and asset name if not set.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    include_sample_metadata: bool = Field(
        default=True, description="Include sample data preview in metadata"
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])",
    )

    # --------------------------------------------------------------------------

    def _resolve_destination(self):
        """Build the dlt `destination` argument."""
        if not self.destination:
            return "duckdb"

        creds: Optional[str] = None
        if self.destination_credentials_url:
            creds = self.destination_credentials_url
        elif self.destination_credentials_env_var:
            creds = os.environ.get(self.destination_credentials_env_var)

        if creds:
            factory = getattr(dlt.destinations, self.destination, None)
            if factory is not None:
                return factory(credentials=creds)
        return self.destination

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        access_token = self.access_token
        advertiser_ids_str = self.advertiser_ids
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
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        component = self

        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "mssql": "mssql", "clickhouse": "clickhouse", "duckdb": "duckdb",
            "motherduck": "duckdb", "databricks": "databricks", "athena": "athena",
            "synapse": "azure", "fabric": "azure", "filesystem": "filesystem",
            "delta": "delta", "iceberg": "iceberg", "weaviate": "weaviate",
            "qdrant": "qdrant", "lancedb": "lance", "lance": "lance",
            "huggingface": "huggingface",
        }
        _inferred_kinds = list(self.kinds or [])
        if destination and destination in _kind_map:
            _inferred_kinds.append(_kind_map[destination])
        if not _inferred_kinds:
            for keyword, kind in _kind_map.items():
                if keyword in asset_name.lower():
                    _inferred_kinds.append(kind)
        if not _inferred_kinds:
            _inferred_kinds = ["python"]
        _inferred_kinds = list(dict.fromkeys(_inferred_kinds))

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []

        @asset(
            name=asset_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def tiktok_ads_ingestion_asset(context: AssetExecutionContext):
            from dlt.sources.rest_api import rest_api_source

            context.log.info(
                f"Starting TikTok Ads ingestion for advertisers: {advertiser_ids_str}, "
                f"destination={destination or 'duckdb (in-memory)'}"
            )

            advertiser_ids = [a.strip() for a in advertiser_ids_str.split(',')]
            resources_list = [r.strip() for r in resources_str.split(',')]
            context.log.info(f"Resources to extract: {resources_list}")

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

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination(),
                dataset_name=dataset_name,
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

            context.log.info("Creating TikTok Ads REST API source...")
            source = rest_api_source(config)

            context.log.info("Extracting TikTok Ads data...")
            load_info = pipeline.run(source)
            context.log.info(f"TikTok data loaded: {load_info}")

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "advertiser_ids": MetadataValue.json(advertiser_ids),
                "resources_requested": MetadataValue.json(resources_list),
                "start_date": MetadataValue.text(start_date_str),
                "end_date": MetadataValue.text(end_date_str),
            }

            non_sql_destinations = {
                "filesystem", "weaviate", "qdrant", "lancedb", "lance", "huggingface",
                "delta", "iceberg",
            }
            is_non_sql = destination in non_sql_destinations

            if persist_only or is_non_sql:
                if is_non_sql and not persist_only:
                    context.log.warning(
                        f"destination='{destination}' is not SQL-backed; cannot return DataFrame. "
                        f"Set persist_only=true to silence this warning."
                    )
                return MaterializeResult(metadata=base_metadata)

            all_data = []
            resource_metadata = {}
            with pipeline.sql_client() as client:
                tables_query = (
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{dataset_name}'"
                )
                try:
                    tables_df = client.execute_df(tables_query)
                    table_names = tables_df['table_name'].tolist()
                except Exception:
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

            if not all_data:
                context.log.warning("No data extracted from TikTok Ads.")
                return Output(value=pd.DataFrame(), metadata=base_metadata)

            combined_df = pd.concat(all_data, ignore_index=True)

            context.log.info(
                f"Extraction complete: {len(combined_df)} total rows, "
                f"{len(combined_df.columns)} columns"
            )

            metadata = {
                **base_metadata,
                "row_count": MetadataValue.int(len(combined_df)),
                "column_count": MetadataValue.int(len(combined_df.columns)),
                "resources_loaded": MetadataValue.json(list(resource_metadata.keys())),
            }
            for resource, rows in resource_metadata.items():
                metadata[f"rows_{resource}"] = MetadataValue.int(rows)
            if include_sample and len(combined_df) > 0:
                metadata["sample"] = MetadataValue.md(combined_df.head(10).to_markdown())

            return Output(value=combined_df, metadata=metadata)

        return Definitions(assets=[tiktok_ads_ingestion_asset])
