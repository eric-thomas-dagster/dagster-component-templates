"""Twitter/X Ads Ingestion Component.

Ingest Twitter/X Ads data (campaigns, line items, promoted tweets, and
performance metrics) using dlt's REST API source with the Twitter Ads API.

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


class TwitterAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Twitter/X Ads data using dlt.

    Uses dlt's REST API source to extract Twitter Ads data via the Twitter Ads
    API. dlt handles pagination, rate limiting, and incremental loading
    automatically. Authentication uses OAuth 1.0a.

    Available resources: `accounts`, `campaigns`, `line_items`, `promoted_tweets`,
    `tweets`, `media`, `analytics`.

    Example:

        ```yaml
        type: dagster_component_templates.TwitterAdsIngestionComponent
        attributes:
          asset_name: twitter_ads_data
          consumer_key: "${TWITTER_CONSUMER_KEY}"
          consumer_secret: "${TWITTER_CONSUMER_SECRET}"
          access_token: "${TWITTER_ACCESS_TOKEN}"
          access_token_secret: "${TWITTER_ACCESS_TOKEN_SECRET}"
          account_ids: "abc123,xyz789"
          resources: "campaigns,analytics"
        ```

    To persist into a destination instead of returning a DataFrame, set
    `destination` and (optionally) `dataset_name` / `persist_only` /
    `destination_credentials_url`. See `../DESTINATIONS.md`.
    """

    # --- Source-specific fields ------------------------------------------------

    asset_name: str = Field(
        description="Name of the asset that will hold the Twitter Ads data"
    )

    consumer_key: str = Field(
        description="Twitter API Consumer Key (API Key). Use ${TWITTER_CONSUMER_KEY} for env vars."
    )

    consumer_secret: str = Field(
        description="Twitter API Consumer Secret (API Secret). Use ${TWITTER_CONSUMER_SECRET} for env vars."
    )

    access_token: str = Field(
        description="Twitter Access Token. Use ${TWITTER_ACCESS_TOKEN} for env vars."
    )

    access_token_secret: str = Field(
        description="Twitter Access Token Secret. Use ${TWITTER_ACCESS_TOKEN_SECRET} for env vars."
    )

    account_ids: str = Field(
        description="Comma-separated list of Twitter Ads Account IDs (e.g., 'abc123,xyz789')"
    )

    resources: str = Field(
        default="campaigns,analytics",
        description="Comma-separated list of resources to extract: accounts, campaigns, line_items, promoted_tweets, tweets, media, analytics"
    )

    start_date: Optional[str] = Field(
        default=None,
        description="Start date for analytics (YYYY-MM-DD). Defaults to 30 days ago."
    )

    end_date: Optional[str] = Field(
        default=None,
        description="End date for analytics (YYYY-MM-DD). Defaults to today."
    )

    entity_type: str = Field(
        default="CAMPAIGN",
        description="Analytics entity type: ACCOUNT, FUNDING_INSTRUMENT, CAMPAIGN, LINE_ITEM, or PROMOTED_TWEET"
    )

    granularity: str = Field(
        default="DAY",
        description="Analytics granularity: HOUR, DAY, or TOTAL"
    )

    metric_groups: Optional[str] = Field(
        default=None,
        description="Comma-separated metric groups: ENGAGEMENT, BILLING, VIDEO, MOBILE_CONVERSION, WEB_CONVERSION, MEDIA, LIFE_TIME_VALUE_MOBILE_CONVERSION"
    )

    placement: str = Field(
        default="ALL_ON_TWITTER",
        description="Placement type: ALL_ON_TWITTER or PUBLISHER_NETWORK"
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
        default="twitter_ads", description="Asset group for organization"
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

    partition_type: Optional[str] = Field(

        default=None,

        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', or None for unpartitioned. With a partition type set, the partition key is exposed via context.partition_key for use in filtering / templating.",

    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )


    retry_policy_max_retries: Optional[int] = Field(


        default=None,


        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",


    )


    retry_policy_delay_seconds: Optional[int] = Field(


        default=None,


        description="Seconds between retries (default 1).",


    )


    retry_policy_backoff: str = Field(


        default="exponential",


        description="Backoff strategy: 'linear' or 'exponential'.",


    )



    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        consumer_key = self.consumer_key
        consumer_secret = self.consumer_secret
        access_token = self.access_token
        access_token_secret = self.access_token_secret
        account_ids_str = self.account_ids
        resources_str = self.resources
        start_date = self.start_date
        end_date = self.end_date
        entity_type = self.entity_type
        granularity = self.granularity
        metric_groups_str = self.metric_groups
        placement = self.placement
        description = self.description or "Twitter/X Ads data ingestion via dlt"
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

        # Build partition definition (auto-generated; supports daily, weekly,

        # monthly, hourly partitions out of the box).

        partitions_def = None

        if self.partition_type:

            from dagster import (

                DailyPartitionsDefinition, WeeklyPartitionsDefinition,

                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,

            )

            _pstart = self.partition_start or "2024-01-01"

            if self.partition_type == "daily":

                partitions_def = DailyPartitionsDefinition(start_date=_pstart)

            elif self.partition_type == "weekly":

                partitions_def = WeeklyPartitionsDefinition(start_date=_pstart)

            elif self.partition_type == "monthly":

                partitions_def = MonthlyPartitionsDefinition(start_date=_pstart)

            elif self.partition_type == "hourly":

                partitions_def = HourlyPartitionsDefinition(start_date=_pstart)


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=asset_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def twitter_ads_ingestion_asset(context: AssetExecutionContext):
            from dlt.sources.rest_api import rest_api_source

            context.log.info(
                f"Starting Twitter Ads ingestion for accounts: {account_ids_str}, "
                f"destination={destination or 'duckdb (in-memory)'}"
            )

            account_ids = [a.strip() for a in account_ids_str.split(',')]
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

            # Twitter Ads API uses OAuth 1.0a
            import base64
            import hmac
            import hashlib
            import urllib.parse
            import secrets
            import time

            def generate_oauth_header(method, url, params=None):
                """Generate OAuth 1.0a authorization header for Twitter Ads API."""
                oauth_params = {
                    "oauth_consumer_key": consumer_key,
                    "oauth_token": access_token,
                    "oauth_signature_method": "HMAC-SHA1",
                    "oauth_timestamp": str(int(time.time())),
                    "oauth_nonce": secrets.token_hex(16),
                    "oauth_version": "1.0"
                }

                all_params = {**oauth_params}
                if params:
                    all_params.update(params)

                sorted_params = sorted(all_params.items())
                param_string = "&".join([f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in sorted_params])
                base_string = f"{method}&{urllib.parse.quote(url, safe='')}&{urllib.parse.quote(param_string, safe='')}"

                signing_key = f"{urllib.parse.quote(consumer_secret, safe='')}&{urllib.parse.quote(access_token_secret, safe='')}"

                signature = base64.b64encode(
                    hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha1).digest()
                ).decode()

                oauth_params["oauth_signature"] = signature

                auth_header = "OAuth " + ", ".join([f'{k}="{urllib.parse.quote(str(v), safe="")}"' for k, v in sorted(oauth_params.items())])
                return auth_header

            # Build REST API configuration for Twitter Ads API
            # Base URL: https://ads-api.twitter.com/12
            config = {
                "client": {
                    "base_url": "https://ads-api.twitter.com/12",
                    "auth": {
                        "type": "oauth1",
                        "consumer_key": consumer_key,
                        "consumer_secret": consumer_secret,
                        "access_token": access_token,
                        "access_token_secret": access_token_secret
                    }
                },
                "resources": []
            }

            if "accounts" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"accounts_{account_id}",
                        "endpoint": {
                            "path": f"accounts/{account_id}",
                            "paginator": None
                        }
                    })

            if "campaigns" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"campaigns_{account_id}",
                        "endpoint": {
                            "path": f"accounts/{account_id}/campaigns",
                            "params": {
                                "count": 200,
                                "with_deleted": False
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "next_cursor",
                                "cursor_param": "cursor"
                            }
                        }
                    })

            if "line_items" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"line_items_{account_id}",
                        "endpoint": {
                            "path": f"accounts/{account_id}/line_items",
                            "params": {
                                "count": 200,
                                "with_deleted": False
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "next_cursor",
                                "cursor_param": "cursor"
                            }
                        }
                    })

            if "promoted_tweets" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"promoted_tweets_{account_id}",
                        "endpoint": {
                            "path": f"accounts/{account_id}/promoted_tweets",
                            "params": {
                                "count": 200,
                                "with_deleted": False
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "next_cursor",
                                "cursor_param": "cursor"
                            }
                        }
                    })

            if "tweets" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"tweets_{account_id}",
                        "endpoint": {
                            "path": f"accounts/{account_id}/tweets",
                            "params": {
                                "count": 200,
                                "with_deleted": False
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "next_cursor",
                                "cursor_param": "cursor"
                            }
                        }
                    })

            if "media" in resources_list:
                for account_id in account_ids:
                    config["resources"].append({
                        "name": f"media_{account_id}",
                        "endpoint": {
                            "path": f"accounts/{account_id}/media_library",
                            "params": {
                                "count": 200
                            },
                            "paginator": {
                                "type": "cursor",
                                "cursor_path": "next_cursor",
                                "cursor_param": "cursor"
                            }
                        }
                    })

            if "analytics" in resources_list:
                if metric_groups_str:
                    metric_groups_list = [m.strip() for m in metric_groups_str.split(',')]
                else:
                    metric_groups_list = ["ENGAGEMENT", "BILLING"]

                for account_id in account_ids:
                    analytics_params = {
                        "entity": entity_type,
                        "start_time": start_date_str,
                        "end_time": end_date_str,
                        "granularity": granularity,
                        "metric_groups": ",".join(metric_groups_list),
                        "placement": placement
                    }

                    config["resources"].append({
                        "name": f"analytics_{account_id}",
                        "endpoint": {
                            "path": f"stats/accounts/{account_id}",
                            "params": analytics_params,
                            "paginator": None
                        }
                    })

            context.log.info("Creating Twitter Ads REST API source...")
            source = rest_api_source(config)

            context.log.info("Extracting Twitter Ads data...")
            load_info = pipeline.run(source)
            context.log.info(f"Twitter data loaded: {load_info}")

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "account_ids": MetadataValue.json(account_ids),
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

            if not all_data:
                context.log.warning("No data extracted from Twitter Ads.")
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

        return Definitions(assets=[twitter_ads_ingestion_asset])
