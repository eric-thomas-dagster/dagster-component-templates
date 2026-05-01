"""Pinterest Ads Ingestion Component.

Ingest Pinterest Ads data (campaigns, ad groups, ads, pins, and performance metrics)
using dlt's REST API source with the Pinterest Marketing API.

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


class PinterestAdsIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Pinterest Ads data using dlt.

    Uses dlt's REST API source to extract Pinterest Ads data via the Pinterest
    Marketing API. dlt handles pagination, rate limiting, and incremental loading
    automatically.

    Available resources: `ad_accounts`, `campaigns`, `ad_groups`, `ads`, `pins`,
    `keywords`, `analytics`.

    Example:

        ```yaml
        type: dagster_component_templates.PinterestAdsIngestionComponent
        attributes:
          asset_name: pinterest_ads_data
          access_token: "${PINTEREST_ACCESS_TOKEN}"
          ad_account_ids: "123456789,987654321"
          resources: "campaigns,analytics"
        ```

    To persist into a destination instead of returning a DataFrame, set
    `destination` and (optionally) `dataset_name` / `persist_only` /
    `destination_credentials_url`. See `../DESTINATIONS.md`.
    """

    # --- Source-specific fields ------------------------------------------------

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
        default="pinterest_ads", description="Asset group for organization"
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
        """Build the dlt `destination` argument.

        Returns a `dlt.destinations.<name>(credentials=...)` factory call when
        inline credentials are provided; otherwise returns the bare destination
        string and lets dlt's config layer resolve credentials from env vars.
        """
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
            # Long-tail destination not exposed as a factory — fall back to
            # the bare string and let dlt resolve credentials from env vars.
        return self.destination

    partition_type: Optional[str] = Field(

        default=None,

        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', or None for unpartitioned. With a partition type set, the partition key is exposed via context.partition_key for use in filtering / templating.",

    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )


    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        access_token = self.access_token
        ad_account_ids_str = self.ad_account_ids
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
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        component = self

        # Infer kinds from destination + asset name
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


        @asset(partitions_def=partitions_def, 
            name=asset_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def pinterest_ads_ingestion_asset(context: AssetExecutionContext):
            from dlt.sources.rest_api import rest_api_source

            context.log.info(
                f"Starting Pinterest Ads ingestion for accounts: {ad_account_ids_str}, "
                f"destination={destination or 'duckdb (in-memory)'}"
            )

            # Parse account IDs and resources
            ad_account_ids = [a.strip() for a in ad_account_ids_str.split(',')]
            resources_list = [r.strip() for r in resources_str.split(',')]
            context.log.info(f"Resources to extract: {resources_list}")

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

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination(),
                dataset_name=dataset_name,
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
            context.log.info(f"Pinterest data loaded: {load_info}")

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "ad_account_ids": MetadataValue.json(ad_account_ids),
                "resources_requested": MetadataValue.json(resources_list),
                "start_date": MetadataValue.text(start_date_str),
                "end_date": MetadataValue.text(end_date_str),
            }

            # Decide return shape
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

            # Query the destination back into a DataFrame
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

            if not all_data:
                context.log.warning("No data extracted from Pinterest Ads.")
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

        return Definitions(assets=[pinterest_ads_ingestion_asset])
