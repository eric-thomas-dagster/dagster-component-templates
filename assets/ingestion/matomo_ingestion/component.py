"""Matomo Ingestion Component.

Ingest Matomo analytics report data using dlt's verified `matomo` source.

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


class MatomoIngestionComponent(Component, Model, Resolvable):
    """Component for ingesting Matomo analytics data using dlt.

    Extracts Matomo reports (e.g., VisitsSummary, Actions, Events, Goals) for
    a given site ID. dlt handles API pagination and incremental loading.

    Example:

        ```yaml
        type: dagster_component_templates.MatomoIngestionComponent
        attributes:
          asset_name: matomo_data
          api_url: "https://analytics.example.com"
          api_token: "{{ env('MATOMO_API_TOKEN') }}"
          site_id: "1"
          reports: [VisitsSummary, Actions]
        ```

    To persist into a destination instead of returning a DataFrame, set
    `destination` and (optionally) `dataset_name` / `persist_only` /
    `destination_credentials_url`. See `../DESTINATIONS.md`.
    """

    # --- Source-specific fields ------------------------------------------------

    asset_name: str = Field(description="Name of the asset to create")

    api_url: str = Field(description="Matomo instance URL (e.g., https://analytics.example.com)")

    api_token: str = Field(description="Matomo API token for authentication")

    site_id: str = Field(description="Matomo site ID to extract data from")

    reports: List[str] = Field(default=["VisitsSummary", "Actions"], description="List of Matomo report types to extract (e.g., VisitsSummary, Actions, Events, Goals)")

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
        default="matomo", description="Asset group for organization"
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

    include_preview_metadata: bool = Field(
        default=True, description="Include sample data preview in metadata"
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
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
        api_url = self.api_url
        api_token = self.api_token
        site_id = self.site_id
        reports = self.reports
        description = self.description or f"Matomo data ({', '.join(reports)})"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
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
        _inferred_kinds = list(dict.fromkeys(_inferred_kinds))  # de-dupe, preserve order

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
        def matomo_ingestion_asset(context: AssetExecutionContext):
            from dlt.sources.matomo import matomo_reports

            context.log.info(
                f"Starting Matomo ingestion: site={site_id}, reports={reports}, "
                f"destination={destination or 'duckdb (in-memory)'}"
            )

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination(),
                dataset_name=dataset_name,
            )

            source = matomo_reports(
                api_url=api_url,
                api_token=api_token,
                site_id=site_id,
                queries=reports,
            )

            load_info = pipeline.run(source)
            context.log.info(f"Matomo data loaded: {load_info}")

            resource_keys = [r.lower() for r in reports]

            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "resources_extracted": MetadataValue.json(list(resource_keys)),
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
            for resource_name in resource_keys:
                try:
                    query = f"SELECT * FROM {dataset_name}.{resource_name}"
                    with pipeline.sql_client() as client:
                        with client.execute_query(query) as cursor:
                            columns = [d[0] for d in cursor.description]
                            rows = cursor.fetchall()
                    if rows:
                        df = pd.DataFrame(rows, columns=columns)
                        df["_resource_type"] = resource_name
                        all_data.append(df)
                        context.log.info(f"Extracted {len(df)} rows from {resource_name}")
                except Exception as e:
                    context.log.warning(f"Could not extract {resource_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted.")
                return Output(value=pd.DataFrame(), metadata=base_metadata)

            combined_df = pd.concat(all_data, ignore_index=True)
            context.log.info(
                f"Ingestion complete: {len(combined_df)} total rows from {len(all_data)} resources"
            )

            metadata = {
                **base_metadata,
                "row_count": MetadataValue.int(len(combined_df)),
                "resource_types": MetadataValue.json(
                    list(combined_df["_resource_type"].unique())
                    if "_resource_type" in combined_df.columns
                    else []
                ),
            }
            if include_preview and len(combined_df) > 0:
                try:
                    _prev = combined_df.sample(min(preview_rows, len(combined_df))) if len(combined_df) > preview_rows * 10 else combined_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return Output(value=combined_df, metadata=metadata)

        return Definitions(assets=[matomo_ingestion_asset])
