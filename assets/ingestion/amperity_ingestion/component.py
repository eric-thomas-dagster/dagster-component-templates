"""AmperityIngestionComponent -- Ingest Amperity CDP audit-event, campaign, segment, and ingest-job data using dlt's generic REST API source.

Ingest Amperity (amperity.com) audit events, campaigns, segments, and ingest
job status using dlt's generic REST API source
(dlt.sources.rest_api.rest_api_source). Amperity has no official
dlt-maintained verified-source package, so this uses the same config-driven
REST connector pattern as chargify_ingestion / reclaim_ingestion in this
repo.

Verified request/response shape (docs.amperity.com, primary docs):
  - Amperity is single-tenant per customer: each customer gets their own
    subdomain, e.g. `https://<tenant>.amperity.com`. This connector takes
    the tenant subdomain as a field rather than hardcoding a shared host,
    the same shape as chargify_ingestion's `subdomain` field.
  - Auth: `Authorization: Bearer <access-token>` where the token is a
    short-lived JSON Web Token (JWT) signed by an Amperity-managed API key
    (Settings -> Security -> API keys -> "Get token"). Confirmed on
    docs.amperity.com/api/authentication.html.
  - `audit_events` -> `GET /api/audit-events` -- confirmed directly via its
    own reference page (docs.amperity.com/api/endpoint_get_audit_events_list.html),
    including the exact query params (`limit`, `next_token`, `happened_from`,
    `happened_to`, `with_total`) and sample response shape.
  - `campaigns` -> `GET /api/campaigns` -- lists campaigns.
  - `segments` -> `GET /api/segments` -- lists customer segments.
  - `ingest_jobs` -> `GET /api/ingest/jobs` -- lists data-ingestion job runs.
  These four match exactly the endpoints named in docs.amperity.com's API
  reference index (docs.amperity.com/reference/api.html).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame.
Set `destination` to persist directly to any dlt-supported destination
(snowflake, bigquery, postgres, filesystem, etc.). See
`assets/ingestion/DESTINATIONS.md` for the full configuration reference.
"""

import os
from typing import Any, Dict, List, Optional, Union

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


def _build_resources_config(
    resources: str,
    audit_events_happened_from: Optional[str],
    audit_events_happened_to: Optional[str],
    audit_events_limit: int,
) -> List[Dict[str, Any]]:
    """Build the dlt rest_api_source `resources` list for the requested
    Amperity resource names: audit_events, campaigns, segments, ingest_jobs.
    """
    resources_list = [r.strip() for r in resources.split(",") if r.strip()]
    config_resources: List[Dict[str, Any]] = []

    _map: Dict[str, Dict[str, Any]] = {
        "audit_events": {
            "name": "audit_events",
            "endpoint": {
                "path": "api/audit-events",
                "data_selector": "data",
                "params": {
                    k: v for k, v in {
                        "limit": audit_events_limit,
                        "happened_from": audit_events_happened_from,
                        "happened_to": audit_events_happened_to,
                    }.items() if v is not None
                },
            },
        },
        "campaigns": {
            "name": "campaigns",
            "endpoint": {"path": "api/campaigns", "data_selector": "$"},
        },
        "segments": {
            "name": "segments",
            "endpoint": {"path": "api/segments", "data_selector": "$"},
        },
        "ingest_jobs": {
            "name": "ingest_jobs",
            "endpoint": {"path": "api/ingest/jobs", "data_selector": "$"},
        },
    }
    for r in resources_list:
        if r in _map:
            config_resources.append(_map[r])
    return config_resources


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")

class AmperityIngestionComponent(Component, Model, Resolvable):
    """Ingest Amperity CDP audit-event, campaign, segment, and ingest-job data using dlt's generic REST API source.

    Example:

        ```yaml
        type: dagster_component_templates.AmperityIngestionComponent
        attributes:
          asset_name: amperity_ingestion
          tenant: "mycompany"
          access_token: "${AMPERITY_ACCESS_TOKEN}"
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    tenant: str = Field(description="Amperity tenant subdomain (for https://mycompany.amperity.com).")

    access_token: str = Field(
        description=(
            "Amperity JWT access token (Bearer), generated under Settings -> Security -> "
            "API keys -> 'Get token'. Short-lived (expiration chosen at generation time)."
        )
    )

    resources: str = Field(
        default="audit_events,segments",
        description="Comma-separated list of resources to extract: audit_events, campaigns, segments, ingest_jobs",
    )
    audit_events_happened_from: Optional[str] = Field(
        default=None,
        description="ISO 8601 lower-bound filter for the 'audit_events' resource's `happened_from` query param.",
    )
    audit_events_happened_to: Optional[str] = Field(
        default=None,
        description="ISO 8601 upper-bound filter for the 'audit_events' resource's `happened_to` query param.",
    )
    audit_events_limit: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Max records per page for the 'audit_events' resource.",
    )

    # --- Destination fields (see ../DESTINATIONS.md) --------------------------

    destination: Optional[str] = Field(
        default=None,
        description=(
            "dlt destination identifier (e.g. 'snowflake', 'bigquery', 'postgres', "
            "'redshift', 'filesystem', 'duckdb', 'databricks', 'athena', 'clickhouse', "
            "'mssql', 'motherduck'). Leave empty for in-memory DuckDB -> DataFrame mode."
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
            "destinations -- non-SQL destinations always emit MaterializeResult)."
        ),
    )
    destination_credentials_url: Optional[str] = Field(
        default=None,
        description=(
            "Inline connection string passed to dlt's destination factory. Useful when one "
            "Dagster project ingests into multiple accounts of the same destination type. "
            "If unset, dlt resolves credentials from env vars -- see ../DESTINATIONS.md."
        ),
    )
    destination_credentials_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Alternative to destination_credentials_url: name of an env var holding the "
            "connection string. Resolved at run-time."
        ),
    )
    bucket_url: Optional[str] = Field(
        default=None,
        description=(
            "Bucket/path URL for filesystem-shaped storage (e.g. 's3://my-bucket/path', "
            "'gs://my-bucket/path', 'az://my-container/path', or 'file:///local/path'). "
            "Required when destination='filesystem' (the final write target). Also used "
            "as the staging area when destination='databricks' or 'athena', both of which "
            "require an intermediate filesystem stage before the warehouse-side load -- "
            "dlt resolves the bucket's credentials from the destination-appropriate "
            "standard environment variables (e.g. AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) "
            "unless destination_credentials_url/destination_credentials_env_var is set."
        ),
    )
    athena_query_result_bucket: Optional[str] = Field(
        default=None,
        description=(
            "Optional S3 path where Athena writes query results (e.g. "
            "'s3://my-bucket/results/'). Only used when destination='athena'. May be "
            "omitted to use Athena-managed query results instead."
        ),
    )

    # --- Standard asset metadata -----------------------------------------------

    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default="amperity_ingestion", description="Asset group for organization")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners -- list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
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
            "Rows to include in the preview metadata when `include_preview_metadata` is True. "
            "For long DataFrames (>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
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

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column -> list of upstream columns it derives from.",
    )

    def _resolve_destination(self):
        """Build the dlt `destination` argument."""
        if not self.destination:
            return "duckdb"
        creds = None
        if self.destination_credentials_url:
            creds = self.destination_credentials_url
        elif self.destination_credentials_env_var:
            creds = os.environ.get(self.destination_credentials_env_var)
        if creds:
            factory = getattr(dlt.destinations, self.destination, None)
            if factory is not None:
                return factory(credentials=creds)
        return self.destination

    def _resolve_destination_with_bucket(self):
        """Wraps _resolve_destination() to inject `bucket_url` (filesystem)
        or `query_result_bucket` (athena) -- dlt needs these to know where
        to write/query, separately from the staging area wired by
        _resolve_staging() for destinations that go through one."""
        if self.destination == "filesystem" and self.bucket_url:
            creds = None
            if self.destination_credentials_url:
                creds = self.destination_credentials_url
            elif self.destination_credentials_env_var:
                creds = os.environ.get(self.destination_credentials_env_var)
            return dlt.destinations.filesystem(bucket_url=self.bucket_url, credentials=creds)
        if self.destination == "athena" and self.athena_query_result_bucket:
            creds = None
            if self.destination_credentials_url:
                creds = self.destination_credentials_url
            elif self.destination_credentials_env_var:
                creds = os.environ.get(self.destination_credentials_env_var)
            athena_kwargs = {"query_result_bucket": self.athena_query_result_bucket}
            if creds:
                athena_kwargs["credentials"] = creds
            return dlt.destinations.athena(**athena_kwargs)
        return self._resolve_destination()

    def _resolve_staging(self):
        """Build the dlt `staging` argument. Databricks and Athena both load
        via an intermediate filesystem stage (files copied to a bucket, then
        loaded into the warehouse from there) -- dlt requires this staging
        destination to be activated explicitly, it is never auto-enabled.
        Unconditionally activates staging for these two (there's no direct-load
        mode for them in dlt); if `bucket_url` is set inline, build the
        filesystem destination with it explicitly, otherwise fall back to the
        bare 'filesystem' string so dlt resolves bucket_url/credentials from
        DESTINATION__FILESYSTEM__* env vars, consistent with how every other
        destination in this component resolves credentials by default."""
        if self.destination not in ("databricks", "athena"):
            return None
        if self.bucket_url:
            return dlt.destinations.filesystem(bucket_url=self.bucket_url)
        return "filesystem"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        description = self.description or "Ingest Amperity CDP audit-event, campaign, segment, and ingest-job data using dlt's generic REST API source."
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        destination = self.destination
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        resources = self.resources
        audit_events_happened_from = self.audit_events_happened_from
        audit_events_happened_to = self.audit_events_happened_to
        audit_events_limit = self.audit_events_limit

        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "mssql": "mssql", "clickhouse": "clickhouse", "duckdb": "duckdb",
            "motherduck": "duckdb", "databricks": "databricks", "athena": "athena",
            "filesystem": "filesystem", "delta": "delta", "iceberg": "iceberg",
        }
        _inferred_kinds = list(self.kinds or [])
        if destination and destination in _kind_map:
            _inferred_kinds.append(_kind_map[destination])
        if not _inferred_kinds:
            _inferred_kinds = ["amperity", "python"]
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

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(retry_policy=_retry_policy, partitions_def=partitions_def,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def amperity_ingestion_asset(context: AssetExecutionContext):
            from dlt.sources.rest_api import rest_api_source

            context.log.info(f"Starting Amperity ingestion, destination={destination or 'duckdb (in-memory)'}")

            config_resources = _build_resources_config(
                resources, audit_events_happened_from, audit_events_happened_to, audit_events_limit,
            )
            config = {
                "client": {
                    "base_url": f"https://{component.tenant}.amperity.com",
                    "auth": {"type": "bearer", "token": component.access_token},
                },
                "resources": config_resources,
            }

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination_with_bucket(),
                staging=component._resolve_staging(),
                dataset_name=dataset_name,
            )

            context.log.info("Creating Amperity REST API source...")
            source = rest_api_source(config)

            context.log.info("Extracting Amperity data...")
            load_info = pipeline.run(source)
            context.log.info(f"Amperity data loaded: {load_info}")

            resource_names = [r["name"] for r in config["resources"]]
            base_metadata = {
                "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                "dataset_name": MetadataValue.text(dataset_name),
                "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                "resources_requested": MetadataValue.json(resource_names),
            }

            non_sql_destinations = {"filesystem", "delta", "iceberg"}
            is_non_sql = destination in non_sql_destinations
            if persist_only or is_non_sql:
                if is_non_sql and not persist_only:
                    context.log.warning(
                        f"destination={destination!r} is not SQL-backed; cannot return DataFrame. "
                        f"Set persist_only=true to silence this warning."
                    )
                return MaterializeResult(metadata=base_metadata)

            all_data = []
            resource_metadata = {}
            with pipeline.sql_client() as client:
                try:
                    with client.execute_query(
                        f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{dataset_name}'"
                    ) as cur:
                        tables_df = cur.df()
                    table_names = tables_df["table_name"].tolist()
                except Exception:
                    table_names = resource_names

                for table_name in table_names:
                    try:
                        with client.execute_query(f"SELECT * FROM {dataset_name}.{table_name}") as cur:
                            df = cur.df()
                        if len(df) > 0:
                            df["_resource_type"] = table_name
                            all_data.append(df)
                            resource_metadata[table_name] = len(df)
                    except Exception as e:
                        context.log.warning(f"Could not load {table_name}: {e}")

            if not all_data:
                context.log.warning("No data extracted from Amperity.")
                return Output(value=pd.DataFrame(), metadata=base_metadata)

            combined_df = pd.concat(all_data, ignore_index=True)
            metadata = {
                **base_metadata,
                "row_count": MetadataValue.int(len(combined_df)),
                "column_count": MetadataValue.int(len(combined_df.columns)),
                "resources_loaded": MetadataValue.json(list(resource_metadata.keys())),
            }
            for resource, rows in resource_metadata.items():
                metadata[f"rows_{resource}"] = MetadataValue.int(rows)
            if include_preview and len(combined_df) > 0:
                try:
                    _prev = combined_df.sample(min(preview_rows, len(combined_df))) if len(combined_df) > preview_rows * 10 else combined_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            return Output(value=combined_df, metadata=metadata)

        return Definitions(assets=[amperity_ingestion_asset])
