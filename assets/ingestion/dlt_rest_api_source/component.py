"""DltRestApiSourceComponent -- Bring-your-own REST API source config, using
dlt's generic REST API source engine directly (dlt.sources.rest_api.rest_api_source).

This is the escape hatch for the long tail of vendors this catalog will
never hand-build a dedicated component for. Every hand-built `*_ingestion`
component in this repo (chargify_ingestion, reclaim_ingestion,
hotjar_ingestion, and ~20 others this session) is a thin Python wrapper
around exactly the same underlying call:

    from dlt.sources.rest_api import rest_api_source
    source = rest_api_source({
        "client": {"base_url": ..., "auth": {...}},
        "resources": [{"name": ..., "endpoint": {"path": ..., ...}}, ...],
    })

Those wrappers exist to give a vendor typed fields (`subdomain`, `api_key`,
a `resources` enum) plus committed tests pinned to that vendor's real API
shape. This component skips the typed-fields layer entirely and exposes
the underlying `client` / `resources` config dicts DIRECTLY as resolvable
YAML attributes, so any REST API -- one this catalog has no dedicated
component for yet, an internal/private API, or a one-off integration not
worth hand-building -- can be wired up with zero new Python code.

Secrets: dlt_rest_api_source does no secret-resolution of its own. Use
this repo's existing `{{ env.VAR_NAME }}` component-YAML templating
(the same Jinja-based resolution already used by e.g.
integrations/snowflake_workspace's nested `workspace:` block) anywhere
inside `client:` / `resources:` -- it's resolved by dg's component loader
before these values ever reach this component, so arbitrarily nested
auth/header/param dicts can pull secrets from the environment with no
special-casing here. See example.yaml.

See https://dlthub.com/context/source/<vendor> for dltHub's community
"Context" marketplace of pre-researched `rest_api_source` config snippets
for specific vendors -- a good starting point to paste into `client:` /
`resources:` here (see README.md for more).

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


def _validate_rest_api_config(client: Dict[str, Any], resources: List[Dict[str, Any]]) -> None:
    """Validate the raw dlt `rest_api_source` config shape with clear,
    actionable errors instead of a raw pydantic/dlt traceback.

    This component is a pure passthrough to
    `dlt.sources.rest_api.rest_api_source(config)` -- there's no
    vendor-specific logic to validate against a known API shape, so this
    is the one real guardrail standing between a misconfigured `client:` /
    `resources:` block and a cryptic failure deep inside dlt's resource
    extraction (or, worse, a silent no-op).
    """
    if not isinstance(client, dict) or not client.get("base_url"):
        raise ValueError(
            "dlt_rest_api_source: 'client.base_url' is required, e.g.\n"
            "  client:\n"
            "    base_url: \"https://api.example.com\""
        )
    if not resources:
        raise ValueError(
            "dlt_rest_api_source: 'resources' must be a non-empty list of dlt "
            "resource configs, e.g.\n"
            "  resources:\n"
            "    - name: customers\n"
            "      endpoint:\n"
            "        path: customers"
        )
    for i, resource in enumerate(resources):
        if not isinstance(resource, dict):
            raise ValueError(
                f"dlt_rest_api_source: resources[{i}] must be a mapping (dict), "
                f"got {type(resource).__name__}. Each entry needs at least "
                f"'name' and 'endpoint: {{path: ...}}'."
            )
        name = resource.get("name")
        if not name:
            raise ValueError(
                f"dlt_rest_api_source: resources[{i}] is missing the required "
                f"'name' field, e.g. {{name: 'customers', endpoint: {{path: 'customers'}}}}"
            )
        endpoint = resource.get("endpoint")
        if not isinstance(endpoint, dict):
            got = "nothing (key not set)" if endpoint is None else type(endpoint).__name__
            raise ValueError(
                f"dlt_rest_api_source: resources[{i}] ({name!r}) has a malformed "
                f"'endpoint' -- expected a mapping like {{path: 'customers', "
                f"data_selector: '$'}}, got {got}."
            )
        if not endpoint.get("path"):
            raise ValueError(
                f"dlt_rest_api_source: resources[{i}] ({name!r})'s 'endpoint' is "
                f"missing the required 'path' field, e.g. endpoint: {{path: 'customers'}}"
            )


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

class DltRestApiSourceComponent(Component, Model, Resolvable):
    """Bring-your-own REST API source config -- the generic escape hatch for
    any REST API, using dlt's `rest_api_source` engine directly.

    Advanced / escape-hatch option: if a dedicated hand-built component for
    your vendor already exists in this catalog, prefer that one -- it has
    typed fields and committed tests pinned to that vendor's real API
    shape. Use this component for the long tail: a vendor with no
    dedicated component yet, an internal/private API, or a one-off
    integration not worth hand-building.

    `client` and `resources` are the exact `dlt.sources.rest_api.rest_api_source`
    config shape (https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api):

        client:
          base_url: "https://api.example.com"
          auth:
            type: bearer           # or api_key / http_basic / oauth2_client_credentials
            token: "{{ env.API_TOKEN }}"
        resources:
          - name: customers
            endpoint:
              path: customers
              data_selector: "$"

    See https://dlthub.com/context/source/<vendor> for pre-researched
    config snippets for specific vendors that can often be pasted in here
    directly.

    Example:

        ```yaml
        type: dagster_component_templates.DltRestApiSourceComponent
        attributes:
          asset_name: my_vendor_ingestion
          client:
            base_url: "https://api.example.com"
            auth:
              type: bearer
              token: "{{ env.MY_VENDOR_API_TOKEN }}"
          resources:
            - name: customers
              endpoint:
                path: customers
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    client: Dict[str, Any] = Field(
        description=(
            "dlt rest_api_source `client` config -- required keys/shape: "
            "`base_url` (required), `auth` (a dict; supports dlt's built-in "
            "auth types: {type: bearer, token: ...}, {type: api_key, "
            "name: ..., api_key: ..., location: header|query}, "
            "{type: http_basic, username: ..., password: ...}, "
            "{type: oauth2_client_credentials, access_token_url: ..., "
            "client_id: ..., client_secret: ...}), optional `headers` "
            "(dict), optional `paginator` (dict, e.g. {type: cursor, "
            "cursor_param: ..., cursor_path: ...} or {type: json_link, "
            "next_url_path: ...}). Use '{{ env.VAR_NAME }}' for any secret "
            "value -- resolved by dg's component-YAML loader before this "
            "component ever sees it."
        )
    )

    resources: List[Dict[str, Any]] = Field(
        description=(
            "dlt rest_api_source `resources` list -- one dict per resource, "
            "each requiring `name` and `endpoint` (`endpoint.path` is "
            "required; optional `endpoint.data_selector`, `endpoint.params`, "
            "`endpoint.paginator`). Supports dlt's dependent-resource "
            "`resolve` shape for parent/child chaining, e.g. a child "
            "resource's endpoint.params can be "
            "`{survey_id: {type: resolve, resource: surveys, field: id}}` "
            "to issue one request per row of a parent 'surveys' resource "
            "(see hotjar_ingestion in this repo for a full worked example "
            "of this exact shape). Must be non-empty."
        )
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
    group_name: Optional[str] = Field(default="dlt_rest_api_source", description="Asset group for organization")
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
        description="Asset kinds for the Dagster catalog. Auto-inferred from destination if not set.",
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
        description = self.description or "Ingest data from a user-configured REST API using dlt's generic REST API source (bring-your-own config)."
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        destination = self.destination
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        client_config = self.client
        resources_config = self.resources

        # Fail fast with a clear message at Definitions-build time, before
        # this ever reaches dlt's own (much less friendly) validation.
        _validate_rest_api_config(client_config, resources_config)

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
            _inferred_kinds = ["rest_api", "python"]
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
        def dlt_rest_api_source_asset(context: AssetExecutionContext):
            from dlt.sources.rest_api import rest_api_source

            context.log.info(f"Starting dlt_rest_api_source ingestion, destination={destination or 'duckdb (in-memory)'}")

            config = {
                "client": client_config,
                "resources": resources_config,
            }

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination_with_bucket(),
                staging=component._resolve_staging(),
                dataset_name=dataset_name,
            )

            context.log.info("Creating REST API source from user-supplied config...")
            source = rest_api_source(config)

            context.log.info("Extracting data...")
            load_info = pipeline.run(source)
            context.log.info(f"Data loaded: {load_info}")

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
                context.log.warning("No data extracted.")
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

        return Definitions(assets=[dlt_rest_api_source_asset])
