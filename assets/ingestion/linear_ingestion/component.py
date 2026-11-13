"""LinearIngestionComponent -- Ingest Linear issue-tracker data.

Linear's API is GraphQL-only (https://api.linear.app/graphql) -- there is no
REST surface, so this connector does NOT use dlt's generic
rest_api_source (that machinery assumes REST paths/pagination and would be
a poor, misleading fit here). Instead it defines plain dlt @dlt.resource
generators that page through Linear's GraphQL cursor pagination
(pageInfo.hasNextPage / endCursor) directly via `requests`, then hands
those resources to a normal dlt pipeline -- so destination flexibility
(snowflake/bigquery/postgres/filesystem/duckdb/etc, same as every other
ingestion component in this repo) is preserved even though the fetch
mechanism differs.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame.
Set `destination` to persist directly to any dlt-supported destination.
See `assets/ingestion/DESTINATIONS.md` for the full configuration reference.
"""

import os
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd
import requests
import dlt
from dagster import (
    EnvVar,
    BackfillPolicy,
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


# BEGIN column-hashing helper (inlined per component; keep copies identical, see tests/test_column_hashing_ingestion.py)
def _resolve_hash_salt(hash_salt, hash_salt_env_var):
    if hash_salt:
        return hash_salt
    if hash_salt_env_var:
        val = os.environ.get(hash_salt_env_var)
        if val is None:
            raise ValueError(
                f"hash_salt_env_var={hash_salt_env_var!r} references an environment "
                "variable that is not set."
            )
        return val
    return None


def _make_hash_columns_map(columns, salt):
    import hashlib

    def _hash_map(doc):
        for col in columns:
            value = doc.get(col)
            if value is not None:
                doc[col] = hashlib.sha256((str(value) + salt).encode()).hexdigest()
        return doc

    return _hash_map


def _apply_hash_columns(resources, hash_columns, hash_salt, hash_salt_env_var):
    """Pseudonymizes `hash_columns` (salted SHA-256, hex-encoded) on every resource in
    `resources` (an iterable of DltResource objects -- e.g. source.resources.values()
    or a selected_resources list) before the data reaches the destination -- dlt's own
    documented deterministic-hashing pattern for PII (https://dlthub.com/docs/
    general-usage/customising-pipelines/pseudonymizing_columns), via
    DltResource.add_map(), wired up as a config option instead of requiring a custom
    Python module."""
    if not hash_columns:
        return
    salt = _resolve_hash_salt(hash_salt, hash_salt_env_var)
    if not salt:
        raise ValueError(
            "hash_columns is set but neither hash_salt nor hash_salt_env_var is -- a "
            "salt is required (a hardcoded/shared salt would let the hash be reversed "
            "by dictionary attack on common values, e.g. emails)."
        )
    hash_map = _make_hash_columns_map(hash_columns, salt)
    for resource in resources:
        resource.add_map(hash_map)
# END column-hashing helper
# BEGIN dlt-advanced-options helper (inlined per component; keep copies identical, see tests/test_dlt_advanced_options_ingestion.py)
_SCHEMA_CONTRACT_MAP = {
    "allow_all": {"tables": "evolve", "columns": "evolve", "data_type": "evolve"},
    "allow_new_columns": {"tables": "evolve", "columns": "evolve", "data_type": "freeze"},
    "block_all": {"tables": "evolve", "columns": "freeze", "data_type": "freeze"},
}


def _resolve_schema_contract(on_schema_change):
    contract = _SCHEMA_CONTRACT_MAP.get(on_schema_change)
    if contract is None:
        raise ValueError(
            f"on_schema_change={on_schema_change!r} must be one of {sorted(_SCHEMA_CONTRACT_MAP)}."
        )
    return dict(contract)


def _make_exclude_columns_map(columns):
    def _exclude_map(doc):
        for col in columns:
            doc.pop(col, None)
        return doc

    return _exclude_map


def _apply_exclude_columns(resources, exclude_columns):
    """Drops `exclude_columns` from every row in every resource before the data
    reaches the destination -- same per-resource add_map mechanism as
    _apply_hash_columns, applied first (see call sites) so a column in both ends up
    excluded, not hashed-then-excluded."""
    if not exclude_columns:
        return
    exclude_map = _make_exclude_columns_map(exclude_columns)
    for resource in resources:
        resource.add_map(exclude_map)


def _apply_max_rows_per_resource(resources, max_rows_per_resource):
    """Caps how many rows each resource loads -- a real cap on real rows landed in the
    real destination, never a dry run. DltResource.add_limit(count_rows=True) counts
    actual rows, not pages/yields (verified against dlt 1.30.0)."""
    if not max_rows_per_resource:
        return
    for resource in resources:
        resource.add_limit(max_items=max_rows_per_resource, count_rows=True)
# END dlt-advanced-options helper


def _dedupe_by_key(df: "pd.DataFrame", key_col: str, order_cols: Tuple[str, ...] = ()) -> Tuple["pd.DataFrame", int]:
    """Safety net against duplicate rows in a returned DataFrame, regardless of
    write_disposition -- keeps one row per `key_col`, preferring the row with the highest
    value in the first of `order_cols` that's actually present as a column (not every
    Linear resource has the same timestamp fields -- issues/projects have `updatedAt`,
    teams/cycles don't, so `createdAt` is tried next; with none present, the last row
    seen for that key is kept, which is stable but arbitrary). Returns (deduped_df, rows_removed)."""
    if key_col not in df.columns or df.empty:
        return df, 0
    before = len(df)
    sort_col = next((c for c in order_cols if c in df.columns), None)
    if sort_col:
        df = df.sort_values(sort_col, kind="stable").drop_duplicates(subset=key_col, keep="last")
    else:
        df = df.drop_duplicates(subset=key_col, keep="last")
    return df.reset_index(drop=True), before - len(df)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class LinearIngestionComponent(Component, Model, Resolvable):
    """Ingest Linear issue-tracker data via Linear's GraphQL API.

    Example:

        ```yaml
        type: dagster_community_components.LinearIngestionComponent
        attributes:
          asset_name: linear_ingestion
          api_key: "${LINEAR_API_KEY}"
        ```
    """

    asset_name: str = Field(description="Name of the asset that will hold the data")

    api_key: str = Field(description="Linear personal API key or OAuth2 access token. Sent raw in the Authorization header (no 'Bearer ' prefix for a personal API key).")

    resources: str = Field(
        default="issues,projects,teams",
        description="Comma-separated list of resources to extract: issues, projects, teams, cycles"
    )

    asset_shape: Literal["single", "multi_asset"] = Field(
        default="single",
        description=(
            "'single' (default, matches every other multi-resource ingestion component in this "
            "repo): one asset, keyed `asset_name`, covering every resource in `resources` as a "
            "single unit -- rows from every resource are unioned into one returned DataFrame "
            "tagged by a `_resource_type` column. 'multi_asset': each resource in `resources` is "
            "instead its own independently-materializable asset, keyed `{asset_name}_{resource}` "
            "-- select just the resource(s) you want; materializing several together still "
            "batches them through one shared dlt pipeline call (one Dagster step), not parallel "
            "per-resource execution. Opt into 'multi_asset' when you want to select/retry/schedule "
            "individual resources separately."
        ),
    )

    hash_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Column names to pseudonymize (salted SHA-256, hex-encoded) before the data "
            "reaches the destination -- same deterministic-hashing pattern dlt's own docs "
            "recommend for PII (https://dlthub.com/docs/general-usage/customising-pipelines/"
            "pseudonymizing_columns). A column missing from a given resource's rows is "
            "skipped, not an error. Requires hash_salt or hash_salt_env_var."
        ),
    )
    hash_salt: Optional[str] = Field(
        default=None,
        description=(
            "Salt for hash_columns. Required (this or hash_salt_env_var) when hash_columns "
            "is set -- a hardcoded/shared salt would let the hash be reversed by dictionary "
            "attack on common values. Use \"{{ env.VAR_NAME }}\" for the inline form. Keep it "
            "stable across runs: changing it changes every hash."
        ),
    )
    hash_salt_env_var: Optional[str] = Field(
        default=None,
        description="Alternative to hash_salt: name of an env var holding the salt. Resolved at run-time.",
    )
    on_schema_change: Literal["allow_all", "allow_new_columns", "block_all"] = Field(
        default="allow_all",
        description=(
            "What to do when incoming data's shape differs from the destination table's "
            "established schema (new columns, or a column's data type changing). "
            "'allow_all' (default, today's behavior): accept any change. "
            "'allow_new_columns': accept new columns, but reject (fail the run) a column "
            "whose data type would change. 'block_all': reject both new columns and data "
            "type changes. A resource's own first materialization can always create its "
            "initial schema in every mode -- 'block_all' only locks down an ALREADY-"
            "established table's columns/types. A rejected change fails the run with a "
            "clear error naming the table and column -- it is never silently dropped."
        ),
    )
    exclude_columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Column names to drop from every row before it reaches the destination. "
            "Applied before hash_columns (so a column in both ends up excluded, not "
            "hashed-then-excluded). A column missing from a given resource's rows is "
            "skipped, not an error, matching hash_columns."
        ),
    )
    max_rows_per_resource: Optional[int] = Field(
        default=None,
        description=(
            "Caps how many rows each resource loads. Unset (default): no cap. This still "
            "loads real rows into the real destination -- it is not a dry run or a preview "
            "-- meant for a first run to check a connection and the shape of the data "
            "cheaply, not for routine use. Never applied unless you set it."
        ),
    )
    write_disposition: Literal["replace", "append", "merge"] = Field(
        default="replace",
        description=(
            "dlt write disposition for every resource's destination table. 'replace' "
            "(default): each run fully replaces every table. dlt's own default is "
            "'append', which has no notion of a previous run's rows becoming stale -- every "
            "re-run keeps adding another full copy of every issue/project/team/cycle on top "
            "of the last (a real, previously-shipped bug: row_count doubled on every rerun, "
            "with no way to tell the loads apart downstream). 'merge': upserts keyed on "
            "Linear's own `id` field (stable across runs for every resource type) -- keeps "
            "rows from a prior run this run's `resources` selection or partition filter "
            "didn't touch. 'append': the old behavior, kept as an explicit opt-in for anyone "
            "who deliberately wants a historical log rather than current-state rows."
        ),
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
    group_name: Optional[str] = Field(default="linear_ingestion", description="Asset group for organization")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners -- list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the date-window "
            "filter below already handles a multi-partition range correctly). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. 'single_run' requires a partition-"
            "range-aware IO manager (e.g. a DB-table IO manager with "
            "partition_expr set) -- a generic filesystem/default IO manager "
            "cannot persist a single_run output spanning multiple partitions "
            "and will raise a clear error at materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
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
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def _resolve_destination(self):
        """Build the dlt `destination` argument."""
        if not self.destination:
            return "duckdb"
        creds = None
        if self.destination_credentials_url:
            creds = self.destination_credentials_url
        elif self.destination_credentials_env_var:
            creds = EnvVar(self.destination_credentials_env_var).get_value()
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
                creds = EnvVar(self.destination_credentials_env_var).get_value()
            return dlt.destinations.filesystem(bucket_url=self.bucket_url, credentials=creds)
        if self.destination == "athena" and self.athena_query_result_bucket:
            creds = None
            if self.destination_credentials_url:
                creds = self.destination_credentials_url
            elif self.destination_credentials_env_var:
                creds = EnvVar(self.destination_credentials_env_var).get_value()
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

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Linear Ingestion', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)
        component = self
        asset_name = self.asset_name
        description = self.description or "Ingest Linear issue-tracker data via Linear's GraphQL API."
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        destination = self.destination
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        resources = self.resources
        hash_columns = self.hash_columns
        hash_salt = self.hash_salt
        hash_salt_env_var = self.hash_salt_env_var
        on_schema_change = self.on_schema_change
        exclude_columns = self.exclude_columns
        max_rows_per_resource = self.max_rows_per_resource
        write_disposition = self.write_disposition
        api_key = self.api_key

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
            _inferred_kinds = ["linear", "python"]
        _inferred_kinds = list(dict.fromkeys(_inferred_kinds))

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )


        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _resources_list_outer = [r.strip() for r in resources.split(",") if r.strip()]
        output_names = {r: f"{asset_name}_{r}" for r in _resources_list_outer}

        _query_map = {
            "issues": (
                'query { issues(first: 100@AFTER@@FILTER@) { nodes { id identifier title priority '
                'createdAt updatedAt state { name } assignee { name } team { key } } pageInfo '
                '{ hasNextPage endCursor } } }',
                "issues",
                True,
            ),
            "projects": (
                'query { projects(first: 100@AFTER@) { nodes { id name state createdAt updatedAt '
                'targetDate } pageInfo { hasNextPage endCursor } } }',
                "projects",
                False,
            ),
            "teams": (
                'query { teams(first: 100@AFTER@) { nodes { id name key createdAt } pageInfo '
                '{ hasNextPage endCursor } } }',
                "teams",
                False,
            ),
            "cycles": (
                'query { cycles(first: 100@AFTER@) { nodes { id number name startsAt endsAt '
                'team { key } } pageInfo { hasNextPage endCursor } } }',
                "cycles",
                False,
            ),
        }

        def _make_resource(name, query_template, node_path, supports_filter, filter_gql):
            @dlt.resource(name=name)
            def _resource():
                cursor = None
                while True:
                    after_clause = f', after: "{cursor}"' if cursor else ""
                    query = query_template.replace("@AFTER@", after_clause)
                    query = query.replace("@FILTER@", filter_gql if supports_filter else "")
                    resp = requests.post(
                        "https://api.linear.app/graphql",
                        json={"query": query},
                        headers={"Authorization": api_key, "Content-Type": "application/json"},
                        timeout=60,
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                    if "errors" in payload:
                        raise RuntimeError(f"Linear GraphQL error for {name}: {payload['errors']}")
                    node = payload["data"][node_path]
                    yield node["nodes"]
                    if not node["pageInfo"]["hasNextPage"]:
                        break
                    cursor = node["pageInfo"]["endCursor"]
            return _resource

        if self.asset_shape == "single":
            @asset(retry_policy=_retry_policy, partitions_def=partitions_def,
                backfill_policy=backfill_policy,
                key=AssetKey.from_user_string(asset_name),
                description=description,
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def linear_ingestion_asset(context: AssetExecutionContext):
                if _column_lineage_md:
                    context.add_output_metadata(_column_lineage_md)
                context.log.info(f"Starting Linear ingestion, destination={destination or 'duckdb (in-memory)'}")

                resources_list = [r.strip() for r in resources.split(",")]

                _filter_gql = ""
                if context.has_partition_key or context.has_partition_key_range:
                    try:
                        _window = context.partition_time_window
                        _start_iso = _window.start.isoformat() + "Z"
                        _end_iso = _window.end.isoformat() + "Z"
                        _filter_gql = ', filter: { createdAt: { gte: "%s", lt: "%s" } }' % (_start_iso, _end_iso)
                    except Exception:
                        pass

                dlt_resources = []
                for r in resources_list:
                    if r in _query_map:
                        query_template, node_path, supports_filter = _query_map[r]
                        dlt_resources.append(_make_resource(r, query_template, node_path, supports_filter, _filter_gql)())
                hint_kwargs: Dict[str, Any] = {"write_disposition": write_disposition}
                if write_disposition == "merge":
                    hint_kwargs["primary_key"] = "id"
                for _r in dlt_resources:
                    _r.apply_hints(**hint_kwargs)

                pipeline = dlt.pipeline(
                    pipeline_name=f"{asset_name}_pipeline",
                    destination=component._resolve_destination_with_bucket(),
                    staging=component._resolve_staging(),
                    dataset_name=dataset_name,
                )

                context.log.info("Extracting Linear data via GraphQL...")
                if on_schema_change != "allow_all":
                    for _r in dlt_resources:
                        _r.apply_hints(schema_contract=_resolve_schema_contract(on_schema_change))
                if exclude_columns:
                    _apply_exclude_columns(dlt_resources, exclude_columns)
                if hash_columns:
                    _apply_hash_columns(dlt_resources, hash_columns, hash_salt, hash_salt_env_var)
                if max_rows_per_resource:
                    _apply_max_rows_per_resource(dlt_resources, max_rows_per_resource)
                load_info = pipeline.run(dlt_resources)
                context.log.info(f"Linear data loaded: {load_info}")
                load_id = load_info.loads_ids[0] if getattr(load_info, "loads_ids", None) else None

                resource_names = resources_list
                base_metadata = {
                    "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                    "dataset_name": MetadataValue.text(dataset_name),
                    "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                    "resources_requested": MetadataValue.json(resource_names),
                    "write_disposition": MetadataValue.text(write_disposition),
                    "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
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
                duplicate_rows_removed_total = 0
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
                            df, removed = _dedupe_by_key(df, "id", ("updatedAt", "createdAt"))
                            if removed:
                                duplicate_rows_removed_total += removed
                                context.log.warning(f"Dropped {removed} duplicate id row(s) from {table_name!r}.")
                            if len(df) > 0:
                                df["_resource_type"] = table_name
                                all_data.append(df)
                                resource_metadata[table_name] = len(df)
                        except Exception as e:
                            context.log.warning(f"Could not load {table_name}: {e}")

                if not all_data:
                    context.log.warning("No data extracted from Linear.")
                    return Output(value=pd.DataFrame(), metadata=base_metadata)

                combined_df = pd.concat(all_data, ignore_index=True)
                metadata = {
                    **base_metadata,
                    "row_count": MetadataValue.int(len(combined_df)),
                    "rows_returned": MetadataValue.int(len(combined_df)),
                    "duplicate_rows_removed": MetadataValue.int(duplicate_rows_removed_total),
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

            return Definitions(assets=[linear_ingestion_asset])

        from dagster import AssetOut, multi_asset

        outs = {
            output_names[r]: AssetOut(
                description=f"Linear resource reader: {r}",
                is_required=False,
                group_name=group_name,
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
            )
            for r in _resources_list_outer
        }

        @multi_asset(
            outs=outs,
            name=asset_name,
            can_subset=True,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def linear_ingestion_assets(context: AssetExecutionContext):
            try:
                selected_output_names = set(context.op_execution_context.selected_output_names)
            except Exception:  # noqa: BLE001
                selected_output_names = set(output_names.values())
            selected_names = [r for r in _resources_list_outer if output_names[r] in selected_output_names]
            if not selected_names:
                context.log.info("linear_ingestion: no resources selected; skipping.")
                return

            context.log.info(
                f"Starting Linear ingestion for {selected_names!r}, "
                f"destination={destination or 'duckdb (in-memory)'}"
            )

            _filter_gql = ""
            if context.has_partition_key or context.has_partition_key_range:
                try:
                    _window = context.partition_time_window
                    _start_iso = _window.start.isoformat() + "Z"
                    _end_iso = _window.end.isoformat() + "Z"
                    _filter_gql = ', filter: { createdAt: { gte: "%s", lt: "%s" } }' % (_start_iso, _end_iso)
                except Exception:
                    pass

            dlt_resources = []
            for r in selected_names:
                if r in _query_map:
                    query_template, node_path, supports_filter = _query_map[r]
                    dlt_resources.append(_make_resource(r, query_template, node_path, supports_filter, _filter_gql)())
            hint_kwargs: Dict[str, Any] = {"write_disposition": write_disposition}
            if write_disposition == "merge":
                hint_kwargs["primary_key"] = "id"
            for _r in dlt_resources:
                _r.apply_hints(**hint_kwargs)

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination_with_bucket(),
                staging=component._resolve_staging(),
                dataset_name=dataset_name,
            )

            context.log.info("Extracting Linear data via GraphQL...")
            if on_schema_change != "allow_all":
                for _r in dlt_resources:
                    _r.apply_hints(schema_contract=_resolve_schema_contract(on_schema_change))
            if exclude_columns:
                _apply_exclude_columns(dlt_resources, exclude_columns)
            if hash_columns:
                _apply_hash_columns(dlt_resources, hash_columns, hash_salt, hash_salt_env_var)
            if max_rows_per_resource:
                _apply_max_rows_per_resource(dlt_resources, max_rows_per_resource)
            load_info = pipeline.run(dlt_resources)
            context.log.info(f"Linear data loaded: {load_info}")
            load_id = load_info.loads_ids[0] if getattr(load_info, "loads_ids", None) else None

            non_sql_destinations = {"filesystem", "delta", "iceberg"}
            is_non_sql = destination in non_sql_destinations

            if persist_only or is_non_sql:
                if is_non_sql and not persist_only:
                    context.log.warning(
                        f"destination={destination!r} is not SQL-backed; cannot return a DataFrame. "
                        f"Set persist_only=true to silence this warning."
                    )
                for r in selected_names:
                    yield MaterializeResult(
                        asset_key=AssetKey.from_user_string(output_names[r]),
                        metadata={
                            "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                            "dataset_name": MetadataValue.text(dataset_name),
                            "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                            "resource": MetadataValue.text(r),
                            "write_disposition": MetadataValue.text(write_disposition),
                            "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
                        },
                    )
                return

            with pipeline.sql_client() as client:
                for r in selected_names:
                    try:
                        with client.execute_query(f"SELECT * FROM {dataset_name}.{r}") as cur:
                            df = cur.df()
                    except Exception as e:
                        context.log.warning(f"Could not load {r}: {e}")
                        df = pd.DataFrame()

                    df, duplicate_rows_removed = _dedupe_by_key(df, "id", ("updatedAt", "createdAt"))
                    if duplicate_rows_removed:
                        context.log.warning(f"Dropped {duplicate_rows_removed} duplicate id row(s) from {r!r}.")

                    metadata = {
                        "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                        "dataset_name": MetadataValue.text(dataset_name),
                        "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                        "resource": MetadataValue.text(r),
                        "write_disposition": MetadataValue.text(write_disposition),
                        "load_id": MetadataValue.text(load_id) if load_id else MetadataValue.null(),
                        "row_count": MetadataValue.int(len(df)),
                        "rows_returned": MetadataValue.int(len(df)),
                        "duplicate_rows_removed": MetadataValue.int(duplicate_rows_removed),
                    }
                    if include_preview and len(df) > 0:
                        try:
                            _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                            metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                        except Exception as _e:
                            context.log.warning(f"preview emission failed: {_e}")

                    yield Output(value=df, output_name=output_names[r], metadata=metadata)

        return Definitions(assets=[linear_ingestion_assets])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
