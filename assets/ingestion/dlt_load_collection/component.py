"""DltLoadCollectionComponent -- declare a collection of dlt resources (tables,
REST endpoints, or file globs) entirely in YAML, no custom Python module
required, across three of dlt's own built-in generic source engines:
`dlt.sources.sql_database`, `dlt.sources.rest_api`, and `dlt.sources.filesystem`.

This is the sibling to `integrations/dlt_pipeline_assets` (which requires a
hand-written `@dlt.source` function in a separate module this component
imports by dotted path) for the subset of dlt sources that are themselves
pure config, not custom code. A list of `{name, primary_key, columns, ...}`
dicts (sql_resources / rest_api_resources / filesystem_resources, depending
on source_type) is all that's needed -- the same declarative shape
regardless of which of the three engines is backing it.

Note: `assets/ingestion/dlt_rest_api_source` is this component's predecessor
for REST APIs and is now deprecated in favor of this one's `source_type:
rest_api` mode (same `client`/`resources` shape, with sql_database/filesystem
sources and every feature added since also available). Its code stays in
place for existing installs.

Every hand-built vendor-specific `*_ingestion` component in this catalog
(stripe_ingestion, salesforce_ingestion, zendesk_ingestion, ...) calls a
dlt vendor source package (`stripe_source`, `salesforce_source`,
`zendesk_support`, ...) directly inside its own component.py -- the
customer never points at an external module for those. This component is
for the cases a typed vendor component doesn't cover: a generic SQL
database, a generic REST API, or a generic set of files in a bucket.

asset_shape:
  'single' (default) -- one asset that unions every resource's rows into one
    DataFrame (tagged with a `_resource_type` column), matching the default
    shape of every other ingestion component in this catalog.
  'multi_asset' -- one independently-materializable asset per resource.
    Materializing one resource never builds, fetches, or persists the
    others: each dlt resource object is constructed only when its own
    output is selected for the current run.

table_name_map overrides the destination table name per resource, keyed by
the resource's declared `name` (same convention as the just-finished
asset_shape/table_name_map rollout across zendesk_ingestion,
servicenow_ingestion, github_ingestion, and others).
"""

import os
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
import pandas as pd
import dlt
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
    multi_asset,
)
from pydantic import Field


# Keys this component attaches to a `resources:` entry that are pure Dagster/catalog
# concepts (asset description/owners/tags/metadata/key) -- never real dlt resource
# config. Stripped before a resource dict is handed to a real dlt call (sql_table's
# kwargs and filesystem's apply_hints() already only pull specific known keys, so
# this only matters for rest_api, which otherwise passes the dict through almost
# verbatim and would fail dlt's own strict EndpointResource validation.
_ASSET_LEVEL_RESOURCE_KEYS = {"asset_key", "description", "owners", "tags", "metadata"}

# Shared across sql_resources/rest_api_resources/filesystem_resources -- every resource dict,
# regardless of which of the three fields it lives in, supports these same keys.
_SHARED_RESOURCE_KEY_DOCS = (
    "Optional 'write_disposition' ('append' / 'replace' / 'merge', the last needing "
    "'primary_key' or 'merge_key' -- validated, since merge with neither silently falls "
    "back to appending). If unset: inferred as 'merge' when 'primary_key' is set (a "
    "declared natural key implies dedup-on-rerun is wanted), else dlt's own 'append' "
    "default -- which means every materialization appends, so re-materializing the same "
    "partition duplicates rows unless a key is declared. Optional 'merge_key' (str or "
    "list of str), an alternative to 'primary_key' for merge's dedup key. "
    "Any key prefixed with 'x_' (e.g. 'x_min_rows', 'x_contract_columns') is reserved for "
    "subclass/user custom metadata -- read it off the resources field directly in a "
    "subclass, never passed to the underlying dlt call (required for rest_api, whose "
    "EndpointResource validation rejects unknown keys; sql_database/filesystem already "
    "ignore unrecognized keys harmlessly, but the 'x_' convention is recommended "
    "everywhere for consistency). The remaining keys only take effect in "
    "asset_shape='multi_asset' mode (single-shape has one asset, so there's no "
    "per-resource asset identity to set them on): 'description', 'owners' (list of str), "
    "'tags' (dict, merged over the component-level asset_tags), 'metadata' (dict, static "
    "`AssetOut` metadata), and 'asset_key' (str, or list of str for a multi-segment key) "
    "to fully control that resource's asset key instead of the default "
    "'{asset_name}_{resource_name}'."
)


def _resolve_write_disposition(r: Dict[str, Any]) -> Optional[str]:
    """An explicit 'write_disposition' always wins. Otherwise, a 'primary_key' with no
    explicit write_disposition infers 'merge' -- dlt's own default ('append') would
    otherwise silently duplicate rows on every re-materialization of the same data,
    which is surprising when a natural key is already declared. No primary_key and no
    explicit write_disposition: return None, so the underlying dlt call keeps its own
    'append' default unchanged."""
    wd = r.get("write_disposition")
    if wd:
        return wd
    if r.get("primary_key"):
        return "merge"
    return None


_RESOURCES_FIELD_NAME = {"sql_database": "sql_resources", "rest_api": "rest_api_resources", "filesystem": "filesystem_resources"}


def _validate_resources(source_type: str, resources: List[Dict[str, Any]]) -> None:
    """Validate the active resources list with clear, actionable errors instead of a
    raw dlt/pydantic traceback deep inside resource construction."""
    field = _RESOURCES_FIELD_NAME[source_type]
    if not resources:
        raise ValueError(f"DltLoadCollectionComponent: '{field}' must be a non-empty list.")
    seen = set()
    for i, r in enumerate(resources):
        if not isinstance(r, dict):
            raise ValueError(
                f"DltLoadCollectionComponent: {field}[{i}] must be a mapping (dict), got {type(r).__name__}."
            )
        name = r.get("name")
        if not name:
            raise ValueError(f"DltLoadCollectionComponent: {field}[{i}] is missing the required 'name' field.")
        if name in seen:
            raise ValueError(f"DltLoadCollectionComponent: duplicate resource name {name!r} in '{field}'.")
        seen.add(name)
        if source_type == "rest_api":
            endpoint = r.get("endpoint")
            if not isinstance(endpoint, dict) or not endpoint.get("path"):
                raise ValueError(
                    f"DltLoadCollectionComponent: {field}[{i}] ({name!r}) needs "
                    f"'endpoint: {{path: ...}}', e.g.\n"
                    f"  - name: {name}\n    endpoint:\n      path: {name}"
                )
        elif source_type == "filesystem":
            if not r.get("file_glob"):
                raise ValueError(
                    f"DltLoadCollectionComponent: {field}[{i}] ({name!r}) needs 'file_glob', "
                    f"e.g. 'orders/*.csv'."
                )
        write_disposition = r.get("write_disposition")
        if write_disposition is not None and write_disposition not in ("append", "replace", "merge"):
            raise ValueError(
                f"DltLoadCollectionComponent: {field}[{i}] ({name!r}).write_disposition must be "
                f"one of 'append'/'replace'/'merge', got {write_disposition!r}."
            )
        if write_disposition == "merge" and not (r.get("primary_key") or r.get("merge_key")):
            raise ValueError(
                f"DltLoadCollectionComponent: {field}[{i}] ({name!r}) sets "
                f"write_disposition='merge' but has no 'primary_key' or 'merge_key' -- dlt's merge "
                f"strategy has no natural key to dedupe on and silently falls back to appending "
                f"(verified: 2 loads of the same 2 rows produced 4). Set one of the two."
            )
        asset_key = r.get("asset_key")
        if asset_key is not None and not isinstance(asset_key, (str, list)):
            raise ValueError(
                f"DltLoadCollectionComponent: {field}[{i}] ({name!r}).asset_key must be a string "
                f"or a list of strings, got {type(asset_key).__name__}."
            )


# BEGIN partition row-filter helper (inlined per component; keep copies identical, see tests/test_partition_port_ingestion.py)
from typing import Literal  # noqa: E402,F401


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the frame is returned unchanged."""
    import pandas as pd

    if not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition: no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        return df[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df
# END partition row-filter helper


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


def _table_schema_metadata(df: pd.DataFrame) -> Dict[str, Any]:
    if df is None or len(df.columns) == 0:
        return {}
    columns = [dg.TableColumn(name=str(c), type=str(df[c].dtype)) for c in df.columns]
    return {"dagster/column_schema": MetadataValue.table_schema(dg.TableSchema(columns=columns))}


class DltLoadCollectionComponent(Component, Model, Resolvable):
    """Declare a collection of dlt resources (tables / REST endpoints / file
    globs) entirely in YAML -- no custom Python module required.

    Each source_type has its OWN resources field (sql_resources / rest_api_resources /
    filesystem_resources) instead of one shared 'resources' field -- a single field whose
    item shape silently changed based on a sibling source_type value turned out to be a
    real collision for schema-driven UI tooling (no way to express "this array's item
    shape depends on another field's value" in this repo's schema.json dialect, since
    `x-dagster-visible-when` only gates top-level fields). Three independently-typed,
    independently-gated fields are unambiguous the same way connection_string/client/
    filesystem_bucket_url already are.

    Example (sql_database):

        ```yaml
        type: dagster_community_components.DltLoadCollectionComponent
        attributes:
          asset_name: warehouse_core_tables
          source_type: sql_database
          connection_string: "{{ env('WAREHOUSE_CONNECTION_STRING') }}"
          schema_name: public
          sql_resources:
            - name: customers
              primary_key: id
            - name: orders
              primary_key: order_id
              columns: [order_id, customer_id, amount, created_at]
        ```

    Example (rest_api):

        ```yaml
        type: dagster_community_components.DltLoadCollectionComponent
        attributes:
          asset_name: my_api_tables
          source_type: rest_api
          client:
            base_url: "https://api.example.com"
            auth:
              type: bearer
              token: "{{ env('MY_API_TOKEN') }}"
          rest_api_resources:
            - name: customers
              primary_key: id
              endpoint:
                path: customers
        ```

    Example (filesystem):

        ```yaml
        type: dagster_community_components.DltLoadCollectionComponent
        attributes:
          asset_name: landing_zone_files
          source_type: filesystem
          filesystem_bucket_url: "s3://my-bucket/landing"
          filesystem_resources:
            - name: orders
              file_glob: "orders/*.csv"
              primary_key: order_id
            - name: events
              file_glob: "events/*.parquet"
              file_format: parquet
        ```
    """

    asset_name: str = Field(description="Name of the asset (or asset prefix, in multi_asset mode) that will hold the data")
    source_type: Literal["sql_database", "rest_api", "filesystem"] = Field(
        description=(
            "Which dlt built-in generic source engine builds these resources: 'sql_database' "
            "(dlt.sources.sql_database.sql_table, one call per declared resource), 'rest_api' "
            "(dlt.sources.rest_api.rest_api_source, same config shape as dlt_rest_api_source), "
            "or 'filesystem' (dlt.sources.filesystem.filesystem + a csv/parquet/jsonl reader, "
            "one per declared resource)."
        )
    )

    sql_resources: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Required when source_type='sql_database' (ignored otherwise). One dict per table, "
            "each requiring 'name' (also the destination table name, unless overridden via "
            "table_name_map). Optional 'primary_key' (str or list of str); 'columns' (if set) is "
            "a list of column names to extract (others excluded) -- or a dict, whose keys are "
            "used the same way; types always come from the real database via reflection. Optional "
            "per-resource 'schema' overrides the top-level schema_name. " + _SHARED_RESOURCE_KEY_DOCS
        ),
    )
    rest_api_resources: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Required when source_type='rest_api' (ignored otherwise). One dict per resource, "
            "each requiring 'name' and 'endpoint: {path: ...}' (same shape as "
            "dlt_rest_api_source's `resources`). 'columns' here is a dlt column-schema dict, e.g. "
            "{id: {data_type: bigint}}. " + _SHARED_RESOURCE_KEY_DOCS
        ),
    )
    filesystem_resources: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Required when source_type='filesystem' (ignored otherwise). One dict per file-glob, "
            "each requiring 'name' and 'file_glob' (e.g. 'orders/*.csv'); optional per-resource "
            "'file_format' overrides the top-level file_format. " + _SHARED_RESOURCE_KEY_DOCS
        ),
    )

    asset_shape: Literal["single", "multi_asset"] = Field(
        default="single",
        description=(
            "'single' (default, matches every other multi-table ingestion component in this "
            "repo): one asset that unions every resource's rows into one DataFrame, tagged with "
            "a `_resource_type` column. 'multi_asset': one independently-materializable asset "
            "per resource -- materializing one resource never builds, fetches, or persists the "
            "others."
        ),
    )
    table_name_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Override the destination table name for specific resources, keyed by the "
            "resource's declared 'name': {resource_name: destination_table_name}. Resources "
            "not listed keep their declared name as the destination table name."
        ),
    )

    # --- source_type='sql_database' fields --------------------------------------

    connection_string: Optional[str] = Field(
        default=None,
        description=(
            "SQLAlchemy connection string for the source database, e.g. "
            "'postgresql://user:pass@host:5432/db'. Required when source_type='sql_database' "
            "(or connection_string_env_var). Use \"{{ env('VAR_NAME') }}\" for secrets."
        ),
    )
    connection_string_env_var: Optional[str] = Field(
        default=None,
        description="Alternative to connection_string: name of an env var holding the SQLAlchemy connection string. Resolved at run-time.",
    )
    schema_name: Optional[str] = Field(
        default=None,
        description="Source database schema name (sql_database only). Per-resource 'schema' overrides this.",
    )
    sql_backend: Literal["sqlalchemy", "pyarrow", "pandas", "connectorx"] = Field(
        default="sqlalchemy",
        description="dlt sql_table extraction backend (sql_database only).",
    )

    # --- source_type='rest_api' fields -------------------------------------------

    client: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "dlt rest_api_source 'client' config (base_url required; optional auth, headers, "
            "paginator -- see dlt_rest_api_source's README for the full shape reference). "
            "Required when source_type='rest_api'."
        ),
    )

    # --- source_type='filesystem' fields -----------------------------------------

    filesystem_bucket_url: Optional[str] = Field(
        default=None,
        description=(
            "Bucket/path URL to read SOURCE files from (e.g. 's3://my-bucket/landing', "
            "'gs://my-bucket/landing', 'az://my-container/landing', or 'file:///local/path'). "
            "Required when source_type='filesystem'. Credentials resolve from the cloud "
            "provider's standard env vars (e.g. AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY), same "
            "as the destination-side filesystem convention in ../DESTINATIONS.md. Named "
            "'filesystem_bucket_url' (not the shared 'bucket_url') since this is the SOURCE "
            "bucket -- 'bucket_url' below is the DESTINATION bucket, matching every other "
            "ingestion component in this repo."
        ),
    )
    file_format: Literal["csv", "parquet", "jsonl"] = Field(
        default="csv",
        description="Reader applied to each matched file (filesystem only). Per-resource 'file_format' overrides this.",
    )

    # --- Destination fields (see ../DESTINATIONS.md) --------------------------------

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
        description="Alternative to destination_credentials_url: name of an env var holding the connection string. Resolved at run-time.",
    )
    bucket_url: Optional[str] = Field(
        default=None,
        description=(
            "Bucket/path URL for filesystem-shaped DESTINATION storage -- same field name/meaning "
            "as every other ingestion component in this repo. Required when destination='filesystem' "
            "is the final target; also used as the staging area when destination='databricks' or "
            "'athena'. See ../DESTINATIONS.md. (The filesystem SOURCE bucket, for "
            "source_type='filesystem', is the separate 'filesystem_bucket_url' field above.)"
        ),
    )
    athena_query_result_bucket: Optional[str] = Field(
        default=None,
        description="Optional S3 path where Athena writes query results. Only used when destination='athena'.",
    )

    # --- Standard asset metadata -----------------------------------------------

    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default="dlt_load_collection", description="Asset group for organization")
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
        description="Asset kinds for the Dagster catalog. Auto-inferred from source_type/destination if not set.",
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
        description="Rows to include in the preview metadata when include_preview_metadata is True.",
    )
    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'cron', 'static', 'multi', 'dynamic', or None for unpartitioned.",
    )
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(default=0, description="Passed through to the underlying time-based partitions definition. 0 (default) excludes the current, still-in-progress window.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the underlying partitions definition (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    partition_date_column: Optional[Union[str, int]] = Field(default=None, description="Column used to filter the returned DataFrame to the current date partition (time partitions).")
    partition_static_column: Optional[Union[str, int]] = Field(default=None, description="Column used to filter the returned DataFrame to the current static/dynamic partition key.")

    partition_window_params: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "source_type='rest_api' only: push the partition's time window into the API "
            "requests so each partition fetches only its own slice: {start_param: ..., "
            "end_param: ..., format: iso | date | epoch | epoch_ms}. Ignored (with a warning) "
            "for source_type='sql_database'/'filesystem'. Needs a time partition_type."
        ),
    )
    partition_window_resources: Optional[List[str]] = Field(
        default=None,
        description="Resources that take partition_window_params (default: all of them, rest_api only).",
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description="Backfill strategy for a partitioned asset. Unset keeps Dagster's default.",
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run (default 1).",
    )

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description=(
            "Column-level lineage, emitted as `dagster/column_lineage` metadata: {output column: "
            "[upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is the "
            "only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. "
            "Only applies in asset_shape='single' -- in 'multi_asset' mode there are several output "
            "assets and no single obvious upstream-to-output column mapping, so it's ignored there."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def _resolve_destination(self):
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
        if self.destination not in ("databricks", "athena"):
            return None
        if self.bucket_url:
            return dlt.destinations.filesystem(bucket_url=self.bucket_url)
        return "filesystem"

    def _active_resources(self) -> List[Dict[str, Any]]:
        """The resources list for whichever source_type is configured --
        sql_resources / rest_api_resources / filesystem_resources."""
        return {
            "sql_database": self.sql_resources,
            "rest_api": self.rest_api_resources,
            "filesystem": self.filesystem_resources,
        }[self.source_type] or []

    def _dest_table_names(self) -> Dict[str, str]:
        resource_names = [r["name"] for r in self._active_resources()]
        overrides = self.table_name_map or {}
        unknown = set(overrides) - set(resource_names)
        if unknown:
            raise ValueError(
                f"DltLoadCollectionComponent: table_name_map has key(s) not in the active resources field: {sorted(unknown)}."
            )
        return {name: overrides.get(name, name) for name in resource_names}

    def _resolve_source_credentials(self) -> Optional[str]:
        if self.connection_string:
            return self.connection_string
        if self.connection_string_env_var:
            return os.environ.get(self.connection_string_env_var)
        return None

    def _build_pipeline(self, pipeline_name: str, dataset_name: str):
        """Overridable seam: a subclass can swap in a differently-configured
        pipeline (e.g. a demo-mode in-memory one) without touching build_defs."""
        return dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=self._resolve_destination_with_bucket(),
            staging=self._resolve_staging(),
            dataset_name=dataset_name,
        )

    def _build_resources(self, selected_names: Optional[List[str]] = None, run_context=None):
        """Build exactly the requested resources -- unselected ones are never
        constructed, so a 'multi_asset' run selecting one resource never fetches
        (or in rest_api's case, even requests) the others.

        Overridable seam: a subclass can wrap or replace this entirely (e.g. to
        add resources this component doesn't natively support) without touching
        build_defs.

        Returns (run_arg, names): `run_arg` is what gets passed to
        `pipeline.run(...)` -- a List[DltResource] for sql_database/filesystem
        (each resource is built independently), or the single DltSource for
        rest_api (its resources share one client/auth context, so they're
        built together from one filtered config). `names` is always the plain
        list of selected resource names, derived from `resources` config --
        never from introspecting the built dlt object, which a bring-your-own
        config object has no obligation to expose uniformly.
        """
        source_type = self.source_type
        resources_config = self._active_resources()
        dest_table_names = self._dest_table_names()
        resource_names = list(dest_table_names.keys())
        wanted = set(selected_names) if selected_names is not None else set(resource_names)
        built: Dict[str, Any] = {}

        if source_type == "sql_database":
            from dlt.sources.sql_database import sql_table
            creds = self._resolve_source_credentials()
            if not creds:
                raise ValueError(
                    "DltLoadCollectionComponent: could not resolve a connection string for "
                    "source_type='sql_database' (checked connection_string and connection_string_env_var)."
                )
            for r in resources_config:
                name = r["name"]
                if name not in wanted:
                    continue
                cols = r.get("columns")
                included_columns = list(cols.keys()) if isinstance(cols, dict) else cols
                kwargs = dict(
                    credentials=creds,
                    table=name,
                    schema=r.get("schema") or self.schema_name,
                    primary_key=r.get("primary_key"),
                    included_columns=included_columns,
                    backend=self.sql_backend,
                    defer_table_reflect=True,
                )
                if r.get("merge_key"):
                    kwargs["merge_key"] = r["merge_key"]
                _wd = _resolve_write_disposition(r)
                if _wd:
                    kwargs["write_disposition"] = _wd
                built[name] = sql_table(**kwargs)

        elif source_type == "rest_api":
            from dlt.sources.rest_api import rest_api_source
            _resources_cfg = [
                {k: v for k, v in r.items() if k not in _ASSET_LEVEL_RESOURCE_KEYS and not k.startswith("x_")}
                for r in resources_config if r["name"] in wanted
            ]
            for r in _resources_cfg:
                dest = dest_table_names[r["name"]]
                if dest != r["name"]:
                    r["table_name"] = dest
                _wd = _resolve_write_disposition(r)
                if _wd:
                    r["write_disposition"] = _wd
            partition_window_params = self.partition_window_params if source_type == "rest_api" else None
            if partition_window_params and run_context is not None:
                _apply_partition_window(
                    {"resources": _resources_cfg}, run_context, partition_window_params, self.partition_window_resources
                )
            elif self.partition_date_column and run_context is not None and (run_context.has_partition_key or run_context.has_partition_key_range):
                run_context.log.warning(
                    "partition_date_column filters the rows this asset returns, but the extract "
                    "still fetches the full source on every partition; set partition_window_params "
                    "to push the window to the API."
                )
            _names = [r["name"] for r in _resources_cfg]
            if not _resources_cfg:
                return [], []
            config = {"client": self.client, "resources": _resources_cfg}
            source = rest_api_source(config)
            return source, _names

        elif source_type == "filesystem":
            from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
            _readers = {"csv": read_csv, "parquet": read_parquet, "jsonl": read_jsonl}
            for r in resources_config:
                name = r["name"]
                if name not in wanted:
                    continue
                fmt = r.get("file_format") or self.file_format
                reader_factory = _readers.get(fmt)
                if reader_factory is None:
                    raise ValueError(
                        f"DltLoadCollectionComponent: resources[{name!r}].file_format must be "
                        f"one of csv/parquet/jsonl, got {fmt!r}."
                    )
                res = (filesystem(bucket_url=self.filesystem_bucket_url, file_glob=r["file_glob"]) | reader_factory()).with_name(name)
                hint_kwargs: Dict[str, Any] = {}
                if r.get("primary_key"):
                    hint_kwargs["primary_key"] = r["primary_key"]
                if r.get("merge_key"):
                    hint_kwargs["merge_key"] = r["merge_key"]
                if r.get("columns"):
                    hint_kwargs["columns"] = r["columns"]
                _wd = _resolve_write_disposition(r)
                if _wd:
                    hint_kwargs["write_disposition"] = _wd
                if hint_kwargs:
                    res.apply_hints(**hint_kwargs)
                built[name] = res

        for name, res in built.items():
            dest = dest_table_names[name]
            if dest != name:
                res.apply_hints(table_name=dest)
        return list(built.values()), list(built.keys())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dlt Load Collection', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        source_type = self.source_type

        _active_field = {
            "sql_database": "sql_resources", "rest_api": "rest_api_resources", "filesystem": "filesystem_resources",
        }[source_type]
        _inactive_fields = {
            "sql_resources": self.sql_resources, "rest_api_resources": self.rest_api_resources,
            "filesystem_resources": self.filesystem_resources,
        }
        if _inactive_fields.pop(_active_field) is None:
            raise ValueError(f"DltLoadCollectionComponent: '{_active_field}' is required when source_type={source_type!r}.")
        _misplaced = [name for name, value in _inactive_fields.items() if value]
        if _misplaced:
            raise ValueError(
                f"DltLoadCollectionComponent: {_misplaced} set but source_type={source_type!r} "
                f"uses '{_active_field}' instead -- they're ignored. Remove them or set source_type accordingly."
            )
        resources_config = self._active_resources()

        _validate_resources(source_type, resources_config)
        if source_type == "sql_database" and not (self.connection_string or self.connection_string_env_var):
            raise ValueError(
                "DltLoadCollectionComponent: 'connection_string' or 'connection_string_env_var' "
                "is required when source_type='sql_database'."
            )
        if source_type == "rest_api" and not self.client:
            raise ValueError("DltLoadCollectionComponent: 'client' is required when source_type='rest_api'.")
        if source_type == "filesystem" and not self.filesystem_bucket_url:
            raise ValueError("DltLoadCollectionComponent: 'filesystem_bucket_url' is required when source_type='filesystem'.")
        if self.partition_window_params and source_type != "rest_api":
            import warnings
            warnings.warn(
                "DltLoadCollectionComponent: partition_window_params is only applied for "
                f"source_type='rest_api' -- ignored for source_type={source_type!r}."
            )

        dest_table_names = self._dest_table_names()
        resource_names = list(dest_table_names.keys())

        description = self.description or f"Ingest data via dlt's generic {source_type} source engine (bring-your-own config)."
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        destination = self.destination
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only

        _default_kind = {"sql_database": "sql_database", "rest_api": "rest_api", "filesystem": "filesystem"}[source_type]
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "mssql": "mssql", "clickhouse": "clickhouse", "duckdb": "duckdb",
            "motherduck": "duckdb", "databricks": "databricks", "athena": "athena",
            "filesystem": "filesystem", "delta": "delta", "iceberg": "iceberg",
        }
        _inferred_kinds = list(self.kinds or [])
        if not _inferred_kinds:
            _inferred_kinds.append(_default_kind)
            if destination and destination in _kind_map:
                _inferred_kinds.append(_kind_map[destination])
            _inferred_kinds.append("python")
        _inferred_kinds = list(dict.fromkeys(_inferred_kinds))

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset,
            timezone=self.partition_timezone,
        )
        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy as _BackfillPolicy
            _backfill_policy_obj = _BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy as _BackfillPolicy
            _backfill_policy_obj = _BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_window_params = self.partition_window_params if source_type == "rest_api" else None
        partition_window_resources = self.partition_window_resources

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        def _query_back(client, name: str, context) -> pd.DataFrame:
            dest_table = dest_table_names[name]
            try:
                with client.execute_query(f"SELECT * FROM {dataset_name}.{dest_table}") as cur:
                    cols = [d[0] for d in cur.description]
                    rows = cur.fetchall()
                return pd.DataFrame(rows, columns=cols)
            except Exception as e:
                context.log.warning(f"Could not load {dest_table}: {e}")
                return pd.DataFrame()

        non_sql_destinations = {"filesystem", "delta", "iceberg"}

        if self.asset_shape == "single":
            _column_lineage_md = _column_lineage_metadata(self)

            @asset(
                retry_policy=_retry_policy, partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
                key=AssetKey.from_user_string(asset_name),
                description=description,
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def dlt_load_collection_asset(context: AssetExecutionContext):
                if _column_lineage_md:
                    context.add_output_metadata(_column_lineage_md)
                context.log.info(
                    f"Starting dlt_load_collection ingestion (source_type={source_type}), "
                    f"destination={destination or 'duckdb (in-memory)'}"
                )
                run_arg, built_names = component._build_resources(run_context=context)
                pipeline = component._build_pipeline(f"{asset_name}_pipeline", dataset_name)
                context.log.info("Extracting data...")
                load_info = pipeline.run(run_arg)
                context.log.info(f"Data loaded: {load_info}")

                base_metadata = {
                    "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                    "dataset_name": MetadataValue.text(dataset_name),
                    "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                    "source_type": MetadataValue.text(source_type),
                    "resources_requested": MetadataValue.json(built_names),
                }

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
                    for name in built_names:
                        df = _query_back(client, name, context)
                        if len(df) > 0:
                            df["_resource_type"] = name
                            all_data.append(df)
                            resource_metadata[name] = len(df)

                if not all_data:
                    context.log.warning("No data extracted.")
                    return Output(value=pd.DataFrame(), metadata=base_metadata)

                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df = _filter_df_to_partition(combined_df, context, partition_date_column, partition_static_column)
                metadata = {
                    **base_metadata,
                    **_table_schema_metadata(combined_df),
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

            return Definitions(assets=[dlt_load_collection_asset])

        # --- asset_shape == "multi_asset" ---------------------------------------

        resources_by_name = {r["name"]: r for r in resources_config}
        # The output name is always this stable, single-segment form -- it's internal
        # Dagster-op plumbing (selected_output_names / Output(output_name=...)), not
        # user-facing. A resource's 'asset_key' override only changes the ASSET KEY
        # (the catalog-facing identity), via AssetOut(key=...) below.
        output_names = {name: f"{asset_name}_{name}" for name in resource_names}

        def _resource_asset_key(name):
            override = resources_by_name[name].get("asset_key")
            if override:
                return AssetKey(override) if isinstance(override, list) else AssetKey.from_user_string(override)
            return AssetKey.from_user_string(output_names[name])

        asset_keys = {name: _resource_asset_key(name) for name in resource_names}
        outs = {
            output_names[name]: AssetOut(
                key=asset_keys[name],
                is_required=False,
                description=resources_by_name[name].get("description") or description,
                owners=resources_by_name[name].get("owners") or owners,
                tags={**_all_tags, **(resources_by_name[name].get("tags") or {})},
                metadata=resources_by_name[name].get("metadata"),
                freshness_policy=_freshness_policy,
            )
            for name in resource_names
        }

        @multi_asset(
            outs=outs,
            name=asset_name,
            can_subset=True,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=_retry_policy,
            group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def dlt_load_collection_assets(context: AssetExecutionContext):
            try:
                selected = set(context.op_execution_context.selected_output_names)
            except Exception:
                selected = set(output_names.values())
            selected_resource_names = [name for name in resource_names if output_names[name] in selected]

            context.log.info(
                f"Starting dlt_load_collection ingestion (source_type={source_type}, "
                f"resources={selected_resource_names}), destination={destination or 'duckdb (in-memory)'}"
            )
            run_arg, _ = component._build_resources(run_context=context, selected_names=selected_resource_names)
            pipeline = component._build_pipeline(f"{asset_name}_pipeline", dataset_name)
            load_info = pipeline.run(run_arg)
            context.log.info(f"Data loaded: {load_info}")

            is_non_sql = destination in non_sql_destinations
            with_sql = not (persist_only or is_non_sql)
            if is_non_sql and not persist_only:
                context.log.warning(
                    f"destination={destination!r} is not SQL-backed; cannot return DataFrames. "
                    f"Set persist_only=true to silence this warning."
                )

            if not with_sql:
                for name in selected_resource_names:
                    yield MaterializeResult(
                        asset_key=asset_keys[name],
                        metadata={
                            "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                            "dataset_name": MetadataValue.text(dataset_name),
                            "source_type": MetadataValue.text(source_type),
                            "table_name": MetadataValue.text(dest_table_names[name]),
                        },
                    )
                return

            with pipeline.sql_client() as client:
                for name in selected_resource_names:
                    df = _query_back(client, name, context)
                    df = _filter_df_to_partition(df, context, partition_date_column, partition_static_column)
                    metadata = {
                        "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                        "dataset_name": MetadataValue.text(dataset_name),
                        "source_type": MetadataValue.text(source_type),
                        "table_name": MetadataValue.text(dest_table_names[name]),
                        "row_count": MetadataValue.int(len(df)),
                        "column_count": MetadataValue.int(len(df.columns)),
                        **_table_schema_metadata(df),
                    }
                    if include_preview and len(df) > 0:
                        try:
                            _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                            metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                        except Exception as _e:
                            context.log.warning(f"preview emission failed: {_e}")
                    yield Output(value=df, output_name=output_names[name], metadata=metadata)

        return Definitions(assets=[dlt_load_collection_assets])


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


# BEGIN partition window helper (inlined per component; keep copies identical, see tests/test_partition_window_ingestion.py)
def _apply_partition_window(config, context, window_params, resources=None):
    """Push the run's partition window into the API requests, so a partition fetches only its own slice.

    `window_params` is {"start_param": ..., "end_param": ..., "format": "iso" | "date" | "epoch" | "epoch_ms"}; the window
    of a time partition (the whole range for a backfill run) is written into each resource's `endpoint.params`
    (all resources, or only those named in `resources`). Returns True when the window was applied."""
    from datetime import timezone

    if not window_params or not (context.has_partition_key or context.has_partition_key_range):
        return False
    try:
        window = context.partition_time_window
    except Exception:
        return False  # static/dynamic partitions have no time window
    start, end = (t.astimezone(timezone.utc) for t in (window.start, window.end))
    fmt = window_params.get("format", "iso")
    render = {
        "iso": lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date": lambda t: t.strftime("%Y-%m-%d"),
        "epoch": lambda t: int(t.timestamp()),
        "epoch_ms": lambda t: int(t.timestamp() * 1000),
    }.get(fmt)
    if render is None:
        raise ValueError(f"partition_window_params.format must be one of iso/date/epoch/epoch_ms, got {fmt!r}")
    values = {k: render(t) for k, t in (("start_param", start), ("end_param", end)) if window_params.get(k)}
    if not values:
        raise ValueError("partition_window_params needs start_param and/or end_param")
    wanted = set(resources) if resources else None
    applied = 0
    for i, res in enumerate(config.get("resources", [])):
        if isinstance(res, str):
            if wanted is not None and res not in wanted:
                continue
            res = config["resources"][i] = {"name": res, "endpoint": {"path": res}}
        if not isinstance(res, dict):
            continue
        name = res.get("name") or (res.get("endpoint") or {}).get("path")
        if wanted is not None and name not in wanted:
            continue
        endpoint = res.setdefault("endpoint", {})
        if isinstance(endpoint, str):
            endpoint = res["endpoint"] = {"path": endpoint}
        params = endpoint.setdefault("params", {})
        for key, value in ((window_params.get(k), v) for k, v in values.items()):
            params[key] = value
        applied += 1
    return applied > 0
# END partition window helper


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
