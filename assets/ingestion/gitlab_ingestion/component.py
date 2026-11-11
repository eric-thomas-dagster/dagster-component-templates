"""GitLab Ingestion Component.

Read records from one or more GitLab REST API v4 resources (projects, issues,
merge_requests, commits -- `tables` is a comma-separated list) using dlt's
generic REST API source (dlt.sources.rest_api.rest_api_source). GitLab has no
official dlt-maintained verified-source package (confirmed against dlthub's
own verified-sources catalog, https://dlthub.com/docs/dlt-ecosystem/
verified-sources/ -- GitHub, Salesforce, Zendesk, etc. are listed; GitLab is
not), so this uses the same config-driven REST connector pattern as
servicenow_ingestion / workday_ingestion / the ad-platform ingestions in this
repo.

By default (`asset_shape='single'`, matching every other multi-table
ingestion component in this repo, e.g. salesforce_ingestion/zendesk_ingestion/
servicenow_ingestion), every table lands under ONE asset keyed `asset_name`,
unioned into one returned DataFrame tagged by a `_table` column. Set
`asset_shape='multi_asset'` to instead give each table its OWN
independently-materializable Dagster asset (`@multi_asset(can_subset=True)`,
asset-keyed `{asset_name}_{table}`) and its own dlt resource -- materializing
just one table then does not fetch or persist the others.

API details verified against GitLab's own docs (docs.gitlab.com/ee/api/rest/,
/authentication.html, /projects.html, /issues.html, /merge_requests.html,
/commits.html, 2026-11):
  - Auth: a personal/project/group access token sent in the `PRIVATE-TOKEN`
    header (dlt auth type "api_key", location="header").
  - Base URL: `https://<host>/api/v4` -- `https://gitlab.com/api/v4` for
    SaaS, but the host (and therefore the whole base_url) must be
    configurable for self-managed GitLab instances. Exposed as `base_url`.
  - Pagination: GitLab's default is offset pagination (`page`/`per_page`
    query params, max per_page=100) but it also returns a standard RFC-5988
    `Link` response header with `rel="next"` -- the same mechanism GitHub's
    API uses. dlt's `HeaderLinkPaginator` (built exactly for this) is used
    instead of hand-rolling page-number increments; GitLab's keyset
    pagination (`pagination=keyset`) is an alternative GitLab recommends for
    very large collections on specific endpoints/orderings, but is not used
    here since HeaderLinkPaginator already handles the default (and far more
    broadly supported) offset-pagination Link header generically across all
    four resource types.
  - Scoping: `issues`, `merge_requests`, and `commits` are all endpoints
    under `/projects/:id/...` and therefore require `project_id` (GitLab
    accepts either the numeric project ID or a URL-encoded
    `namespace/project` path). `projects` can instead be scoped to
    `group_id` (`GET /groups/:id/projects`, listing every project in that
    group) or left unscoped (`GET /projects`, every project visible to the
    token). Both project_id and group_id are supported as separate,
    independently optional fields since they genuinely serve different
    endpoints -- not interchangeable aliases for the same scope.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame.
Set `destination` to persist directly to any dlt-supported destination
(snowflake, bigquery, postgres, filesystem, etc.). See
`assets/ingestion/DESTINATIONS.md` for the full configuration reference.

No column_lineage field: like servicenow_ingestion (this component's closest
structural precedent -- also no official dlt source, also multi-table with
both asset_shape branches), this component can produce several independently
-schemaed table-assets from one instance (`tables`), and column_lineage's
mapping is inherently single-upstream-asset-shaped -- it doesn't generalize
across heterogeneous tables (projects vs. issues vs. commits have entirely
different columns).
"""

import os
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import dlt
from dagster import (
    EnvVar,
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
def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


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


_PROJECT_SCOPED_TABLES = {"issues", "merge_requests", "commits"}
_KNOWN_TABLES = {"projects", "issues", "merge_requests", "commits"}
_ENDPOINT_PATHS = {
    "issues": "projects/{project_id}/issues",
    "merge_requests": "projects/{project_id}/merge_requests",
    "commits": "projects/{project_id}/repository/commits",
}


class GitlabIngestionComponent(Component, Model, Resolvable):
    """Read records from one or more GitLab REST API v4 resources using dlt's generic REST API source.

    By default (`asset_shape='single'`), every table in `tables` lands under ONE
    asset keyed `asset_name`, unioned into one DataFrame tagged by a `_table`
    column -- matching every other multi-table ingestion component in this repo
    (servicenow_ingestion, salesforce_ingestion, zendesk_ingestion). Set
    `asset_shape='multi_asset'` to instead give each table its own asset, keyed
    `{asset_name}_{table}` -- e.g. `tables: issues,merge_requests` with
    `asset_name: gl` then produces `gl_issues` and `gl_merge_requests` as two
    independently-materializable assets (`can_subset=True`): materializing one
    does not fetch or persist the other.

    `issues`, `merge_requests`, and `commits` are project-scoped
    (`GET /projects/:id/...`) and require `project_id`. `projects` can instead
    be scoped to `group_id` (`GET /groups/:id/projects`) or left unscoped
    (`GET /projects`, every project visible to the token).

    Example (single project, DataFrame mode, self-managed instance):
        ```yaml
        type: dagster_community_components.GitlabIngestionComponent
        attributes:
          asset_name: platform_repo
          base_url: https://gitlab.mycompany.com/api/v4
          access_token_env_var: GITLAB_TOKEN
          project_id: "group/platform"
          tables: issues,merge_requests,commits
          state: opened
          group_name: gitlab
        ```

    Example (every table independently materializable, persisted to Snowflake):
        ```yaml
        type: dagster_community_components.GitlabIngestionComponent
        attributes:
          asset_name: gitlab
          asset_shape: multi_asset
          access_token_env_var: GITLAB_TOKEN
          project_id: "42"
          tables: projects,issues,merge_requests,commits
          destination: snowflake
          dataset_name: gitlab
          persist_only: true
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")
    access_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var with a GitLab personal, project, or group access token. Sent in the "
            "'PRIVATE-TOKEN' header on every request (GitLab's recommended auth header -- "
            "see docs.gitlab.com/ee/api/rest/authentication.html). Required for any private "
            "project/group; a public project's read endpoints can be reached without one, "
            "but GitLab rate-limits unauthenticated requests much more aggressively."
        ),
    )
    base_url: str = Field(
        default="https://gitlab.com/api/v4",
        description=(
            "GitLab REST API v4 base URL. Defaults to GitLab.com SaaS. For a self-managed "
            "GitLab instance, set this to '<https://your-gitlab-host>/api/v4' -- the path "
            "must still end in '/api/v4'."
        ),
    )
    project_id: Optional[str] = Field(
        default=None,
        description=(
            "GitLab project ID (numeric, e.g. '42') or URL-encoded path "
            "(e.g. 'my-group/my-project'). Required when `tables` includes 'issues', "
            "'merge_requests', or 'commits' -- those are all project-scoped endpoints "
            "(GET /projects/:id/...). Not used by the 'projects' table."
        ),
    )
    group_id: Optional[str] = Field(
        default=None,
        description=(
            "GitLab group ID (numeric) or URL-encoded path (e.g. 'my-group'). Only affects "
            "the 'projects' table: when set, lists every project in that group "
            "(GET /groups/:id/projects) instead of every project visible to the token "
            "(GET /projects, the default when neither group_id nor project_id-based scoping "
            "applies)."
        ),
    )
    tables: str = Field(
        description=(
            "Comma-separated list of GitLab resources to query: 'projects', 'issues', "
            "'merge_requests', 'commits'. With asset_shape='single' (default), all tables "
            "are fetched together under one asset keyed `asset_name`. With "
            "asset_shape='multi_asset', each table instead becomes its own "
            "independently-materializable Dagster asset, keyed `{asset_name}_{table}` -- "
            "materializing one table does not fetch or persist the others. "
            "'issues'/'merge_requests'/'commits' require project_id; 'projects' uses "
            "group_id (or is unscoped) instead."
        ),
    )
    asset_shape: Literal["single", "multi_asset"] = Field(
        default="single",
        description=(
            "'single' (default, matches every other multi-table ingestion component in this "
            "repo, e.g. servicenow_ingestion/salesforce_ingestion/zendesk_ingestion): one "
            "asset, keyed `asset_name`, covering every table in `tables` as a single unit -- "
            "rows from every table are unioned into one returned DataFrame tagged by a "
            "`_table` column. 'multi_asset': each table in `tables` is instead its own "
            "independently-materializable asset -- select just the table(s) you want; "
            "materializing several together still batches them through one shared dlt "
            "pipeline call (one Dagster step), not parallel per-table execution. Opt into "
            "'multi_asset' when you want to select/retry/schedule individual tables "
            "separately; stay on 'single' when you just want to monitor 'did the GitLab "
            "pipeline run'."
        ),
    )
    table_name_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Override the destination table name for specific GitLab resources, e.g. "
            "{'merge_requests': 'raw_mrs'}. dlt otherwise names each destination table after "
            "its GitLab resource name (the default every ingestion component in this repo "
            "relies on -- see salesforce_ingestion). Tables in `tables` that aren't listed "
            "here keep their GitLab name as-is. Every key must also appear in `tables`."
        ),
    )
    state: Optional[str] = Field(
        default=None,
        description=(
            "Filter 'issues'/'merge_requests' by state: 'opened', 'closed', 'merged' "
            "(merge_requests only), or 'locked' (merge_requests only). Unset (default) "
            "returns every state. Not used by 'projects'/'commits'."
        ),
    )
    updated_after: Optional[str] = Field(
        default=None,
        description=(
            "Filter 'issues'/'merge_requests' to records updated on or after this ISO 8601 "
            "timestamp (e.g. '2024-01-01T00:00:00Z'). Not used by 'projects'/'commits'."
        ),
    )
    since: Optional[str] = Field(
        default=None,
        description=(
            "Filter 'commits' to commits on or after this ISO 8601 timestamp "
            "(e.g. '2024-01-01T00:00:00Z'). Only used by 'commits'."
        ),
    )
    until: Optional[str] = Field(
        default=None,
        description=(
            "Filter 'commits' to commits on or before this ISO 8601 timestamp. Only used "
            "by 'commits'."
        ),
    )
    ref_name: Optional[str] = Field(
        default=None,
        description=(
            "Branch, tag, or revision range to list 'commits' from. Defaults to the "
            "project's default branch when unset. Only used by 'commits'."
        ),
    )
    per_page: int = Field(
        default=100,
        description="Records fetched per API call (GitLab's 'per_page' param). Default 100, GitLab's documented maximum.",
    )
    max_records: Optional[int] = Field(
        default=None,
        description="Cap on total records returned across all pages, per table. None (default) = no cap.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Enable TLS verification. Set false only for self-managed instances with self-signed certs.",
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

    deps: Optional[List[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage",
    )
    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.")
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
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.")
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the returned DataFrame to the current static/dynamic partition key (e.g. 'customer_id').",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill strategy for a partitioned asset. 'single_run' materializes a whole backfill range in one run "
            "(rows are filtered to the whole range); 'multi_run' launches one run per partition, optionally capped by "
            "backfill_policy_max_partitions_per_run. Unset keeps Dagster's default. Needs a partition-range-aware IO "
            "manager for single_run."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run (default 1).",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from destination/component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    # No column_lineage field: see module docstring -- this component can produce several
    # independently-schemaed table-assets from one instance (`tables`), and column_lineage's
    # mapping is inherently single-upstream-asset-shaped -- it doesn't generalize across
    # heterogeneous tables (same reasoning as servicenow_ingestion).

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 5 rows "
            "as a markdown table). Used by builder UIs to render asset shape "
            "without warehouse access."
        ),
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

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

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
        """Wraps _resolve_destination() to inject `bucket_url` (filesystem) or `query_result_bucket`
        (athena) -- dlt needs these to know where to write/query, separately from the staging area
        wired by _resolve_staging() for destinations that go through one."""
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
        """Databricks and Athena both load via an intermediate filesystem stage -- dlt requires this
        staging destination to be activated explicitly, it is never auto-enabled."""
        if self.destination not in ("databricks", "athena"):
            return None
        if self.bucket_url:
            return dlt.destinations.filesystem(bucket_url=self.bucket_url)
        return "filesystem"

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='GitLab Ingestion', editable=True)

    def _build_resource_config(self, tables_list, dest_table_names):
        """Build the dlt rest_api_source `resources` list entries for `tables_list`."""
        resources = []
        for t in tables_list:
            if t == "projects":
                if self.group_id:
                    path = f"groups/{self.group_id}/projects"
                else:
                    path = "projects"
                params: Dict[str, Any] = {"per_page": self.per_page}
            elif t in _ENDPOINT_PATHS:
                path = _ENDPOINT_PATHS[t].format(project_id=self.project_id)
                params = {"per_page": self.per_page}
                if t in ("issues", "merge_requests"):
                    if self.state:
                        params["state"] = self.state
                    if self.updated_after:
                        params["updated_after"] = self.updated_after
                elif t == "commits":
                    if self.since:
                        params["since"] = self.since
                    if self.until:
                        params["until"] = self.until
                    if self.ref_name:
                        params["ref_name"] = self.ref_name
            else:
                raise ValueError(
                    f"GitlabIngestionComponent: unknown table {t!r}; must be one of {sorted(_KNOWN_TABLES)}."
                )
            resources.append(
                {
                    "name": t,
                    "table_name": dest_table_names[t],
                    "endpoint": {
                        "path": path,
                        "params": params,
                    },
                }
            )
        return resources

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        component = self
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        tables_list = [t.strip() for t in self.tables.split(",") if t.strip()]
        if not tables_list:
            raise ValueError("GitlabIngestionComponent: 'tables' must list at least one GitLab resource.")
        _unknown_tables = set(tables_list) - _KNOWN_TABLES
        if _unknown_tables:
            raise ValueError(
                f"GitlabIngestionComponent: 'tables' has unknown entries {sorted(_unknown_tables)}; "
                f"must be one of {sorted(_KNOWN_TABLES)}."
            )
        _project_scoped_requested = sorted(set(tables_list) & _PROJECT_SCOPED_TABLES)
        if _project_scoped_requested and not self.project_id:
            raise ValueError(
                f"GitlabIngestionComponent: tables {_project_scoped_requested} are project-scoped "
                "(GET /projects/:id/...) and require 'project_id' to be set."
            )
        output_names = {t: f"{asset_name}_{t}" for t in tables_list}
        _table_name_map = self.table_name_map or {}
        _unknown_overrides = set(_table_name_map) - set(tables_list)
        if _unknown_overrides:
            raise ValueError(
                f"GitlabIngestionComponent: table_name_map has key(s) not in 'tables': "
                f"{sorted(_unknown_overrides)}."
            )
        dest_table_names = {t: _table_name_map.get(t, t) for t in tables_list}
        base_url = self.base_url.rstrip("/")
        destination = self.destination
        dataset_name = self.dataset_name or asset_name
        persist_only = self.persist_only
        deps = self.deps
        group_name = self.group_name
        max_records = self.max_records

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        hash_columns = self.hash_columns
        hash_salt = self.hash_salt
        hash_salt_env_var = self.hash_salt_env_var
        on_schema_change = self.on_schema_change
        exclude_columns = self.exclude_columns
        max_rows_per_resource = self.max_rows_per_resource

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy as _BackfillPolicy
            _backfill_policy_obj = _BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy as _BackfillPolicy
            _backfill_policy_obj = _BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)

        # Infer kinds from destination/component name if not explicitly set
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
            _inferred_kinds = ["gitlab", "python"]
        _inferred_kinds = list(dict.fromkeys(_inferred_kinds))

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        def _build_pipeline_config(selected_tables):
            token = _require_env(component.access_token_env_var) if component.access_token_env_var else None
            auth = (
                {"type": "api_key", "name": "PRIVATE-TOKEN", "api_key": token, "location": "header"}
                if token
                else None
            )

            import requests as _requests
            from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

            session = _requests.Session()
            session.verify = component.verify_ssl

            client_config: Dict[str, Any] = {
                "base_url": base_url,
                "session": session,
                "paginator": HeaderLinkPaginator(),
            }
            if auth:
                client_config["auth"] = auth

            config: Dict[str, Any] = {
                "client": client_config,
                "resources": component._build_resource_config(selected_tables, dest_table_names),
            }
            if on_schema_change != "allow_all":
                config["resource_defaults"] = {"schema_contract": _resolve_schema_contract(on_schema_change)}
            return config

        if self.asset_shape == "single":
            @asset(retry_policy=_retry_policy, partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
                key=AssetKey.from_user_string(asset_name),
                description=self.description or f"GitLab pipeline: {', '.join(tables_list)}",
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                deps=[AssetKey.from_user_string(d) for d in (deps or [])],
            )
            def gitlab_pipeline_asset(context: AssetExecutionContext):
                if context.has_partition_key:
                    context.log.info(f"Running for partition: {context.partition_key}")

                from dlt.sources.rest_api import rest_api_source

                context.log.info(f"Starting GitLab ingestion, destination={destination or 'duckdb (in-memory)'}")

                config = _build_pipeline_config(tables_list)

                pipeline = dlt.pipeline(
                    pipeline_name=f"{asset_name}_pipeline",
                    destination=component._resolve_destination_with_bucket(),
                    staging=component._resolve_staging(),
                    dataset_name=dataset_name,
                )

                context.log.info(f"Creating GitLab REST API source for tables {tables_list!r}...")
                source = rest_api_source(config)
                if exclude_columns:
                    _apply_exclude_columns(source.resources.values(), exclude_columns)
                if hash_columns:
                    _apply_hash_columns(source.resources.values(), hash_columns, hash_salt, hash_salt_env_var)
                if max_rows_per_resource:
                    _apply_max_rows_per_resource(source.resources.values(), max_rows_per_resource)

                context.log.info("Extracting GitLab data...")
                load_info = pipeline.run(source)
                context.log.info(f"GitLab data loaded: {load_info}")

                base_metadata = {
                    "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                    "dataset_name": MetadataValue.text(dataset_name),
                    "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                    "tables_requested": MetadataValue.json(tables_list),
                    "destination_table_names": MetadataValue.json(dest_table_names),
                }

                non_sql_destinations = {"filesystem", "delta", "iceberg"}
                is_non_sql = destination in non_sql_destinations
                if persist_only or is_non_sql:
                    if is_non_sql and not persist_only:
                        context.log.warning(
                            f"destination={destination!r} is not SQL-backed; cannot return a DataFrame. "
                            f"Set persist_only=true to silence this warning."
                        )
                    return MaterializeResult(metadata=base_metadata)

                from dagster import TableColumn, TableSchema

                all_data = []
                resource_metadata = {}
                with pipeline.sql_client() as client:
                    for t in tables_list:
                        try:
                            with client.execute_query(f"SELECT * FROM {dataset_name}.{dest_table_names[t]}") as cur:
                                df = cur.df()
                            if len(df) > 0:
                                df["_table"] = t
                                all_data.append(df)
                                resource_metadata[t] = len(df)
                        except Exception as e:
                            context.log.warning(f"Could not load {t} (destination table {dest_table_names[t]!r}): {e}")

                if not all_data:
                    context.log.warning(f"No data extracted from GitLab tables {tables_list!r}.")
                    return Output(value=pd.DataFrame(), metadata=base_metadata)

                pipeline_df = pd.concat(all_data, ignore_index=True)
                pipeline_df = _filter_df_to_partition(pipeline_df, context, partition_date_column, partition_static_column)
                if max_records is not None and len(pipeline_df) > max_records:
                    pipeline_df = pipeline_df.head(max_records)

                metadata = {
                    **base_metadata,
                    "row_count": MetadataValue.int(len(pipeline_df)),
                    "column_count": MetadataValue.int(len(pipeline_df.columns)),
                    "tables_loaded": MetadataValue.json(list(resource_metadata.keys())),
                    "dagster/column_schema": MetadataValue.table_schema(TableSchema(columns=[
                        TableColumn(name=str(col), type=str(pipeline_df.dtypes[col])) for col in pipeline_df.columns
                    ])),
                }
                for _table_name, _rows in resource_metadata.items():
                    metadata[f"rows_{_table_name}"] = MetadataValue.int(_rows)
                if include_preview and len(pipeline_df) > 0:
                    try:
                        _prev = pipeline_df.sample(min(preview_rows, len(pipeline_df))) if len(pipeline_df) > preview_rows * 10 else pipeline_df.head(preview_rows)
                        metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                    except Exception as _e:
                        context.log.warning(f"preview emission failed: {_e}")
                return Output(value=pipeline_df, metadata=metadata)

            return Definitions(assets=[gitlab_pipeline_asset])

        from dagster import AssetOut, multi_asset

        # Each table gets its own independently-materializable asset (can_subset=True):
        # picking just "issues" only fetches/loads the issues resource, not the others.
        # is_required=False lets the compute yield only the selected table(s) per run.
        outs = {
            output_names[t]: AssetOut(
                description=self.description or f"GitLab resource reader: {t}",
                is_required=False,
                group_name=group_name,
                owners=owners,
                tags=_all_tags,
                kinds=set(_inferred_kinds),
                freshness_policy=_freshness_policy,
            )
            for t in tables_list
        }

        @multi_asset(
            outs=outs,
            name=asset_name,
            can_subset=True,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(d) for d in (deps or [])],
        )
        def gitlab_ingestion_assets(context: AssetExecutionContext):
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")

            try:
                selected_output_names = set(context.op_execution_context.selected_output_names)
            except Exception:  # noqa: BLE001
                selected_output_names = set(output_names.values())
            selected_tables = [t for t in tables_list if output_names[t] in selected_output_names]
            if not selected_tables:
                context.log.info("gitlab_ingestion: no tables selected; skipping.")
                return

            from dlt.sources.rest_api import rest_api_source

            context.log.info(
                f"Starting GitLab ingestion for {selected_tables!r}, "
                f"destination={destination or 'duckdb (in-memory)'}"
            )

            config = _build_pipeline_config(selected_tables)

            pipeline = dlt.pipeline(
                pipeline_name=f"{asset_name}_pipeline",
                destination=component._resolve_destination_with_bucket(),
                staging=component._resolve_staging(),
                dataset_name=dataset_name,
            )

            context.log.info(f"Creating GitLab REST API source for tables {selected_tables!r}...")
            source = rest_api_source(config)
            if exclude_columns:
                _apply_exclude_columns(source.resources.values(), exclude_columns)
            if hash_columns:
                _apply_hash_columns(source.resources.values(), hash_columns, hash_salt, hash_salt_env_var)
            if max_rows_per_resource:
                _apply_max_rows_per_resource(source.resources.values(), max_rows_per_resource)

            context.log.info("Extracting GitLab data...")
            load_info = pipeline.run(source)
            context.log.info(f"GitLab data loaded: {load_info}")

            non_sql_destinations = {"filesystem", "delta", "iceberg"}
            is_non_sql = destination in non_sql_destinations

            if persist_only or is_non_sql:
                if is_non_sql and not persist_only:
                    context.log.warning(
                        f"destination={destination!r} is not SQL-backed; cannot return a DataFrame. "
                        f"Set persist_only=true to silence this warning."
                    )
                for t in selected_tables:
                    yield MaterializeResult(
                        asset_key=AssetKey.from_user_string(output_names[t]),
                        metadata={
                            "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                            "dataset_name": MetadataValue.text(dataset_name),
                            "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                            "table": MetadataValue.text(t),
                            "destination_table_name": MetadataValue.text(dest_table_names[t]),
                        },
                    )
                return

            from dagster import TableColumn, TableSchema

            with pipeline.sql_client() as client:
                for t in selected_tables:
                    try:
                        with client.execute_query(f"SELECT * FROM {dataset_name}.{dest_table_names[t]}") as cur:
                            df = cur.df()
                    except Exception as e:
                        context.log.warning(f"Could not load {t} (destination table {dest_table_names[t]!r}): {e}")
                        df = pd.DataFrame()

                    df = _filter_df_to_partition(df, context, partition_date_column, partition_static_column)
                    if max_records is not None and len(df) > max_records:
                        df = df.head(max_records)

                    metadata = {
                        "destination": MetadataValue.text(destination or "duckdb (in-memory)"),
                        "dataset_name": MetadataValue.text(dataset_name),
                        "pipeline_name": MetadataValue.text(f"{asset_name}_pipeline"),
                        "table": MetadataValue.text(t),
                        "destination_table_name": MetadataValue.text(dest_table_names[t]),
                        "dagster/row_count": MetadataValue.int(len(df)),
                        "column_count": MetadataValue.int(len(df.columns)),
                        "dagster/column_schema": MetadataValue.table_schema(TableSchema(columns=[
                            TableColumn(name=str(col), type=str(df.dtypes[col])) for col in df.columns
                        ])),
                    }
                    if include_preview and len(df) > 0:
                        try:
                            _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                            metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                        except Exception as _e:
                            context.log.warning(f"preview emission failed: {_e}")

                    yield Output(value=df, output_name=output_names[t], metadata=metadata)

        return Definitions(assets=[gitlab_ingestion_assets])


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
