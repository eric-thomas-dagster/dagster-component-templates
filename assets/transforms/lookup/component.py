"""LookupMatch.

Azure Data Factory's "Lookup" data flow transform: for each row in a primary
stream, look up a matching row in a secondary/lookup stream by key column(s).

Differs from a plain left join (already covered by `dataframe_join`) in two
ways:
  1. Deterministic multiple-match resolution -- when several lookup rows
     match the same key, `match_sort_by` (+ `match_sort_ascending`) picks
     exactly ONE candidate instead of fanning out into multiple output rows.
  2. `output_mode='is_match'` -- emit a single boolean column indicating
     whether a match was found, instead of pulling the matched row's columns
     into the output (useful for a later filter / conditional-split step).
"""
from typing import Any, Dict, List, Optional, Union, Literal

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    MetadataValue,
)
from pydantic import Field


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

    # Both shapes set: ambiguous. Pick one.
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



def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy), .get_connection() (DBAPI), or .get_client() (vendor
    client -- dispatched by the client's own shape since "get_client" means
    something different per vendor: BigQuery's .query(sql).to_dataframe(),
    Redshift's .execute_query(sql, fetch_results=True, cursor_factory=
    RealDictCursor)), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


def _normalize_match_sort_ascending(sort_by: Optional[List], sort_ascending) -> Optional[List[bool]]:
    """Expand `match_sort_ascending` (bool or list) to one entry per `match_sort_by`
    column. Shared by both the pandas and polars match-resolution paths."""
    if not sort_by:
        return None
    if isinstance(sort_ascending, (list, tuple)):
        if len(sort_ascending) != len(sort_by):
            raise ValueError(
                f"LookupMatchComponent: `match_sort_ascending` list length "
                f"({len(sort_ascending)}) must match `match_sort_by` list length ({len(sort_by)})."
            )
        return list(sort_ascending)
    return [True if sort_ascending is None else sort_ascending] * len(sort_by)


def _resolve_one_match_per_key(
    lookup_df: "pd.DataFrame", key_cols: List, sort_by: Optional[List], sort_ascending,
) -> "pd.DataFrame":
    """Collapse the lookup stream to exactly one row per key, the way ADF's Lookup
    transform's sort conditions do: when several rows share a key, sort the
    candidates and take the first. Unset `sort_by`: first-occurrence / arbitrary
    pick (pandas' own `drop_duplicates(keep='first')` row order), documented as
    non-deterministic -- callers who need a specific match MUST set `sort_by`.
    """
    asc = _normalize_match_sort_ascending(sort_by, sort_ascending)
    if sort_by:
        lookup_df = lookup_df.sort_values(by=sort_by, ascending=asc, kind="mergesort")
    return lookup_df.drop_duplicates(subset=key_cols, keep="first")


# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



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


class LookupMatchComponent(Component, Model, Resolvable):
    """Azure Data Factory's "Lookup" data flow transform: for each row in a
    primary stream, find a matching row in a secondary/lookup stream by key
    column(s). Deterministic even when several lookup rows share a key
    (`match_sort_by`), and can emit either the matched columns or a single
    boolean `isMatch()`-style column (`output_mode='is_match'`).
    """

    asset_name: str = Field(description="Output Dagster asset name")
    backend: str = Field(
        default="pandas",
        description=(
            "'pandas' (default) or 'polars'. Polars uses .join() + "
            ".unique(keep='first') for the match-sort dedup, with the same "
            "on/left_on/right_on semantics, and returns a polars DataFrame."
        ),
    )
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "PRIMARY stream asset key providing a DataFrame. Mutually "
            "exclusive with `source` -- set exactly one."
        ),
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull the primary stream directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    lookup_asset_key: str = Field(
        description="LOOKUP (secondary/reference) stream asset key -- the table rows are matched against."
    )
    on: Optional[List[Union[str, int]]] = Field(default=None, description="Column(s) to match on (same name in both streams)")
    left_on: Optional[List[Union[str, int]]] = Field(default=None, description="Primary-stream match columns (when column names differ)")
    right_on: Optional[List[Union[str, int]]] = Field(default=None, description="Lookup-stream match columns (when column names differ)")
    match_sort_by: Optional[List[str]] = Field(
        default=None,
        description=(
            "When several lookup rows match the same key, sort the candidates by these "
            "columns and take exactly one (the first after sorting) -- e.g. 'match the most "
            "recent effective_date' or 'match the highest priority'. ADF Lookup's sort "
            "conditions. Unset: first-occurrence / arbitrary pick (NOT deterministic across "
            "runs with reordered upstream data) -- set this whenever a specific match matters."
        ),
    )
    match_sort_ascending: Optional[Union[bool, List[bool]]] = Field(
        default=None,
        description=(
            "Sort direction(s) for `match_sort_by`. A single bool applies to every sort "
            "column; a list gives each column its own direction (must match `match_sort_by`'s "
            "length). Defaults to ascending (True) per column when unset. Ignored unless "
            "`match_sort_by` is set."
        ),
    )
    output_mode: str = Field(
        default="columns",
        description=(
            "'columns' (default): pull the matched row's columns into the output, same shape "
            "as a left join -- null/NaN for no match. 'is_match': ADF Lookup's isMatch() -- "
            "add exactly one boolean column (named by `output_column`) indicating whether a "
            "match was found, and carry NO lookup-side columns into the output."
        ),
    )
    output_column: Optional[str] = Field(
        default=None,
        description=(
            "output_mode='is_match' only: name for the boolean match-found column "
            "(default 'is_match')."
        ),
    )
    suffixes: List[str] = Field(
        default=["_x", "_y"],
        description=(
            "output_mode='columns' only: suffixes for primary/lookup column names that "
            "collide post-merge (same convention as `dataframe_join`)."
        ),
    )
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output data in metadata (for builder UIs).",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
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
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the primary-stream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

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
        description="Column used to filter the primary-stream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the upstream "
            "DataFrame is filtered once per partition key in the range and the "
            "matching rows are combined). 'multi_run' launches one run per "
            "partition (optionally capped via backfill_policy_max_partitions_per_run). "
            "Leave unset for Dagster's default (one run per partition, uncapped). "
            "Ignored when partition_type/partition_dimensions is not set. "
            "'single_run' requires a partition-range-aware IO manager -- a "
            "generic filesystem/default IO manager cannot persist a single_run "
            "output spanning multiple partitions and will raise a clear error at "
            "materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
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
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_description(cls) -> str:
        return "Match rows against a lookup stream by key column(s), with deterministic multiple-match resolution (ADF's Lookup transform)."

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Lookup Match', editable=True)

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("LookupMatchComponent: set exactly one of `upstream_asset_key` or `source`.")
        if not (self.on or (self.left_on and self.right_on)):
            raise ValueError("LookupMatchComponent: set `on`, or both `left_on` and `right_on`.")
        if self.output_mode not in ("columns", "is_match"):
            raise ValueError(f"LookupMatchComponent: output_mode must be 'columns' or 'is_match', got {self.output_mode!r}")
        backend = (self.backend or "pandas").lower()
        if backend not in ("pandas", "polars"):
            raise ValueError(f"backend must be 'pandas' or 'polars', got {self.backend!r}")

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        lookup_asset_key = self.lookup_asset_key
        on = self.on
        left_on = self.left_on
        right_on = self.right_on
        match_sort_by = self.match_sort_by
        match_sort_ascending = self.match_sort_ascending
        output_mode = self.output_mode
        output_column = self.output_column or "is_match"
        suffixes = self.suffixes or ["_x", "_y"]
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        group_name = self.group_name

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        owners = self.owners or []
        column_lineage = self.column_lineage

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        ins = {
            "lookup": AssetIn(key=AssetKey.from_user_string(lookup_asset_key)),
        }
        if upstream_asset_key:
            ins["primary"] = AssetIn(key=AssetKey.from_user_string(upstream_asset_key))

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins=ins,
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            description=self.description or LookupMatchComponent.get_description(),
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, lookup: pd.DataFrame, **kwargs) -> Any:
            primary = kwargs.get("primary")
            if primary is None:
                primary = _ingest_warehouse_query(self.source, context)
            # Defensive Output/MaterializeResult unwrap -- tolerates upstream
            # authors who annotate `-> Output` or return
            # `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(primary, "value") and hasattr(primary, "metadata"):
                primary = primary.value
            if hasattr(lookup, "value") and hasattr(lookup, "metadata"):
                lookup = lookup.value
            try:
                import polars as pl
                _is_polars_in = isinstance(primary, pl.DataFrame) or isinstance(lookup, pl.DataFrame)
            except Exception:
                pl = None  # type: ignore
                _is_polars_in = False
            # Normalize inputs to pandas first (covers mixed input types).
            # Partition filtering / dict-concat bridging below are pandas-only;
            # the polars backend converts back via pl.from_pandas() right
            # before the join itself, further down.
            if pl is not None and isinstance(primary, pl.DataFrame):
                primary = primary.to_pandas()
            if pl is not None and isinstance(lookup, pl.DataFrame):
                lookup = lookup.to_pandas()
            # partition bridge dict-concat: when an unpartitioned asset
            # consumes a partitioned upstream, Dagster's IO manager loads
            # ALL partitions as a dict; concat to a single DataFrame before
            # any DataFrame ops.
            if isinstance(primary, dict):
                _frames = [v for v in primary.values() if isinstance(v, pd.DataFrame)]
                primary = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            if isinstance(lookup, dict):
                _frames = [v for v in lookup.values() if isinstance(v, pd.DataFrame)]
                lookup = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # Filter primary to current partition if partitioned.
            if context.has_partition_key or context.has_partition_key_range:
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = primary
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    primary = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in primary.columns and _date_key:
                        _col_dates = pd.to_datetime(primary[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        primary = primary[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in primary.columns and _static_key:
                        primary = primary[primary[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in primary.columns and not _is_multi:
                        primary = primary[primary[partition_static_column].astype(str) == str(_pk)]
                    if primary is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(primary.index)
                primary = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base

            l_keys = list(on) if on else list(left_on)
            r_keys = list(on) if on else list(right_on)

            if backend == "polars":
                if pl is None:
                    raise ImportError("polars backend requested but `polars` is not installed.")
                primary_pl = pl.from_pandas(primary)
                lookup_pl = pl.from_pandas(lookup)
                # Same match-sort-dedup-then-join technique as polars_pipeline's
                # own `lookup` op: sort by match_sort_by/descending (if set),
                # THEN take exactly one row per key via .unique(keep='first')
                # BEFORE the join -- a true 1:1 lookup regardless of how many
                # lookup rows share a key, instead of a join fan-out. Always
                # deduped (sort or not), matching the pandas path's own
                # `_resolve_one_match_per_key` (first-occurrence pick when
                # `match_sort_by` is unset).
                if match_sort_by:
                    _asc = _normalize_match_sort_ascending(match_sort_by, match_sort_ascending)
                    _descending = [not a for a in _asc]
                    lookup_pl = lookup_pl.sort(by=match_sort_by, descending=_descending).unique(
                        subset=r_keys, keep="first", maintain_order=True
                    )
                else:
                    lookup_pl = lookup_pl.unique(subset=r_keys, keep="first", maintain_order=True)
                _resolved_n = lookup_pl.height

                if output_mode == "is_match":
                    # Existence-only check -- reduce the lookup side to just its
                    # join keys plus a marker column, so the join can neither
                    # fan out nor pull in any other lookup-side column.
                    _probe_col = f"__lookup_probe_{output_column}__"
                    _right_probe = lookup_pl.select(r_keys).with_columns(
                        pl.lit(True).alias(_probe_col)
                    ).unique(subset=r_keys, keep="first", maintain_order=True)
                    merged_pl = primary_pl.join(
                        _right_probe, left_on=l_keys, right_on=r_keys, how="left", maintain_order="left"
                    )
                    merged_pl = merged_pl.with_columns(
                        pl.col(_probe_col).fill_null(False).alias(output_column)
                    ).drop(_probe_col)
                    if r_keys != l_keys:
                        _dupe_r_keys = [k for k in r_keys if k in merged_pl.columns and k not in l_keys]
                        if _dupe_r_keys:
                            merged_pl = merged_pl.drop(_dupe_r_keys)
                else:
                    _suffix = suffixes[1] if len(suffixes) > 1 else "_y"
                    merged_pl = primary_pl.join(
                        lookup_pl, left_on=l_keys, right_on=r_keys, how="left", suffix=_suffix, maintain_order="left"
                    )
                    if l_keys != r_keys:
                        _dupe_r_keys = [k for k in r_keys if k in merged_pl.columns and k not in l_keys]
                        if _dupe_r_keys:
                            merged_pl = merged_pl.drop(_dupe_r_keys)

                context.log.info(
                    f"LookupMatch (polars): matched primary (n={len(primary)}) against lookup "
                    f"(n={len(lookup)}, resolved to {_resolved_n} unique keys) "
                    f"on {l_keys} (output_mode={output_mode})"
                )

                _merged_for_meta = merged_pl.to_pandas()
                from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
                _col_schema = TableSchema(columns=[
                    TableColumn(name=str(col), type=str(_merged_for_meta.dtypes[col]))
                    for col in _merged_for_meta.columns
                ])
                _metadata = {
                    "dagster/row_count": MetadataValue.int(len(_merged_for_meta)),
                    "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                }
                _effective_lineage = column_lineage
                if not _effective_lineage:
                    try:
                        _upstream_cols = set(primary.columns) | set(lookup.columns)
                        _effective_lineage = {
                            col.name: [col.name] for col in _col_schema.columns
                            if col.name in _upstream_cols
                        }
                    except Exception:
                        pass
                if _effective_lineage:
                    _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                    if _upstream_key:
                        _lineage_deps = {}
                        for out_col, in_cols in _effective_lineage.items():
                            _lineage_deps[str(out_col)] = [
                                TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                                for ic in in_cols
                            ]
                        _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                            TableColumnLineage(_lineage_deps)
                        )
                _emit_preview(context, merged_pl, include_preview, preview_rows)
                context.add_output_metadata(_metadata)
                return merged_pl

            lookup_resolved = _resolve_one_match_per_key(lookup, r_keys, match_sort_by, match_sort_ascending)

            if output_mode == "is_match":
                _lookup_keys_only = lookup_resolved[r_keys].drop_duplicates()
                if r_keys != l_keys:
                    _lookup_keys_only = _lookup_keys_only.rename(columns=dict(zip(r_keys, l_keys)))
                merged = primary.merge(_lookup_keys_only, on=l_keys, how="left", indicator="_lookup_match")
                merged[output_column] = merged["_lookup_match"] == "both"
                merged = merged.drop(columns=["_lookup_match"])
            else:
                merged = primary.merge(
                    lookup_resolved,
                    how="left",
                    left_on=l_keys,
                    right_on=r_keys,
                    suffixes=tuple(suffixes),
                )
                # Same-name join keys collapse into a single column via pandas'
                # own merge semantics; only drop the lookup-side duplicate when
                # the key names actually differed (both survive the merge).
                if l_keys != r_keys:
                    _dupe_r_keys = [k for k in r_keys if k in merged.columns and k not in l_keys]
                    if _dupe_r_keys:
                        merged = merged.drop(columns=_dupe_r_keys)

            context.log.info(
                f"LookupMatch: matched primary (n={len(primary)}) against lookup "
                f"(n={len(lookup)}, resolved to {len(lookup_resolved)} unique keys) "
                f"on {l_keys} (output_mode={output_mode})"
            )

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(merged.dtypes[col]))
                for col in merged.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(merged)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(primary.columns) | set(lookup.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            _emit_preview(context, merged, include_preview, preview_rows)
            context.add_output_metadata(_metadata)
            return merged

        from dagster import build_column_schema_change_checks

        _schema_checks = build_column_schema_change_checks(assets=[_asset])

        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))


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

# BEGIN output-preview helper (inlined per component; keep copies identical, see tests/test_output_preview.py)
def _emit_preview(context, df, enabled, n):
    """Attach a markdown preview of up to `n` rows to the asset's output metadata when `enabled`."""
    if not enabled or df is None or not hasattr(df, "head") or len(df) == 0:
        return
    try:
        import dagster as _dg

        if hasattr(df, "to_pandas"):
            df = df.to_pandas()
        sample = df.sample(min(n, len(df)), random_state=0) if len(df) > n * 10 else df.head(n)
        try:
            text = sample.to_markdown(index=False)
        except ImportError:
            text = "```\n" + sample.to_string(index=False) + "\n```"
        context.add_output_metadata({"preview": _dg.MetadataValue.md(text)})
    except Exception as exc:
        context.log.warning(f"preview emission failed: {exc}")
# END output-preview helper
