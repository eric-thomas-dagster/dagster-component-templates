"""HrisNormalizerComponent — vendor-agnostic HRIS schema normalization.

Takes any DataFrame of employee data (from Workday, BambooHR, ADP,
Gusto, Rippling, Hibob, an internal HRIS export, a CSV, anything) and
maps it to a canonical employee schema downstream HR analytics
components can rely on.

Canonical schema (one row per employee):
  employee_id, email, first_name, last_name, full_name,
  manager_employee_id, department, job_title, location, country,
  employment_status, employment_type, hire_date, termination_date,
  tenure_days, is_active

The mapping is config-driven — provide a vendor-specific
`column_map: { canonical_field: source_column }` mapping. Optional
status / type / boolean normalization rules cover the most common
vendor differences.
"""

import datetime as _dt
import os
from typing import Any, Dict, List, Literal, Optional, Union

import pandas as pd

from dagster import (
    BackfillPolicy,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


_CANONICAL_FIELDS = [
    "employee_id",
    "email",
    "first_name",
    "last_name",
    "full_name",
    "manager_employee_id",
    "department",
    "job_title",
    "location",
    "country",
    "employment_status",
    "employment_type",
    "hire_date",
    "termination_date",
]


_DEFAULT_STATUS_MAP = {
    "active":          "active",
    "a":               "active",
    "current":         "active",
    "inactive":        "inactive",
    "terminated":      "terminated",
    "terminate":       "terminated",
    "term":            "terminated",
    "leave":           "on_leave",
    "leave of absence": "on_leave",
    "loa":             "on_leave",
    "pending":         "pending",
    "future":          "pending",
}

_DEFAULT_TYPE_MAP = {
    "full_time":  "full_time",
    "fulltime":   "full_time",
    "ft":         "full_time",
    "part_time":  "part_time",
    "parttime":   "part_time",
    "pt":         "part_time",
    "contractor": "contractor",
    "contract":   "contractor",
    "intern":     "intern",
    "temp":       "temporary",
    "temporary":  "temporary",
}



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


class HrisNormalizerComponent(Component, Model, Resolvable):
    """Vendor-agnostic HRIS normalization. Maps any employee DataFrame to
    a canonical schema usable by downstream `hr_metrics` and other
    HR-aware components.
    """

    asset_name: str = Field(description="Output asset name (the normalized employee table).")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream DataFrame asset — the raw HRIS data.. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )

    column_map: Dict[str, Union[str, int]] = Field(
        description=(
            "{canonical_field: source_column}. Canonical fields: "
            "employee_id, email, first_name, last_name, full_name, "
            "manager_employee_id, department, job_title, location, country, "
            "employment_status, employment_type, hire_date, termination_date. "
            "Missing canonical fields are filled with None."
        ),
    )

    status_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Ergonomic shorthand for value_maps['employment_status']. "
            "{vendor_value: canonical_value}. Defaults applied: "
            "active/inactive/terminated/on_leave/pending."
        ),
    )
    type_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Ergonomic shorthand for value_maps['employment_type']. "
            "Defaults applied: full_time/part_time/contractor/intern/temporary."
        ),
    )
    value_maps: Optional[Dict[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "Generic per-field value normalization. {canonical_field: {vendor_value: canonical_value}}. "
            "Apply to ANY canonical field (department, location, country, job_title, etc.). "
            "Wins over status_map / type_map when both are set for the same field."
        ),
    )

    derive_full_name: bool = Field(
        default=True,
        description="If full_name isn't mapped, derive it from first_name + last_name.",
    )
    compute_tenure: bool = Field(
        default=True,
        description="Adds a tenure_days column (today - hire_date if active, else termination_date - hire_date).",
    )
    derive_is_active: bool = Field(
        default=True,
        description="Adds an is_active boolean column based on the normalized employment_status.",
    )

    drop_extra_columns: bool = Field(
        default=False,
        description="If True, only the canonical columns are kept. If False (default), source columns are preserved with a vendor_ prefix.",
    )
    case_insensitive_map: bool = Field(
        default=True,
        description="Lowercase + strip vendor values before applying status_map / type_map.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream rows to the current static/dynamic partition key (e.g. 'customer_id').",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Name of the static axis in a multi-partition, e.g. 'customer' or 'region'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (the row-filter "
            "logic below already handles a multi-partition range correctly). "
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners'), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("HrisNormalizerComponent: set exactly one of `upstream_asset_key` or `source`.")
        partitions_def = None
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        unknown_targets = [k for k in self.column_map.keys() if k not in _CANONICAL_FIELDS]
        if unknown_targets:
            raise ValueError(
                f"HrisNormalizerComponent: column_map keys must be canonical fields. "
                f"Unknown: {unknown_targets}. Allowed: {_CANONICAL_FIELDS}"
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key) if self.upstream_asset_key else None
        column_map = dict(self.column_map)

        # Build the per-field value-normalization config. Generic
        # value_maps wins; status_map / type_map are sugar that lands
        # in value_maps['employment_status'] / value_maps['employment_type'].
        value_maps_combined: Dict[str, Dict[str, str]] = {
            "employment_status": {**_DEFAULT_STATUS_MAP, **(self.status_map or {})},
            "employment_type":   {**_DEFAULT_TYPE_MAP,   **(self.type_map or {})},
        }
        for canon_field, vmap in (self.value_maps or {}).items():
            if canon_field not in _CANONICAL_FIELDS:
                raise ValueError(
                    f"value_maps key {canon_field!r} is not a canonical field. "
                    f"Allowed: {_CANONICAL_FIELDS}"
                )
            # Generic value_maps overrides ergonomic shorthands when both set.
            value_maps_combined[canon_field] = {
                **value_maps_combined.get(canon_field, {}),
                **vmap,
            }
        derive_full_name = self.derive_full_name
        compute_tenure = self.compute_tenure
        derive_is_active = self.derive_is_active
        drop_extra_columns = self.drop_extra_columns
        case_insensitive_map = self.case_insensitive_map

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Canonical HRIS schema mapped from {self.upstream_asset_key}.",
            group_name=self.group_name,
            kinds={"hris", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins=({"upstream": AssetIn(key=upstream_key)} if upstream_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
        )
        def _asset(context: AssetExecutionContext, **kwargs):
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(self.source, context)
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            # Filter to current partition if partitioned
            if context.has_partition_key or context.has_partition_key_range:
                # One filter pass per partition key: a normal tick gives exactly one,
                # a single_run backfill gives the whole range. Rows are selected by
                # position so keys that match overlapping rows never duplicate them.
                _pk_base = upstream
                _pk_pos = _pk_base.reset_index(drop=True) if hasattr(_pk_base, 'reset_index') else _pk_base
                _pk_keep = set()
                _pk_filtered = False
                for _pk_iter in context.partition_keys:
                    upstream = _pk_pos
                    _pk = _pk_iter
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                        upstream = upstream[_col_dates == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                    if upstream is not _pk_pos:
                        _pk_filtered = True
                        _pk_keep.update(upstream.index)
                upstream = _pk_base.iloc[sorted(_pk_keep)] if _pk_filtered else _pk_base
            df = upstream.copy()
            if df.empty:
                empty = pd.DataFrame(columns=_CANONICAL_FIELDS)
                return Output(value=empty, metadata={"row_count": MetadataValue.int(0)})

            # Build the canonical-output DataFrame.
            out: Dict[str, Any] = {}
            for canon, src in column_map.items():
                if src not in df.columns:
                    context.log.warning(
                        f"column_map: source column {src!r} → {canon!r} not in upstream. Filling with None."
                    )
                    out[canon] = pd.Series([None] * len(df))
                else:
                    out[canon] = df[src]
            for canon in _CANONICAL_FIELDS:
                if canon not in out:
                    out[canon] = pd.Series([None] * len(df))

            normalized = pd.DataFrame(out)

            # Derive full_name if missing.
            if derive_full_name and (normalized["full_name"].isna().all() or normalized["full_name"].astype(str).str.strip().eq("").all()):
                fn = normalized["first_name"].fillna("").astype(str).str.strip()
                ln = normalized["last_name"].fillna("").astype(str).str.strip()
                derived = (fn + " " + ln).str.strip()
                normalized["full_name"] = derived.where(derived.ne(""), other=None)

            # Apply per-field value normalization. When case_insensitive_map
            # is True we lowercase BOTH the input value AND the map keys
            # before lookup, so user maps like {A: active} match raw values
            # like 'A' / 'a'. Loops over EVERY configured canonical field
            # (status, type, plus anything in value_maps).
            def _build_lookup(mapping: Dict[str, str]) -> Dict[str, str]:
                if not case_insensitive_map:
                    return dict(mapping)
                return {str(k).strip().lower(): v for k, v in mapping.items()}

            def _norm_value(v: Any, mapping: Dict[str, str]) -> Optional[str]:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                key = str(v).strip()
                if case_insensitive_map:
                    key = key.lower()
                return mapping.get(key, str(v))  # fallback: pass through

            for canon_field, vmap in value_maps_combined.items():
                if canon_field not in normalized.columns:
                    continue
                lookup = _build_lookup(vmap)
                normalized[canon_field] = normalized[canon_field].apply(
                    lambda v, _l=lookup: _norm_value(v, _l)
                )

            # Parse dates.
            for col in ("hire_date", "termination_date"):
                normalized[col] = pd.to_datetime(normalized[col], errors="coerce", utc=False).dt.date.astype(object).where(pd.notna(pd.to_datetime(normalized[col], errors='coerce')), None)

            # Compute tenure_days.
            if compute_tenure:
                today = _dt.date.today()
                def _tenure(row):
                    hd = row.get("hire_date")
                    td = row.get("termination_date")
                    if not hd or pd.isna(hd):
                        return None
                    end = td if (td and not pd.isna(td)) else today
                    try:
                        return (end - hd).days
                    except Exception:
                        return None
                normalized["tenure_days"] = normalized.apply(_tenure, axis=1)

            # Derive is_active.
            if derive_is_active:
                normalized["is_active"] = normalized["employment_status"].apply(
                    lambda s: s == "active" if isinstance(s, str) else False
                )

            # Optionally preserve source columns with vendor_ prefix.
            if not drop_extra_columns:
                for col in df.columns:
                    if col not in column_map.values():
                        new_name = f"vendor_{col}"
                        if new_name not in normalized.columns:
                            normalized[new_name] = df[col].values

            preview_md = normalized.head(10).to_markdown(index=False) if not normalized.empty else "(empty)"
            md = {
                "row_count":     MetadataValue.int(len(normalized)),
                "column_count":  MetadataValue.int(len(normalized.columns)),
                "active_count":  MetadataValue.int(int(normalized.get("is_active", pd.Series([], dtype=bool)).sum())),
                "preview":       MetadataValue.md(preview_md or ""),
            }
            if "department" in normalized.columns:
                md["department_count"] = MetadataValue.int(int(normalized["department"].nunique(dropna=True)))
            return Output(value=normalized, metadata=md)

        return Definitions(assets=[_asset])


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
