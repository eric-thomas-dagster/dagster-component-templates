"""Feature Scaler Asset Component.

Rescale numeric DataFrame columns using one of: standard (z-score), min-max,
robust (median/IQR), or max-abs scaling. Each strategy mirrors the
corresponding scikit-learn scaler but is implemented directly in pandas/numpy
so the component has no extra runtime dependency.
"""
from typing import Any, Dict, List, Optional, Union, Literal

import numpy as np
import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field



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


class FeatureScalerComponent(Component, Model, Resolvable):
    """Rescale numeric columns using standard, min-max, robust, or max-abs scaling.

    - `standard`: (x − mean) / std  (z-score)
    - `minmax`:   (x − min) / (max − min) → [0, 1] (or to feature_range)
    - `robust`:   (x − median) / IQR  (resistant to outliers)
    - `maxabs`:   x / max(|x|)  → [-1, 1] (preserves sparsity)
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    strategy: str = Field(
        default="standard",
        description="Scaling strategy: 'standard', 'minmax', 'robust', or 'maxabs'.",
    )
    columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Columns to scale. None = all numeric columns in the upstream DataFrame.",
    )
    feature_range_min: float = Field(
        default=0.0,
        description="Lower bound for 'minmax' (default 0.0). Ignored for other strategies.",
    )
    feature_range_max: float = Field(
        default=1.0,
        description="Upper bound for 'minmax' (default 1.0). Ignored for other strategies.",
    )
    suffix: Optional[str] = Field(
        default=None,
        description="If set, scaled values go into new columns named '<col><suffix>' (e.g. '_scaled'). Otherwise originals are overwritten.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format")
    partition_date_column: Optional[Union[str, int]] = Field(default=None, description="Column used to filter to the current date partition key.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    partition_static_dim: Optional[str] = Field(default=None, description="Dimension name for the static axis in multi-partitioning.")
    partition_static_column: Optional[Union[str, int]] = Field(default=None, description="Column used to filter to the current static partition dimension.")
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
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — team names or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. Auto-inferred from asset name if empty.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping. Auto-inferred when empty.",
    )

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
        return "Rescale numeric columns using standard, min-max, robust, or max-abs scaling."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("FeatureScalerComponent: set exactly one of `upstream_asset_key` or `source`.")
        # Standard catalog fields — phase 2 wiring
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
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        strategy = self.strategy
        columns = self.columns
        fr_min = self.feature_range_min
        fr_max = self.feature_range_max
        suffix = self.suffix
        group_name = self.group_name

        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
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

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins=({"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))} if upstream_asset_key else None),
            required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
            partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            description=FeatureScalerComponent.get_description(),
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
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
                    _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
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

            target_cols = columns if columns else df.select_dtypes(include="number").columns.tolist()
            stats: Dict[str, Dict[str, float]] = {}

            for col in target_cols:
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found, skipping.")
                    continue
                s = pd.to_numeric(df[col], errors="coerce")
                out_col = f"{col}{suffix}" if suffix else col

                if strategy == "standard":
                    mu, sd = float(s.mean()), float(s.std(ddof=0))
                    sd = sd if sd != 0 else 1.0
                    df[out_col] = (s - mu) / sd
                    stats[col] = {"mean": mu, "std": sd}
                elif strategy == "minmax":
                    lo, hi = float(s.min()), float(s.max())
                    rng = (hi - lo) if hi != lo else 1.0
                    scaled = (s - lo) / rng
                    df[out_col] = scaled * (fr_max - fr_min) + fr_min
                    stats[col] = {"min": lo, "max": hi}
                elif strategy == "robust":
                    med = float(s.median())
                    q1, q3 = float(s.quantile(0.25)), float(s.quantile(0.75))
                    iqr = (q3 - q1) if (q3 - q1) != 0 else 1.0
                    df[out_col] = (s - med) / iqr
                    stats[col] = {"median": med, "iqr": iqr}
                elif strategy == "maxabs":
                    m = float(s.abs().max())
                    m = m if m != 0 else 1.0
                    df[out_col] = s / m
                    stats[col] = {"max_abs": m}
                else:
                    raise ValueError(f"Unknown scaling strategy: {strategy!r}")

                context.log.info(f"Column '{col}' → '{out_col}' scaled with '{strategy}': {stats[col]}")

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(c), type=str(df.dtypes[c])) for c in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "scaling_strategy": MetadataValue.text(strategy),
                "scaling_stats": MetadataValue.json(stats),
            }

            _effective_lineage = column_lineage
            if not _effective_lineage:
                _effective_lineage = {}
                _upstream_cols = set(upstream.columns)
                for c in df.columns:
                    if c in _upstream_cols:
                        _effective_lineage[c] = [c]
                if suffix:
                    for col in target_cols:
                        if col in _upstream_cols:
                            _effective_lineage[f"{col}{suffix}"] = [col]
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {
                        out_col: [TableColumnDep(asset_key=_upstream_key, column_name=ic) for ic in in_cols]
                        for out_col, in_cols in _effective_lineage.items()
                    }
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return df

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[_asset])
        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
