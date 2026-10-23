"""Train/Test Splitter Asset Component.

Split a DataFrame into train, test, and (optional) validation Dagster assets
using either a deterministic random split or a chronological cutoff. Supports
stratified sampling on a label column and group-aware splitting (so all rows
sharing a group key land in the same split — important for leakage prevention).
"""
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
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


class TrainTestSplitterComponent(Component, Model, Resolvable):
    """Split a DataFrame into train / test / (optional) validation assets.

    Three strategies:
    - `random`: Deterministic random split using `random_state`. Optionally stratified
      by `stratify_column` (preserves class balance) or grouped by `group_column`
      (all rows sharing a group key end up in the same split).
    - `time`: Chronological split — earliest rows → train, latest → test.
      Requires `time_column`.
    - `hash`: Stable hash of `group_column` (or row index) → split. Useful when
      you need the split to be reproducible across schema changes.

    Produces 2 or 3 downstream Dagster assets:
      `<asset_name>_train`, `<asset_name>_test`, [`<asset_name>_val`]
    """

    asset_name: str = Field(description="Output asset prefix; produces '<asset_name>_train', '<asset_name>_test', and optionally '_val'.")
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
    strategy: str = Field(default="random", description="Split strategy: 'random', 'time', or 'hash'.")
    test_size: float = Field(default=0.2, description="Fraction allocated to the test set (0.0–1.0).")
    val_size: float = Field(default=0.0, description="Fraction allocated to a validation set. 0.0 = no validation asset emitted.")
    random_state: int = Field(default=42, description="Random seed for 'random' strategy. (sklearn-compatible name)")
    stratify_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column to stratify the random split on (preserves class balance). Mutually exclusive with group_column.",
    )
    group_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column whose values define groups; all rows in a group land in the same split (prevents leakage on related rows).",
    )
    time_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Required for strategy='time'. Column to sort by; oldest rows go to train.",
    )
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output DataFrame in metadata (for builder UIs).",
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows in the preview when include_preview_metadata=True.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name applied to all output assets.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Max acceptable lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule for the freshness policy.")
    column_lineage: Optional[Dict[str, List[str]]] = Field(default=None, description="Column-level lineage (auto-inferred when empty: each output column maps 1:1 to upstream).")


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
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

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
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
        return "Split a DataFrame into train/test (and optional val) assets using random, time, or hash strategies."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            None,  # dynamic_partition_name: not a field of this component
            None,  # partition_dimensions: not a field of this component
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("TrainTestSplitterComponent: set exactly one of `upstream_asset_key` or `source`.")
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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        strategy = self.strategy
        test_size = self.test_size
        val_size = self.val_size
        seed = self.random_state
        stratify_column = self.stratify_column
        group_column = self.group_column
        time_column = self.time_column
        group_name = self.group_name

        if test_size <= 0 or test_size >= 1:
            raise ValueError(f"test_size must be in (0, 1); got {test_size}")
        if val_size < 0 or test_size + val_size >= 1:
            raise ValueError(f"val_size + test_size must be < 1; got {val_size}+{test_size}")
        if strategy == "time" and not time_column:
            raise ValueError("strategy='time' requires time_column")
        if stratify_column and group_column:
            raise ValueError("stratify_column and group_column are mutually exclusive")

        emit_val = val_size > 0

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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage

        def _split(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
            n = len(df)
            if n == 0:
                empty = df.iloc[0:0].copy()
                return {"train": empty, "test": empty.copy(), "val": empty.copy()}

            if strategy == "time":
                df_sorted = df.sort_values(time_column).reset_index(drop=True)
                n_test = int(round(n * test_size))
                n_val = int(round(n * val_size)) if emit_val else 0
                n_train = n - n_test - n_val
                train = df_sorted.iloc[:n_train]
                val = df_sorted.iloc[n_train:n_train + n_val] if emit_val else df_sorted.iloc[0:0]
                test = df_sorted.iloc[n_train + n_val:]
                return {"train": train, "test": test, "val": val}

            if strategy == "hash":
                key_series = df[group_column] if group_column else pd.Series(df.index, index=df.index)
                # Stable hash → bucket in [0, 1)
                bucket = key_series.astype(str).apply(lambda x: (hash((seed, x)) & 0xFFFFFFFF) / 2**32)
                test_cut = 1.0 - test_size
                val_cut = test_cut - val_size
                test = df[bucket >= test_cut]
                val = df[(bucket >= val_cut) & (bucket < test_cut)] if emit_val else df.iloc[0:0]
                train = df[bucket < val_cut]
                return {"train": train, "test": test, "val": val}

            # strategy == "random"
            rng = pd.Series(range(n)).sample(frac=1, random_state=seed).reset_index(drop=True)

            if group_column:
                groups = df[group_column].astype(str)
                unique = groups.drop_duplicates().sample(frac=1, random_state=seed).tolist()
                n_g = len(unique)
                n_test_g = int(round(n_g * test_size))
                n_val_g = int(round(n_g * val_size)) if emit_val else 0
                test_groups = set(unique[:n_test_g])
                val_groups = set(unique[n_test_g:n_test_g + n_val_g]) if emit_val else set()
                test = df[groups.isin(test_groups)]
                val = df[groups.isin(val_groups)] if emit_val else df.iloc[0:0]
                train = df[~groups.isin(test_groups | val_groups)]
                return {"train": train, "test": test, "val": val}

            if stratify_column:
                parts: Dict[str, List[pd.DataFrame]] = {"train": [], "test": [], "val": []}
                for _, sub in df.groupby(stratify_column, dropna=False, sort=False):
                    sub = sub.sample(frac=1, random_state=seed).reset_index(drop=True)
                    sn = len(sub)
                    sn_test = int(round(sn * test_size))
                    sn_val = int(round(sn * val_size)) if emit_val else 0
                    sn_train = sn - sn_test - sn_val
                    parts["train"].append(sub.iloc[:sn_train])
                    if emit_val:
                        parts["val"].append(sub.iloc[sn_train:sn_train + sn_val])
                    parts["test"].append(sub.iloc[sn_train + sn_val:])
                return {
                    "train": pd.concat(parts["train"]) if parts["train"] else df.iloc[0:0],
                    "test": pd.concat(parts["test"]) if parts["test"] else df.iloc[0:0],
                    "val": pd.concat(parts["val"]) if parts["val"] and emit_val else df.iloc[0:0],
                }

            # Plain random split
            shuffled = df.sample(frac=1, random_state=seed).reset_index(drop=True)
            n_test = int(round(n * test_size))
            n_val = int(round(n * val_size)) if emit_val else 0
            n_train = n - n_test - n_val
            return {
                "train": shuffled.iloc[:n_train],
                "test": shuffled.iloc[n_train + n_val:],
                "val": shuffled.iloc[n_train:n_train + n_val] if emit_val else df.iloc[0:0],
            }

        # Cache the split result so all asset functions see the same partition
        # (each asset is materialized in its own process, but if invoked together
        # in one run we want a single deterministic split).
        # Since assets in Dagster typically execute independently, we re-derive
        # the split inside each asset using the deterministic seed/hash logic.

        def _make_asset(suffix: str, split_key: str):
            @asset(partitions_def=partitions_def, 
                name=f"{asset_name}_{suffix}",
                ins=({"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))} if upstream_asset_key else None),
                required_resource_keys=({self.source["resource_key"]} if (self.source and self.source.get("resource_key")) else None),
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                description=self.description or (f"{suffix.capitalize()} split ({strategy} strategy, test_size={test_size}, val_size={val_size})."),
                retry_policy=_retry_policy,
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
            def _a(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
                upstream = kwargs.get("upstream")
                if upstream is None:
                    upstream = _ingest_warehouse_query(self.source, context)
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
                splits = _split(upstream)
                out = splits[split_key]
                from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
                _col_schema = TableSchema(columns=[
                    TableColumn(name=str(c), type=str(out.dtypes[c])) for c in out.columns
                ])
                _metadata = {
                    "dagster/row_count": MetadataValue.int(len(out)),
                    "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                    "split_strategy": MetadataValue.text(strategy),
                    "split_role": MetadataValue.text(split_key),
                    "split_fraction": MetadataValue.float(len(out) / max(len(upstream), 1)),
                    "rows_in_upstream": MetadataValue.int(len(upstream)),
                }

                _effective_lineage = column_lineage
                if not _effective_lineage:
                    _effective_lineage = {c: [c] for c in out.columns if c in upstream.columns}
                if _effective_lineage and upstream_asset_key:
                    _upstream_key = AssetKey.from_user_string(upstream_asset_key)
                    _lineage_deps = {
                        out_col: [TableColumnDep(asset_key=_upstream_key, column_name=ic) for ic in in_cols]
                        for out_col, in_cols in _effective_lineage.items()
                    }
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
                if include_preview and len(out) > 0:
                    try:
                        _prev = out.sample(min(preview_rows, len(out))) if len(out) > preview_rows * 10 else out.head(preview_rows)
                        _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                    except Exception as _e:
                        context.log.warning(f"preview emission failed: {_e}")
                context.add_output_metadata(_metadata)
                return out
            return _a

        train_asset = _make_asset("train", "train")
        test_asset = _make_asset("test", "test")
        assets = [train_asset, test_asset]
        if emit_val:
            assets.append(_make_asset("val", "val"))

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=assets)
        return Definitions(assets=assets, asset_checks=list(_schema_checks))


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


# BEGIN partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port.py)
def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
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
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
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
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
# END partitions-def builder
