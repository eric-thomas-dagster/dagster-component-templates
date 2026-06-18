"""DataframeJoin.

Join two DataFrame assets on common or specified columns.
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


class DataframeJoin(Component, Model, Resolvable):
    """Join two DataFrame assets on common or specified columns."""

    asset_name: str = Field(description="Output Dagster asset name")
    backend: str = Field(
        default="pandas",
        description="'pandas' (default) or 'polars'. Polars uses .join() with the same how/on/left_on/right_on semantics and returns a polars DataFrame.",
    )
    left_asset_key: str = Field(description="Left DataFrame asset key")
    right_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Right DataFrame asset key. Leave unset (or equal to `left_asset_key`) "
            "to self-join — the upstream frame is cloned in-process with all "
            "non-key columns prefixed by `right_prefix` (default 'Right_')."
        ),
    )
    additional_asset_keys: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional list of further DataFrame asset keys to join in after `right` "
            "(N-way merge). Each is merged onto the running result using the same "
            "`how` / `on` / `suffixes`. Cannot be combined with `left_on`/`right_on`."
        ),
    )
    how: str = Field(default="inner", description="Join type: 'inner', 'left', 'right', 'outer', 'cross'")
    on: Optional[List[Union[str, int]]] = Field(default=None, description="Column(s) to join on (same name in both DataFrames)")
    left_on: Optional[List[Union[str, int]]] = Field(default=None, description="Left join columns (when column names differ)")
    right_on: Optional[List[Union[str, int]]] = Field(default=None, description="Right join columns (when column names differ)")
    suffixes: List[str] = Field(default=["_x", "_y"], description="Suffixes for overlapping column names")
    right_prefix: Optional[str] = Field(
        default=None,
        description=(
            "If set, the post-merge `rename` and `drop_columns` will fuzzy-match "
            "keys that start with this prefix against the actual merged column "
            "names: try `prefix + col`, then `col`, then `col + suffixes[1]`. "
            "Useful when the rename map was authored against a tool that "
            "prefixes right-side columns (e.g. 'Right_') but pandas only "
            "suffixes on collision."
        ),
    )
    rename: Optional[Dict[str, Union[str, int]]] = Field(
        default=None,
        description="Post-merge rename map, e.g. {'Right_Age': 'Age At Win'}. Missing keys are ignored.",
    )
    drop_columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Post-merge columns to drop (applied AFTER `rename`). Missing columns are ignored.",
    )
    keep_only_columns: Optional[List[str]] = Field(
        default=None,
        description="If set, keep only these columns post-merge (applied AFTER `rename` and `drop_columns`). Missing columns are ignored.",
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

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
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

    @classmethod
    def get_description(cls) -> str:
        return "Join two DataFrame assets on common or specified columns."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
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
        left_asset_key = self.left_asset_key
        right_asset_key = self.right_asset_key
        how = self.how
        on = self.on
        left_on = self.left_on
        right_on = self.right_on
        suffixes = self.suffixes
        group_name = self.group_name

        additional_asset_keys = list(self.additional_asset_keys or [])
        if additional_asset_keys and (left_on or right_on):
            raise ValueError(
                "additional_asset_keys (N-way join) requires `on=`; `left_on`/`right_on` "
                "only work for 2-way joins where left/right keys differ."
            )

        # Self-join: right_asset_key unset, or equal to left_asset_key. The
        # asset takes only `left`; we synthesize `right` in-process by cloning
        # `left` and applying `right_prefix` to its non-key columns.
        _is_self_join = (
            right_asset_key is None
            or right_asset_key == ""
            or right_asset_key == left_asset_key
        )
        if _is_self_join:
            ins = {
                "left": AssetIn(key=AssetKey.from_user_string(left_asset_key)),
            }
        else:
            ins = {
                "left": AssetIn(key=AssetKey.from_user_string(left_asset_key)),
                "right": AssetIn(key=AssetKey.from_user_string(right_asset_key)),
            }
        # Sanitize each extra asset key into a valid Python identifier suitable for kwargs.
        _extra_slots: List[str] = []
        for _i, _k in enumerate(additional_asset_keys):
            _slot = f"extra_{_i}_" + _k.replace(".", "__").replace("/", "__").replace("-", "_")
            _extra_slots.append(_slot)
            ins[_slot] = AssetIn(key=AssetKey.from_user_string(_k))

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "dataframe_join"  # component directory name
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

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None
        backend = (self.backend or "pandas").lower()
        if backend not in ("pandas", "polars"):
            raise ValueError(f"backend must be 'pandas' or 'polars', got {self.backend!r}")

        right_prefix = self.right_prefix
        post_rename = dict(self.rename or {})
        post_drop = list(self.drop_columns or [])
        post_keep_only = list(self.keep_only_columns or [])
        _left_suffix = suffixes[0] if len(suffixes) > 0 else "_x"
        _right_suffix = suffixes[1] if len(suffixes) > 1 else "_y"
        # Join-key column names — never strip-and-touch these even when a
        # `Right_<key>` drop is requested, because pandas already collapses
        # same-name join keys into a single column.
        _join_keys = set((on or []) + (left_on or []) + (right_on or []))

        def _resolve(key, cols):
            """Resolve a `rename`/`drop` key against the actual post-merge cols.
            Honors common left/right prefix conventions: tries exact match,
            then strips Right_ / Left_ prefix and looks for the bare column
            (no-collision case) or the bare + side-suffix (collision case).
            Returns None if the bare column resolves to a join key (which
            doesn't have a separate right-side copy in pandas)."""
            if key in cols:
                return key
            for pref, suf in (("Right_", _right_suffix), ("Left_", _left_suffix)):
                if key.startswith(pref):
                    bare = key[len(pref):]
                    if bare in _join_keys:
                        return None  # join keys are collapsed; no right-side copy
                    if bare in cols:
                        return bare
                    if (bare + suf) in cols:
                        return bare + suf
                    break
            if right_prefix and key.startswith(right_prefix):
                bare = key[len(right_prefix):]
                if bare in _join_keys:
                    return None
                if bare in cols:
                    return bare
                if (bare + _right_suffix) in cols:
                    return bare + _right_suffix
            return None

        def _apply_post_merge(df):
            cols = set(df.columns)
            if post_rename:
                _map = {}
                for k, v in post_rename.items():
                    resolved = _resolve(k, cols)
                    if resolved is not None:
                        _map[resolved] = v
                if _map:
                    df = df.rename(columns=_map)
                    cols = set(df.columns)
            if post_drop:
                _to_drop = [c for c in (_resolve(k, cols) for k in post_drop) if c is not None]
                if _to_drop:
                    df = df.drop(columns=_to_drop)
                    cols = set(df.columns)
            if post_keep_only:
                _keep = [c for c in post_keep_only if c in cols]
                if _keep:
                    df = df[_keep]
            return df

        extra_slots = list(_extra_slots)

        def _do_join(context: AssetExecutionContext, left: Any, right: Any, **extras) -> Any:
            # Pop the self-join override (passed by the self-join branch
            # @asset). When set, replaces the closure-captured `_right_on_local`
            # so the merge points at the prefixed column names rather than
            # the original (un-prefixed) ones the user configured.
            _override_right_on = extras.pop("_override_right_on", None)
            if _override_right_on is not None:
                _right_on_local = _override_right_on
            else:
                _right_on_local = right_on
            try:
                import polars as pl
                _is_polars_in = isinstance(left, pl.DataFrame) or isinstance(right, pl.DataFrame)
            except Exception:
                pl = None  # type: ignore
                _is_polars_in = False
            # Normalize inputs to pandas first (covers mixed input types)
            if pl is not None and isinstance(left, pl.DataFrame):
                left = left.to_pandas()
            if pl is not None and isinstance(right, pl.DataFrame):
                right = right.to_pandas()
            for slot in list(extras):
                if pl is not None and isinstance(extras.get(slot), pl.DataFrame):
                    extras[slot] = extras[slot].to_pandas()
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(left.dtypes[col]))
                for col in left.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(left)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)
            if backend == "polars":
                if pl is None:
                    raise ImportError("polars backend requested but `polars` is not installed.")
                # polars naming: how='inner'|'left'|'right'|'outer'|'cross'|'semi'|'anti'
                # outer in pandas == 'full' in polars
                _how_map = {"outer": "full"}
                pl_how = _how_map.get(how, how)
                left_pl = pl.from_pandas(left)
                right_pl = pl.from_pandas(right)
                join_kwargs = {"how": pl_how, "suffix": suffixes[1] if len(suffixes) > 1 else "_right"}
                if on:
                    join_kwargs["on"] = on
                elif left_on and _right_on_local:
                    join_kwargs["left_on"] = left_on
                    join_kwargs["right_on"] = _right_on_local
                merged_pl = left_pl.join(right_pl, **join_kwargs)
                for slot in extra_slots:
                    nxt = extras.get(slot)
                    if nxt is None:
                        continue
                    nxt_pl = pl.from_pandas(nxt)
                    merged_pl = merged_pl.join(nxt_pl, **({"how": pl_how, "suffix": suffixes[1] if len(suffixes) > 1 else "_right"} | ({"on": on} if on else {})))
                if post_rename or post_drop or post_keep_only:
                    merged_pl = pl.from_pandas(_apply_post_merge(merged_pl.to_pandas()))
                return merged_pl
            else:
                # If join-key columns have mismatched dtypes between left
                # and right (e.g. Int64 vs str — common when one input was
                # auto-typed from a CSV and the other from inline-typed
                # DataFrame), pandas .merge raises ValueError. Coerce both
                # sides' join keys to a common type via the union dtype of
                # each pair before merging.
                def _align_join_dtypes(l_df, r_df, l_keys, r_keys):
                    for lk, rk in zip(l_keys, r_keys):
                        if lk not in l_df.columns or rk not in r_df.columns:
                            continue
                        ld = str(l_df[lk].dtype)
                        rd = str(r_df[rk].dtype)
                        if ld == rd:
                            continue
                        # If either side is object/string, cast both to str —
                        # always-safe + matches pandas's default mixed-type
                        # behavior under `errors='ignore'`. Numeric-numeric
                        # mismatches (Int64 vs float) coerce to float.
                        if "object" in (ld, rd) or "string" in (ld, rd):
                            l_df[lk] = l_df[lk].astype(str)
                            r_df[rk] = r_df[rk].astype(str)
                        else:
                            try:
                                l_df[lk] = l_df[lk].astype(float)
                                r_df[rk] = r_df[rk].astype(float)
                            except (ValueError, TypeError):
                                l_df[lk] = l_df[lk].astype(str)
                                r_df[rk] = r_df[rk].astype(str)
                    return l_df, r_df

                if on:
                    left, right = _align_join_dtypes(left, right, on, on)
                elif left_on and _right_on_local:
                    left, right = _align_join_dtypes(left, right, left_on, _right_on_local)

                # Tolerate missing join keys: if `on` / `left_on` / `_right_on_local`
                # references columns absent from upstream (broken-upstream chain
                # in an importer-converted workflow), do a row-positional concat
                # rather than crashing. Matches lax behavior on bad
                # configs.
                def _row_concat(_l, _r):
                    _l = _l.reset_index(drop=True)
                    _r = _r.reset_index(drop=True)
                    # Drop overlapping non-key cols from right to avoid dupe names.
                    _dupes = [c for c in _r.columns if c in _l.columns]
                    if _dupes:
                        _r = _r.rename(columns={c: c + suffixes[1] for c in _dupes})
                    n = min(len(_l), len(_r))
                    return pd.concat([_l.head(n), _r.head(n)], axis=1)
                _missing_on = [c for c in (on or []) if c not in left.columns or c not in right.columns]
                _missing_left_on = [c for c in (left_on or []) if c not in left.columns]
                _missing_right_on = [c for c in (_right_on_local or []) if c not in right.columns]
                if _missing_on or _missing_left_on or _missing_right_on:
                    context.log.warning(
                        f"dataframe_join: missing join keys "
                        f"on={_missing_on} left_on={_missing_left_on} right_on={_missing_right_on}; "
                        "row-positional concat fallback."
                    )
                    merged = _row_concat(left, right)
                else:
                    # If `right_prefix` is set, prefix only the right-side
                    # columns that would otherwise COLLIDE with the left after
                    # merge:
                    #   • non-key cols that exist on both sides (would get
                    #     suffixed by pandas — apply the prefix instead)
                    #   • _right_on_local keys whose name differs from the
                    #     corresponding left_on key (pandas keeps both post-
                    #     merge; apply the prefix to the right one so callers
                    #     who reference `<prefix><name>` find it)
                    # Same-name join keys (whether via `on=` or matching
                    # left_on/_right_on_local) get COLLAPSED by pandas → never
                    # prefixed. Right's other unique cols stay as-is.
                    _right_in = right
                    _right_on_in = _right_on_local
                    if right_prefix:
                        _join_keys_both = set((on or []) + (_right_on_local or []))
                        # 1. Non-key cols on both sides (collision case)
                        _overlap = [
                            c for c in _right_in.columns
                            if c in left.columns and c not in _join_keys_both
                        ]
                        # 2. _right_on_local keys whose paired left_on key has a
                        #    different name (pandas keeps both post-merge)
                        _diff_name_right_keys = []
                        if left_on and _right_on_local and len(left_on) == len(_right_on_local):
                            for _lk, _rk in zip(left_on, _right_on_local):
                                if _lk != _rk:
                                    _diff_name_right_keys.append(_rk)
                        _to_prefix = list(set(_overlap) | set(_diff_name_right_keys))
                        if _to_prefix:
                            _rename_map = {c: f"{right_prefix}{c}" for c in _to_prefix}
                            _right_in = _right_in.rename(columns=_rename_map)
                            # Update _right_on_local to point at renamed keys.
                            if _right_on_in:
                                _right_on_in = [_rename_map.get(c, c) for c in _right_on_in]
                    try:
                        merged = left.merge(
                            _right_in,
                            how=how,
                            on=on,
                            left_on=left_on,
                            right_on=_right_on_in,
                            suffixes=tuple(suffixes),
                        )
                    except Exception as _merge_err:
                        # Pandas raises MergeError when `suffixes` would create
                        # a duplicate column. Cases include:
                        #   1) Suffixed name already exists (left has both
                        #      `X` and `X_x` from a previous merge).
                        #   2) _right_on_local key also exists on left side as a
                        #      non-key column — pandas sees it as overlap.
                        # Fix: rename EVERY right column that collides with
                        # left (incl. _right_on_local keys), with unique tag. Then
                        # patch left_on/_right_on_local to the renamed keys.
                        _join_keys_both = set(on or [])
                        _renames: Dict[str, str] = {}
                        for _c in right.columns:
                            if _c in left.columns and _c not in _join_keys_both:
                                # Pick a tag that's free in both frames.
                                _tag = "__r"
                                _new = f"{_c}{_tag}"
                                _n = 1
                                while _new in left.columns or _new in right.columns:
                                    _n += 1
                                    _new = f"{_c}__r{_n}"
                                _renames[_c] = _new
                        if _renames:
                            context.log.warning(
                                f"dataframe_join: pandas.merge failed ({_merge_err}); "
                                f"pre-renaming {len(_renames)} right-side overlap cols."
                            )
                            _r2 = right.rename(columns=_renames)
                            _right_on2 = [_renames.get(c, c) for c in (_right_on_local or [])] if _right_on_local else None
                            merged = left.merge(
                                _r2,
                                how=how,
                                on=on,
                                left_on=left_on,
                                right_on=_right_on2,
                                suffixes=tuple(suffixes),
                            )
                        else:
                            raise
                for slot in extra_slots:
                    nxt = extras.get(slot)
                    if nxt is None:
                        continue
                    if on:
                        merged, nxt = _align_join_dtypes(merged, nxt, on, on)
                    merged = merged.merge(
                        nxt,
                        how=how,
                        on=on,
                        suffixes=tuple(suffixes),
                    )
                merged = _apply_post_merge(merged)
                return merged

        # Two-branch asset decoration so Dagster's `ins=` inference (driven
        # by the function signature) doesn't try to wire a phantom `right`
        # input when the join is self-referential.
        if _is_self_join:
            # Adjust right_on so the keys point at the prefixed clone's
            # columns. Stash on a closure-local since `right_on` itself is
            # captured from the outer scope and Python rebinding would
            # break the regular-join branch below.
            _self_prefix = right_prefix or "Right_"
            if right_on:
                _self_right_on = [
                    c if c.startswith(_self_prefix) else f"{_self_prefix}{c}"
                    for c in right_on
                ]
            else:
                _self_right_on = right_on

            @asset(
                partitions_def=partitions_def,
                key=AssetKey.from_user_string(asset_name),
                ins=ins,
                group_name=group_name,
                retry_policy=_retry_policy,
                freshness_policy=_freshness_policy,
                owners=self.owners or [],
                tags=_all_tags,
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _asset(context: AssetExecutionContext, left: Any) -> Any:
                _right = left.rename(columns=lambda c: f"{_self_prefix}{c}").copy()
                return _do_join(context, left, _right, _override_right_on=_self_right_on)
        else:
            @asset(
                partitions_def=partitions_def,
                key=AssetKey.from_user_string(asset_name),
                ins=ins,
                group_name=group_name,
                retry_policy=_retry_policy,
                freshness_policy=_freshness_policy,
                owners=self.owners or [],
                tags=_all_tags,
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _asset(context: AssetExecutionContext, left: Any, right: Any, **extras) -> Any:
                return _do_join(context, left, right, **extras)

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
