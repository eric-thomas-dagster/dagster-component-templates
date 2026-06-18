"""Summarize Component.

Group and aggregate a DataFrame by one or more columns. Equivalent to SQL GROUP BY
or the Summarize tool.
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


def _spatial_agg(series, mode: str):
    """Spatial aggregations: collect geometries in a group, then
    return convex_hull / unary_union / centroid / envelope. Accepts shapely
    objects or WKT/GeoJSON strings."""
    try:
        from shapely import wkt as _wkt
        from shapely.geometry import shape as _shape
        from shapely.geometry.base import BaseGeometry
        from shapely.ops import unary_union
        import json as _json
    except ImportError:
        # Without shapely we can't compute; return the first non-null val.
        for _v in series:
            if _v is not None:
                return _v
        return None

    def _to_geom(v):
        if v is None:
            return None
        if isinstance(v, BaseGeometry):
            return v
        if isinstance(v, dict):
            return _shape(v)
        s = str(v).strip()
        if not s or s.lower() == "nan":
            return None
        try:
            if s.startswith("{"):
                return _shape(_json.loads(s))
            return _wkt.loads(s)
        except Exception:
            return None

    geoms = [g for g in (_to_geom(v) for v in series) if g is not None]
    if not geoms:
        return None
    if mode == "intersect":
        # Pairwise intersection across the whole group. Result is the geometry
        # common to every input shape; empty geometry if any pair is disjoint.
        from functools import reduce
        return reduce(lambda a, b: a.intersection(b), geoms)
    combined = unary_union(geoms)
    if mode == "combine":
        return combined
    if mode == "convex_hull":
        return combined.convex_hull
    if mode == "center":
        return combined.centroid
    if mode == "envelope":
        return combined.envelope
    return combined


class SummarizeComponent(Component, Model, Resolvable):
    """Group and aggregate a DataFrame by one or more columns."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    backend: str = Field(
        default="pandas",
        description=(
            "Execution backend: 'pandas' (default) or 'polars'. Polars gives "
            "substantially better performance + lower memory on large frames "
            "and supports the same aggregations API. Either way the component "
            "accepts pandas OR polars input; when backend='polars' the output "
            "is a polars DataFrame, otherwise pandas."
        ),
    )
    group_by: List[Union[str, int]] = Field(description="Columns to group by")
    group_by_rename: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Post-aggregation rename map applied to group_by columns. "
            "Useful when the source column name should be presented under a "
            "different label in the output (e.g. group_by=['Value'], "
            "group_by_rename={'Value': 'Team'}). Missing keys ignored."
        ),
    )
    aggregations: Dict = Field(
        description=(
            "Mapping of output column name to aggregation. Two forms:\n"
            "  - Simple: `revenue: sum` — aggregate that column with that func.\n"
            "  - Named:  `avg_rating: {col: rating, agg: mean}` — output a "
            "named column from a chosen source column. Use this when you "
            "need two aggregations on the same source column "
            "(e.g. avg_rating=(rating, mean) AND num_ratings=(rating, count))."
        )
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

    @classmethod
    def get_description(cls) -> str:
        return "Group and aggregate a DataFrame by one or more columns."

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
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        group_by = self.group_by
        aggregations = self.aggregations
        group_name = self.group_name
        backend = (self.backend or "pandas").lower()
        if backend not in ("pandas", "polars"):
            raise ValueError(f"backend must be 'pandas' or 'polars', got {self.backend!r}")

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
        _comp_name = "summarize"  # component directory name
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


        # Build-time column lineage inference from group_by + aggregations config
        if not column_lineage:
            _build_time_lineage = {}
            for col in (self.group_by or []):
                _build_time_lineage[col] = [col]
            for out_col in (self.aggregations or {}).keys():
                _build_time_lineage[out_col] = [out_col]
            column_lineage = _build_time_lineage or None

        @asset(
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            description=SummarizeComponent.get_description(),
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> Any:
            # Detect input frame type. Component accepts pandas OR polars at
            # runtime regardless of the configured `backend` — backend chooses
            # the EXECUTION + OUTPUT type, not the input type.
            _is_polars_in = False
            try:
                import polars as pl
                _is_polars_in = isinstance(upstream, pl.DataFrame)
            except Exception:
                pl = None  # type: ignore

            # Filter to current partition if partitioned (pandas path only —
            # we convert to pandas first to keep the partition filter code
            # one-shape, then hand off to the configured backend).
            if _is_polars_in:
                upstream = upstream.to_pandas()
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
            # Split aggregations into the two supported forms:
            #   simple: {out_col: func} → that column with that func, same name out
            #   named:  {out_col: {col: c, agg: f}} → produce named out_col from (c, f)
            # Named form lets you produce multiple aggregations from the same
            # source column (avg_rating + num_ratings both off `rating`).
            # Some ETL tools use slightly different aggregate
            # names than pandas. Translate at config-resolve time so the
            # rest of the asset's agg pipeline doesn't care which dialect
            # the user / importer wrote. Identity entries (sum / mean /
            # min / max / count / median / first / last / std / var) pass
            # through unchanged.
            _ALTERYX_TO_PANDAS_AGG = {
                "avg": "mean",
                "average": "mean",
                "countdistinct": "nunique",
                "count_distinct": "nunique",
                "distinct_count": "nunique",
                "nuniq": "nunique",
                "stddev": "std",
                "variance": "var",
                "concat": "concat",          # handled by registry resolver, leave as-is
                "concat_unique": "concat_unique",
                "groupconcat": "concat",
                "list": "list",
                "mode": "mode",
                # Reporting aggregations: stack values vertically /
                # horizontally into a report cell. Pandas equivalent is
                # newline-joined string for vertical, comma-joined for horizontal.
                "rptvertical": lambda s: "\n".join(map(str, s)),
                "rpthorizontal": lambda s: ", ".join(map(str, s)),
                "rpttext": lambda s: "\n".join(map(str, s)),
                # String aggregations: shortest / longest value in group.
                "shortest": lambda s: min((str(v) for v in s if v is not None), key=len, default=None),
                "longest": lambda s: max((str(v) for v in s if v is not None), key=len, default=None),
                # Spatial aggregations: combine all geometries in
                # the group then derive a shape. Returns shapely geometry.
                "spatialobjconvexhull": lambda s: _spatial_agg(s, "convex_hull"),
                "spatialobjcombine": lambda s: _spatial_agg(s, "combine"),
                "spatialobjcenter": lambda s: _spatial_agg(s, "center"),
                "spatialobjenvelope": lambda s: _spatial_agg(s, "envelope"),
                "spatialobjintersect": lambda s: _spatial_agg(s, "intersect"),
                # GroupBy / Sort markers — non-aggregating, drop.
                "groupby": None,
                "sort": None,
            }
            def _translate_agg(f: Any) -> Any:
                if not isinstance(f, str):
                    return f
                key = f.strip().lower()
                if key in _ALTERYX_TO_PANDAS_AGG:
                    return _ALTERYX_TO_PANDAS_AGG[key] or f  # None falls back to original
                return f

            _named: Dict[str, Any] = {}
            _simple: Dict[str, Any] = {}
            for _out, _spec in aggregations.items():
                if isinstance(_spec, dict) and "col" in _spec and "agg" in _spec:
                    _named[_out] = (_spec["col"], _translate_agg(_spec["agg"]))
                else:
                    _simple[_out] = _translate_agg(_spec)

            # No tolerance for missing group_by / agg columns: a Summarize that
            # silently drops columns produces wrong output. If the workflow
            # references a column that doesn't exist, the user needs to see
            # the failure to fix the configuration. Pandas's KeyError on the
            # groupby/agg call surfaces a clear message at the right step.

            if backend == "polars":
                if pl is None:
                    raise ImportError("polars backend requested but `polars` is not installed. Add it to the project.")
                def _pl_agg(src_col: str, out_col: str, func: str):
                    """Map pandas agg-func name → polars expression."""
                    col = pl.col(src_col)
                    f = func.lower()
                    if f == "sum":      expr = col.sum()
                    elif f == "mean":   expr = col.mean()
                    elif f in ("min",): expr = col.min()
                    elif f in ("max",): expr = col.max()
                    elif f == "count":  expr = col.count()
                    elif f == "median": expr = col.median()
                    elif f == "std":    expr = col.std()
                    elif f == "var":    expr = col.var()
                    elif f == "first":  expr = col.first()
                    elif f == "last":   expr = col.last()
                    elif f in ("nunique", "n_unique"): expr = col.n_unique()
                    else:
                        raise ValueError(f"polars backend doesn't support agg func {func!r}. Use sum/mean/min/max/count/median/std/var/first/last/nunique.")
                    return expr.alias(out_col)
                pl_df = pl.from_pandas(upstream)
                _aggs = []
                for _out, _func in _simple.items():
                    _aggs.append(_pl_agg(_out, _out, _func))   # simple form: source col == out col
                for _out, (_src, _func) in _named.items():
                    _aggs.append(_pl_agg(_src, _out, _func))
                result = pl_df.group_by(group_by).agg(_aggs).sort(group_by)
                _result_for_metadata = result.to_pandas()
            else:
                # Empty aggregations + non-empty group_by = "deduplicate by
                # these columns" semantically (matches the typical Summarize with
                # only GroupBy actions). pandas groupby().agg({}) raises
                # "No objects to concatenate" — short-circuit to drop_duplicates.
                if not _named and not _simple and group_by:
                    result = upstream[group_by].drop_duplicates().reset_index(drop=True)
                    _result_for_metadata = result
                    context.log.info(
                        f"Summarize on tool returned {len(result)} unique "
                        f"groups of {group_by} (no aggregations configured)."
                    )
                    return result
                # Empty aggregations + empty group_by = degenerate config.
                # fallback: emit a single row with the total record count.
                if not _named and not _simple and not group_by:
                    import pandas as _pd
                    result = _pd.DataFrame([{"row_count": len(upstream)}])
                    _result_for_metadata = result
                    context.log.warning(
                        "Summarize with no group_by AND no aggregations — "
                        "returning a single-row {row_count: N} DataFrame."
                    )
                    return result
                # Some vendor-specific aggs aren't native pandas — convert to
                # callables here so `groupby().agg()` accepts them. Keeps the
                # exposed names ('concat', 'concat_unique', 'mode', 'list')
                # working in both _simple and _named forms.
                def _to_callable(f: Any) -> Any:
                    if not isinstance(f, str):
                        return f
                    key = f.lower()
                    if key == "concat":
                        return lambda s: ", ".join(str(v) for v in s.dropna())
                    if key == "concat_unique":
                        return lambda s: ", ".join(sorted({str(v) for v in s.dropna()}))
                    if key == "mode":
                        return lambda s: (s.mode().iloc[0] if not s.mode().empty else None)
                    if key == "list":
                        return lambda s: list(s.dropna())
                    if key in ("countnull", "count_null", "null_count", "countnulls"):
                        return lambda s: int(s.isna().sum())
                    if key in ("countnonnull", "count_non_null", "non_null_count"):
                        return lambda s: int(s.notna().sum())
                    if key in ("countdistinct", "count_distinct", "nunique"):
                        return "nunique"
                    if key in ("first",):
                        return "first"
                    if key in ("last",):
                        return "last"
                    if key in ("spatialobjcombine", "spatial_obj_combine", "spatial_union"):
                        # Aggregate Shapely geometries by unary union.
                        def _spatial_combine(s):
                            try:
                                from shapely.ops import unary_union
                                from shapely import wkt as _wkt
                                from shapely.geometry.base import BaseGeometry
                            except ImportError as e:
                                raise ImportError("shapely required for spatial agg: pip install shapely") from e
                            geoms = []
                            for v in s.dropna():
                                if isinstance(v, BaseGeometry):
                                    geoms.append(v)
                                else:
                                    try:
                                        geoms.append(_wkt.loads(str(v)))
                                    except Exception:
                                        continue
                            if not geoms:
                                return None
                            return unary_union(geoms)
                        return _spatial_combine
                    return f
                _simple = {k: _to_callable(v) for k, v in _simple.items()}
                _named = {k: (src, _to_callable(fn)) for k, (src, fn) in _named.items()}

                # Coerce object-dtype source columns to numeric for arithmetic
                # aggregations (sum/mean/avg/min/max/std/var/median). Otherwise
                # pandas raises "agg function failed [how->mean,dtype->object]"
                # whenever the upstream column came in as strings (CSV reads,
                # passthrough from a tool that didn't infer dtype, etc.).
                _NUMERIC_AGG_NAMES = {
                    "sum", "mean", "avg", "average", "min", "max",
                    "std", "stddev", "var", "variance", "median",
                }
                def _needs_numeric(fn) -> bool:
                    if isinstance(fn, str):
                        return fn.lower() in _NUMERIC_AGG_NAMES
                    return getattr(fn, "__name__", "").lower() in _NUMERIC_AGG_NAMES
                _to_coerce: set = set()
                for _out, _spec in _simple.items():
                    if _needs_numeric(_spec) and _out in upstream.columns and upstream[_out].dtype == "object":
                        _to_coerce.add(_out)
                for _out, (_src, _func) in _named.items():
                    if _needs_numeric(_func) and _src in upstream.columns and upstream[_src].dtype == "object":
                        _to_coerce.add(_src)
                if _to_coerce:
                    import pandas as _pd
                    upstream = upstream.copy()
                    for _c in _to_coerce:
                        upstream[_c] = _pd.to_numeric(upstream[_c], errors="coerce")
                    context.log.warning(
                        f"Summarize: coerced object-dtype source columns to "
                        f"numeric for arithmetic aggregations: {sorted(_to_coerce)}"
                    )

                # Empty group_by → aggregate the whole frame as a single
                # row. `df.groupby([])` raises 'No group keys passed', so
                # take the df.agg() path instead which returns a Series
                # per simple aggregation or a DataFrame per named.
                if not group_by:
                    import pandas as _pd
                    rows: Dict[str, Any] = {}
                    for _out, _spec in _simple.items():
                        # `size` is row-count of the whole frame, not a column op.
                        if isinstance(_spec, str) and _spec == "size":
                            rows[_out] = len(upstream)
                            continue
                        # Whole-frame agg: apply func to the matching source column.
                        # `_simple` keys are the output column name AND source.
                        rows[_out] = upstream[_out].agg(_spec)
                    for _out, (_src, _func) in _named.items():
                        if isinstance(_func, str) and _func == "size":
                            rows[_out] = len(upstream)
                            continue
                        rows[_out] = upstream[_src].agg(_func)
                    result = _pd.DataFrame([rows])
                    _result_for_metadata = result
                    context.log.info(
                        f"Summarized {len(upstream)} rows into 1 row "
                        f"(no group_by, whole-frame aggregation)."
                    )
                else:
                    _grouped = upstream.groupby(group_by)
                    # `size` is a row-count-per-group transform — pandas
                    # treats it specially (`groupby.size()` returns a Series
                    # without per-column dispatch). Pop any `size` entries
                    # from `_simple` / `_named` and compute them once outside
                    # the agg-dict path.
                    _size_outputs: list = []
                    _new_simple: Dict[str, Any] = {}
                    for _k, _v in _simple.items():
                        if isinstance(_v, str) and _v == "size":
                            _size_outputs.append(_k)
                        else:
                            _new_simple[_k] = _v
                    _simple = _new_simple
                    _named_size: list = []
                    _new_named: Dict[str, Any] = {}
                    for _k, _v in _named.items():
                        if isinstance(_v, tuple) and len(_v) == 2 and _v[1] == "size":
                            _named_size.append(_k)
                        else:
                            _new_named[_k] = _v
                    _named = _new_named
                    if _named and _simple:
                        _simple_df = _grouped.agg(_simple).reset_index()
                        _named_df = _grouped.agg(**_named).reset_index()
                        result = _simple_df.merge(_named_df, on=group_by)
                    elif _named:
                        result = _grouped.agg(**_named).reset_index()
                    elif _simple:
                        result = _grouped.agg(_simple).reset_index()
                    else:
                        # Only size aggregations — start from the group_by
                        # frame with one row per group.
                        result = upstream[group_by].drop_duplicates().reset_index(drop=True)
                    # Layer the row-count columns on top.
                    if _size_outputs or _named_size:
                        _sizes = _grouped.size().rename("__size__").reset_index()
                        result = result.merge(_sizes, on=group_by, how="left")
                        for _out in _size_outputs + _named_size:
                            result[_out] = result["__size__"]
                        result = result.drop(columns=["__size__"])
                # Apply group_by_rename if provided — renames the group-by
                # output columns post-aggregation so downstream tools can
                # reference the friendly name.
                _gbr = self.group_by_rename
                if _gbr:
                    _map = {k: v for k, v in _gbr.items() if k in (
                        result.columns if hasattr(result, "columns") else list(result.columns)
                    )}
                    if _map:
                        result = result.rename(columns=_map)
                _row_count = len(result)
                _result_for_metadata = result
            context.log.info(
                f"Summarized {len(upstream)} rows into {len(result)} groups "
                f"on columns: {group_by}"
            )
            # Build column schema metadata — use pandas view of result so
            # the dtypes lookup works identically for both backends.
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _meta_df = _result_for_metadata
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(_meta_df.dtypes[col]))
                for col in _meta_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(_meta_df)),
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
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(_meta_df) > 0:
                try:
                    _prev = _meta_df.sample(min(preview_rows, len(_meta_df))) if len(_meta_df) > preview_rows * 10 else _meta_df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)
            return result

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[_asset])


        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
