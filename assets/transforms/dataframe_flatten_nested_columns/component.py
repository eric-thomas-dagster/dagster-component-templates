"""DataframeFlattenNestedColumnsComponent — JSON-stringify dict/list columns.

Many DataFrame sinks (BigQuery `load_table_from_dataframe`, Snowflake's pandas
writer, plain CSV writers) can't infer a column type for object-dtype columns
holding nested dicts/lists. This component is the standard fix: walk each row
of an upstream DataFrame, find columns whose values are dicts or lists, and
JSON-encode those values to strings.

Common chain:
  cloud_logging_query_asset → dataframe_flatten_nested_columns → dataframe_to_bigquery

Configurable scopes:
  - `columns`: explicit allowlist. Default: every object-dtype column with at
    least one dict/list value.
  - `exclude_columns`: skip these even if they contain dicts/lists.
"""

from typing import Any, Dict, List, Optional, Union

import json
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
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class DataframeFlattenNestedColumnsComponent(Component, Model, Resolvable):
    """JSON-stringify dict / list values in selected DataFrame columns."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    expand: bool = Field(
        default=True,
        description=(
            "If True (default), nested dict values are expanded into new "
            "columns via pandas.json_normalize — e.g. row with `properties: "
            "{mag: 3.4}` becomes new columns `properties.mag` (or `mag` if "
            "strip_prefix=True). If False, nested dicts/lists are just "
            "JSON-serialized in place (legacy behavior)."
        ),
    )
    separator: str = Field(
        default=".",
        description="Separator used between parent/child keys when expand=True (e.g. '.', '_').",
    )
    strip_prefix: bool = Field(
        default=False,
        description=(
            "When expand=True, drop the parent column name from expanded "
            "columns. E.g. `properties.mag` becomes `mag`. Useful when you "
            "only care about the innermost fields."
        ),
    )
    expand_arrays: bool = Field(
        default=False,
        description=(
            "When expand=True, ALSO split lists-of-primitives into indexed "
            "columns. E.g. `coordinates: [lon, lat, depth]` becomes columns "
            "`coordinates.0`, `coordinates.1`, `coordinates.2` (or without "
            "prefix if strip_prefix=True). Handy for GeoJSON coordinate arrays. "
            "Only splits when every non-null value in the column is a list AND "
            "the elements are primitives (not dicts)."
        ),
    )
    array_label_map: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description=(
            "Optional per-column labels for the indexed array columns. "
            "E.g. `{coordinates: [lon, lat, depth]}` names them "
            "`coordinates.lon`, `coordinates.lat`, `coordinates.depth` (or "
            "just `lon`/`lat`/`depth` with strip_prefix=True). If missing, "
            "positional integer indices are used."
        ),
    )

    columns: Optional[List[Union[str, int]]] = Field(
        default=None,
        description="Explicit columns to flatten. Default: every column with at least one dict/list value.",
    )
    exclude_columns: Optional[List[str]] = Field(
        default=None,
        description="Skip these columns even if they contain dicts/lists.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
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
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
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

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        explicit_cols = self.columns
        exclude = set(self.exclude_columns or [])

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Flatten nested dict/list columns to JSON strings.",
            group_name=self.group_name,
            kinds={"pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> Output:
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            df = upstream.copy()

            if explicit_cols is not None:
                target_cols = [c for c in explicit_cols if c in df.columns]
            else:
                target_cols = [
                    c for c in df.columns
                    if c not in exclude
                    and df[c].dtype == object
                    and bool(df[c].apply(lambda v: isinstance(v, (dict, list))).any())
                ]

            flattened: List[str] = []
            new_columns: List[str] = []

            if self.expand:
                # Real flatten via pd.json_normalize on each nested column.
                # Rows without a dict value in that column keep NaN.
                sep = self.separator or "."
                _label_map = self.array_label_map or {}
                for col in target_cols:
                    if col in exclude:
                        continue
                    _series = df[col]
                    _dict_mask = _series.apply(lambda v: isinstance(v, dict))
                    _list_mask = _series.apply(lambda v: isinstance(v, list))

                    if not bool(_dict_mask.any()):
                        # No dict values in this column. Two options:
                        #  1. expand_arrays=True + list-of-primitives → split
                        #     into indexed columns (coordinates → .0/.1/.2 or
                        #     .lon/.lat/.depth via array_label_map).
                        #  2. otherwise → JSON-serialize (legacy behavior).
                        _is_primitives = (
                            self.expand_arrays and bool(_list_mask.any())
                            and _series.where(_list_mask, [])
                                .apply(lambda lst: all(
                                    not isinstance(x, (dict, list)) for x in (lst or [])
                                ))
                                .all()
                        )
                        if _is_primitives:
                            _lists = _series.where(_list_mask, []).tolist()
                            _max_len = max((len(x) for x in _lists), default=0)
                            _labels = _label_map.get(col) or []
                            _cols_to_add = {}
                            for _i in range(_max_len):
                                _key = _labels[_i] if _i < len(_labels) else str(_i)
                                _name = _key if self.strip_prefix else f"{col}{sep}{_key}"
                                _cols_to_add[_name] = [
                                    (lst[_i] if isinstance(lst, list) and _i < len(lst) else None)
                                    for lst in _lists
                                ]
                            df = df.drop(columns=[col])
                            for _name, _vals in _cols_to_add.items():
                                if _name in df.columns:
                                    df = df.drop(columns=[_name])
                                df[_name] = _vals
                            flattened.append(col)
                            new_columns.extend(_cols_to_add.keys())
                            continue

                        # Fallback: JSON-serialize lists in place.
                        df[col] = _series.apply(
                            lambda v: json.dumps(v, default=str) if isinstance(v, list) else v
                        )
                        flattened.append(col)
                        continue

                    _normalized = pd.json_normalize(
                        _series.where(_dict_mask, {}).tolist(), sep=sep
                    )
                    _normalized.index = df.index
                    # Column name policy: `<col><sep><subkey>` OR strip parent.
                    if self.strip_prefix:
                        _normalized.columns = [str(_c) for _c in _normalized.columns]
                    else:
                        _normalized.columns = [f"{col}{sep}{_c}" for _c in _normalized.columns]
                    # Drop source col and merge in normalized. If any name
                    # collision, drop the pre-existing one (from df).
                    df = df.drop(columns=[col])
                    _dupes = [_c for _c in _normalized.columns if _c in df.columns]
                    if _dupes:
                        df = df.drop(columns=_dupes)
                    df = pd.concat([df, _normalized], axis=1)
                    flattened.append(col)
                    new_columns.extend(_normalized.columns.tolist())

                # Second pass: after json_normalize creates NEW nested columns
                # (e.g. `geometry.coordinates` from expanding `geometry`), those
                # may still be lists-of-primitives. Re-scan and split them if
                # expand_arrays=True.
                if self.expand_arrays:
                    _pending = [
                        _c for _c in list(df.columns)
                        if _c not in exclude
                        and df[_c].dtype == object
                        and bool(df[_c].apply(lambda v: isinstance(v, list)).any())
                    ]
                    for col in _pending:
                        _series = df[col]
                        _list_mask = _series.apply(lambda v: isinstance(v, list))
                        _is_primitives = (
                            _series.where(_list_mask, [])
                            .apply(lambda lst: all(
                                not isinstance(x, (dict, list)) for x in (lst or [])
                            ))
                            .all()
                        )
                        if not _is_primitives:
                            continue
                        _lists = _series.where(_list_mask, []).tolist()
                        _max_len = max((len(x) for x in _lists), default=0)
                        if _max_len == 0:
                            continue
                        # array_label_map lookup — try the FULL column name first
                        # (e.g. "geometry.coordinates") AND the trailing suffix
                        # ("coordinates") for convenience.
                        _labels = _label_map.get(col) or _label_map.get(col.rsplit(sep, 1)[-1]) or []
                        _cols_to_add = {}
                        for _i in range(_max_len):
                            _key = _labels[_i] if _i < len(_labels) else str(_i)
                            _name = _key if self.strip_prefix else f"{col}{sep}{_key}"
                            _cols_to_add[_name] = [
                                (lst[_i] if isinstance(lst, list) and _i < len(lst) else None)
                                for lst in _lists
                            ]
                        df = df.drop(columns=[col])
                        for _name, _vals in _cols_to_add.items():
                            if _name in df.columns:
                                df = df.drop(columns=[_name])
                            df[_name] = _vals
                        flattened.append(col)
                        new_columns.extend(_cols_to_add.keys())
            else:
                for col in target_cols:
                    if col in exclude:
                        continue
                    df[col] = df[col].apply(
                        lambda v: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
                    )
                    flattened.append(col)

            return Output(
                value=df,
                metadata={
                    "rows":              MetadataValue.int(len(df)),
                    "columns_flattened": MetadataValue.json(flattened),
                    "new_columns":       MetadataValue.json(new_columns),
                    "columns_untouched": MetadataValue.json([c for c in df.columns if c not in new_columns]),
                    "preview":           MetadataValue.md(df.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
