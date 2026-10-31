"""Dataframe → Delta Table sink component.

Write a pandas DataFrame to an EXISTING Delta Lake table. Uses `delta-rs` (no
Spark / JVM). For Delta tables shared with other engines — Databricks, Trino,
Spark, Flink — where Dagster is one of the writers.

Differs from `dataframe_to_databricks` (Databricks-specific, Unity Catalog
focused) and from the official `dagster_deltalake.DeltaLakePyarrowIOManager`
(which OWNS the table as an IO manager).

Modes: `append`, `overwrite`, `merge` (basic MERGE — match-then-update by key).
For complex MERGE logic (when-matched-update / when-not-matched-insert /
when-matched-delete), use Spark/Databricks SQL.
"""

import os
from typing import Any, Dict, List, Optional
from typing import Literal

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
    MaterializeResult,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _resolve_env_vars(d: Optional[Dict[str, str]]) -> Dict[str, str]:
    if not d:
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            out[k] = os.environ.get(v[2:-1], "")
        else:
            out[k] = v
    return out


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition,
        WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition,
        HourlyPartitionsDefinition,
        StaticPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if not partition_type:
        return None
    _values = (
        [str(v).strip() for v in partition_values if str(v).strip()]
        if isinstance(partition_values, (list, tuple))
        else [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    )
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
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
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")



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


class _PartitionKeyContext:
    """Delegates to the real context but pins ONE key of a backfill range as the partition key."""

    def __init__(self, context, key):
        self._ctx = context
        self._key = key

    def __getattr__(self, name):
        return getattr(self._ctx, name)

    @property
    def has_partition_key(self):
        return True

    @property
    def partition_key(self):
        return self._key

    @property
    def has_partition_key_range(self):
        return False

    @property
    def partition_keys(self):
        return [self._key]


def _path_is_partition_templated(path):
    """True when path has a {placeholder} other than {run_id} / ${ENV_VAR}."""
    import re

    return bool(re.search(r"(?<!\$)\{(?!run_id\})", path or ""))


def _write_each_partition_key(context, upstream, write_one, can_split_rows):
    """Run ``write_one`` once per partition key of a single_run/multi_run backfill range.

    A path templated on the partition key must produce one file per key. The
    upstream value for the range is either a ``{partition_key: DataFrame}`` dict
    (what a partition-aware IO manager hands an asset consuming a range) -- sliced
    per key here -- or one frame spanning the range, which is only splittable
    when the component filters rows per key (``can_split_rows``).
    """
    if not isinstance(upstream, dict) and not can_split_rows:
        raise ValueError(
            "A backfill range (backfill_policy) with a path templated on the partition key needs "
            "either a per-partition upstream (dict of DataFrames per partition key) or "
            "partition_date_column/partition_static_column to split rows per key; "
            "otherwise every key's file would receive the whole range."
        )
    metas = []
    for _k in context.partition_keys:
        _piece = upstream[_k] if isinstance(upstream, dict) and _k in upstream else upstream
        _res = write_one(_PartitionKeyContext(context, _k), _piece)
        metas.append(dict(_res.metadata or {}))
    merged = dict(metas[-1])
    for _name in ("row_count", "dagster/row_count", "rows_written"):
        if all(_name in _m for _m in metas):
            merged[_name] = MetadataValue.int(sum(int(getattr(_m[_name], "value", _m[_name])) for _m in metas))
    merged["partition_keys_written"] = MetadataValue.int(len(metas))
    return merged



class DataframeToDeltaTableComponent(Component, Model, Resolvable):
    """Sink — write a DataFrame to an existing Delta Lake table.

    Example — append to S3-backed Delta table:

        ```yaml
        type: dagster_community_components.DataframeToDeltaTableComponent
        attributes:
          asset_name: events_delta_loaded
          upstream_asset_key: curated_events
          table_uri: s3://my-bucket/lakehouse/events
          storage_options:
            AWS_REGION: us-east-1
          mode: append
          partition_by: [event_date]
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset producing the DataFrame")

    table_uri: str = Field(description="Delta table location (s3://, az://, gs://, /local, uc://)")
    storage_options: Optional[Dict[str, str]] = Field(
        default=None,
        description="Cloud creds + config. ${ENV_VAR} expansion supported.",
    )

    mode: str = Field(
        default="append",
        description=(
            "'append' | 'overwrite' | 'merge' (basic) | 'error' | 'ignore'. "
            "For complex MERGE logic, use Spark/Databricks SQL."
        ),
    )

    # --- Partition / overwrite behavior -------------------------------------

    partition_by: Optional[List[str]] = Field(
        default=None,
        description="Partition columns. Set ONCE at table creation — must match the table's existing partitioning.",
    )
    overwrite_partition_filter: Optional[List[List[str]]] = Field(
        default=None,
        description=(
            "When mode='overwrite': replace only the rows matching these partition predicates "
            "(format same as delta_ingestion's partition_filters). Supports {partition_key}."
        ),
    )

    # --- Merge options -----------------------------------------------------

    merge_predicate: Optional[str] = Field(
        default=None,
        description=(
            "When mode='merge': SQL predicate matching target ↔ source rows by key, "
            "e.g. 'target.id = source.id'. Required for merge mode."
        ),
    )

    # --- Schema --------------------------------------------------------------

    schema_mode: Optional[str] = Field(
        default=None,
        description=(
            "Schema evolution. 'merge' allows new columns to be added; None enforces the "
            "existing schema. Use 'merge' cautiously — it can mask upstream bugs."
        ),
    )

    skip_columns: Optional[List[str]] = Field(default=None)

    # --- Standard fields ---------------------------------------------------

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default="delta_sink")
    owners: Optional[List[str]] = Field(default=None)
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)

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
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in ONE run. When overwrite_partition_filter "
            "is templated on the partition key, one write per key in the range "
            "is issued (each replacing only that key's slice) -- this needs a "
            "per-partition upstream (a partition-aware IO manager handing the "
            "asset a dict of DataFrames keyed by partition key; this component has "
            "no row-level partition filter) and raises a clear error otherwise. "
            "Without a partition-key template the whole range is written in one "
            "call. 'multi_run' launches one run per partition (optionally "
            "capped via backfill_policy_max_partitions_per_run). Leave unset for "
            "Dagster's default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. Requires a partition-range-aware IO manager "
            "for the upstream asset (e.g. a DB-table IO manager with partition_expr "
            "set) -- a generic filesystem IO manager cannot persist/load a single_run "
            "output spanning multiple partitions and raises a clear error at "
            "materialization time."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        component = self
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        description = self.description or f"Delta sink ({self.mode}) → {self.table_uri}"

        kinds = list(self.kinds or []) or ["delta"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds:
            all_tags[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name
        ,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            from dagster import BackfillPolicy
            _backfill_policy_obj = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        def _write_one(context, upstream):
            # partition bridge dict-concat: a backfill range (or an unpartitioned
            # consumer of a partitioned upstream) arrives as {partition_key: DataFrame}.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            from deltalake import write_deltalake, DeltaTable

            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(
                    f"Upstream must produce a pandas DataFrame, got {type(upstream).__name__}"
                )
            df = upstream
            if component.skip_columns:
                df = df.drop(columns=[c for c in component.skip_columns if c in df.columns])

            partition_key = context.partition_key if context.has_partition_key else None
            storage_opts = _resolve_env_vars(component.storage_options)

            mode = component.mode.lower()

            kwargs: Dict[str, Any] = {}
            if storage_opts:
                kwargs["storage_options"] = storage_opts
            if component.partition_by:
                kwargs["partition_by"] = list(component.partition_by)
            if component.schema_mode:
                kwargs["schema_mode"] = component.schema_mode

            if mode == "merge":
                if not component.merge_predicate:
                    raise ValueError("mode='merge' requires merge_predicate.")
                dt = DeltaTable(component.table_uri, storage_options=storage_opts or None)
                # delta-rs MERGE API: TableMerger.execute()
                merge = (
                    dt.merge(
                        source=df,
                        predicate=component.merge_predicate,
                        source_alias="source",
                        target_alias="target",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                )
                merge.execute()
            elif mode == "overwrite" and component.overwrite_partition_filter:
                # Partition-level overwrite via predicate
                pfilters = []
                for f in component.overwrite_partition_filter:
                    col, op, val = f[0], f[1], f[2]
                    if partition_key is not None and isinstance(val, str) and "{partition_key}" in val:
                        val = val.replace("{partition_key}", partition_key)
                    pfilters.append((col, op, val))
                kwargs["mode"] = "overwrite"
                import inspect

                if "partition_filters" in inspect.signature(write_deltalake).parameters:
                    kwargs["partition_filters"] = pfilters
                else:
                    # deltalake >= 1.0 removed partition_filters in favour of a SQL `predicate`.
                    def _lit(v):
                        return v if isinstance(v, (int, float)) and not isinstance(v, bool) else "'" + str(v).replace("'", "''") + "'"

                    _clauses = []
                    for _c, _op, _v in pfilters:
                        if isinstance(_v, (list, tuple, set)):
                            _clauses.append(f"{_c} {_op.upper()} ({', '.join(_lit(x) for x in _v)})")
                        else:
                            _clauses.append(f"{_c} {_op} {_lit(_v)}")
                    kwargs["predicate"] = " AND ".join(_clauses)
                write_deltalake(component.table_uri, df, **kwargs)
            elif mode in ("append", "overwrite", "error", "ignore"):
                kwargs["mode"] = mode
                write_deltalake(component.table_uri, df, **kwargs)
            else:
                raise ValueError(f"unknown mode: {mode!r}")

            # Read back the new version for metadata
            dt = DeltaTable(component.table_uri, storage_options=storage_opts or None)

            metadata: Dict[str, Any] = {
                "rows_written": MetadataValue.int(len(df)),
                "mode": MetadataValue.text(mode),
                "table_uri": MetadataValue.text(component.table_uri),
                "delta_version": MetadataValue.int(dt.version()),
            }
            if partition_key:
                metadata["partition_key"] = MetadataValue.text(partition_key)
            try:
                history = dt.history(limit=1)
                if history:
                    metadata["latest_commit_operation"] = MetadataValue.text(
                        history[0].get("operation", "")
                    )
            except Exception:
                pass
            context.log.info(
                f"Delta sink: wrote {len(df)} rows ({mode}) → {component.table_uri} (v{dt.version()})"
            )
            # MaterializeResult, not Output(value=None): a sink has no value to persist, and a
            # single_run backfill range makes the IO manager fail in handle_output.
            return MaterializeResult(metadata=metadata)

        @asset(backfill_policy=_backfill_policy_obj, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=self.owners or [],
            tags=all_tags,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            partitions_def=partitions_def,
            retry_policy=retry_policy,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            freshness_policy=freshness_policy,
        )
        def dataframe_to_delta_table_asset(context: AssetExecutionContext, upstream):
            # single_run/multi_run backfill range + path templated on the partition
            # key: one write per key. Otherwise one write (a non-templated path gets
            # the whole range's rows in a single file).
            if (
                context.has_partition_key_range
                and not context.has_partition_key
                and component.mode.lower() == "overwrite"
                and any("{partition_key}" in str(_v) for _f in (component.overwrite_partition_filter or []) for _v in _f)
            ):
                _merged = _write_each_partition_key(
                    context, upstream, _write_one, can_split_rows=False
                )
                return MaterializeResult(metadata=_merged)
            return _write_one(context, upstream)

        return Definitions(assets=[dataframe_to_delta_table_asset])


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
