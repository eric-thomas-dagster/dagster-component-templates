"""Dataframe → Avro sink component.

Companion to `dataframe_to_parquet`. Writes a pandas DataFrame as Avro to
a local path or any fsspec-supported URI (s3://, gs://, abfss://, ...).
Schema is inferred from pandas dtypes; nullable types become Avro unions
with `null`. For producers that need explicit schema control, pass
`avro_schema:` as YAML and we'll skip inference and use what you give us.
"""
import os
from typing import Any, Dict, List, Optional
from typing import Literal

import pandas as pd
import dagster as dg
from dagster import AssetExecutionContext, AssetIn, AssetKey, MaterializeResult, MetadataValue, Output, asset
from pydantic import Field


# Pandas dtype → Avro type (nullable variants use union with "null").
_PANDAS_TO_AVRO = {
    "int8":    "int",
    "int16":   "int",
    "int32":   "int",
    "int64":   "long",
    "uint8":   "int",
    "uint16":  "int",
    "uint32":  "long",
    "uint64":  "long",
    "float32": "float",
    "float64": "double",
    "bool":    "boolean",
    "object":  "string",
    "string":  "string",
    "category": "string",
}


_AVRO_TIMESTAMP_MICROS = {"type": "long", "logicalType": "timestamp-micros"}


def _infer_avro_schema(df: pd.DataFrame, record_name: str) -> Dict[str, Any]:
    """Build a record-type Avro schema by mapping each column's dtype.

    All fields are emitted as nullable Avro unions (`["null", T]`) so the
    schema works even when pandas has NaNs. Caller can override with an
    explicit `avro_schema:` if they need stricter typing or named records.

    Datetime columns use Avro's ``timestamp-micros`` logical type so
    downstream readers (BigQuery, Snowflake, Iceberg, Spark) restore them
    as real timestamps. The previous "ISO string" encoding silently
    degraded the column's type — downstream typed engines saw it as
    plain STRING.
    """
    fields: List[Dict[str, Any]] = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype.startswith("datetime64"):
            avro_type: Any = _AVRO_TIMESTAMP_MICROS
        else:
            avro_type = _PANDAS_TO_AVRO.get(dtype, "string")
        fields.append({"name": str(col), "type": ["null", avro_type], "default": None})
    return {
        "type": "record",
        "name": record_name,
        "fields": fields,
    }


def _coerce_for_avro(records: List[Dict[str, Any]]) -> None:
    """In-place: convert pandas Timestamp / NaT / numpy types to plain Python
    types so fastavro can serialize.

    Timestamps are emitted as ``datetime`` (fastavro recognizes them and
    converts to micros), NOT as ISO strings — the schema is now
    ``timestamp-micros`` so a string would fail validation.
    """
    import math
    from datetime import datetime
    try:
        import pandas as _pd
        _Timestamp = _pd.Timestamp
        _NaT = _pd.NaT
    except ImportError:
        _Timestamp = None
        _NaT = None
    for row in records:
        for k, v in list(row.items()):
            # NaN → None for nullable unions
            if isinstance(v, float) and math.isnan(v):
                row[k] = None
                continue
            # pandas.NaT → None (NaT is a singleton; isnan() above doesn't catch it)
            if _NaT is not None and v is _NaT:
                row[k] = None
                continue
            # pandas.Timestamp → datetime (fastavro encodes timestamp-micros from datetime)
            if _Timestamp is not None and isinstance(v, _Timestamp):
                row[k] = v.to_pydatetime()
                continue
            # Already a datetime → leave as-is (fastavro handles it)
            if isinstance(v, datetime):
                continue
            # numpy scalars → native Python
            if hasattr(v, "item") and not isinstance(v, (str, bytes, dict, list)):
                try:
                    row[k] = v.item()
                except (ValueError, AttributeError):
                    pass


def _resolve_path(file_path: str, context: AssetExecutionContext) -> str:
    """Substitute env vars + {partition_key} into the destination path."""
    out = os.path.expandvars(file_path)
    if context.has_partition_key:
        out = out.replace("{partition_key}", str(context.partition_key))
    return out


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
):
    if not partition_type:
        return None
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start or "2024-01-01T00:00:00")
    if partition_type == "static":
        vals = partition_values or []
        if isinstance(vals, str):
            vals = [v.strip() for v in vals.split(",") if v.strip()]
        return StaticPartitionsDefinition(list(vals))
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type=dynamic requires dynamic_partition_name")
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
    for _name in ("row_count", "dagster/row_count"):
        if all(_name in _m for _m in metas):
            merged[_name] = MetadataValue.int(sum(int(getattr(_m[_name], "value", _m[_name])) for _m in metas))
    merged["partition_keys_written"] = MetadataValue.int(len(metas))
    return merged



class DataframeToAvroComponent(dg.Component, dg.Model, dg.Resolvable):
    """Write a pandas DataFrame as Avro to a local path or fsspec URI.

    Schema is inferred from pandas dtypes by default (all columns emitted
    as nullable Avro unions). Pass `avro_schema:` as a YAML dict to
    override inference with an explicit producer schema.
    """

    asset_name: str = Field(description="Output asset name")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key (must materialize a pandas DataFrame)")

    file_path: str = Field(
        description=(
            "Destination path or fsspec URI. Local (`/tmp/out.avro`) and remote "
            "URIs (`s3://...`, `gs://...`, `abfss://...`) both work via fsspec. "
            "`{partition_key}` is substituted from the asset's partition key when "
            "the asset is partitioned. Supports `${ENV_VAR}` substitution."
        )
    )
    codec: str = Field(
        default="null",
        description="Avro compression codec: null | snappy | deflate | bzip2 | xz | zstandard",
    )
    record_name: str = Field(
        default="Record",
        description="Name embedded in the inferred Avro schema's `name` field. Ignored if `avro_schema:` is set.",
    )
    avro_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Optional explicit Avro schema dict (overrides inference). Use when "
            "the downstream consumer needs a specific schema name / namespace / "
            "non-nullable typing. Example: "
            "`{type: record, name: Event, namespace: com.acme, fields: [...]}`"
        ),
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None, description="daily | weekly | monthly | hourly | static | dynamic | None")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in ONE run. If file_path is "
            "templated on the partition key (e.g. {partition_key}), one file is "
            "written per key in the range -- this needs a per-partition upstream "
            "(a partition-aware IO manager that hands the asset a dict of "
            "DataFrames keyed by partition key; this component has no row-level "
            "partition filter) and raises a clear error otherwise. A non-templated "
            "file_path gets the whole range in a single file. 'multi_run' launches one run per partition (optionally "
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps',), csv=('partition_values',), tags=())

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        file_path = self.file_path
        codec = self.codec
        record_name = self.record_name
        explicit_schema = self.avro_schema
        description = self.description or f"Write upstream DataFrame as Avro to {file_path}"
        group_name = self.group_name
        deps_keys = [AssetKey.from_user_string(k) for k in (self.deps or [])]

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start,
            self.partition_values, self.dynamic_partition_name,
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
            import fastavro
            import fsspec

            if not isinstance(upstream, pd.DataFrame):
                raise TypeError(f"Expected pandas DataFrame, got {type(upstream).__name__}")

            resolved = _resolve_path(file_path, context)
            schema = explicit_schema or _infer_avro_schema(upstream, record_name)
            records = upstream.to_dict(orient="records")
            _coerce_for_avro(records)

            with fsspec.open(resolved, "wb") as f:
                fastavro.writer(f, schema, records, codec=codec)

            context.log.info(f"Wrote {len(records)} rows to {resolved} (Avro, codec={codec})")

            # MaterializeResult (not Output(value=None)): a sink has no value to persist, and a
            # single_run backfill range makes the IO manager fail on handle_output.
            return MaterializeResult(
                metadata={
                    "destination":   MetadataValue.text(resolved),
                    "row_count":     MetadataValue.int(len(records)),
                    "column_count":  MetadataValue.int(len(upstream.columns)),
                    "codec":         MetadataValue.text(codec),
                    "schema_inferred": MetadataValue.bool(explicit_schema is None),
                },
            )

        @asset(backfill_policy=_backfill_policy_obj, 
            key=AssetKey.from_user_string(asset_name),
            description=description,
            group_name=group_name,
            partitions_def=partitions_def,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=deps_keys,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # single_run/multi_run backfill range + path templated on the partition
            # key: one write per key. Otherwise one write (a non-templated path gets
            # the whole range's rows in a single file).
            if (
                context.has_partition_key_range
                and not context.has_partition_key
                and _path_is_partition_templated(file_path)
            ):
                _merged = _write_each_partition_key(
                    context, upstream, _write_one, can_split_rows=False
                )
                return MaterializeResult(metadata=_merged)
            return _write_one(context, upstream)

        return dg.Definitions(assets=[_asset])


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
