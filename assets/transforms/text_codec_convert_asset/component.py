"""TextCodecConvertAssetComponent — convert text between codecs (ASCII↔EBCDIC, etc.).

Two modes — pick one:

1. **String column → re-encoded string column** (`mode: string`):
   Takes a column of Python `str` (already decoded text) and writes it as
   the SAME column but encoded-then-decoded through a different codec.
   Useful for content that lives in DataFrames and needs to be sanitized
   to a target charset (e.g. drop non-ASCII characters from a string field
   before pushing to a system that doesn't tolerate them).

2. **File path → new file path** (`mode: file`):
   Reads each input file as `from_codec` bytes, decodes to Python str,
   re-encodes as `to_codec`, writes the new file. The canonical mainframe
   interop pattern: take an EBCDIC fixed-width file from a z/OS export
   and convert it to ASCII / UTF-8 for downstream parsing.

Codec catalog (stdlib `codecs`):
  - ASCII / Unicode: `ascii`, `utf-8`, `utf-16`, `utf-32`, `latin-1`
  - Windows: `cp1252` (Windows-1252)
  - **EBCDIC family** (IBM mainframe):
    * `cp037`  — US/Canada (most common)
    * `cp1140` — US-with-Euro symbol
    * `cp273`  — Germany/Austria
    * `cp500`  — Western Europe (Latin-1 EBCDIC)
    * `cp1047` — Open Systems Latin-1 EBCDIC
    * `cp875`  — Greek

Banking, insurance, healthcare, and many federal-agency systems still
ship EBCDIC files daily — this is a real, common ingest pattern.
"""

import codecs
import os
from typing import Any, Dict, List, Literal, Optional, Union

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


class TextCodecConvertAssetComponent(Component, Model, Resolvable):
    """Convert text between codecs (e.g. ASCII↔EBCDIC) per-row."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream DataFrame asset key.")

    mode: Literal["string", "file"] = Field(
        description=(
            "`string`: convert a column of Python str values in place. "
            "`file`: read each file path's bytes, recode, write a new file."
        ),
    )

    from_codec: str = Field(
        description=(
            "Source codec name (Python stdlib codec). "
            "Common EBCDIC: cp037, cp1140, cp273, cp500, cp1047."
        ),
    )
    to_codec: str = Field(
        description="Target codec name. e.g. `utf-8`, `ascii`, `cp037`.",
    )
    errors: Literal["strict", "ignore", "replace", "backslashreplace"] = Field(
        default="strict",
        description=(
            "Behavior on un-encodable / un-decodable characters. "
            "`replace` substitutes `?` / `\\ufffd` — useful for non-strict ETL."
        ),
    )

    # mode='string' fields
    source_column: Optional[Union[str, int]] = Field(
        default=None,
        description="mode='string': column of str values to convert. Required for string mode.",
    )
    target_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "mode='string': new column to write the result into. "
            "If equal to `source_column`, overwrites in place. "
            "Default: source_column."
        ),
    )

    # mode='file' fields
    source_path_column: Optional[Union[str, int]] = Field(
        default=None,
        description="mode='file': column of input file paths. Required for file mode.",
    )
    output_dir: str = Field(
        default="/tmp/codec_converted",
        description="mode='file': directory to write output files into.",
    )
    output_filename_template: Optional[str] = Field(
        default=None,
        description=(
            "mode='file': filename template (no dir). Supports `{<column>}` + `{row_index}`. "
            "Default: <basename>_<to_codec>.<orig_ext>."
        ),
    )
    output_path_column: Union[str, int] = Field(
        default="converted_path",
        description="mode='file': column to write the output file path into.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=('partition_values',), tags=('tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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

        # Validate codec names early — fail fast at scaffold time.
        try:
            codecs.lookup(self.from_codec)
            codecs.lookup(self.to_codec)
        except LookupError as e:
            raise ValueError(f"unknown codec: {e}")

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        mode = self.mode
        from_codec = self.from_codec
        to_codec = self.to_codec
        errors = self.errors
        source_column = self.source_column
        target_column = self.target_column or self.source_column
        source_path_column = self.source_path_column
        output_dir = self.output_dir
        filename_tpl = self.output_filename_template
        path_col = self.output_path_column

        if mode == "string" and not source_column:
            raise ValueError("mode='string' requires source_column.")
        if mode == "file" and not source_path_column:
            raise ValueError("mode='file' requires source_path_column.")

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Text codec convert: {from_codec} → {to_codec} ({mode}).",
            group_name=self.group_name,
            kinds={"codec", "text"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
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
            df = upstream.copy().reset_index(drop=True)

            if mode == "string":
                if source_column not in df.columns:
                    raise ValueError(
                        f"source_column={source_column!r} not in upstream: {list(df.columns)}"
                    )

                def _recode_str(s: Any) -> Any:
                    if not isinstance(s, str):
                        return s
                    try:
                        as_bytes = s.encode(to_codec, errors=errors)
                        return as_bytes.decode(to_codec, errors=errors)
                    except Exception:
                        return None

                df[target_column] = df[source_column].apply(_recode_str)
                processed = int(df[target_column].notna().sum())
                return Output(
                    value=df,
                    metadata={
                        "rows":          MetadataValue.int(len(df)),
                        "processed":     MetadataValue.int(processed),
                        "from_codec":    MetadataValue.text(from_codec),
                        "to_codec":      MetadataValue.text(to_codec),
                        "mode":          MetadataValue.text(mode),
                    },
                )

            # mode == "file"
            if source_path_column not in df.columns:
                raise ValueError(
                    f"source_path_column={source_path_column!r} not in upstream: {list(df.columns)}"
                )

            os.makedirs(output_dir, exist_ok=True)
            out_paths: List[Optional[str]] = []
            errors_col: List[Optional[str]] = []
            sizes_in: List[Optional[int]] = []
            sizes_out: List[Optional[int]] = []

            for i, row in df.iterrows():
                src = row[source_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    out_paths.append(None); errors_col.append(f"missing: {src!r}")
                    sizes_in.append(None); sizes_out.append(None)
                    continue
                try:
                    with open(src, "rb") as fh:
                        raw = fh.read()
                    sizes_in.append(len(raw))
                    text = raw.decode(from_codec, errors=errors)
                    out_bytes = text.encode(to_codec, errors=errors)

                    base = os.path.basename(src)
                    name, ext = os.path.splitext(base)
                    row_dict = {c: row[c] for c in df.columns}
                    row_dict["row_index"] = i
                    if filename_tpl:
                        try:
                            fname = filename_tpl.format(**row_dict)
                        except (KeyError, IndexError):
                            fname = f"{name}_{to_codec}{ext}"
                    else:
                        fname = f"{name}_{to_codec}{ext}"
                    out_path = os.path.join(output_dir, fname)
                    with open(out_path, "wb") as fh:
                        fh.write(out_bytes)
                    sizes_out.append(len(out_bytes))
                    out_paths.append(out_path); errors_col.append(None)
                except Exception as e:
                    out_paths.append(None); errors_col.append(str(e))
                    sizes_out.append(None)

            df[path_col] = out_paths
            df["codec_error"] = errors_col
            df["size_bytes_in"] = sizes_in
            df["size_bytes_out"] = sizes_out

            ok = int(sum(1 for p in out_paths if p))
            return Output(
                value=df,
                metadata={
                    "rows":          MetadataValue.int(len(df)),
                    "converted":     MetadataValue.int(ok),
                    "failed":        MetadataValue.int(len(df) - ok),
                    "from_codec":    MetadataValue.text(from_codec),
                    "to_codec":      MetadataValue.text(to_codec),
                    "mode":          MetadataValue.text(mode),
                    "output_dir":    MetadataValue.path(output_dir),
                },
            )

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
