"""File Lister — list files matching a glob from any fsspec location into a DataFrame."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
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
    Canonical implementation — copied as-is per FIELD_CONVENTIONS.md."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

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
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
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


def _list_files(path: str, max_files: Optional[int]) -> List[Dict[str, Any]]:
    """List every file matching `path` (a glob pattern or fsspec URI —
    s3://, gs://, abfss:// / abfs:// / az://, or a local path) via
    fsspec's protocol-agnostic filesystem interface. Returns one dict per
    match with whatever `info()` fields that backend actually provides
    (size/mtime keys vary by backend; both are pulled defensively).
    """
    import fsspec

    fs, _, paths = fsspec.get_fs_token_paths(path)
    # A path with no glob pattern (no `*`/`?`/`[`) that happens to be a
    # directory resolves to the directory itself, not its contents --
    # confirmed live: 'a/b/c/' with no `*.png` suffix returns
    # ['a/b/c'], a single directory entry, not the 7 files inside it.
    # The natural thing a user types is "point me at this folder", not
    # a glob -- auto-expand a lone directory result into its immediate
    # children rather than making every caller remember to append `/*`.
    if len(paths) == 1 and fs.isdir(paths[0]):
        paths = [p for p in fs.ls(paths[0], detail=False) if not fs.isdir(p)]
    if max_files is not None:
        paths = paths[:max_files]
    out = []
    for p in paths:
        info = fs.info(p)
        size = info.get("size")
        modified = info.get("mtime") or info.get("LastModified") or info.get("last_modified")
        # Reconstruct a fully-qualified URI (fs.info's paths are protocol-
        # stripped for remote backends) so `local_path` downloads and any
        # downstream re-open of the ORIGINAL location both work.
        protocol = fs.protocol if isinstance(fs.protocol, str) else (fs.protocol[0] if fs.protocol else "file")
        full_uri = p if protocol == "file" else f"{protocol}://{p}"
        out.append({
            "path": full_uri,
            "_fs_path": p,
            "filename": p.rsplit("/", 1)[-1],
            "size": size,
            "modified_at": str(modified) if modified is not None else None,
        })
    return out, fs



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


class FileListerComponent(Component, Model, Resolvable):
    """
    List files matching a glob pattern from any fsspec-supported location
    (s3://, gs://, abfss:// / abfs:// / az://, or a local path) into a
    DataFrame — one row per file. Designed as the upstream for
    document/image/audio extractors (ocr_extractor, audio_transcriber,
    document_ai_extractor, image_llm_extractor, ...), which all consume
    an `upstream_asset_key` + a column of file paths.

    By default (`download: true`) each matched file is downloaded to a
    local cache directory and `local_path` points at the local copy —
    extractors that only open local paths (most of the current family)
    work immediately with zero changes. Set `download: false` to list
    metadata only (no bytes moved) when you just need a file inventory,
    or when a downstream component already fsspec-opens `path` directly.
    """

    asset_name: str = Field(description="Name of the asset")
    path: str = Field(
        description=(
            "Glob pattern or fsspec URI to list, e.g. 's3://my-bucket/invoices/**/*.pdf', 'gs://my-bucket/audio/*.wav', 'abfss://container@account.dfs.core.windows.net/docs/*.png', or a local path like '/data/incoming/*.pdf'. Auth uses fsspec's ambient credential discovery (same as file_ingestion). On a partitioned run {partition_key}, {partition_date}, {partition_start} and {partition_end} (and {partition_<dimension>} for multi-partitions) are substituted."
        ),
    )
    download: bool = Field(
        default=True,
        description=(
            "Download each matched file to a local cache directory and "
            "populate `local_path` with the cached copy. Set false to "
            "list metadata only (path/size/modified_at, no bytes moved)."
        ),
    )
    download_dir: Optional[str] = Field(
        default=None,
        description=(
            "Local directory to cache downloaded files in. Auto-generated "
            "under the system temp dir (per asset_name) if unset. Ignored "
            "when download is false."
        ),
    )
    max_files: Optional[int] = Field(
        default=None,
        description=(
            "Safety cap on how many matched files to list/download in one "
            "materialize. Unset means no cap — be deliberate with broad "
            "globs against a large bucket."
        ),
    )
    description: Optional[str] = Field(default="", description="Description of the asset")
    group_name: Optional[str] = Field(default="", description="Asset group name for organization")
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
        description="Asset kinds for the Dagster catalog, e.g. ['s3', 'python']. Auto-inferred from `path`'s protocol if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output data in metadata (first rows as markdown table). Used by builder UIs to render asset shape without warehouse access.",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows to include in the preview metadata when include_preview_metadata is True.",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'dynamic', 'multi', or None for unpartitioned.",
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
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    partition_static_dim: Optional[str] = Field(default=None, description="Dimension name for the static axis in multi-partitioning.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        _column_lineage_md = _column_lineage_metadata(self)
        asset_name = self.asset_name
        path_pattern = self.path
        download = self.download
        download_dir = self.download_dir
        max_files = self.max_files
        description = self.description
        group_name = self.group_name or None
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        owners = self.owners or []
        column_lineage = self.column_lineage

        # Infer `kinds` from the URI's protocol when not explicitly set —
        # same spirit as file_ingestion's keyword-in-asset-name inference,
        # but keyed off the actual path scheme, which is what's really
        # true here.
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _scheme = path_pattern.split("://", 1)[0].lower() if "://" in path_pattern else "local"
            _scheme_kind_map = {"s3": "s3", "gs": "gcp", "gcs": "gcp", "abfss": "azure", "abfs": "azure", "az": "azure"}
            _inferred_kinds = [_scheme_kind_map.get(_scheme, "python")]

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        @asset(
            retry_policy=_retry_policy,
            partitions_def=partitions_def,
            key=AssetKey.from_user_string(asset_name),
            description=description or f"Files matching {path_pattern}",
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=group_name,
            metadata={"path": path_pattern, "download": download},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def file_lister_asset(context: AssetExecutionContext) -> pd.DataFrame:
            """Lists files matching `path_pattern` and (optionally)
            downloads each to a local cache directory."""
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            context.log.info(f"Listing files matching {_substitute_partition(path_pattern, _partition_vars(context))!r}")
            matches, fs = _list_files(_substitute_partition(path_pattern, _partition_vars(context)), max_files)
            context.log.info(f"Found {len(matches)} file(s)")

            if download and matches:
                import tempfile
                from pathlib import Path as _Path

                cache_dir = _Path(download_dir) if download_dir else _Path(tempfile.gettempdir()) / "dagster_file_lister" / asset_name
                cache_dir.mkdir(parents=True, exist_ok=True)
                for m in matches:
                    local_target = cache_dir / m["filename"]
                    try:
                        fs.get(m["_fs_path"], str(local_target))
                        m["local_path"] = str(local_target)
                    except Exception as e:
                        context.log.warning(f"Failed to download {m['path']}: {e}")
                        m["local_path"] = None
            else:
                # No download requested (or nothing matched) -- downstream
                # components that fsspec-open `path` directly can still
                # use it; local_path is just unavailable.
                for m in matches:
                    m["local_path"] = m["path"] if download is False else None

            df = pd.DataFrame(
                [{k: v for k, v in m.items() if not k.startswith("_")} for m in matches],
                columns=["path", "local_path", "filename", "size", "modified_at"],
            )
            context.add_output_metadata({
                "file_count": len(df),
                "path_pattern": _substitute_partition(path_pattern, _partition_vars(context)),
                "downloaded": download,
            })
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return df

        return Definitions(assets=[file_lister_asset])


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

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper

# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper
