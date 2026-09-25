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
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
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


def _list_files(path: str, max_files: Optional[int]) -> List[Dict[str, Any]]:
    """List every file matching `path` (a glob pattern or fsspec URI —
    s3://, gs://, abfss:// / abfs:// / az://, or a local path) via
    fsspec's protocol-agnostic filesystem interface. Returns one dict per
    match with whatever `info()` fields that backend actually provides
    (size/mtime keys vary by backend; both are pulled defensively).
    """
    import fsspec

    fs, _, paths = fsspec.get_fs_token_paths(path)
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
            "Glob pattern or fsspec URI to list, e.g. "
            "'s3://my-bucket/invoices/**/*.pdf', "
            "'gs://my-bucket/audio/*.wav', "
            "'abfss://container@account.dfs.core.windows.net/docs/*.png', "
            "or a local path like '/data/incoming/*.pdf'. Auth uses "
            "fsspec's ambient credential discovery (same as file_ingestion)."
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
    asset_tags: Optional[Dict[str, str]] = Field(
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
        description="Column-level lineage: output column → list of upstream columns it derives from.",
    )
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'dynamic', 'multi', or None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static or multi partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition (when partition_type='dynamic').")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides flat fields when set.")
    partition_static_dim: Optional[str] = Field(default=None, description="Dimension name for the static axis in multi-partitioning.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

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
            context.log.info(f"Listing files matching {path_pattern!r}")
            matches, fs = _list_files(path_pattern, max_files)
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
                "path_pattern": path_pattern,
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
