"""ImageTransformAssetComponent — Pillow-based image resize / crop / format-convert.

Reads a column of image file paths, applies one or more transforms per row,
writes the output to a new file, and adds the output path as a column.

Configurable ops (applied in order):
  - `resize`              — max dimensions (preserves aspect ratio by default)
  - `crop`                — center / box crop
  - `convert_to`          — output format (jpg/png/webp/bmp/tiff)
  - `quality`             — JPEG/WebP quality 1-100
  - `grayscale`           — convert to single-channel

Useful for:
  - ML preprocessing (downsample for model input)
  - Thumbnail generation
  - Format/format conversion (HEIC → JPG, PNG → WebP)
  - Storage cost reduction (raw camera → 1024px webp)

Pure Pillow; no external services.
"""

import os
from typing import Any, Dict, List, Literal, Optional, Tuple

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


class ImageTransformAssetComponent(Component, Model, Resolvable):
    """Resize / crop / convert / grayscale image files in a DataFrame column."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    image_path_column: str = Field(
        default="file_path",
        description="Column containing local image file paths.",
    )
    output_dir: str = Field(
        default="/tmp/transformed_images",
        description="Filesystem dir to write transformed images into.",
    )
    output_filename_template: Optional[str] = Field(
        default=None,
        description=(
            "Filename template (no dir). Supports `{<column>}` and `{row_index}`. "
            "Default: <input_basename>_t.<ext>."
        ),
    )
    output_path_column: str = Field(
        default="transformed_path",
        description="New column to write the transformed file path into.",
    )

    # Transform ops
    resize_to: Optional[List[int]] = Field(
        default=None,
        description="`[width, height]` max-dimensions. Aspect ratio preserved by default.",
    )
    preserve_aspect_ratio: bool = Field(default=True)
    crop_to: Optional[List[int]] = Field(
        default=None,
        description="`[width, height]` center-crop dimensions. Applied AFTER resize if both set.",
    )
    convert_to: Optional[Literal["jpg", "jpeg", "png", "webp", "bmp", "tiff"]] = Field(
        default=None,
        description="Output format. Determines file extension AND encoding.",
    )
    quality: int = Field(
        default=85,
        description="JPEG/WebP quality 1-100. Ignored for lossless formats.",
        ge=1, le=100,
    )
    grayscale: bool = Field(default=False, description="Convert to single-channel grayscale.")

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
        image_path_column = self.image_path_column
        output_dir = self.output_dir
        filename_tpl = self.output_filename_template
        path_col = self.output_path_column
        resize_to = tuple(self.resize_to) if self.resize_to else None
        preserve_aspect = self.preserve_aspect_ratio
        crop_to = tuple(self.crop_to) if self.crop_to else None
        convert_to = self.convert_to
        quality = self.quality
        grayscale = self.grayscale

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Image transform (resize/crop/convert/grayscale).",
            group_name=self.group_name,
            kinds={"pillow", "image"},
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
            try:
                from PIL import Image
            except ImportError:
                raise ImportError("pip install pillow")

            if image_path_column not in upstream.columns:
                raise ValueError(
                    f"image_path_column={image_path_column!r} not in upstream: {list(upstream.columns)}"
                )

            os.makedirs(output_dir, exist_ok=True)
            df = upstream.copy().reset_index(drop=True)

            ext_map = {"jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "webp": "WEBP", "bmp": "BMP", "tiff": "TIFF"}
            out_paths: List[Optional[str]] = []
            errors: List[Optional[str]] = []
            sizes_before: List[Optional[Tuple[int, int]]] = []
            sizes_after:  List[Optional[Tuple[int, int]]] = []

            for i, row in df.iterrows():
                src = row[image_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    out_paths.append(None); errors.append(f"missing: {src!r}")
                    sizes_before.append(None); sizes_after.append(None)
                    continue

                try:
                    img = Image.open(src)
                    sizes_before.append(img.size)

                    if grayscale:
                        img = img.convert("L")
                    elif img.mode not in ("RGB", "RGBA"):
                        img = img.convert("RGB")

                    if resize_to:
                        if preserve_aspect:
                            img.thumbnail(resize_to, Image.LANCZOS)  # in-place, preserves ratio
                        else:
                            img = img.resize(resize_to, Image.LANCZOS)

                    if crop_to:
                        cw, ch = crop_to
                        w, h = img.size
                        left = max(0, (w - cw) // 2)
                        top  = max(0, (h - ch) // 2)
                        img = img.crop((left, top, left + cw, top + ch))

                    sizes_after.append(img.size)

                    # Determine output path
                    base = os.path.splitext(os.path.basename(src))[0]
                    out_ext = convert_to or os.path.splitext(src)[1].lstrip(".").lower()
                    pil_format = ext_map.get(out_ext.lower(), "PNG")
                    row_dict = {c: row[c] for c in df.columns}
                    row_dict["row_index"] = i
                    if filename_tpl:
                        try:
                            fname = filename_tpl.format(**row_dict)
                        except (KeyError, IndexError):
                            fname = f"{base}_t.{out_ext}"
                    else:
                        fname = f"{base}_t.{out_ext}"
                    out_path = os.path.join(output_dir, fname)

                    save_kwargs: Dict[str, Any] = {}
                    if pil_format in ("JPEG", "WEBP"):
                        save_kwargs["quality"] = quality
                    if pil_format == "JPEG" and img.mode == "RGBA":
                        img = img.convert("RGB")
                    img.save(out_path, pil_format, **save_kwargs)
                    out_paths.append(out_path); errors.append(None)
                except Exception as e:
                    out_paths.append(None); errors.append(str(e))
                    sizes_after.append(None)

            df[path_col] = out_paths
            df["transform_error"] = errors
            df["size_before"] = [f"{wh[0]}x{wh[1]}" if wh else None for wh in sizes_before]
            df["size_after"]  = [f"{wh[0]}x{wh[1]}" if wh else None for wh in sizes_after]

            ok = int(sum(1 for p in out_paths if p))
            return Output(
                value=df,
                metadata={
                    "rows":         MetadataValue.int(len(df)),
                    "transformed":  MetadataValue.int(ok),
                    "failed":       MetadataValue.int(len(df) - ok),
                    "output_dir":   MetadataValue.path(output_dir),
                    "ops":          MetadataValue.json({
                        "resize_to":   list(resize_to) if resize_to else None,
                        "crop_to":     list(crop_to) if crop_to else None,
                        "convert_to":  convert_to,
                        "grayscale":   grayscale,
                        "quality":     quality,
                    }),
                },
            )

        return Definitions(assets=[_asset])
