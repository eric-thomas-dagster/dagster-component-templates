"""SyntheticImageGeneratorComponent — generate sample PNGs for Vision / OCR demos.

PNG sibling of `synthetic_pdf_generator`. Built-in shapes-and-colors set is
designed to exercise Cloud Vision label detection cleanly (a red apple, a
blue car, a green plant — all reliably labeled by VISION).

Emits a DataFrame with one row per image:
  - `sku`        — caller-provided identifier
  - `name`       — short caption
  - `file_path`  — absolute path to the written PNG

Used as the upstream of:
  - `vision_api_asset` (label + object + face detection)
  - `gemini_image_generation` (when you want a real image as a control)
  - any custom image-processing pipeline
"""

import os
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
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


_DEFAULT_IMAGES: List[Dict[str, Any]] = [
    {"sku": "FRUIT-1", "name": "apple",       "kind": "fruit"},
    {"sku": "VEH-1",   "name": "blue car",    "kind": "vehicle"},
    {"sku": "PLANT-1", "name": "green plant", "kind": "plant"},
]


def _render_default(draw, name: str):
    """Render one of the built-in shapes onto a draw context."""
    if name == "apple":
        # Red circle + small brown stem
        draw.ellipse([60, 60, 260, 260], fill=(220, 30, 30))
        draw.rectangle([155, 30, 165, 70], fill=(80, 50, 30))
    elif name == "blue car":
        # Blue body + two black wheels
        draw.rectangle([30, 80, 290, 150], fill=(40, 80, 200))
        draw.ellipse([60, 130, 110, 180], fill=(30, 30, 30))
        draw.ellipse([210, 130, 260, 180], fill=(30, 30, 30))
    elif name == "green plant":
        # Green leaves + brown stem
        draw.rectangle([150, 220, 170, 300], fill=(80, 50, 30))
        draw.ellipse([100, 60, 220, 240], fill=(40, 160, 60))
    else:
        # Solid mid-gray placeholder so callers see something
        draw.rectangle([60, 60, 260, 260], fill=(180, 180, 180))


class SyntheticImageGeneratorComponent(Component, Model, Resolvable):
    """Generate sample PNG images and emit a DataFrame describing them."""

    asset_name: str = Field(description="Output asset name.")
    output_dir: str = Field(
        default="/tmp/synthetic_images",
        description="Filesystem directory to write PNGs into (created if missing).",
    )

    samples: str = Field(
        default="default",
        description="`default` to use the built-in 3-image set, or `custom` to use `images` instead.",
    )

    images: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "When samples='custom', list of {sku, name, kind} dicts. The built-in "
            "renderer recognizes the names 'apple', 'blue car', 'green plant'; other "
            "names render a solid gray placeholder. Use samples='default' for the "
            "Vision-friendly set."
        ),
    )

    width:  int = Field(default=320)
    height: int = Field(default=320)

    inject_exif: bool = Field(
        default=False,
        description=(
            "If True, write a synthetic JPEG (not PNG) per image with realistic EXIF "
            "(Make, Model, DateTimeOriginal, ISO, GPS) — for demos of `image_exif_extractor`. "
            "Output extension flips to `.jpg` when this is on."
        ),
    )
    exif_make:  str = Field(default="DagsterCam")
    exif_model: str = Field(default="DG-1")
    exif_gps_lat: float = Field(default=37.7749, description="Decimal degrees; used as the demo GPS lat.")
    exif_gps_lon: float = Field(default=-122.4194, description="Decimal degrees; used as the demo GPS lon.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
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
        output_dir = self.output_dir
        if self.samples == "custom":
            if not self.images:
                raise ValueError("samples='custom' requires `images` to be set.")
            images = self.images
        else:
            images = _DEFAULT_IMAGES
        w, h = self.width, self.height
        inject_exif = self.inject_exif
        exif_make, exif_model = self.exif_make, self.exif_model
        exif_lat, exif_lon = self.exif_gps_lat, self.exif_gps_lon

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Synthetic PNGs ({len(images)}) for Vision / image demos.",
            group_name=self.group_name,
            kinds={"pillow"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from PIL import Image, ImageDraw
            except ImportError:
                raise ImportError("pip install pillow")
            if inject_exif:
                try:
                    import piexif  # type: ignore
                except ImportError:
                    raise ImportError("inject_exif=True requires `pip install piexif`")

            os.makedirs(output_dir, exist_ok=True)
            rows: List[Dict[str, Any]] = []
            for i, d in enumerate(images):
                sku  = str(d["sku"])
                name = str(d.get("name", sku))
                kind = str(d.get("kind", "image"))
                ext = "jpg" if inject_exif else "png"
                path = os.path.join(output_dir, f"{sku}.{ext}")
                img = Image.new("RGB", (w, h), color=(255, 255, 255))
                draw = ImageDraw.Draw(img)
                _render_default(draw, name)

                if inject_exif:
                    # Build EXIF: Make/Model/DateTime + GPS shifted slightly per image so each is distinct
                    def _deg_to_rational(d: float):
                        d = abs(d)
                        deg = int(d)
                        m = (d - deg) * 60
                        mins = int(m)
                        s = (m - mins) * 60
                        # rationals as (num, denom) tuples; seconds at 1/100 resolution
                        return [(deg, 1), (mins, 1), (int(s * 100), 100)]
                    lat = exif_lat + 0.001 * i
                    lon = exif_lon + 0.001 * i
                    exif_dict = {
                        "0th": {
                            piexif.ImageIFD.Make: exif_make,
                            piexif.ImageIFD.Model: exif_model,
                            piexif.ImageIFD.Software: "dagster-synthetic-image-generator",
                        },
                        "Exif": {
                            piexif.ExifIFD.DateTimeOriginal: "2025:01:15 12:00:00",
                            piexif.ExifIFD.ISOSpeedRatings: 200,
                            piexif.ExifIFD.FNumber: (28, 10),
                            piexif.ExifIFD.ExposureTime: (1, 60),
                            piexif.ExifIFD.FocalLength: (35, 1),
                        },
                        "GPS": {
                            piexif.GPSIFD.GPSLatitudeRef:  "N" if lat >= 0 else "S",
                            piexif.GPSIFD.GPSLatitude:     _deg_to_rational(lat),
                            piexif.GPSIFD.GPSLongitudeRef: "E" if lon >= 0 else "W",
                            piexif.GPSIFD.GPSLongitude:    _deg_to_rational(lon),
                        },
                        "1st": {},
                        "thumbnail": None,
                    }
                    exif_bytes = piexif.dump(exif_dict)
                    img.save(path, "JPEG", exif=exif_bytes, quality=90)
                else:
                    img.save(path, "PNG")
                rows.append({"sku": sku, "name": name, "kind": kind, "file_path": path})

            df = pd.DataFrame(rows)
            return Output(
                value=df,
                metadata={
                    "output_dir":  MetadataValue.path(output_dir),
                    "image_count": MetadataValue.int(len(df)),
                    "size":        MetadataValue.text(f"{w}x{h}"),
                    "preview":     MetadataValue.md(df.to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
