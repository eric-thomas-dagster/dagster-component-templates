"""ImageExifExtractorComponent — pull EXIF + IPTC + XMP-ish metadata from images.

Reads a column of image file paths and emits the original DataFrame
augmented with EXIF metadata columns. Pillow handles the EXIF parse;
no extra dependencies beyond `pillow`.

Common extracted fields:
  - Camera: `exif_make`, `exif_model`, `exif_software`
  - Capture: `exif_datetime_original`, `exif_iso`, `exif_focal_length_mm`,
            `exif_exposure_time`, `exif_f_number`
  - GPS: `exif_gps_lat`, `exif_gps_lon`, `exif_gps_altitude_m`
  - Image: `exif_orientation`, `exif_width`, `exif_height`

Plus a `exif_raw` column holding the full EXIF dict (useful for ad-hoc
analysis without re-opening the file).

Use cases:
  - **PII / compliance**: GPS in EXIF leaks user location — block or
    redact before publishing
  - **ML training**: filter by camera type, dates, ISO ranges
  - **Asset management**: group photos by location / date for sorting
"""

import os
from fractions import Fraction
from typing import Any, Dict, List, Optional

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


def _to_float(v: Any) -> Optional[float]:
    """Pillow returns EXIF rationals as `IFDRational` (subclass of Fraction).
    Convert to plain float; return None on failure."""
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, (Fraction,)) or hasattr(v, "numerator"):
            return float(v)
        return float(str(v))
    except Exception:
        return None


def _gps_to_decimal(coord, ref: Optional[str]) -> Optional[float]:
    """GPS coordinates come as (degrees, minutes, seconds) rationals + a
    direction ref (N/S/E/W). Convert to signed decimal degrees."""
    if not coord:
        return None
    try:
        d, m, s = [_to_float(c) or 0.0 for c in coord]
        dec = d + (m / 60.0) + (s / 3600.0)
        if ref in ("S", "W"):
            dec = -dec
        return dec
    except Exception:
        return None


def _extract_exif(path: str) -> Dict[str, Any]:
    """Open the image and pull out the common EXIF fields. Returns a dict
    with `exif_*` keys; missing fields are None."""
    from PIL import Image, ExifTags

    result: Dict[str, Any] = {
        "exif_width": None, "exif_height": None,
        "exif_make": None, "exif_model": None, "exif_software": None,
        "exif_datetime_original": None, "exif_iso": None,
        "exif_focal_length_mm": None, "exif_exposure_time": None, "exif_f_number": None,
        "exif_orientation": None,
        "exif_gps_lat": None, "exif_gps_lon": None, "exif_gps_altitude_m": None,
        "exif_raw": None,
    }
    with Image.open(path) as img:
        result["exif_width"] = img.size[0]
        result["exif_height"] = img.size[1]

        # _getexif() may return None for images without EXIF (e.g. PNGs)
        try:
            exif = img._getexif()
        except Exception:
            exif = None
        if not exif:
            return result

        # Resolve tag-id → human name via Pillow's table
        name_by_tag = ExifTags.TAGS
        gps_tag = next((k for k, v in name_by_tag.items() if v == "GPSInfo"), None)
        raw: Dict[str, Any] = {}
        for tag_id, value in exif.items():
            name = name_by_tag.get(tag_id, f"tag_{tag_id}")
            raw[name] = value if not hasattr(value, "decode") else value

        # Make raw JSON-friendly (strip bytes/IFDRational where possible)
        json_raw: Dict[str, Any] = {}
        for k, v in raw.items():
            if isinstance(v, bytes):
                try:
                    v = v.decode("utf-8", errors="replace")
                except Exception:
                    v = str(v)
            elif hasattr(v, "numerator"):
                v = float(v)
            elif isinstance(v, dict):
                v = {str(kk): float(vv) if hasattr(vv, "numerator") else str(vv) for kk, vv in v.items()}
            elif isinstance(v, tuple):
                v = [float(x) if hasattr(x, "numerator") else x for x in v]
            json_raw[k] = v
        result["exif_raw"] = json_raw

        result["exif_make"]     = json_raw.get("Make")
        result["exif_model"]    = json_raw.get("Model")
        result["exif_software"] = json_raw.get("Software")
        result["exif_datetime_original"] = json_raw.get("DateTimeOriginal") or json_raw.get("DateTime")
        result["exif_iso"]      = json_raw.get("ISOSpeedRatings") or json_raw.get("PhotographicSensitivity")
        result["exif_focal_length_mm"] = _to_float(json_raw.get("FocalLength"))
        result["exif_exposure_time"]   = _to_float(json_raw.get("ExposureTime"))
        result["exif_f_number"]        = _to_float(json_raw.get("FNumber"))
        result["exif_orientation"]     = json_raw.get("Orientation")

        # GPS
        gps = exif.get(gps_tag) if gps_tag else None
        if isinstance(gps, dict):
            gps_named: Dict[str, Any] = {}
            for k, v in gps.items():
                gname = ExifTags.GPSTAGS.get(k, f"gps_{k}")
                gps_named[gname] = v
            result["exif_gps_lat"] = _gps_to_decimal(gps_named.get("GPSLatitude"), gps_named.get("GPSLatitudeRef"))
            result["exif_gps_lon"] = _gps_to_decimal(gps_named.get("GPSLongitude"), gps_named.get("GPSLongitudeRef"))
            alt = gps_named.get("GPSAltitude")
            result["exif_gps_altitude_m"] = _to_float(alt)

    return result


class ImageExifExtractorComponent(Component, Model, Resolvable):
    """Extract EXIF metadata from a column of image file paths."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    image_path_column: str = Field(
        default="file_path",
        description="Column containing local image file paths.",
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
        image_path_column = self.image_path_column

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Extract EXIF metadata from image files.",
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
                from PIL import Image  # noqa: F401  — module-level check
            except ImportError:
                raise ImportError("pip install pillow")

            if image_path_column not in upstream.columns:
                raise ValueError(
                    f"image_path_column={image_path_column!r} not in upstream: {list(upstream.columns)}"
                )

            df = upstream.copy().reset_index(drop=True)
            extracted: List[Dict[str, Any]] = []
            errors: List[Optional[str]] = []
            gps_count = 0
            for _, row in df.iterrows():
                src = row[image_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    extracted.append({}); errors.append(f"missing: {src!r}")
                    continue
                try:
                    md = _extract_exif(src)
                    extracted.append(md); errors.append(None)
                    if md.get("exif_gps_lat") is not None:
                        gps_count += 1
                except Exception as e:
                    extracted.append({}); errors.append(str(e))

            exif_df = pd.DataFrame(extracted)
            out = pd.concat([df.reset_index(drop=True), exif_df.reset_index(drop=True)], axis=1)
            out["exif_error"] = errors

            has_make = int(out["exif_make"].notna().sum()) if "exif_make" in out.columns else 0
            return Output(
                value=out,
                metadata={
                    "rows":           MetadataValue.int(len(out)),
                    "with_camera":    MetadataValue.int(has_make),
                    "with_gps":       MetadataValue.int(gps_count),
                    "preview":        MetadataValue.md(out.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
