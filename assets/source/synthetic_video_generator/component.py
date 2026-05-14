"""SyntheticVideoGeneratorComponent — generate tiny test videos via ffmpeg.

Emits a DataFrame with one row per generated video file. Uses ffmpeg's
built-in `testsrc` (color-bar pattern) + `sine` audio generators — so
the output is a real, valid MP4 with video and audio tracks, but the
content is just a moving test pattern. Perfect for exercising
`video_metadata_extractor`, `video_frame_extract_asset`, and
`video_audio_extract_asset` end-to-end without committing binary
fixtures.

Requires `ffmpeg` in PATH.
"""

import os
import shutil
import subprocess
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


_DEFAULT_CLIPS: List[Dict[str, Any]] = [
    {"clip_id": "vid-001", "duration_seconds": 2.0, "fps": 24, "width": 320, "height": 240, "tone_hz": 440.0},
    {"clip_id": "vid-002", "duration_seconds": 1.0, "fps": 30, "width": 640, "height": 480, "tone_hz": 880.0},
]


class SyntheticVideoGeneratorComponent(Component, Model, Resolvable):
    """Generate small test MP4s (color-bars video + sine tone audio) via ffmpeg."""

    asset_name: str = Field(description="Output asset name.")

    output_dir: str = Field(
        default="/tmp/synthetic_videos",
        description="Filesystem directory to write videos into (created if missing).",
    )

    samples: str = Field(
        default="default",
        description="`default` to use the built-in 2-clip set, or `custom` to use `clips` instead.",
    )

    clips: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "When samples='custom', list of {clip_id, duration_seconds, fps, width, height, tone_hz} dicts. "
            "tone_hz is the audio track's sine-tone frequency in Hz."
        ),
    )

    ffmpeg_binary: str = Field(default="ffmpeg")

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
            if not self.clips:
                raise ValueError("samples='custom' requires `clips` to be set.")
            clips: List[Dict[str, Any]] = list(self.clips)
        else:
            clips = list(_DEFAULT_CLIPS)
        ffmpeg_binary = self.ffmpeg_binary

        @asset(
            name=asset_name,
            description=self.description or f"Synthetic MP4 videos ({len(clips)} clips) via ffmpeg testsrc.",
            group_name=self.group_name,
            kinds={"ffmpeg", "video"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            if shutil.which(ffmpeg_binary) is None:
                raise RuntimeError(
                    f"ffmpeg binary {ffmpeg_binary!r} not in PATH. "
                    "Install: macOS `brew install ffmpeg`, Debian/Ubuntu `apt install ffmpeg`."
                )
            os.makedirs(output_dir, exist_ok=True)

            rows: List[Dict[str, Any]] = []
            for c in clips:
                cid = str(c["clip_id"])
                dur = float(c.get("duration_seconds", 2.0))
                fps = int(c.get("fps", 24))
                w   = int(c.get("width", 320))
                h   = int(c.get("height", 240))
                tone = float(c.get("tone_hz", 440.0))

                path = os.path.join(output_dir, f"{cid}.mp4")
                cmd = [
                    ffmpeg_binary, "-y",
                    "-f", "lavfi", "-i", f"testsrc=duration={dur}:size={w}x{h}:rate={fps}",
                    "-f", "lavfi", "-i", f"sine=frequency={tone}:duration={dur}",
                    "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
                    "-c:a", "aac", "-shortest",
                    path,
                ]
                try:
                    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)
                    size = os.path.getsize(path)
                    rows.append({
                        "clip_id":          cid,
                        "duration_seconds": dur,
                        "fps":              fps,
                        "width":            w,
                        "height":           h,
                        "tone_hz":          tone,
                        "file_path":        path,
                        "file_size_bytes":  size,
                    })
                except subprocess.CalledProcessError as e:
                    rows.append({
                        "clip_id":   cid,
                        "file_path": None,
                        "error":     (e.stderr or "").splitlines()[-1] if e.stderr else str(e),
                    })

            df = pd.DataFrame(rows)
            return Output(
                value=df,
                metadata={
                    "output_dir": MetadataValue.path(output_dir),
                    "clip_count": MetadataValue.int(int(df["file_path"].notna().sum()) if "file_path" in df.columns else 0),
                    "preview":    MetadataValue.md(df.to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
