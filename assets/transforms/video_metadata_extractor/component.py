"""VideoMetadataExtractorComponent — pull video / audio stream metadata via ffprobe.

Reads a column of video file paths, runs `ffprobe -show_format -show_streams`
per file, and augments the DataFrame with codec / resolution / fps /
duration / bitrate columns plus an `streams_raw` JSON column with the
full per-stream detail.

Common columns added:
  - Container: `video_format_name`, `video_duration_seconds`, `video_size_bytes`, `video_bit_rate`
  - Primary video stream: `video_codec`, `video_width`, `video_height`,
    `video_fps`, `video_pix_fmt`
  - Primary audio stream: `audio_codec`, `audio_sample_rate`,
    `audio_channels`, `audio_bit_rate`
  - `streams_raw`: list of per-stream dicts (codec, type, full metadata)

Use cases:
  - Validate ingest: require resolution >= 1080p, duration <= 10min
  - Sort / route: 4K → high-quality bucket, 720p → standard
  - Audit content library: codec distribution, deprecated formats
"""

import json
import os
import shutil
import subprocess
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


def _parse_fps(rate_str: Optional[str]) -> Optional[float]:
    """`avg_frame_rate` comes as `24/1`, `30000/1001`, etc. Convert to float."""
    if not rate_str or "/" not in rate_str:
        return None
    try:
        num, den = rate_str.split("/")
        den_f = float(den)
        if den_f == 0:
            return None
        return float(num) / den_f
    except (ValueError, ZeroDivisionError):
        return None


def _probe(path: str, ffprobe_binary: str) -> Dict[str, Any]:
    cmd = [
        ffprobe_binary, "-v", "error",
        "-show_format", "-show_streams",
        "-of", "json",
        path,
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=60)
    data = json.loads(result.stdout)
    fmt = data.get("format") or {}
    streams = data.get("streams") or []

    out: Dict[str, Any] = {
        "video_format_name":     fmt.get("format_name"),
        "video_duration_seconds": float(fmt["duration"]) if fmt.get("duration") else None,
        "video_size_bytes":      int(fmt["size"]) if fmt.get("size") else None,
        "video_bit_rate":        int(fmt["bit_rate"]) if fmt.get("bit_rate") else None,
        "video_codec":           None,
        "video_width":           None,
        "video_height":          None,
        "video_fps":             None,
        "video_pix_fmt":         None,
        "audio_codec":           None,
        "audio_sample_rate":     None,
        "audio_channels":        None,
        "audio_bit_rate":        None,
        "streams_raw":           streams,
    }
    for s in streams:
        if s.get("codec_type") == "video" and out["video_codec"] is None:
            out["video_codec"]  = s.get("codec_name")
            out["video_width"]  = s.get("width")
            out["video_height"] = s.get("height")
            out["video_fps"]    = _parse_fps(s.get("avg_frame_rate"))
            out["video_pix_fmt"] = s.get("pix_fmt")
        elif s.get("codec_type") == "audio" and out["audio_codec"] is None:
            out["audio_codec"]       = s.get("codec_name")
            out["audio_sample_rate"] = int(s["sample_rate"]) if s.get("sample_rate") else None
            out["audio_channels"]    = s.get("channels")
            out["audio_bit_rate"]    = int(s["bit_rate"]) if s.get("bit_rate") else None
    return out


class VideoMetadataExtractorComponent(Component, Model, Resolvable):
    """Extract container + stream metadata from video files via ffprobe."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    video_path_column: str = Field(
        default="file_path",
        description="Column containing local video file paths.",
    )
    ffprobe_binary: str = Field(default="ffprobe")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        video_path_column = self.video_path_column
        ffprobe_binary = self.ffprobe_binary

        @asset(
            name=asset_name,
            description=self.description or "Extract container + stream metadata via ffprobe.",
            group_name=self.group_name,
            kinds={"ffmpeg", "video"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            if shutil.which(ffprobe_binary) is None:
                raise RuntimeError(
                    f"ffprobe binary {ffprobe_binary!r} not in PATH. Install ffmpeg (ffprobe ships with it)."
                )
            if video_path_column not in upstream.columns:
                raise ValueError(
                    f"video_path_column={video_path_column!r} not in upstream: {list(upstream.columns)}"
                )

            df = upstream.copy().reset_index(drop=True)
            extracted: List[Dict[str, Any]] = []
            errors: List[Optional[str]] = []
            for _, row in df.iterrows():
                src = row[video_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    extracted.append({}); errors.append(f"missing: {src!r}")
                    continue
                try:
                    extracted.append(_probe(src, ffprobe_binary)); errors.append(None)
                except subprocess.CalledProcessError as e:
                    extracted.append({})
                    errors.append((e.stderr or "").splitlines()[-1] if e.stderr else str(e))
                except Exception as e:
                    extracted.append({}); errors.append(str(e))

            meta_df = pd.DataFrame(extracted)
            out = pd.concat([df.reset_index(drop=True), meta_df.reset_index(drop=True)], axis=1)
            out["video_meta_error"] = errors

            ok = int(out["video_codec"].notna().sum()) if "video_codec" in out.columns else 0
            return Output(
                value=out,
                metadata={
                    "rows":            MetadataValue.int(len(out)),
                    "with_video":      MetadataValue.int(ok),
                    "with_audio":      MetadataValue.int(int(out["audio_codec"].notna().sum()) if "audio_codec" in out.columns else 0),
                    "preview":         MetadataValue.md(out.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
