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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
