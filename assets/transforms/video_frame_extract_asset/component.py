"""VideoFrameExtractAssetComponent — pull N frames per video as image files.

For each row of an upstream DataFrame holding a video file path, runs
ffmpeg to extract one of three frame patterns:

  - `every_seconds`: one frame every N seconds (e.g. 1 frame/s → many)
  - `every_n_frames`: one frame every N frames in the source
  - `fixed_count`: spread N frames evenly over the duration

Emits a NEW DataFrame with one row per extracted frame — perfect for
feeding `vision_api_asset`, `image_exif_extractor`, `gemini_llm`
(multimodal) downstream.

Requires `ffmpeg` in PATH.
"""

import os
import shutil
import subprocess
from typing import Any, Dict, List, Literal, Optional

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


class VideoFrameExtractAssetComponent(Component, Model, Resolvable):
    """Extract N frames per video as image files; emit a per-frame DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key (video paths).")

    video_path_column: str = Field(default="file_path", description="Column of video file paths.")
    video_id_column: Optional[str] = Field(
        default=None,
        description="Optional column to carry forward as `video_id` on each frame row. Default: row index.",
    )

    output_dir: str = Field(default="/tmp/extracted_frames")
    output_filename_template: str = Field(
        default="{video_basename}_f{frame_index:04d}.jpg",
        description=(
            "Filename template. Supports `{video_basename}`, `{frame_index}`, and any source column."
        ),
    )
    image_format: Literal["jpg", "png"] = Field(default="jpg")
    image_quality: int = Field(default=85, ge=1, le=100, description="JPEG quality 1–100 (PNG ignores).")

    mode: Literal["every_seconds", "every_n_frames", "fixed_count"] = Field(
        default="every_seconds",
        description="How to choose frames.",
    )
    every_seconds: float = Field(default=1.0, description="`every_seconds`: 1 frame per N seconds.")
    every_n_frames: int = Field(default=30, description="`every_n_frames`: 1 frame per N source frames.")
    fixed_count: int = Field(default=10, description="`fixed_count`: total frames spread evenly.")

    ffmpeg_binary: str = Field(default="ffmpeg")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        video_path_column = self.video_path_column
        video_id_column = self.video_id_column
        output_dir = self.output_dir
        filename_tpl = self.output_filename_template
        image_format = self.image_format
        image_quality = self.image_quality
        mode = self.mode
        every_seconds = self.every_seconds
        every_n_frames = self.every_n_frames
        fixed_count = self.fixed_count
        ffmpeg_binary = self.ffmpeg_binary

        @asset(
            name=asset_name,
            description=self.description or f"Extract frames via ffmpeg ({mode}).",
            group_name=self.group_name,
            kinds={"ffmpeg", "video", "image"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            if shutil.which(ffmpeg_binary) is None:
                raise RuntimeError(
                    f"ffmpeg binary {ffmpeg_binary!r} not in PATH."
                )
            if video_path_column not in upstream.columns:
                raise ValueError(
                    f"video_path_column={video_path_column!r} not in upstream: {list(upstream.columns)}"
                )

            os.makedirs(output_dir, exist_ok=True)
            df = upstream.copy().reset_index(drop=True)
            frames_rows: List[Dict[str, Any]] = []
            errors_total = 0

            for i, row in df.iterrows():
                src = row[video_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    errors_total += 1
                    continue
                video_basename = os.path.splitext(os.path.basename(src))[0]
                video_id = (
                    str(row[video_id_column]) if video_id_column and video_id_column in df.columns
                    else f"v{i}"
                )

                # Build ffmpeg select filter for the chosen mode
                if mode == "every_seconds":
                    # 1 frame per `every_seconds` — use fps filter at 1/every_seconds
                    vf = f"fps=1/{every_seconds}"
                elif mode == "every_n_frames":
                    vf = f"select=not(mod(n\\,{every_n_frames}))"
                else:  # fixed_count — sample N frames evenly across duration
                    # Use thumbnail filter then trim to fixed_count via separate -vframes
                    vf = "thumbnail"

                pattern = os.path.join(output_dir, f"{video_basename}_f%04d.{image_format}")
                cmd: List[str] = [
                    ffmpeg_binary, "-y", "-i", src,
                    "-vf", vf,
                ]
                if mode == "every_n_frames":
                    cmd.extend(["-vsync", "vfr"])
                if mode == "fixed_count":
                    cmd.extend(["-vframes", str(fixed_count)])
                if image_format == "jpg":
                    # qscale:v on a 1–31 scale (lower is better). Map 100→2, 1→31 linearly.
                    qscale = max(2, min(31, int(31 - (image_quality / 100.0) * 29)))
                    cmd.extend(["-qscale:v", str(qscale)])
                cmd.append(pattern)

                try:
                    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
                except subprocess.CalledProcessError:
                    errors_total += 1
                    continue

                # Collect emitted frames
                for fname in sorted(os.listdir(output_dir)):
                    if not fname.startswith(f"{video_basename}_f"):
                        continue
                    if not fname.endswith(f".{image_format}"):
                        continue
                    fpath = os.path.join(output_dir, fname)
                    # frame index from filename
                    try:
                        frame_index = int(fname.replace(f"{video_basename}_f", "").split(".")[0])
                    except ValueError:
                        frame_index = -1
                    # Optionally rename to honor template
                    row_dict = {c: row[c] for c in df.columns}
                    row_dict["video_basename"] = video_basename
                    row_dict["frame_index"] = frame_index
                    try:
                        target = filename_tpl.format(**row_dict)
                    except (KeyError, IndexError, ValueError):
                        target = fname
                    if target != fname:
                        new_path = os.path.join(output_dir, target)
                        if new_path != fpath:
                            os.rename(fpath, new_path)
                            fpath = new_path
                    frames_rows.append({
                        "video_id":      video_id,
                        "source_video":  src,
                        "frame_index":   frame_index,
                        "file_path":     fpath,
                    })

            frames_df = pd.DataFrame(frames_rows)
            return Output(
                value=frames_df,
                metadata={
                    "videos":     MetadataValue.int(len(df)),
                    "frames":     MetadataValue.int(len(frames_df)),
                    "errors":     MetadataValue.int(errors_total),
                    "mode":       MetadataValue.text(mode),
                    "output_dir": MetadataValue.path(output_dir),
                    "preview":    MetadataValue.md(frames_df.head(10).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
