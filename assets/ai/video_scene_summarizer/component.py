"""VideoSceneSummarizerComponent — a "table of contents" for video.

Nothing in the catalog today combines scene detection with vision-LLM
understanding: `video_frame_extract_asset` pulls frames on a fixed
schedule (every N seconds/frames, or a fixed count) with no idea which
frames are actually interesting; `audio_transcriber`/`speech_to_text_asset`
only handle the audio track. This component detects scene CHANGES via
ffmpeg (real content-aware boundaries, not a fixed cadence), extracts one
representative frame per scene, and summarizes each with a vision-capable
LLM via litellm — emitting one row per scene: timestamp, frame path, and
a short description of what's happening.

Requires `ffmpeg` in PATH and a vision-capable model (gpt-4o,
claude-3-5-sonnet, gemini-1.5-pro, etc.).
"""

import base64
import json
import os
import re
import shutil
import subprocess
from typing import Any, Dict, List, Optional, Union

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
from pydantic import ConfigDict, Field


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
):
    """Construct a Dagster partitions_def from the canonical partition fields.
    Canonical implementation — copied as-is per FIELD_CONVENTIONS.md."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
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
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# ffmpeg's showinfo filter prints one line per emitted frame to stderr,
# e.g. "... pts_time:12.34 ...". This is the standard way to recover
# WHICH timestamp each select='gt(scene,...)' frame actually landed on --
# there's no other way to correlate an output JPEG back to a source time.
_PTS_TIME_RE = re.compile(r"pts_time:([\d.]+)")


def _detect_scenes(
    ffmpeg_binary: str, video_path: str, output_pattern: str,
    scene_threshold: float, max_scenes: int, image_quality: int,
) -> List[float]:
    """Run ffmpeg's scene-change filter, extracting one frame per detected
    scene boundary into `output_pattern` (a printf-style path). Returns
    the timestamp (seconds) of each emitted frame, in order, parsed from
    showinfo's stderr output -- ffmpeg gives no other way to know which
    source timestamp a given output frame came from."""
    qscale = max(2, min(31, int(31 - (image_quality / 100.0) * 29)))
    cmd = [
        ffmpeg_binary, "-y", "-i", video_path,
        "-vf", f"select='gt(scene,{scene_threshold})',showinfo",
        "-fps_mode", "vfr", "-qscale:v", str(qscale),
        "-frames:v", str(max_scenes),
        output_pattern,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        stderr = result.stderr or ""
        # Checked live: when `select` matches ZERO frames (a genuinely
        # static/no-cuts clip), ffmpeg exits NONZERO here -- the image
        # encoder never gets opened (no frame to infer dimensions from),
        # not because anything is actually wrong. That's "no scenes
        # detected", which the caller already has a real fallback for
        # (extract the first frame instead); only treat this as a real
        # failure when the INPUT itself couldn't be read at all.
        if "Nothing was written into output file" in stderr or "streams received no packets" in stderr:
            return []
        raise RuntimeError(stderr.splitlines()[-1] if stderr else "ffmpeg failed")
    return [float(m) for m in _PTS_TIME_RE.findall(result.stderr or "")]


def _extract_first_frame(ffmpeg_binary: str, video_path: str, output_path: str, image_quality: int) -> None:
    """Fallback when scene detection finds nothing (a short/static clip,
    e.g. synthetic_video_generator's test pattern) -- always emit AT LEAST
    the first frame so a real video never produces zero rows silently."""
    qscale = max(2, min(31, int(31 - (image_quality / 100.0) * 29)))
    cmd = [ffmpeg_binary, "-y", "-i", video_path, "-frames:v", "1", "-qscale:v", str(qscale), output_path]
    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=120)


def _summarize_frame(frame_path: str, model: str, api_key_env_var: str, prompt: str, max_retries: int) -> str:
    """One vision-LLM call per scene frame -- same base64 content-block
    pattern as image_llm_extractor/structured_document_extractor."""
    try:
        from litellm import completion
    except ImportError:
        raise ImportError("litellm required: pip install litellm")

    with open(frame_path, "rb") as f:
        img_data = base64.b64encode(f.read()).decode("utf-8")
    resp = completion(
        model=model,
        messages=[{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img_data}"}},
            ],
        }],
        api_key=os.environ.get(api_key_env_var),
        num_retries=max_retries,
    )
    return (resp.choices[0].message.content or "").strip()


class VideoSceneSummarizerComponent(Component, Model, Resolvable):
    """Detect scene changes in a video and summarize each one with a
    vision-LLM call -- a "table of contents" for video, one row per scene
    (timestamp, frame, summary), instead of a fixed-cadence frame dump.
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key providing video file paths.")

    video_path_column: Union[str, int] = Field(default="file_path", description="Column of video file paths.")
    video_id_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column to carry forward as `video_id` on each scene row. Default: row index.",
    )

    scene_threshold: float = Field(
        default=0.4, ge=0.0, le=1.0,
        description="ffmpeg scene-change sensitivity (0-1). Lower catches more/subtler cuts; higher only catches hard cuts.",
    )
    max_scenes_per_video: int = Field(default=10, description="Safety cap on scenes extracted (and summarized) per video.")
    output_dir: str = Field(default="/tmp/video_scenes", description="Filesystem directory for extracted scene frames.")
    image_quality: int = Field(default=85, ge=1, le=100, description="JPEG quality 1-100 for extracted frames.")

    model_id: str = Field(alias="model", default="gpt-4o", description="Vision-capable LLM model name (litellm format).")
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Environment variable holding the API key.")
    summary_prompt: str = Field(
        default="Describe what's happening in this video frame in one or two sentences. Be specific about action, setting, and any visible text.",
        description="Instruction sent to the vision LLM alongside each scene frame.",
    )
    llm_max_retries: int = Field(default=2, description="Retry a scene's LLM call this many times on transient errors before giving up on it.")

    ffmpeg_binary: str = Field(default="ffmpeg")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    include_preview_metadata: bool = Field(default=False, description="Include a preview of the output data in metadata.")
    preview_rows: int = Field(default=25, ge=1, le=500)

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'static'/'dynamic'/None.")
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO), required for time-based types.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
        )

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            freshness_policy = FreshnessPolicy(maximum_lag_minutes=self.freshness_max_lag_minutes, cron_schedule=self.freshness_cron)

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
        video_path_column = self.video_path_column
        video_id_column = self.video_id_column
        scene_threshold = self.scene_threshold
        max_scenes = self.max_scenes_per_video
        output_dir = self.output_dir
        image_quality = self.image_quality
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        summary_prompt = self.summary_prompt
        llm_max_retries = self.llm_max_retries
        ffmpeg_binary = self.ffmpeg_binary
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or "Per-scene video summary: timestamp, frame, and a vision-LLM description of each detected scene.",
            group_name=self.group_name,
            kinds={"ffmpeg", "video", "llm"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap -- same rationale
            # as video_frame_extract_asset: tolerates upstream authors who
            # annotate `-> Output` or return Output(value=df, ...).
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            if shutil.which(ffmpeg_binary) is None:
                raise RuntimeError(f"ffmpeg binary {ffmpeg_binary!r} not in PATH.")
            if video_path_column not in upstream.columns:
                raise ValueError(f"video_path_column={video_path_column!r} not in upstream: {list(upstream.columns)}")

            os.makedirs(output_dir, exist_ok=True)
            df = upstream.copy().reset_index(drop=True)
            scene_rows: List[Dict[str, Any]] = []
            video_errors = 0
            llm_failures = 0

            for i, row in df.iterrows():
                src = row[video_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    video_errors += 1
                    continue
                video_basename = os.path.splitext(os.path.basename(src))[0]
                video_id = str(row[video_id_column]) if video_id_column and video_id_column in df.columns else f"v{i}"
                # `_{i}_` disambiguates videos that share a basename (e.g.
                # 'clip.mp4' from two different source directories in the
                # same batch) -- without it, the second video's frames
                # silently overwrite the first's on disk (both write to
                # output_dir/clip_sceneNNN.jpg), corrupting the first
                # video's already-recorded frame_path rows with no error.
                # Confirmed live: two same-named videos in one batch
                # produced a row whose frame_path pointed to a file that
                # had since been overwritten by the other video's frame.
                file_prefix = f"{video_basename}_{i}_scene"
                pattern = os.path.join(output_dir, f"{file_prefix}%03d.jpg")

                try:
                    timestamps = _detect_scenes(ffmpeg_binary, src, pattern, scene_threshold, max_scenes, image_quality)
                except Exception as e:
                    context.log.warning(f"Scene detection failed for {src!r}: {e}")
                    video_errors += 1
                    continue

                frame_paths = sorted(
                    p for p in [os.path.join(output_dir, f) for f in os.listdir(output_dir)]
                    if os.path.basename(p).startswith(file_prefix) and p.endswith(".jpg")
                )
                if not frame_paths:
                    # No scene changes detected (common for short/static
                    # clips) -- still emit ONE row from the first frame
                    # rather than silently producing nothing for this video.
                    fallback_path = os.path.join(output_dir, f"{file_prefix}000.jpg")
                    try:
                        _extract_first_frame(ffmpeg_binary, src, fallback_path, image_quality)
                        frame_paths = [fallback_path]
                        timestamps = [0.0]
                    except Exception as e:
                        context.log.warning(f"Fallback first-frame extraction failed for {src!r}: {e}")
                        video_errors += 1
                        continue

                # ffmpeg's showinfo emits one pts_time per ACTUAL selected
                # frame; if that count doesn't match the files on disk
                # (extremely rare, but silently zipping mismatched lists
                # would mislabel every subsequent scene's timestamp), pad
                # with None rather than guess.
                if len(timestamps) < len(frame_paths):
                    timestamps = timestamps + [None] * (len(frame_paths) - len(timestamps))

                for scene_index, (fpath, ts) in enumerate(zip(frame_paths, timestamps)):
                    try:
                        summary = _summarize_frame(fpath, model, api_key_env_var, summary_prompt, llm_max_retries)
                    except Exception as e:
                        context.log.warning(f"Summarization failed for {fpath!r}: {e}")
                        summary = None
                        llm_failures += 1
                    scene_rows.append({
                        "video_id":         video_id,
                        "source_video":     src,
                        "scene_index":      scene_index,
                        "timestamp_seconds": ts,
                        "frame_path":       fpath,
                        "summary":          summary,
                    })

            scenes_df = pd.DataFrame(scene_rows)
            metadata: Dict[str, Any] = {
                "videos":        MetadataValue.int(len(df)),
                "scenes":        MetadataValue.int(len(scenes_df)),
                "video_errors":  MetadataValue.int(video_errors),
                "llm_failures":  MetadataValue.int(llm_failures),
                "output_dir":    MetadataValue.path(output_dir),
            }
            if include_preview and len(scenes_df) > 0:
                try:
                    _prev = scenes_df.sample(min(preview_rows, len(scenes_df))) if len(scenes_df) > preview_rows * 10 else scenes_df.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=scenes_df, metadata=metadata)

        return Definitions(assets=[_asset])
