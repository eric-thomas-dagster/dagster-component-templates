"""VideoAudioExtractAssetComponent — pull the audio track from a video as a file.

Per-row: take a video file path, run ffmpeg `-vn -acodec ...` to dump the
audio track to a separate file. Add the audio file path back to the
DataFrame.

The standard "transcribe a video" first step:

  videos → video_audio_extract_asset → audio files → speech_to_text_asset → transcripts
                                                  → litellm_audio_transcription (Whisper)

Choose `target_format` based on what your STT expects (Whisper wants
16kHz mono WAV; Google Cloud Speech wants 16kHz FLAC).
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


class VideoAudioExtractAssetComponent(Component, Model, Resolvable):
    """Extract the audio track from a video file via ffmpeg."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key (video paths).")

    video_path_column: str = Field(default="file_path")

    output_dir: str = Field(default="/tmp/extracted_audio")
    output_filename_template: Optional[str] = Field(
        default=None,
        description=(
            "Filename template (no dir). Supports `{<column>}` + `{row_index}`. "
            "Default: <video_basename>.<target_format>."
        ),
    )
    output_path_column: str = Field(default="audio_path")

    target_format: Literal["wav", "mp3", "flac", "ogg", "aac", "opus"] = Field(
        default="wav",
        description="Output codec / extension.",
    )
    sample_rate: Optional[int] = Field(
        default=None,
        description="Resample on extraction. Common: 16000 (Whisper / Cloud Speech v1), 22050, 44100.",
    )
    channels: Optional[int] = Field(
        default=None,
        description="1 = downmix to mono, 2 = stereo. Omit to preserve source.",
    )
    bitrate: Optional[str] = Field(
        default=None,
        description="For lossy formats: e.g. `128k`. Ignored for WAV/FLAC.",
    )

    ffmpeg_binary: str = Field(default="ffmpeg")

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
        video_path_column = self.video_path_column
        output_dir = self.output_dir
        filename_tpl = self.output_filename_template
        path_col = self.output_path_column
        target_format = self.target_format
        sample_rate = self.sample_rate
        channels = self.channels
        bitrate = self.bitrate
        ffmpeg_binary = self.ffmpeg_binary

        @asset(
            name=asset_name,
            description=self.description or f"Video → audio track ({target_format}) via ffmpeg.",
            group_name=self.group_name,
            kinds={"ffmpeg", "video", "audio"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            if shutil.which(ffmpeg_binary) is None:
                raise RuntimeError(f"ffmpeg binary {ffmpeg_binary!r} not in PATH.")
            if video_path_column not in upstream.columns:
                raise ValueError(
                    f"video_path_column={video_path_column!r} not in upstream: {list(upstream.columns)}"
                )

            os.makedirs(output_dir, exist_ok=True)
            df = upstream.copy().reset_index(drop=True)
            paths: List[Optional[str]] = []
            errors: List[Optional[str]] = []

            for i, row in df.iterrows():
                src = row[video_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    paths.append(None); errors.append(f"missing: {src!r}")
                    continue

                base = os.path.splitext(os.path.basename(src))[0]
                row_dict = {c: row[c] for c in df.columns}
                row_dict["row_index"] = i
                if filename_tpl:
                    try:
                        fname = filename_tpl.format(**row_dict)
                    except (KeyError, IndexError):
                        fname = f"{base}.{target_format}"
                else:
                    fname = f"{base}.{target_format}"
                out_path = os.path.join(output_dir, fname)

                cmd: List[str] = [ffmpeg_binary, "-y", "-i", src, "-vn"]
                if sample_rate is not None:
                    cmd.extend(["-ar", str(sample_rate)])
                if channels is not None:
                    cmd.extend(["-ac", str(channels)])
                if bitrate:
                    cmd.extend(["-b:a", bitrate])
                cmd.append(out_path)

                try:
                    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300)
                    paths.append(out_path); errors.append(None)
                except subprocess.CalledProcessError as e:
                    paths.append(None)
                    errors.append((e.stderr or "").splitlines()[-1] if e.stderr else str(e))
                except Exception as e:
                    paths.append(None); errors.append(str(e))

            df[path_col] = paths
            df["audio_extract_error"] = errors

            ok = int(sum(1 for p in paths if p))
            return Output(
                value=df,
                metadata={
                    "rows":          MetadataValue.int(len(df)),
                    "extracted":     MetadataValue.int(ok),
                    "failed":        MetadataValue.int(len(df) - ok),
                    "target_format": MetadataValue.text(target_format),
                    "output_dir":    MetadataValue.path(output_dir),
                },
            )

        return Definitions(assets=[_asset])
