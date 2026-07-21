"""AudioTransformAssetComponent — ffmpeg-based audio resample / convert / trim.

Reads a column of audio file paths, applies ffmpeg ops per row, writes the
output to a new file, and adds the output path as a column.

Common ops:
  - `target_format`     — `mp3` / `wav` / `flac` / `ogg` / `aac` / `opus`
  - `sample_rate`       — Hz (e.g. 16000 for Whisper, 44100 standard)
  - `channels`          — 1 (mono) or 2 (stereo)
  - `bitrate`           — e.g. `64k`, `128k`, `192k` (lossy formats only)
  - `start_seconds` / `end_seconds` — trim
  - `normalize`         — loudness-normalize via `loudnorm` filter

Useful for:
  - Standardizing audio before transcription (Whisper expects 16kHz mono)
  - Format conversion (MP4 → WAV, M4A → MP3)
  - Trimming long recordings to a clip
  - Loudness normalization across mixed sources

Requires `ffmpeg` in PATH. No Python audio deps; ffmpeg handles everything.
"""

import os
import shutil
import subprocess
from typing import Any, Dict, List, Literal, Optional, Union

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


class AudioTransformAssetComponent(Component, Model, Resolvable):
    """Run ffmpeg per row to resample / convert / trim audio files."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    audio_path_column: Union[str, int] = Field(
        default="file_path",
        description="Column containing local audio file paths.",
    )
    output_dir: str = Field(
        default="/tmp/transformed_audio",
        description="Filesystem dir to write transformed audio into.",
    )
    output_filename_template: Optional[str] = Field(
        default=None,
        description=(
            "Filename template (no dir). Supports `{<column>}` and `{row_index}`. "
            "Default: <input_basename>_t.<target_format>."
        ),
    )
    output_path_column: Union[str, int] = Field(default="transformed_path")

    target_format: Literal["mp3", "wav", "flac", "ogg", "aac", "opus"] = Field(
        default="wav",
        description="Output codec / container. Extension is set from this.",
    )
    sample_rate: Optional[int] = Field(
        default=None,
        description="Resample to this Hz. E.g. 16000 for Whisper, 44100 CD-quality. Omit to leave unchanged.",
    )
    channels: Optional[int] = Field(
        default=None,
        description="Force channel count. 1 = mono, 2 = stereo. Omit to leave unchanged.",
    )
    bitrate: Optional[str] = Field(
        default=None,
        description="Output bitrate (e.g. `64k`, `128k`, `192k`). Lossy formats only.",
    )

    start_seconds: Optional[float] = Field(default=None, description="Trim start offset in seconds.")
    end_seconds:   Optional[float] = Field(default=None, description="Trim end offset in seconds.")

    normalize: bool = Field(
        default=False,
        description="Apply `loudnorm` filter for EBU R128 loudness normalization.",
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
        audio_path_column = self.audio_path_column
        output_dir = self.output_dir
        filename_tpl = self.output_filename_template
        path_col = self.output_path_column
        target_format = self.target_format
        sample_rate = self.sample_rate
        channels = self.channels
        bitrate = self.bitrate
        start_seconds = self.start_seconds
        end_seconds = self.end_seconds
        normalize = self.normalize
        ffmpeg_binary = self.ffmpeg_binary

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Audio transform → {target_format} via ffmpeg.",
            group_name=self.group_name,
            kinds={"ffmpeg", "audio"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap — see summarize for the rationale.
            # Tolerates upstream authors who annotate `-> Output` or
            # return `Output(value=df, ...)` / `MaterializeResult(value=df)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned
            # asset consumes a partitioned upstream, Dagster's IO
            # manager loads ALL partitions as a dict; concat to
            # a single DataFrame before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            if shutil.which(ffmpeg_binary) is None:
                raise RuntimeError(
                    f"ffmpeg binary {ffmpeg_binary!r} not found in PATH. Install ffmpeg "
                    "(macOS: `brew install ffmpeg`; Debian/Ubuntu: `apt install ffmpeg`)."
                )
            if audio_path_column not in upstream.columns:
                raise ValueError(
                    f"audio_path_column={audio_path_column!r} not in upstream: {list(upstream.columns)}"
                )

            os.makedirs(output_dir, exist_ok=True)
            df = upstream.copy().reset_index(drop=True)

            out_paths: List[Optional[str]] = []
            errors: List[Optional[str]] = []

            for i, row in df.iterrows():
                src = row[audio_path_column]
                if not isinstance(src, str) or not os.path.isfile(src):
                    out_paths.append(None); errors.append(f"missing: {src!r}")
                    continue

                base = os.path.splitext(os.path.basename(src))[0]
                row_dict = {c: row[c] for c in df.columns}
                row_dict["row_index"] = i
                if filename_tpl:
                    try:
                        fname = filename_tpl.format(**row_dict)
                    except (KeyError, IndexError):
                        fname = f"{base}_t.{target_format}"
                else:
                    fname = f"{base}_t.{target_format}"
                out_path = os.path.join(output_dir, fname)

                cmd: List[str] = [ffmpeg_binary, "-y", "-i", src]
                if start_seconds is not None:
                    cmd.extend(["-ss", str(start_seconds)])
                if end_seconds is not None:
                    cmd.extend(["-to", str(end_seconds)])
                if sample_rate is not None:
                    cmd.extend(["-ar", str(sample_rate)])
                if channels is not None:
                    cmd.extend(["-ac", str(channels)])
                if bitrate:
                    cmd.extend(["-b:a", bitrate])
                if normalize:
                    cmd.extend(["-af", "loudnorm=I=-16:TP=-1.5:LRA=11"])
                cmd.append(out_path)

                try:
                    result = subprocess.run(
                        cmd, check=True, capture_output=True, text=True, timeout=300,
                    )
                    out_paths.append(out_path); errors.append(None)
                except subprocess.CalledProcessError as e:
                    out_paths.append(None)
                    errors.append((e.stderr or "").splitlines()[-1] if e.stderr else str(e))
                except Exception as e:
                    out_paths.append(None); errors.append(str(e))

            df[path_col] = out_paths
            df["transform_error"] = errors

            ok = int(sum(1 for p in out_paths if p))
            return Output(
                value=df,
                metadata={
                    "rows":          MetadataValue.int(len(df)),
                    "transformed":   MetadataValue.int(ok),
                    "failed":        MetadataValue.int(len(df) - ok),
                    "output_dir":    MetadataValue.path(output_dir),
                    "target_format": MetadataValue.text(target_format),
                    "ops":           MetadataValue.json({
                        "sample_rate":   sample_rate,
                        "channels":      channels,
                        "bitrate":       bitrate,
                        "start_seconds": start_seconds,
                        "end_seconds":   end_seconds,
                        "normalize":     normalize,
                    }),
                },
            )

        return Definitions(assets=[_asset])
