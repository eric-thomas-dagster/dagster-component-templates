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


class AudioTransformAssetComponent(Component, Model, Resolvable):
    """Run ffmpeg per row to resample / convert / trim audio files."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    audio_path_column: str = Field(
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
    output_path_column: str = Field(default="transformed_path")

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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
            name=asset_name,
            description=self.description or f"Audio transform → {target_format} via ffmpeg.",
            group_name=self.group_name,
            kinds={"ffmpeg", "audio"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
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
