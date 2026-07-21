"""SyntheticAudioGeneratorComponent — generate sample WAV files for audio demos.

Emits a DataFrame with one row per generated audio file. Uses only Python
stdlib (`wave` + `struct` + `math`) — no audio libraries required.

Built-in samples: a couple of seconds of pure sine tones at different
frequencies (so they're distinguishable but tiny on disk). For demos
that need REAL speech audio, use `synthetic_data_generator` with
`schema_type: audio_samples` to get GCS URIs of Google's public speech
samples.

Useful as the upstream of:
  - `audio_transform_asset` (resample / convert / normalize)
  - `litellm_audio_transcription` (will transcribe to empty/garbage, but
     exercises the auth + I/O path)
"""

import math
import os
import struct
import wave
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


_DEFAULT_TONES: List[Dict[str, Any]] = [
    {"clip_id": "tone-440",  "frequency_hz": 440.0,  "duration_seconds": 1.0, "kind": "sine"},   # A4
    {"clip_id": "tone-880",  "frequency_hz": 880.0,  "duration_seconds": 1.0, "kind": "sine"},   # A5
    {"clip_id": "tone-1760", "frequency_hz": 1760.0, "duration_seconds": 0.5, "kind": "sine"},   # A6
]


def _render_sine_wav(path: str, frequency_hz: float, duration_seconds: float,
                     sample_rate: int = 44100, amplitude: float = 0.5) -> None:
    """Write a single-channel 16-bit PCM WAV containing a pure sine tone."""
    n_samples = int(duration_seconds * sample_rate)
    max_int16 = 32767
    with wave.open(path, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)  # 16-bit
        w.setframerate(sample_rate)
        for i in range(n_samples):
            v = amplitude * math.sin(2 * math.pi * frequency_hz * (i / sample_rate))
            w.writeframesraw(struct.pack("<h", int(v * max_int16)))


class SyntheticAudioGeneratorComponent(Component, Model, Resolvable):
    """Generate sample WAV files (sine tones) and emit a DataFrame describing them."""

    asset_name: str = Field(description="Output asset name.")

    output_dir: str = Field(
        default="/tmp/synthetic_audio",
        description="Filesystem directory to write WAVs into (created if missing).",
    )

    samples: str = Field(
        default="default",
        description="`default` to use the built-in 3-tone set, or `custom` to use `clips` instead.",
    )

    clips: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "When samples='custom', list of {clip_id, frequency_hz, duration_seconds, kind?} dicts. "
            "Only `sine` is implemented by the built-in renderer; other `kind` values fall back to sine."
        ),
    )

    sample_rate: int = Field(default=44100, description="Output WAV sample rate (Hz).")

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
        sample_rate = self.sample_rate
        if self.samples == "custom":
            if not self.clips:
                raise ValueError("samples='custom' requires `clips` to be set.")
            clips: List[Dict[str, Any]] = list(self.clips)
        else:
            clips = list(_DEFAULT_TONES)

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Synthetic WAV audio ({len(clips)} clips, {sample_rate} Hz).",
            group_name=self.group_name,
            kinds={"wave", "audio"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext):
            os.makedirs(output_dir, exist_ok=True)
            rows: List[Dict[str, Any]] = []
            for c in clips:
                clip_id = str(c["clip_id"])
                freq = float(c.get("frequency_hz", 440.0))
                dur = float(c.get("duration_seconds", 1.0))
                kind = str(c.get("kind", "sine"))
                path = os.path.join(output_dir, f"{clip_id}.wav")
                _render_sine_wav(path, freq, dur, sample_rate=sample_rate)
                rows.append({
                    "clip_id":          clip_id,
                    "kind":             kind,
                    "frequency_hz":     freq,
                    "duration_seconds": dur,
                    "sample_rate":      sample_rate,
                    "file_path":        path,
                    "file_size_bytes":  os.path.getsize(path),
                })

            df = pd.DataFrame(rows)
            return Output(
                value=df,
                metadata={
                    "output_dir":  MetadataValue.path(output_dir),
                    "clip_count":  MetadataValue.int(len(df)),
                    "sample_rate": MetadataValue.int(sample_rate),
                    "preview":     MetadataValue.md(df.to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
