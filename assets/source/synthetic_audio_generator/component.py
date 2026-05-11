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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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
            name=asset_name,
            description=self.description or f"Synthetic WAV audio ({len(clips)} clips, {sample_rate} Hz).",
            group_name=self.group_name,
            kinds={"wave", "audio"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
        )
        def _asset(context: AssetExecutionContext) -> Output:
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
