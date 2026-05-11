"""LitellmTextToSpeechComponent — multi-provider TTS via LiteLLM.

Generates audio files from a text column. One file per row. Routes to
OpenAI TTS, Azure OpenAI TTS, ElevenLabs, and others through LiteLLM.

Provider examples:
  - `openai/tts-1`           — OpenAI TTS standard (6 voices, 24kHz)
  - `openai/tts-1-hd`        — OpenAI TTS high-quality (slower, ~2x cost)
  - `openai/gpt-4o-mini-tts` — OpenAI's newest, prompt-steerable
  - `elevenlabs/eleven_multilingual_v2` — ElevenLabs multilingual
  - `elevenlabs/eleven_turbo_v2_5`      — ElevenLabs low-latency
  - `azure/<deployment>`     — Azure OpenAI TTS

Set the matching `api_key_env_var` for whichever provider you're targeting.
"""

import os
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


class LitellmTextToSpeechComponent(Component, Model, Resolvable):
    """Multi-provider text-to-speech via LiteLLM."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    model: str = Field(
        description=(
            "LiteLLM model id. e.g. `openai/tts-1`, `openai/tts-1-hd`, "
            "`elevenlabs/eleven_multilingual_v2`, `azure/<deployment>`."
        ),
    )
    api_key_env_var: str = Field(description="Env var holding the API key for the selected provider.")

    text_column: str = Field(description="Column containing text to speak.")
    output_dir: str = Field(default="/tmp/litellm_tts_audio")
    output_filename_template: str = Field(
        default="{row_index}.mp3",
        description="Filename template; supports `{<column>}` and `{row_index}`. Extension determines format.",
    )
    output_path_column: str = Field(default="audio_path")

    voice: Optional[str] = Field(
        default=None,
        description=(
            "Voice id. Provider-specific: OpenAI = alloy/echo/fable/onyx/nova/shimmer. "
            "ElevenLabs = voice UUID. Required for most providers."
        ),
    )
    response_format: Literal["mp3", "opus", "aac", "flac", "wav", "pcm"] = Field(
        default="mp3",
        description="Audio format. Match the filename extension if you set it explicitly.",
    )
    speed: float = Field(default=1.0, description="OpenAI: 0.25–4.0 speed multiplier. Other providers may ignore.")

    rate_limit_delay: float = Field(default=0.0)
    max_retries: int = Field(default=3)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        if not os.environ.get(self.api_key_env_var):
            # Resolve at run time — but warn early if obviously missing.
            pass

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        model = self.model
        api_key_env_var = self.api_key_env_var
        text_column = self.text_column
        output_dir = self.output_dir
        filename_tpl = self.output_filename_template
        path_col = self.output_path_column
        voice = self.voice
        response_format = self.response_format
        speed = self.speed
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries

        @asset(
            name=asset_name,
            description=self.description or f"LiteLLM TTS via {model}.",
            group_name=self.group_name,
            kinds={"litellm", "text-to-speech", "ai"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                import time
                import litellm
            except ImportError:
                raise ImportError("pip install litellm")

            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                raise ValueError(f"env var {api_key_env_var!r} is empty — required for {model}.")

            if text_column not in upstream.columns:
                raise ValueError(f"text_column={text_column!r} not in upstream: {list(upstream.columns)}")

            os.makedirs(output_dir, exist_ok=True)

            df = upstream.copy().reset_index(drop=True)
            paths: List[Optional[str]] = []
            errors: List[Optional[str]] = []

            for i, row in df.iterrows():
                text_val = row[text_column]
                if not isinstance(text_val, str) or not text_val.strip():
                    paths.append(None); errors.append("empty text")
                    continue

                row_dict = {c: row[c] for c in df.columns}
                row_dict["row_index"] = i
                try:
                    fname = filename_tpl.format(**row_dict)
                except (KeyError, IndexError):
                    fname = f"{i}.{response_format}"
                if "." not in fname:
                    fname += f".{response_format}"
                out_path = os.path.join(output_dir, fname)

                kwargs: Dict[str, Any] = {
                    "model":           model,
                    "input":           text_val,
                    "api_key":         api_key,
                    "response_format": response_format,
                }
                if voice:
                    kwargs["voice"] = voice
                if speed != 1.0:
                    kwargs["speed"] = speed

                attempt = 0
                last_err = None
                resp = None
                while attempt <= max_retries:
                    try:
                        resp = litellm.speech(**kwargs)
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        attempt += 1
                        if attempt > max_retries:
                            break
                        time.sleep((2 ** attempt) * 0.5)

                if last_err is not None or resp is None:
                    paths.append(None); errors.append(str(last_err))
                else:
                    # LiteLLM returns a response whose `.read()` (or `content`) yields bytes.
                    audio_bytes = getattr(resp, "content", None)
                    if audio_bytes is None and hasattr(resp, "read"):
                        audio_bytes = resp.read()
                    if audio_bytes is None:
                        paths.append(None); errors.append("no audio bytes returned")
                    else:
                        with open(out_path, "wb") as fh:
                            fh.write(audio_bytes)
                        paths.append(out_path); errors.append(None)

                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            df[path_col] = paths
            df["tts_error"] = errors

            ok = int(sum(1 for p in paths if p))
            return Output(
                value=df,
                metadata={
                    "rows":         MetadataValue.int(len(df)),
                    "audio_files":  MetadataValue.int(ok),
                    "failed":       MetadataValue.int(len(df) - ok),
                    "model":        MetadataValue.text(model),
                    "voice":        MetadataValue.text(voice or "(provider default)"),
                    "format":       MetadataValue.text(response_format),
                    "output_dir":   MetadataValue.path(output_dir),
                },
            )

        return Definitions(assets=[_asset])
