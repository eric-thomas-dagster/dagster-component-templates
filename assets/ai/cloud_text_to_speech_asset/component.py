"""CloudTextToSpeechAssetComponent — Cloud Text-to-Speech (WaveNet / Neural2 / Chirp).

Generates audio files (MP3/WAV/OGG) from a text column. One file per row.
Adds two columns to the output DataFrame:
  - `<output_path_column>`: filesystem path to the written audio file
  - `<output_bytes_column>` (optional): raw audio bytes (for in-memory downstream)

Voice options:
  - **Standard voices**: cheapest, basic quality. ~$4/1M chars.
  - **WaveNet voices**: $16/1M chars. Recognizably better than standard.
  - **Neural2 voices**: $16/1M chars. The recommended default in most languages.
  - **Studio voices**: $160/1M chars, English-only, professional-narrator quality.
  - **Chirp 3 voices** (preview): newest, instruction-following.

The complement of `speech_to_text_asset` — together they form a full
speech-to-speech translation pipeline:

  audio    → speech_to_text_asset   → transcript (en)
           → translation_api_asset  → transcript (es)
           → cloud_text_to_speech_asset → audio (es)
"""

import json
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


class CloudTextToSpeechAssetComponent(Component, Model, Resolvable):
    """Synthesize speech audio from a text column via Cloud Text-to-Speech."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(
        default=None,
        description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.",
    )

    text_column: str = Field(description="Column containing text to speak.")
    output_dir: str = Field(
        default="/tmp/tts_audio",
        description="Filesystem dir to write audio files into (created if missing).",
    )
    output_filename_template: str = Field(
        default="{row_index}.mp3",
        description=(
            "Filename template (without dir). Supports row column substitutions like "
            "`{<column>}_{row_index}.mp3`. Default appends row_index → `0.mp3`, `1.mp3`."
        ),
    )
    output_path_column: str = Field(
        default="audio_path",
        description="New column to write the file path into.",
    )

    language_code: str = Field(
        default="en-US",
        description="BCP-47 language code (e.g. en-US, es-ES, ja-JP, fr-FR). Determines available voices.",
    )
    voice_name: Optional[str] = Field(
        default=None,
        description=(
            "Specific voice id (e.g. `en-US-Neural2-D`, `es-ES-Neural2-A`). "
            "Browse at https://cloud.google.com/text-to-speech/docs/voices. "
            "If omitted, Google picks a default for the language."
        ),
    )
    audio_encoding: Literal["MP3", "LINEAR16", "OGG_OPUS", "MULAW", "ALAW"] = Field(
        default="MP3",
        description="Output audio encoding. MP3 is the standard; LINEAR16 for uncompressed WAV.",
    )
    speaking_rate: float = Field(
        default=1.0,
        description="Speed multiplier (0.25–4.0). 1.0 = normal.",
    )
    pitch: float = Field(
        default=0.0,
        description="Pitch in semitones (-20.0 to 20.0). 0.0 = baseline.",
    )

    rate_limit_delay: float = Field(default=0.0, description="Sleep between rows.")
    max_retries: int = Field(default=3)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        text_column = self.text_column
        output_dir = self.output_dir
        filename_tpl = self.output_filename_template
        path_col = self.output_path_column
        language_code = self.language_code
        voice_name = self.voice_name
        audio_encoding = self.audio_encoding
        speaking_rate = self.speaking_rate
        pitch = self.pitch
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries

        _ext_map = {"MP3": ".mp3", "LINEAR16": ".wav", "OGG_OPUS": ".ogg", "MULAW": ".mulaw", "ALAW": ".alaw"}

        @asset(
            name=asset_name,
            description=self.description or f"Cloud TTS ({language_code}, {voice_name or 'default voice'}).",
            group_name=self.group_name,
            kinds={"google", "text-to-speech", "ai"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                import time
                from google.cloud import texttospeech_v1
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-texttospeech google-auth")

            if text_column not in upstream.columns:
                raise ValueError(f"text_column={text_column!r} not in upstream: {list(upstream.columns)}")

            os.makedirs(output_dir, exist_ok=True)
            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = texttospeech_v1.TextToSpeechClient(credentials=sa_creds)

            voice = texttospeech_v1.VoiceSelectionParams(
                language_code=language_code,
                name=voice_name or "",
            )
            audio_config = texttospeech_v1.AudioConfig(
                audio_encoding=getattr(texttospeech_v1.AudioEncoding, audio_encoding),
                speaking_rate=speaking_rate,
                pitch=pitch,
            )
            ext = _ext_map.get(audio_encoding, ".bin")

            df = upstream.copy().reset_index(drop=True)
            paths: List[Optional[str]] = []
            errors: List[Optional[str]] = []

            for i, row in df.iterrows():
                text_val = row[text_column]
                if not isinstance(text_val, str) or not text_val.strip():
                    paths.append(None); errors.append("empty text")
                    continue

                # Build filename from template — fall back to row_index if template references unknown keys
                row_dict = {c: row[c] for c in df.columns}
                row_dict["row_index"] = i
                try:
                    fname = filename_tpl.format(**row_dict)
                except (KeyError, IndexError):
                    fname = f"{i}{ext}"
                if "." not in fname:
                    fname += ext
                out_path = os.path.join(output_dir, fname)

                attempt = 0
                last_err = None
                resp = None
                synth_input = texttospeech_v1.SynthesisInput(text=text_val)
                while attempt <= max_retries:
                    try:
                        resp = client.synthesize_speech(
                            input=synth_input, voice=voice, audio_config=audio_config,
                        )
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
                    with open(out_path, "wb") as fh:
                        fh.write(resp.audio_content)
                    paths.append(out_path); errors.append(None)

                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            df[path_col] = paths
            df["tts_error"] = errors

            ok = int(sum(1 for p in paths if p))
            return Output(
                value=df,
                metadata={
                    "rows":           MetadataValue.int(len(df)),
                    "audio_files":    MetadataValue.int(ok),
                    "failed":         MetadataValue.int(len(df) - ok),
                    "voice":          MetadataValue.text(voice_name or f"{language_code} default"),
                    "encoding":       MetadataValue.text(audio_encoding),
                    "output_dir":     MetadataValue.path(output_dir),
                },
            )

        return Definitions(assets=[_asset])
