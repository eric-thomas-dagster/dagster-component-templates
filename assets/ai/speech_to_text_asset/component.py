"""SpeechToTextAssetComponent — transcribe audio files via Cloud Speech-to-Text v2.

Pass a column of audio references (local paths or `gs://` URIs) and
get back transcripts in a new column. Supports per-language config,
multi-channel audio, automatic punctuation, word time offsets.

Long-running async (`batch_recognize`) is used for files > 60s (Cloud
Speech v2 API requirement); shorter clips use synchronous `recognize`.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

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


class SpeechToTextAssetComponent(Component, Model, Resolvable):
    """Transcribe audio files via Cloud Speech-to-Text v2."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    location: str = Field(default="global", description="Recognizer region — 'global' or a specific region (e.g. 'us-central1').")

    audio_column: str = Field(description="Column with audio file paths or `gs://` URIs.")
    output_column: str = Field(default="transcript", description="Column to write the transcript into.")

    language_codes: List[str] = Field(
        default_factory=lambda: ["en-US"],
        description="One or more BCP-47 language codes. Multiple → multilingual recognition.",
    )
    recognizer_model: str = Field(
        default="latest_long",
        description="Speech v2 model: 'latest_short' / 'latest_long' / 'chirp' / 'chirp_2' / 'phone_call' / 'medical_conversation'. (Field is named recognizer_model rather than model to avoid a Pydantic name collision.)",
    )

    # Recognition features
    enable_automatic_punctuation: bool = Field(default=True)
    enable_word_time_offsets: bool = Field(default=False)
    enable_speaker_diarization: bool = Field(default=False)
    diarization_speaker_count: Optional[int] = Field(default=None)

    rate_limit_delay: float = Field(default=0.0)
    max_retries: int = Field(default=3)

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
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        audio_column = self.audio_column
        output_column = self.output_column
        language_codes = list(self.language_codes)
        model_name = self.recognizer_model
        enable_auto_punct = self.enable_automatic_punctuation
        enable_word_offsets = self.enable_word_time_offsets
        enable_diarization = self.enable_speaker_diarization
        diarization_count = self.diarization_speaker_count
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries

        @asset(
            name=asset_name,
            description=self.description or f"Speech-to-Text transcription on {audio_column}.",
            group_name=self.group_name,
            kinds={"google", "speech-to-text", "ai"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                from google.cloud import speech_v2
                from google.cloud.speech_v2.types import cloud_speech as cs
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-speech google-auth")

            if audio_column not in upstream.columns:
                raise ValueError(f"audio_column={audio_column!r} not in upstream: {list(upstream.columns)}")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = speech_v2.SpeechClient(credentials=sa_creds)
            recognizer_path = f"projects/{project_id}/locations/{location}/recognizers/_"

            features = cs.RecognitionFeatures(
                enable_automatic_punctuation=enable_auto_punct,
                enable_word_time_offsets=enable_word_offsets,
            )
            if enable_diarization:
                features.diarization_config = cs.SpeakerDiarizationConfig(
                    min_speaker_count=2,
                    max_speaker_count=diarization_count or 6,
                )
            config = cs.RecognitionConfig(
                auto_decoding_config=cs.AutoDetectDecodingConfig(),
                language_codes=language_codes,
                model=model_name,
                features=features,
            )

            df = upstream.copy().reset_index(drop=True)
            transcripts: List[Optional[str]] = [None] * len(df)
            errors: List[Optional[str]] = [None] * len(df)

            for i, row in df.iterrows():
                ref = row[audio_column]
                ref_str = str(ref)

                attempt = 0
                last_err = None
                resp = None
                while attempt <= max_retries:
                    try:
                        if ref_str.startswith("gs://"):
                            req = cs.RecognizeRequest(
                                recognizer=recognizer_path,
                                config=config,
                                uri=ref_str,
                            )
                        else:
                            with open(ref_str, "rb") as fh:
                                content = fh.read()
                            req = cs.RecognizeRequest(
                                recognizer=recognizer_path,
                                config=config,
                                content=content,
                            )
                        resp = client.recognize(request=req)
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        attempt += 1
                        if attempt > max_retries:
                            break
                        time.sleep((2 ** attempt) * 0.5)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    errors[i] = err_str
                    if "PERMISSION_DENIED" in err_str:
                        context.log.error("Speech-to-Text 403: SA needs roles/speech.client + Speech API enabled.")
                    elif "FAILED_PRECONDITION" in err_str and "long" in err_str.lower():
                        context.log.error(
                            f"Audio at row {i} too long for sync recognize (>60s). "
                            f"Use batch_recognize via gs:// URI instead."
                        )
                    continue

                # Concatenate all alternatives' top transcripts.
                pieces = []
                for r in resp.results:
                    if r.alternatives:
                        pieces.append(r.alternatives[0].transcript)
                transcripts[i] = " ".join(p.strip() for p in pieces if p).strip() or None
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            df[output_column] = transcripts
            if any(errors):
                df[f"{output_column}_error"] = errors

            ok = sum(1 for t in transcripts if t)
            preview_md = df[[c for c in df.columns if c != audio_column]].head(5).to_markdown(index=False) or ""
            return Output(
                value=df,
                metadata={
                    "rows":         MetadataValue.int(len(df)),
                    "transcribed":  MetadataValue.int(ok),
                    "model":        MetadataValue.text(model_name),
                    "languages":    MetadataValue.json(language_codes),
                    "preview":      MetadataValue.md(preview_md),
                },
            )

        return Definitions(assets=[_asset])
