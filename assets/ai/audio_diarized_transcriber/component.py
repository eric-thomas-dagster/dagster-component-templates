"""AudioDiarizedTranscriberComponent -- transcribe audio AND label who's
speaking when.

Every existing transcription component in this catalog (audio_transcriber,
litellm_audio_transcription, speech_to_text_asset) returns one transcript
string per file -- fine for single-speaker audio, useless for a call,
meeting, or podcast where you need to know WHO said WHAT WHEN. This
component fills that gap: per audio file, it returns a list of
`{speaker, start_seconds, end_seconds, text}` segments (speaker_1,
speaker_2, ...) instead of one flat string.

Two backends, since there's no one right answer for "local library vs.
cloud API" here:

- `google_cloud_speech` (default): Cloud Speech-to-Text v2's native
  SpeakerDiarizationConfig -- one API call per file does both ASR and
  diarization together. No heavy local install, per-request cost, needs
  a GCP service account. This backend is exercised end-to-end in this
  repo's test suite (real generated audio, mocked network call).
- `pyannote`: local pyannote.audio for diarization (who spoke when) +
  local Whisper for transcription (what they said), run as two separate
  passes since pyannote does NOT transcribe -- it only segments speakers.
  Free after setup, but pyannote's actual pretrained pipeline
  (pyannote/speaker-diarization-3.1) is a GATED HuggingFace model: you
  need your own HF token and a one-time terms acceptance on
  huggingface.co before it will download. This backend is implemented
  and internally consistent but has NOT been run end-to-end against a
  real model download in this repo's environment -- see README.

Requires `ffmpeg` in PATH for the pyannote backend (used to cut each
detected speaker turn into its own audio slice before transcribing it).
"""

import os
import tempfile
import time
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
from pydantic import Field


class AudioDiarizedTranscriberComponent(Component, Model, Resolvable):
    """Transcribe audio files AND label who's speaking when.

    Each row in the upstream DataFrame is treated as one audio file.
    Output is a list of speaker-attributed segments per row, not a flat
    string -- pair with a downstream `array_exploder`-style step if you
    want one row per segment instead of one row per file.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(
        description=(
            "Upstream asset key providing a DataFrame of audio file paths -- "
            "e.g. video_audio_extract_asset's output, or synthetic_audio_generator "
            "for a quick test fixture."
        )
    )
    audio_path_column: Union[str, int] = Field(
        default="audio_path",
        description="Column containing local audio file paths. Defaults to 'audio_path' to pair directly with video_audio_extract_asset's output_path_column.",
    )
    output_column: str = Field(
        default="segments",
        description=(
            "Column to write speaker-attributed segments into: a list of "
            "{speaker, start_seconds, end_seconds, text} dicts per row, one per "
            "contiguous speaker turn, ordered by start_seconds. Speaker labels are "
            "'speaker_1', 'speaker_2', ... (relative numbering per file, not a "
            "stable identity across files)."
        ),
    )
    transcript_column: Optional[str] = Field(
        default=None,
        description=(
            "Column to write the full plain-text transcript into (all segments' "
            "text joined in order, speaker labels omitted) -- for callers who just "
            "want the words, not who said them. Defaults to '<output_column>_transcript'."
        ),
    )

    diarization_backend: str = Field(
        default="google_cloud_speech",
        description=(
            "'google_cloud_speech' (default): Cloud Speech-to-Text v2's native "
            "diarization, one cloud API call per file, no heavy local install. "
            "'pyannote': local pyannote.audio (diarization) + local Whisper "
            "(transcription) -- free after setup, but pyannote's pretrained "
            "pipeline needs your own HuggingFace token (gated model). See README "
            "before choosing this backend."
        ),
    )

    # --- google_cloud_speech backend -------------------------------------
    credentials: Optional[Dict[str, Any]] = Field(
        default=None,
        description="google_cloud_speech backend: service account JSON as a dict. Falls back to credentials_path, then GOOGLE_APPLICATION_CREDENTIALS.",
    )
    credentials_path: Optional[str] = Field(
        default=None,
        description="google_cloud_speech backend: path to a service-account JSON file.",
    )
    project_id: Optional[str] = Field(
        default=None,
        description="google_cloud_speech backend: GCP project ID. Defaults to the one embedded in the credentials.",
    )
    location: str = Field(
        default="global",
        description="google_cloud_speech backend: recognizer region -- 'global' or a specific region (e.g. 'us-central1').",
    )
    language_codes: List[str] = Field(
        default_factory=lambda: ["en-US"],
        description="google_cloud_speech backend: one or more BCP-47 language codes.",
    )
    recognizer_model: str = Field(
        default="latest_long",
        description="google_cloud_speech backend: Speech v2 model -- 'latest_short' / 'latest_long' / 'chirp' / 'chirp_2' / 'phone_call' / 'medical_conversation'.",
    )
    min_speaker_count: int = Field(default=2, description="google_cloud_speech backend: minimum expected speakers.")
    max_speaker_count: int = Field(default=6, description="google_cloud_speech backend: maximum expected speakers.")
    max_retries: int = Field(default=3, description="google_cloud_speech backend: retries per file on transient API errors.")
    rate_limit_delay: float = Field(default=0.0, description="google_cloud_speech backend: seconds to sleep between files.")

    # --- pyannote backend --------------------------------------------------
    pyannote_pipeline: str = Field(
        default="pyannote/speaker-diarization-3.1",
        description="pyannote backend: HuggingFace pipeline id. This is a gated model -- accept its terms on huggingface.co with the account that owns hf_token_env_var before first use.",
    )
    hf_token_env_var: str = Field(
        default="HF_TOKEN",
        description="pyannote backend: env var holding a HuggingFace access token with access to pyannote_pipeline.",
    )
    whisper_model_size: str = Field(
        default="base",
        description="pyannote backend: local Whisper model size used to transcribe each detected speaker turn -- tiny/base/small/medium/large.",
    )
    whisper_language: Optional[str] = Field(
        default=None,
        description="pyannote backend: hint language for Whisper (ISO 639-1, e.g. 'en') to skip auto-detection per segment.",
    )
    min_speakers: Optional[int] = Field(default=None, description="pyannote backend: minimum expected speakers (omit to let pyannote infer).")
    max_speakers: Optional[int] = Field(default=None, description="pyannote backend: maximum expected speakers (omit to let pyannote infer).")
    max_segments_per_file: int = Field(
        default=200,
        ge=1,
        description="pyannote backend: safety cap on speaker turns transcribed per file -- guards against a noisy/misdetected file producing thousands of tiny turns and burning huge Whisper runtime.",
    )
    min_segment_seconds: float = Field(
        default=0.3,
        description="pyannote backend: speaker turns shorter than this are dropped before transcription -- pyannote frequently emits sub-100ms micro-turns at speaker-change boundaries that are too short for Whisper to transcribe meaningfully.",
    )
    ffmpeg_binary: str = Field(default="ffmpeg", description="pyannote backend: ffmpeg binary used to cut each speaker turn into its own audio slice.")

    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — list of team names or email addresses.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.")

    include_preview_metadata: bool = Field(default=False, description="Include a preview of the output data in metadata.")
    preview_rows: int = Field(default=25, ge=1, le=500, description="Rows to include in the preview metadata when include_preview_metadata is True.")

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(default=None, description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        backend = self.diarization_backend
        if backend not in ("google_cloud_speech", "pyannote"):
            raise ValueError(
                f"AudioDiarizedTranscriberComponent: diarization_backend must be "
                f"'google_cloud_speech' or 'pyannote', got {backend!r}."
            )

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
        output_column = self.output_column
        transcript_column = self.transcript_column or f"{output_column}_transcript"
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        # google_cloud_speech backend config
        creds_dict = self.credentials
        project_id_field = self.project_id
        location = self.location
        language_codes = list(self.language_codes)
        recognizer_model = self.recognizer_model
        min_speaker_count = self.min_speaker_count
        max_speaker_count = self.max_speaker_count
        max_retries = self.max_retries
        rate_limit_delay = self.rate_limit_delay
        credentials_path = self.credentials_path

        # pyannote backend config
        pyannote_pipeline_id = self.pyannote_pipeline
        hf_token_env_var = self.hf_token_env_var
        whisper_model_size = self.whisper_model_size
        whisper_language = self.whisper_language
        min_speakers = self.min_speakers
        max_speakers = self.max_speakers
        max_segments_per_file = self.max_segments_per_file
        min_segment_seconds = self.min_segment_seconds
        ffmpeg_binary = self.ffmpeg_binary

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Speaker-diarized transcription ({backend}).",
            group_name=self.group_name,
            kinds={"google" if backend == "google_cloud_speech" else "pyannote", "speech-to-text", "ai"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: Any):
            # Defensive Output/MaterializeResult unwrap -- tolerates upstream
            # authors who annotate `-> Output` or return `Output(value=df, ...)`.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned asset
            # consumes a partitioned upstream, Dagster's IO manager loads
            # ALL partitions as a dict; concat before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            if audio_path_column not in upstream.columns:
                raise ValueError(
                    f"audio_path_column={audio_path_column!r} not in upstream: {list(upstream.columns)}"
                )

            df = upstream.copy().reset_index(drop=True)
            segments_rows: List[Optional[List[Dict[str, Any]]]] = [None] * len(df)
            errors: List[Optional[str]] = [None] * len(df)
            total_segments = 0
            diarized_rows = 0

            if backend == "google_cloud_speech":
                _run_google_cloud_speech_backend(
                    context, df, audio_path_column, segments_rows, errors,
                    creds_dict=creds_dict, credentials_path=credentials_path,
                    project_id_field=project_id_field, location=location,
                    language_codes=language_codes, recognizer_model=recognizer_model,
                    min_speaker_count=min_speaker_count, max_speaker_count=max_speaker_count,
                    max_retries=max_retries, rate_limit_delay=rate_limit_delay,
                )
            else:
                _run_pyannote_backend(
                    context, df, audio_path_column, segments_rows, errors,
                    pyannote_pipeline_id=pyannote_pipeline_id, hf_token_env_var=hf_token_env_var,
                    whisper_model_size=whisper_model_size, whisper_language=whisper_language,
                    min_speakers=min_speakers, max_speakers=max_speakers,
                    max_segments_per_file=max_segments_per_file,
                    min_segment_seconds=min_segment_seconds, ffmpeg_binary=ffmpeg_binary,
                )

            transcripts: List[Optional[str]] = []
            for segs in segments_rows:
                if segs:
                    total_segments += len(segs)
                    diarized_rows += 1
                    transcripts.append(" ".join(s["text"] for s in segs if s.get("text")).strip() or None)
                else:
                    transcripts.append(None)

            df[output_column] = segments_rows
            df[transcript_column] = transcripts
            if any(errors):
                df[f"{output_column}_error"] = errors

            metadata = {
                "rows": MetadataValue.int(len(df)),
                "diarized_rows": MetadataValue.int(diarized_rows),
                "total_segments": MetadataValue.int(total_segments),
                "backend": MetadataValue.text(backend),
            }
            if include_preview and len(df) > 0:
                try:
                    _prev_cols = [c for c in df.columns if c != audio_path_column]
                    _prev = df[_prev_cols]
                    _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False) or "")
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[_asset])


def _run_google_cloud_speech_backend(
    context, df, audio_path_column, segments_rows, errors,
    *, creds_dict, credentials_path, project_id_field, location,
    language_codes, recognizer_model, min_speaker_count, max_speaker_count,
    max_retries, rate_limit_delay,
):
    """Cloud Speech-to-Text v2 with native SpeakerDiarizationConfig -- one
    request per file does ASR + diarization together. Same word-grouping
    approach as speech_to_text_asset's diarization fix (duplicated, not
    imported, per this repo's standalone-component convention)."""
    try:
        from google.cloud import speech_v2
        from google.cloud.speech_v2.types import cloud_speech as cs
        from google.oauth2 import service_account
    except ImportError:
        raise ImportError("pip install google-cloud-speech google-auth")

    resolved_creds = creds_dict
    if resolved_creds is None:
        cred_path = credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path:
            import json
            with open(cred_path, "r") as fh:
                resolved_creds = json.load(fh)
    if resolved_creds is None:
        raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

    project_id = project_id_field or resolved_creds.get("project_id")
    sa_creds = service_account.Credentials.from_service_account_info(resolved_creds)
    client = speech_v2.SpeechClient(credentials=sa_creds)
    recognizer_path = f"projects/{project_id}/locations/{location}/recognizers/_"

    features = cs.RecognitionFeatures(
        enable_automatic_punctuation=True,
        enable_word_time_offsets=True,
    )
    features.diarization_config = cs.SpeakerDiarizationConfig(
        min_speaker_count=min_speaker_count,
        max_speaker_count=max_speaker_count,
    )
    config = cs.RecognitionConfig(
        auto_decoding_config=cs.AutoDetectDecodingConfig(),
        language_codes=language_codes,
        model=recognizer_model,
        features=features,
    )

    for i, row in df.iterrows():
        ref_str = str(row[audio_path_column])
        attempt = 0
        last_err = None
        resp = None
        while attempt <= max_retries:
            try:
                if ref_str.startswith("gs://"):
                    req = cs.RecognizeRequest(recognizer=recognizer_path, config=config, uri=ref_str)
                else:
                    with open(ref_str, "rb") as fh:
                        content = fh.read()
                    req = cs.RecognizeRequest(recognizer=recognizer_path, config=config, content=content)
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
            errors[i] = str(last_err) if last_err else "no response"
            continue

        all_words: List[Any] = []
        for r in resp.results:
            if r.alternatives:
                all_words.extend(r.alternatives[0].words)

        if not all_words:
            continue

        segs: List[Dict[str, Any]] = []
        current_speaker = None
        current_words: List[str] = []
        current_start = None
        current_end = None
        for w in all_words:
            label = w.speaker_label or "1"
            if label != current_speaker:
                if current_speaker is not None:
                    segs.append({
                        "speaker": f"speaker_{current_speaker}",
                        "start_seconds": current_start,
                        "end_seconds": current_end,
                        "text": " ".join(current_words).strip(),
                    })
                current_speaker = label
                current_words = []
                current_start = w.start_offset.total_seconds()
            current_words.append(w.word)
            current_end = w.end_offset.total_seconds()
        if current_speaker is not None:
            segs.append({
                "speaker": f"speaker_{current_speaker}",
                "start_seconds": current_start,
                "end_seconds": current_end,
                "text": " ".join(current_words).strip(),
            })
        segments_rows[i] = segs or None
        if rate_limit_delay > 0:
            time.sleep(rate_limit_delay)


def _run_pyannote_backend(
    context, df, audio_path_column, segments_rows, errors,
    *, pyannote_pipeline_id, hf_token_env_var, whisper_model_size, whisper_language,
    min_speakers, max_speakers, max_segments_per_file, min_segment_seconds, ffmpeg_binary,
):
    """Local pyannote.audio (diarization: who spoke when) + local Whisper
    (transcription: what they said) -- pyannote alone only produces speaker
    turns, not text, so this always needs a second ASR pass per turn."""
    import shutil
    import subprocess

    try:
        from pyannote.audio import Pipeline
    except ImportError:
        raise ImportError("pip install pyannote.audio (also requires torch)")
    try:
        import whisper
    except ImportError:
        raise ImportError("pip install openai-whisper")

    if shutil.which(ffmpeg_binary) is None:
        raise RuntimeError(f"ffmpeg binary {ffmpeg_binary!r} not in PATH -- required to cut speaker turns for transcription.")

    hf_token = os.environ.get(hf_token_env_var)
    if not hf_token:
        raise ValueError(
            f"pyannote backend requires env var {hf_token_env_var!r} to hold a HuggingFace "
            f"access token with access to {pyannote_pipeline_id!r} (a gated model -- accept "
            f"its terms on huggingface.co with that token's account first)."
        )

    context.log.info(f"Loading pyannote pipeline: {pyannote_pipeline_id}")
    diarization_pipeline = Pipeline.from_pretrained(pyannote_pipeline_id, use_auth_token=hf_token)
    context.log.info(f"Loading Whisper model: {whisper_model_size}")
    whisper_model = whisper.load_model(whisper_model_size)

    diar_kwargs: Dict[str, Any] = {}
    if min_speakers is not None:
        diar_kwargs["min_speakers"] = min_speakers
    if max_speakers is not None:
        diar_kwargs["max_speakers"] = max_speakers

    for i, row in df.iterrows():
        audio_path = str(row[audio_path_column])
        if not os.path.isfile(audio_path):
            errors[i] = f"file not found: {audio_path!r}"
            continue
        try:
            diarization = diarization_pipeline(audio_path, **diar_kwargs)
        except Exception as e:
            errors[i] = f"diarization failed: {e}"
            continue

        # pyannote's own labels look like 'SPEAKER_00', 'SPEAKER_01', ... --
        # remap to this component's speaker_1/speaker_2/... convention (same
        # scheme as the google_cloud_speech backend) in first-seen order,
        # rather than exposing pyannote's raw internal label spelling.
        _label_map: Dict[str, str] = {}

        def _speaker_label(raw: str) -> str:
            if raw not in _label_map:
                _label_map[raw] = f"speaker_{len(_label_map) + 1}"
            return _label_map[raw]

        turns = [
            (turn.start, turn.end, _speaker_label(speaker))
            for turn, _, speaker in diarization.itertracks(yield_label=True)
            if (turn.end - turn.start) >= min_segment_seconds
        ]
        if len(turns) > max_segments_per_file:
            context.log.warning(
                f"{audio_path}: {len(turns)} speaker turns exceeds max_segments_per_file="
                f"{max_segments_per_file}, truncating (increase the cap if this file "
                f"genuinely has that many turns)."
            )
            turns = turns[:max_segments_per_file]

        segs: List[Dict[str, Any]] = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            for turn_idx, (start, end, speaker) in enumerate(turns):
                seg_path = os.path.join(tmp_dir, f"turn_{turn_idx}.wav")
                cmd = [
                    ffmpeg_binary, "-y", "-i", audio_path,
                    "-ss", str(start), "-to", str(end),
                    "-ar", "16000", "-ac", "1",
                    seg_path,
                ]
                try:
                    subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=60)
                except subprocess.CalledProcessError as e:
                    context.log.warning(f"{audio_path} turn {turn_idx}: ffmpeg cut failed: {e}")
                    continue

                try:
                    kwargs: Dict[str, Any] = {}
                    if whisper_language:
                        kwargs["language"] = whisper_language
                    result = whisper_model.transcribe(seg_path, **kwargs)
                    text = (result.get("text") or "").strip()
                except Exception as e:
                    context.log.warning(f"{audio_path} turn {turn_idx}: transcription failed: {e}")
                    text = ""

                if text:
                    segs.append({
                        "speaker": speaker,
                        "start_seconds": start,
                        "end_seconds": end,
                        "text": text,
                    })

        segments_rows[i] = segs or None
