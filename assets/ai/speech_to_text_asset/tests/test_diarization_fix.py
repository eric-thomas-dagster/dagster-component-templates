"""Committed regression tests for SpeechToTextAssetComponent's speaker
diarization / word-offsets fix.

Before this fix, enable_speaker_diarization and enable_word_time_offsets
were both set on the Cloud Speech v2 request's RecognitionFeatures, but the
response-parsing code only ever read r.alternatives[0].transcript -- it
never touched r.alternatives[0].words, where Speech v2 actually returns
per-word timing and speaker_label. Both fields were pure no-ops: the API
call requested the data, the code silently threw it away.

These tests exercise the fix against the REAL google-cloud-speech
proto-plus types (WordInfo, SpeechRecognitionResult, ...) -- only the
network call itself (SpeechClient.recognize) is mocked.
"""
import datetime
import os
import sys
import types

import dagster as dg
import pandas as pd
import pytest
from google.cloud.speech_v2.types import cloud_speech as cs

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _word(w, start, end, speaker=None):
    kwargs = dict(
        word=w,
        start_offset=datetime.timedelta(seconds=start),
        end_offset=datetime.timedelta(seconds=end),
    )
    if speaker is not None:
        kwargs["speaker_label"] = speaker
    return cs.WordInfo(**kwargs)


@pytest.fixture()
def fake_speech_client(monkeypatch, tmp_path):
    """Patches google.cloud.speech_v2.SpeechClient to return a canned
    two-speaker response, and stubs credential loading so no real GCP
    service account is needed."""
    words = [
        _word("hello", 0.0, 0.5, "1"),
        _word("there", 0.5, 1.0, "1"),
        _word("hi", 1.0, 1.3, "2"),
        _word("friend", 1.3, 1.7, "2"),
    ]
    fake_result = cs.SpeechRecognitionResult(
        alternatives=[cs.SpeechRecognitionAlternative(transcript="hello there hi friend", words=words)]
    )
    fake_response = types.SimpleNamespace(results=[fake_result])
    recognize_calls = []

    class FakeSpeechClient:
        def __init__(self, credentials=None):
            pass

        def recognize(self, request=None):
            recognize_calls.append(request)
            return fake_response

    fake_speech_v2 = types.ModuleType("google.cloud.speech_v2")
    fake_speech_v2.SpeechClient = FakeSpeechClient
    monkeypatch.setitem(sys.modules, "google.cloud.speech_v2", fake_speech_v2)
    import google.cloud as _gc
    monkeypatch.setattr(_gc, "speech_v2", fake_speech_v2, raising=False)

    import google.oauth2.service_account as sa_mod
    monkeypatch.setattr(
        sa_mod.Credentials, "from_service_account_info", staticmethod(lambda info: object())
    )

    audio_path = tmp_path / "call.wav"
    audio_path.write_bytes(b"fake audio bytes")
    return recognize_calls, str(audio_path)


def _materialize(mod, component, audio_path):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    upstream_df = pd.DataFrame({"path": [audio_path]})

    @dg.asset(name="audio_files")
    def audio_files():
        return upstream_df

    return dg.materialize([asset_def, audio_files])


def test_diarization_groups_words_into_speaker_segments(mod, fake_speech_client):
    _, audio_path = fake_speech_client
    component = mod.SpeechToTextAssetComponent(
        asset_name="t",
        upstream_asset_key="audio_files",
        credentials={"project_id": "fake-project"},
        audio_column="path",
        enable_speaker_diarization=True,
        diarization_speaker_count=2,
    )
    result = _materialize(mod, component, audio_path)
    assert result.success

    df = result.output_for_node("t")
    segments = df.loc[0, "transcript_speakers"]
    assert segments is not None and len(segments) == 2
    assert segments[0] == {"speaker": "speaker_1", "start_seconds": 0.0, "end_seconds": 1.0, "text": "hello there"}
    assert segments[1] == {"speaker": "speaker_2", "start_seconds": 1.0, "end_seconds": 1.7, "text": "hi friend"}


def test_word_offsets_written_when_enabled(mod, fake_speech_client):
    _, audio_path = fake_speech_client
    component = mod.SpeechToTextAssetComponent(
        asset_name="t",
        upstream_asset_key="audio_files",
        credentials={"project_id": "fake-project"},
        audio_column="path",
        enable_word_time_offsets=True,
    )
    result = _materialize(mod, component, audio_path)
    assert result.success

    df = result.output_for_node("t")
    words = df.loc[0, "transcript_words"]
    assert words == [
        {"word": "hello", "start_seconds": 0.0, "end_seconds": 0.5},
        {"word": "there", "start_seconds": 0.5, "end_seconds": 1.0},
        {"word": "hi", "start_seconds": 1.0, "end_seconds": 1.3},
        {"word": "friend", "start_seconds": 1.3, "end_seconds": 1.7},
    ]


def test_diarization_forces_word_time_offsets_on_in_the_request(mod, fake_speech_client):
    """Diarization's per-word speaker_label only populates when word-level
    output is requested at all -- enable_speaker_diarization alone must
    still turn on enable_word_time_offsets in the actual request, otherwise
    `words` comes back empty and diarization silently produces nothing
    again, just for a different reason than the original bug."""
    recognize_calls, audio_path = fake_speech_client
    component = mod.SpeechToTextAssetComponent(
        asset_name="t",
        upstream_asset_key="audio_files",
        credentials={"project_id": "fake-project"},
        audio_column="path",
        enable_speaker_diarization=True,
        enable_word_time_offsets=False,
    )
    result = _materialize(mod, component, audio_path)
    assert result.success
    assert recognize_calls[0].config.features.enable_word_time_offsets is True


def test_diarized_rows_metadata_reports_correct_count(mod, fake_speech_client):
    _, audio_path = fake_speech_client
    component = mod.SpeechToTextAssetComponent(
        asset_name="t",
        upstream_asset_key="audio_files",
        credentials={"project_id": "fake-project"},
        audio_column="path",
        enable_speaker_diarization=True,
    )
    result = _materialize(mod, component, audio_path)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["diarized_rows"].value == 1


def test_custom_output_column_names_respected(mod, fake_speech_client):
    _, audio_path = fake_speech_client
    component = mod.SpeechToTextAssetComponent(
        asset_name="t",
        upstream_asset_key="audio_files",
        credentials={"project_id": "fake-project"},
        audio_column="path",
        enable_speaker_diarization=True,
        diarization_output_column="who_said_what",
    )
    result = _materialize(mod, component, audio_path)
    assert result.success
    df = result.output_for_node("t")
    assert "who_said_what" in df.columns
    assert df.loc[0, "who_said_what"] is not None
