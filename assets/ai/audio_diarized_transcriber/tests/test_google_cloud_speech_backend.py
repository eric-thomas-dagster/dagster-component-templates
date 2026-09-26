"""Committed regression tests for AudioDiarizedTranscriberComponent's
google_cloud_speech backend (the default, fully-validated backend -- see
the pyannote backend's own test file for why that one is orchestration-only).

Only the network call (SpeechClient.recognize) is mocked; the audio file
is real (a real generated WAV, read off disk), and the fake response is
built from the real google-cloud-speech proto-plus types (WordInfo,
SpeechRecognitionResult), not hand-rolled dicts.
"""
import datetime
import math
import struct
import sys
import types
import wave

import dagster as dg
import pandas as pd
import pytest
from google.cloud.speech_v2.types import cloud_speech as cs

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _render_sine_wav(path, frequency_hz=440.0, duration_seconds=1.0, sample_rate=16000, amplitude=0.5):
    n_samples = int(duration_seconds * sample_rate)
    max_int16 = 32767
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(sample_rate)
        for i in range(n_samples):
            v = amplitude * math.sin(2 * math.pi * frequency_hz * (i / sample_rate))
            w.writeframesraw(struct.pack("<h", int(v * max_int16)))


def _word(w, start, end, speaker):
    return cs.WordInfo(
        word=w,
        start_offset=datetime.timedelta(seconds=start),
        end_offset=datetime.timedelta(seconds=end),
        speaker_label=speaker,
    )


@pytest.fixture()
def fake_speech_client(monkeypatch, tmp_path):
    words = [
        _word("hi", 0.0, 0.4, "1"),
        _word("there", 0.4, 0.8, "1"),
        _word("hey", 1.0, 1.3, "2"),
        _word("how", 1.3, 1.5, "2"),
        _word("are", 1.5, 1.7, "2"),
        _word("you", 1.7, 2.0, "2"),
    ]
    fake_result = cs.SpeechRecognitionResult(
        alternatives=[cs.SpeechRecognitionAlternative(transcript="hi there hey how are you", words=words)]
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
    _render_sine_wav(audio_path)
    return recognize_calls, str(audio_path)


def _materialize(mod, component, audio_path):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    audio_df = pd.DataFrame({"audio_path": [audio_path]})

    @dg.asset(name="audio_in")
    def audio_in():
        return audio_df

    return dg.materialize([asset_def, audio_in])


def test_real_audio_bytes_reach_the_request(mod, fake_speech_client, tmp_path):
    recognize_calls, audio_path = fake_speech_client
    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="diarized_out",
        upstream_asset_key="audio_in",
        diarization_backend="google_cloud_speech",
        credentials={"project_id": "fake-project"},
    )
    result = _materialize(mod, component, audio_path)
    assert result.success

    import os
    sent_content = recognize_calls[0].content
    assert len(sent_content) == os.path.getsize(audio_path), (
        "the real audio file's bytes must be read and sent -- not a stub/no-op path"
    )


def test_segments_correctly_speaker_attributed_with_real_timings(mod, fake_speech_client):
    _, audio_path = fake_speech_client
    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="diarized_out",
        upstream_asset_key="audio_in",
        diarization_backend="google_cloud_speech",
        credentials={"project_id": "fake-project"},
        min_speaker_count=2,
        max_speaker_count=2,
    )
    result = _materialize(mod, component, audio_path)
    assert result.success

    df = result.output_for_node("diarized_out")
    segments = df.loc[0, "segments"]
    assert segments == [
        {"speaker": "speaker_1", "start_seconds": 0.0, "end_seconds": 0.8, "text": "hi there"},
        {"speaker": "speaker_2", "start_seconds": 1.0, "end_seconds": 2.0, "text": "hey how are you"},
    ]
    assert df.loc[0, "segments_transcript"] == "hi there hey how are you"


def test_output_metadata_correct(mod, fake_speech_client):
    _, audio_path = fake_speech_client
    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="diarized_out",
        upstream_asset_key="audio_in",
        diarization_backend="google_cloud_speech",
        credentials={"project_id": "fake-project"},
    )
    result = _materialize(mod, component, audio_path)
    assert result.success
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["diarized_rows"].value == 1
    assert meta["total_segments"].value == 2
    assert meta["backend"].text == "google_cloud_speech"


def test_neither_backend_credentials_set_raises(mod):
    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="diarized_out",
        upstream_asset_key="audio_in",
        diarization_backend="google_cloud_speech",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    audio_df = pd.DataFrame({"audio_path": ["/fake/does_not_matter.wav"]})

    @dg.asset(name="audio_in")
    def audio_in():
        return audio_df

    result = dg.materialize([asset_def, audio_in], raise_on_error=False)
    assert not result.success


def test_invalid_backend_raises_at_build_defs_time(mod):
    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="diarized_out",
        upstream_asset_key="audio_in",
        diarization_backend="not_a_real_backend",
    )
    with pytest.raises(ValueError, match="diarization_backend"):
        component.build_defs(context=None)
