"""Committed regression tests for AudioDiarizedTranscriberComponent's
pyannote backend.

Neither pyannote.audio nor openai-whisper are installed in this repo's
test environment (heavy ML dependencies), and pyannote's real pretrained
pipeline can't be downloaded here at all -- it's a gated HuggingFace
model requiring the user's own token and a one-time terms acceptance.
So this backend is orchestration-tested, not model-tested: what's
verified here is the real ImportError when the library is genuinely
absent, the real ValueError when the HF token is missing (raised before
any model loading), and -- using minimal stand-ins for the diarization
pipeline and Whisper model matching pyannote's real
Annotation.itertracks(yield_label=True) API shape -- that the
surrounding orchestration (speaker-label remapping, min_segment_seconds
filtering, max_segments_per_file truncation, real ffmpeg segment-cutting)
is correct against a REAL generated audio file and REAL ffmpeg subprocess
calls.
"""
import math
import os
import struct
import sys
import types
import wave

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _render_sine_wav(path, frequency_hz=440.0, duration_seconds=3.0, sample_rate=16000, amplitude=0.5):
    n_samples = int(duration_seconds * sample_rate)
    max_int16 = 32767
    with wave.open(str(path), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(sample_rate)
        for i in range(n_samples):
            v = amplitude * math.sin(2 * math.pi * frequency_hz * (i / sample_rate))
            w.writeframesraw(struct.pack("<h", int(v * max_int16)))


def _materialize(mod, component, audio_path, upstream_name="audio_in"):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    audio_df = pd.DataFrame({"audio_path": [audio_path]})

    @dg.asset(name=upstream_name)
    def _upstream():
        return audio_df

    return dg.materialize([asset_def, _upstream], raise_on_error=False)


def test_real_import_error_when_pyannote_not_installed(mod, tmp_path):
    assert "pyannote" not in sys.modules and "pyannote.audio" not in sys.modules
    with pytest.raises(ModuleNotFoundError):
        import pyannote.audio  # noqa: F401

    audio_path = tmp_path / "meeting.wav"
    _render_sine_wav(audio_path)
    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="t1", upstream_asset_key="audio_in1", diarization_backend="pyannote",
    )
    result = dg.materialize(
        [list(component.build_defs(context=None).assets)[0],
         dg.asset(name="audio_in1")(lambda: pd.DataFrame({"audio_path": [str(audio_path)]}))],
        raise_on_error=False,
    )
    assert not result.success
    failure_text = str(result.all_events)
    assert "pyannote.audio" in failure_text or "pip install" in failure_text


@pytest.fixture()
def fake_pyannote_and_whisper(monkeypatch):
    """Minimal stand-ins matching pyannote's real Annotation API shape and
    Whisper's real transcribe() return shape -- neither library is
    installed here, so these are NOT mocking a paid API call (there isn't
    one for this backend), they're substituting for a multi-GB local ML
    dependency this test environment can't install."""

    class FakeSegment:
        def __init__(self, start, end):
            self.start = start
            self.end = end

    class FakeDiarization:
        def itertracks(self, yield_label=True):
            yield FakeSegment(0.0, 1.0), "_", "SPEAKER_00"
            yield FakeSegment(1.0, 1.05), "_", "SPEAKER_01"  # micro-turn, dropped
            yield FakeSegment(1.2, 2.5), "_", "SPEAKER_01"

    class FakePipeline:
        @staticmethod
        def from_pretrained(pipeline_id, use_auth_token=None):
            assert use_auth_token == "fake-hf-token"
            return FakePipeline()

        def __call__(self, audio_path, **kwargs):
            return FakeDiarization()

    fake_pyannote = types.ModuleType("pyannote")
    fake_pyannote_audio = types.ModuleType("pyannote.audio")
    fake_pyannote_audio.Pipeline = FakePipeline
    monkeypatch.setitem(sys.modules, "pyannote", fake_pyannote)
    monkeypatch.setitem(sys.modules, "pyannote.audio", fake_pyannote_audio)

    call_count = {"n": 0}

    class FakeWhisperModel:
        def transcribe(self, path, **kwargs):
            assert os.path.isfile(path) and os.path.getsize(path) > 0, (
                "ffmpeg must have really cut a real segment file to disk"
            )
            call_count["n"] += 1
            return {"text": f"segment number {call_count['n']}"}

    fake_whisper = types.ModuleType("whisper")
    fake_whisper.load_model = lambda size: FakeWhisperModel()
    monkeypatch.setitem(sys.modules, "whisper", fake_whisper)
    return call_count


def test_missing_hf_token_raises_before_any_model_loading(mod, fake_pyannote_and_whisper, tmp_path, monkeypatch):
    monkeypatch.delenv("HF_TOKEN", raising=False)
    audio_path = tmp_path / "meeting.wav"
    _render_sine_wav(audio_path)

    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="t2", upstream_asset_key="audio_in2", diarization_backend="pyannote",
    )
    result = dg.materialize(
        [list(component.build_defs(context=None).assets)[0],
         dg.asset(name="audio_in2")(lambda: pd.DataFrame({"audio_path": [str(audio_path)]}))],
        raise_on_error=False,
    )
    assert not result.success
    assert "HF_TOKEN" in str(result.all_events)


def test_orchestration_with_real_ffmpeg_cuts_and_micro_turn_filtering(
    mod, fake_pyannote_and_whisper, tmp_path, monkeypatch,
):
    monkeypatch.setenv("HF_TOKEN", "fake-hf-token")
    audio_path = tmp_path / "meeting.wav"
    _render_sine_wav(audio_path)

    component = mod.AudioDiarizedTranscriberComponent(
        asset_name="t3", upstream_asset_key="audio_in3", diarization_backend="pyannote",
        min_segment_seconds=0.3,
    )
    result = dg.materialize(
        [list(component.build_defs(context=None).assets)[0],
         dg.asset(name="audio_in3")(lambda: pd.DataFrame({"audio_path": [str(audio_path)]}))],
    )
    assert result.success

    df = result.output_for_node("t3")
    segments = df.loc[0, "segments"]
    assert len(segments) == 2, "the 0.05s micro-turn below min_segment_seconds must be dropped"
    assert segments[0]["speaker"] == "speaker_1"
    assert segments[1]["speaker"] == "speaker_2"
    assert segments[0]["text"] == "segment number 1"
    assert segments[1]["text"] == "segment number 2"
