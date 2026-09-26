"""Committed regression tests for VideoSceneSummarizerComponent.

Exercises real ffmpeg scene detection + frame extraction against REAL
generated videos (a real 3-scene hard-cut clip, a real single-color
static clip, a real corrupt file) -- only the vision-LLM call itself
(litellm.completion) is mocked, never the media.
"""
import os
import shutil
import subprocess
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pytestmark = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="requires a real ffmpeg binary in PATH")


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake_litellm(monkeypatch):
    calls = []
    state = {"raise_on_call": None}

    def fake_completion(model, messages, api_key=None, **kwargs):
        calls.append({"model": model, "messages": messages, "kwargs": kwargs})
        if state["raise_on_call"] is not None and len(calls) == state["raise_on_call"]:
            raise RuntimeError("simulated transient LLM failure")
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=f"Scene {len(calls)} summary."))]
        )

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)
    return calls, state


def _build_three_scene_video(path):
    """A real MP4 with 2 hard color cuts (red -> blue -> green at t=1s,
    t=2s) -- verified live to produce exactly 2 scene-detected frames at
    the default threshold=0.4."""
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "color=red:size=64x64:duration=1:rate=24",
            "-f", "lavfi", "-i", "color=blue:size=64x64:duration=1:rate=24",
            "-f", "lavfi", "-i", "color=green:size=64x64:duration=1:rate=24",
            "-filter_complex", "[0:v][1:v][2:v]concat=n=3:v=1:a=0[outv]",
            "-map", "[outv]", "-c:v", "libx264", "-pix_fmt", "yuv420p", str(path),
        ],
        check=True, capture_output=True,
    )


def _build_static_video(path):
    """A real MP4 with no scene changes at all -- must fall back to a
    single first-frame row instead of producing zero rows."""
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", "color=red:size=64x64:duration=1:rate=24",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", str(path),
        ],
        check=True, capture_output=True,
    )


def _materialize(component, video_paths):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    videos_df = pd.DataFrame({"file_path": video_paths})

    @dg.asset(name="videos_in")
    def videos_in():
        return videos_df

    return dg.materialize([asset_def, videos_in])


def test_three_scene_video_detects_two_real_boundaries(mod, fake_litellm, tmp_path):
    calls, _ = fake_litellm
    video_path = tmp_path / "clip.mp4"
    _build_three_scene_video(video_path)

    component = mod.VideoSceneSummarizerComponent(
        asset_name="scenes_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "frames"),
        scene_threshold=0.4,
    )
    result = _materialize(component, [str(video_path)])
    assert result.success

    df = result.output_for_node("scenes_out")
    assert len(df) == 2, f"expected exactly 2 detected scene boundaries, got {len(df)}"
    assert df.loc[0, "timestamp_seconds"] == pytest.approx(1.0)
    assert df.loc[1, "timestamp_seconds"] == pytest.approx(2.0)
    for p in df["frame_path"]:
        assert os.path.isfile(p) and os.path.getsize(p) > 0, f"expected a real non-empty frame file at {p}"
    assert df.loc[0, "summary"] == "Scene 1 summary."
    assert df.loc[1, "summary"] == "Scene 2 summary."
    assert len(calls) == 2


def test_static_video_falls_back_to_single_first_frame_row(mod, fake_litellm, tmp_path):
    video_path = tmp_path / "static.mp4"
    _build_static_video(video_path)

    component = mod.VideoSceneSummarizerComponent(
        asset_name="scenes_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "frames"),
    )
    result = _materialize(component, [str(video_path)])
    assert result.success

    df = result.output_for_node("scenes_out")
    assert len(df) == 1, "a genuinely static clip must fall back to exactly one row, not zero"
    assert df.loc[0, "timestamp_seconds"] == 0.0
    assert os.path.isfile(df.loc[0, "frame_path"])


def test_corrupt_video_counted_as_error_not_crashed(mod, fake_litellm, tmp_path):
    fake_video = tmp_path / "corrupt.mp4"
    fake_video.write_bytes(b"this is not a real video file")

    component = mod.VideoSceneSummarizerComponent(
        asset_name="scenes_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "frames"),
    )
    result = _materialize(component, [str(fake_video)])
    assert result.success, "a single corrupt video must not fail the whole materialization"

    df = result.output_for_node("scenes_out")
    assert len(df) == 0
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["video_errors"].value == 1


def test_llm_max_retries_forwarded_to_litellm(mod, fake_litellm, tmp_path):
    calls, _ = fake_litellm
    video_path = tmp_path / "static.mp4"
    _build_static_video(video_path)

    component = mod.VideoSceneSummarizerComponent(
        asset_name="scenes_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "frames"),
        llm_max_retries=7,
    )
    result = _materialize(component, [str(video_path)])
    assert result.success
    assert calls[0]["kwargs"]["num_retries"] == 7


def test_llm_failure_for_one_scene_still_emits_the_row(mod, fake_litellm, tmp_path):
    _, state = fake_litellm
    state["raise_on_call"] = 1  # first (and only, for this static video) call fails
    video_path = tmp_path / "static.mp4"
    _build_static_video(video_path)

    component = mod.VideoSceneSummarizerComponent(
        asset_name="scenes_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "frames"),
    )
    result = _materialize(component, [str(video_path)])
    assert result.success, "an LLM summarization failure must not fail the whole run"

    df = result.output_for_node("scenes_out")
    assert len(df) == 1, "the scene row must still be emitted even when its summary call failed"
    assert df.loc[0, "summary"] is None
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["llm_failures"].value == 1


def test_same_basename_videos_from_different_dirs_do_not_collide(mod, fake_litellm, tmp_path):
    """Two videos named identically ('clip.mp4') from different source
    directories, processed in the same batch with a shared output_dir,
    must not overwrite each other's extracted frame files -- confirmed
    live before the fix that the second video's frame silently
    overwrote the first's, corrupting the first row's frame_path."""
    dir_a = tmp_path / "dirA"
    dir_b = tmp_path / "dirB"
    dir_a.mkdir()
    dir_b.mkdir()
    video_a = dir_a / "clip.mp4"
    video_b = dir_b / "clip.mp4"
    _build_static_video(video_a)
    _build_three_scene_video(video_b)

    component = mod.VideoSceneSummarizerComponent(
        asset_name="scenes_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "frames"),
    )
    result = _materialize(component, [str(video_a), str(video_b)])
    assert result.success

    df = result.output_for_node("scenes_out")
    frame_paths = df["frame_path"].tolist()
    assert len(frame_paths) == len(set(frame_paths)), f"frame_path collision: {frame_paths}"
    for p in frame_paths:
        assert os.path.isfile(p) and os.path.getsize(p) > 0


def test_max_scenes_per_video_caps_detected_scenes(mod, fake_litellm, tmp_path):
    video_path = tmp_path / "clip.mp4"
    _build_three_scene_video(video_path)

    component = mod.VideoSceneSummarizerComponent(
        asset_name="scenes_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "frames"),
        max_scenes_per_video=1,
    )
    result = _materialize(component, [str(video_path)])
    assert result.success

    df = result.output_for_node("scenes_out")
    assert len(df) == 1, "max_scenes_per_video=1 must cap a 2-scene-boundary video down to 1 row"
