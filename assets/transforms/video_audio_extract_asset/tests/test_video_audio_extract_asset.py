"""Committed regression tests for VideoAudioExtractAssetComponent.

No committed test coverage existed for this component at all before this.
Covers real ffmpeg audio extraction against real generated videos, and
the basename-collision fix: two videos sharing a filename (from different
source directories) in the same batch used to silently overwrite each
other's extracted audio file on disk, since the default output filename
depended only on video_basename.
"""
import os
import shutil
import subprocess

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

pytestmark = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="requires a real ffmpeg binary in PATH")


@pytest.fixture()
def mod():
    return load_component_module()


def _build_video_with_audio(path, tone_hz=440.0):
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-f", "lavfi", "-i", f"sine=frequency={tone_hz}:duration=1",
            "-f", "lavfi", "-i", "color=red:size=64x64:duration=1:rate=24",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest",
            str(path),
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


def test_extracts_real_audio_track(mod, tmp_path):
    video_path = tmp_path / "clip.mp4"
    _build_video_with_audio(video_path)

    component = mod.VideoAudioExtractAssetComponent(
        asset_name="audio_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "audio"),
    )
    result = _materialize(component, [str(video_path)])
    assert result.success

    df = result.output_for_node("audio_out")
    audio_path = df.loc[0, "audio_path"]
    assert os.path.isfile(audio_path) and os.path.getsize(audio_path) > 0
    assert df.loc[0, "audio_extract_error"] is None


def test_missing_video_file_recorded_as_error_not_crashed(mod, tmp_path):
    component = mod.VideoAudioExtractAssetComponent(
        asset_name="audio_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "audio"),
    )
    result = _materialize(component, ["/no/such/video.mp4"])
    assert result.success

    df = result.output_for_node("audio_out")
    assert df.loc[0, "audio_path"] is None
    assert df.loc[0, "audio_extract_error"] is not None
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["failed"].value == 1


def test_same_basename_videos_from_different_dirs_do_not_collide(mod, tmp_path):
    dir_a = tmp_path / "dirA"
    dir_b = tmp_path / "dirB"
    dir_a.mkdir()
    dir_b.mkdir()
    video_a = dir_a / "clip.mp4"
    video_b = dir_b / "clip.mp4"
    _build_video_with_audio(video_a, tone_hz=440.0)
    _build_video_with_audio(video_b, tone_hz=880.0)

    component = mod.VideoAudioExtractAssetComponent(
        asset_name="audio_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "audio"),
    )
    result = _materialize(component, [str(video_a), str(video_b)])
    assert result.success

    df = result.output_for_node("audio_out")
    audio_paths = df["audio_path"].tolist()
    assert len(audio_paths) == len(set(audio_paths)), f"audio_path collision: {audio_paths}"
    for p in audio_paths:
        assert os.path.isfile(p) and os.path.getsize(p) > 0


def test_sample_rate_and_channels_applied(mod, tmp_path):
    import wave

    video_path = tmp_path / "clip.mp4"
    _build_video_with_audio(video_path)

    component = mod.VideoAudioExtractAssetComponent(
        asset_name="audio_out",
        upstream_asset_key="videos_in",
        output_dir=str(tmp_path / "audio"),
        target_format="wav",
        sample_rate=16000,
        channels=1,
    )
    result = _materialize(component, [str(video_path)])
    assert result.success

    df = result.output_for_node("audio_out")
    with wave.open(df.loc[0, "audio_path"], "rb") as w:
        assert w.getframerate() == 16000
        assert w.getnchannels() == 1
