"""Committed regression test for VideoFrameExtractAssetComponent's ffmpeg
9.x compatibility fix.

every_n_frames mode used '-vsync vfr', which ffmpeg 9.x rejects outright
('Unrecognized option') rather than just deprecating -- confirmed live
against a real installed ffmpeg 9.0.2 before this fix. The replacement,
'-fps_mode vfr', is the actual flag ffmpeg wants (available since 5.1).

Requires a real `ffmpeg` binary in PATH -- skipped if unavailable, since
this is genuinely testing shell-out compatibility with the installed
ffmpeg version, not something mockable without losing the point of the
test.
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


@pytest.fixture()
def real_test_video(tmp_path):
    video_path = tmp_path / "clip.mp4"
    subprocess.run(
        [
            "ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc=duration=2:size=64x64:rate=24",
            "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", str(video_path),
        ],
        check=True, capture_output=True,
    )
    return str(video_path)


def test_every_n_frames_mode_produces_real_frames(mod, real_test_video, tmp_path):
    frame_dir = tmp_path / "frames"
    component = mod.VideoFrameExtractAssetComponent(
        asset_name="frames_out",
        upstream_asset_key="videos_in",
        video_path_column="file_path",
        output_dir=str(frame_dir),
        mode="every_n_frames",
        every_n_frames=5,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    videos_df = pd.DataFrame({"file_path": [real_test_video]})

    @dg.asset(name="videos_in")
    def videos_in():
        return videos_df

    result = dg.materialize([asset_def, videos_in])
    assert result.success, "the -fps_mode fix must let every_n_frames mode actually run"

    frames_df = result.output_for_node("frames_out")
    assert len(frames_df) > 0, "every_n_frames mode produced zero frames"
    for p in frames_df["file_path"]:
        assert os.path.isfile(p) and os.path.getsize(p) > 0, f"expected a real non-empty frame file at {p}"


def test_same_basename_videos_from_different_dirs_do_not_collide(mod, tmp_path):
    """Two videos named identically ('clip.mp4') from different source
    directories, processed in the same batch with a shared output_dir,
    must not overwrite each other's extracted frame files on disk --
    confirmed live before the fix that the second video's frame silently
    overwrote the first's, corrupting the first row's file_path."""
    dir_a = tmp_path / "dirA"
    dir_b = tmp_path / "dirB"
    dir_a.mkdir()
    dir_b.mkdir()
    video_a = dir_a / "clip.mp4"
    video_b = dir_b / "clip.mp4"
    for path, pattern in ((video_a, "testsrc"), (video_b, "testsrc2")):
        subprocess.run(
            [
                "ffmpeg", "-y", "-f", "lavfi", "-i", f"{pattern}=duration=1:size=64x64:rate=24",
                "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", str(path),
            ],
            check=True, capture_output=True,
        )

    component = mod.VideoFrameExtractAssetComponent(
        asset_name="frames_out",
        upstream_asset_key="videos_in",
        video_path_column="file_path",
        output_dir=str(tmp_path / "frames"),
        mode="fixed_count",
        fixed_count=1,
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    videos_df = pd.DataFrame({"file_path": [str(video_a), str(video_b)]})

    @dg.asset(name="videos_in")
    def videos_in():
        return videos_df

    result = dg.materialize([asset_def, videos_in])
    assert result.success

    frames_df = result.output_for_node("frames_out")
    paths = frames_df["file_path"].tolist()
    assert len(paths) == len(set(paths)), f"file_path collision: {paths}"
    for p in paths:
        assert os.path.isfile(p) and os.path.getsize(p) > 0
