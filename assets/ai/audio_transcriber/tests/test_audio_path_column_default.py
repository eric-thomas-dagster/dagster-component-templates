"""Regression test: audio_path_column now defaults to "audio_path", matching
its sibling audio_diarized_transcriber and the natural output of
video_audio_extract_asset's output_path_column -- previously the only
component in the video/audio family with no default on its path column.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_audio_path_column_defaults_to_audio_path(mod):
    component = mod.AudioTranscriberComponent(
        asset_name="transcribed",
        upstream_asset_key="raw_audio",
    )
    assert component.audio_path_column == "audio_path"


def test_audio_path_column_still_overridable(mod):
    component = mod.AudioTranscriberComponent(
        asset_name="transcribed",
        upstream_asset_key="raw_audio",
        audio_path_column="local_path",
    )
    assert component.audio_path_column == "local_path"
