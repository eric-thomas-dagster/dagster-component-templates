"""build_defs_from_state retries briefly when the directory state is mid-swap.

The state dir is swapped in with two renames, so a reader can land in the microsecond gap between them and
see it missing. Before, that returned empty Definitions (the reporting asset vanished for that load); now it
retries for ~2s first, like enriched_dbt_project's retry-on-load. A state that is genuinely absent still
returns empty Definitions (after the bounded wait)."""
import json
import threading
import time

import dagster as dg

from .conftest import load_component_module


def _component():
    mod = load_component_module()
    return mod.CommunityComponentInstallerComponent(components=["x"])


MANIFEST = {"installed": [{"id": "x", "status": "ok"}], "registry_url": "u"}


def test_manifest_appearing_mid_load_is_picked_up(tmp_path):
    state = tmp_path / "state"

    def appear_later():
        time.sleep(0.3)
        state.mkdir()
        (state / "install_manifest.json").write_text(json.dumps(MANIFEST))

    t = threading.Thread(target=appear_later)
    t.start()
    started = time.time()
    defs = _component().build_defs_from_state(None, state)
    t.join()
    assert time.time() - started >= 0.25, "should have waited for the state to appear"
    assert list(defs.resolve_all_asset_keys()) != [], "reporting asset must exist once the state landed"


def test_present_state_does_not_wait(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    (state / "install_manifest.json").write_text(json.dumps(MANIFEST))
    started = time.time()
    defs = _component().build_defs_from_state(None, state)
    assert time.time() - started < 0.5
    assert list(defs.resolve_all_asset_keys()) != []


def test_genuinely_absent_state_still_returns_empty_defs(tmp_path):
    defs = _component().build_defs_from_state(None, tmp_path / "never")
    assert isinstance(defs, dg.Definitions)
    assert list(defs.resolve_all_asset_keys()) == []


def test_none_state_path_returns_empty_defs_immediately():
    started = time.time()
    assert list(_component().build_defs_from_state(None, None).resolve_all_asset_keys()) == []
    assert time.time() - started < 0.2
