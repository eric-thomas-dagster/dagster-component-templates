"""build_defs_from_state with a real (non-empty) cached state.

Regression: `_build_dataiku_defs._make_asset` read `self.retry_policy_max_retries` inside a module-level
helper (no `self`), so any non-empty state raised NameError and the component's assets never loaded. The
retry_policy_* values are now declared fields passed into the helper and applied to the asset."""
import json

from .conftest import load_component_module


def _state(tmp_path):
    p = tmp_path / "state"
    p.write_text(json.dumps({"PROJ": [{"id": "S1", "name": "nightly"}, {"id": "S2", "name": "hourly"}]}))
    return p


def test_builds_one_asset_per_scenario_from_cached_state(tmp_path):
    mod = load_component_module()
    comp = mod.DataikuAssetComponent(host_env_var="DK_HOST", api_key_env_var="DK_KEY")
    defs = comp.build_defs_from_state(None, _state(tmp_path))
    assert sorted(k.to_user_string() for a in defs.assets for k in a.keys) == ["proj/s1", "proj/s2"]


def test_retry_policy_fields_are_applied(tmp_path):
    mod = load_component_module()
    comp = mod.DataikuAssetComponent(
        host_env_var="DK_HOST", api_key_env_var="DK_KEY",
        retry_policy_max_retries=3, retry_policy_delay_seconds=7, retry_policy_backoff="linear",
    )
    defs = comp.build_defs_from_state(None, _state(tmp_path))
    rp = defs.assets[0].op.retry_policy
    assert (rp.max_retries, rp.delay, rp.backoff.value) == (3, 7, "LINEAR")


def test_no_retry_policy_by_default(tmp_path):
    mod = load_component_module()
    defs = mod.DataikuAssetComponent(host_env_var="DK_HOST", api_key_env_var="DK_KEY").build_defs_from_state(None, _state(tmp_path))
    assert defs.assets[0].op.retry_policy is None
