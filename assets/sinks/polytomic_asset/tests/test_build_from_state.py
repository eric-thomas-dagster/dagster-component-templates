"""build_defs_from_state with a real (non-empty) cached state.

Regression: `_build_polytomic_defs` read `self.retry_policy_max_retries` in a module-level helper (no `self`),
so any non-empty state raised NameError. The values are now declared fields passed into the helper."""
import json

from .conftest import load_component_module


def _state(tmp_path):
    p = tmp_path / "state"
    p.write_text(json.dumps([{"id": "sync1", "name": "Orders to SF"}, {"id": "sync2", "name": "Users"}]))
    return p


def test_builds_one_asset_per_sync_from_cached_state(tmp_path):
    comp = load_component_module().PolytomicAssetComponent(api_key_env_var="PT_KEY")
    defs = comp.build_defs_from_state(None, _state(tmp_path))
    assert sorted(k.to_user_string() for a in defs.assets for k in a.keys) == ["orders_to_sf", "users"]


def test_retry_policy_fields_are_applied(tmp_path):
    comp = load_component_module().PolytomicAssetComponent(
        api_key_env_var="PT_KEY", retry_policy_max_retries=2, retry_policy_delay_seconds=5)
    rp = comp.build_defs_from_state(None, _state(tmp_path)).assets[0].op.retry_policy
    assert (rp.max_retries, rp.delay) == (2, 5)
