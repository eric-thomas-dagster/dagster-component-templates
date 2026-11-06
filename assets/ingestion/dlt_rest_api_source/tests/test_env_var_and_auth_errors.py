"""Clear-error paths for unresolved '_env_var' fields and an unresolved
client.auth.* value -- ported from DltYamlSourceComponent (same gap,
same fix)."""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        client={"base_url": "https://api.example.com"},
        resources=[{"name": "orders", "endpoint": {"path": "orders"}}],
    )
    kwargs.update(overrides)
    return mod.DltRestApiSourceComponent(**kwargs)


def test_destination_credentials_env_var_unset_raises_clearly(mod, monkeypatch):
    monkeypatch.delenv("DLT_REST_API_SOURCE_TEST_UNSET_VAR", raising=False)
    component = _component(
        mod, destination="snowflake", destination_credentials_env_var="DLT_REST_API_SOURCE_TEST_UNSET_VAR",
    )
    with pytest.raises(ValueError, match="DLT_REST_API_SOURCE_TEST_UNSET_VAR"):
        component._resolve_destination()


def test_client_auth_token_resolved_to_none_raises_clearly(mod):
    """A "{{ env.VAR_NAME }}" Jinja reference to an unset var resolves to None
    silently (unlike the "{{ env('VAR_NAME') }}" function form, which raises) --
    this is the resulting None reaching the component, not the template itself."""
    component = _component(
        mod, client={"base_url": "https://api.example.com", "auth": {"type": "bearer", "token": None}},
    )
    with pytest.raises(ValueError, match="client.auth.token"):
        component._build_resources()
