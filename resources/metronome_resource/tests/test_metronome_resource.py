"""Committed regression tests for MetronomeResource / MetronomeResourceComponent.

Metronome's own API call (whatever a consuming component sends via
`requests.Session`) is never exercised here -- these tests cover only what
this resource itself owns: bearer-token header construction, env var
resolution, the missing-env-var failure mode, and base_url
defaulting/override.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_get_client_sets_bearer_auth_header(mod, monkeypatch):
    monkeypatch.setenv("METRONOME_API_KEY", "sk-test-123")
    resource = mod.MetronomeResource(api_key_env_var="METRONOME_API_KEY")

    session = resource.get_client()

    assert session.headers["Authorization"] == "Bearer sk-test-123"
    assert session.headers["Content-Type"] == "application/json"


def test_base_url_defaults_to_v1(mod, monkeypatch):
    monkeypatch.setenv("METRONOME_API_KEY", "sk-test-123")
    resource = mod.MetronomeResource(api_key_env_var="METRONOME_API_KEY")
    assert resource.base_url == "https://api.metronome.com/v1"


def test_base_url_override(mod, monkeypatch):
    monkeypatch.setenv("METRONOME_API_KEY", "sk-test-123")
    resource = mod.MetronomeResource(
        api_key_env_var="METRONOME_API_KEY",
        base_url="https://api.eu.metronome.com/v1",
    )
    assert resource.base_url == "https://api.eu.metronome.com/v1"


def test_get_client_missing_env_var_raises(mod, monkeypatch):
    monkeypatch.delenv("METRONOME_API_KEY", raising=False)
    resource = mod.MetronomeResource(api_key_env_var="METRONOME_API_KEY")

    with pytest.raises(ValueError, match="METRONOME_API_KEY"):
        resource.get_client()


def test_resource_component_build_defs_registers_resource(mod, monkeypatch):
    monkeypatch.setenv("METRONOME_API_KEY", "sk-test-123")
    component = mod.MetronomeResourceComponent(
        resource_key="metronome_resource",
        api_key_env_var="METRONOME_API_KEY",
    )
    defs = component.build_defs(context=None)
    assert "metronome_resource" in defs.resources
    assert isinstance(defs.resources["metronome_resource"], mod.MetronomeResource)
