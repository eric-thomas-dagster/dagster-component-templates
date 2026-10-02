"""Committed regression tests for FigmentResource / FigmentResourceComponent.

Figment's own API call (whatever a consuming component sends via
`requests.Session`) is never exercised here -- these tests cover only what
this resource itself owns: `x-api-key` header construction, env var
resolution, the missing-env-var failure mode, and base_url
defaulting/override.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_get_client_sets_api_key_header(mod, monkeypatch):
    monkeypatch.setenv("FIGMENT_API_KEY", "fig-test-123")
    resource = mod.FigmentResource(api_key_env_var="FIGMENT_API_KEY")

    session = resource.get_client()

    assert session.headers["x-api-key"] == "fig-test-123"
    assert session.headers["Content-Type"] == "application/json"
    # Not Bearer/OAuth -- Figment's own auth scheme is the raw key header.
    assert "Authorization" not in session.headers


def test_base_url_defaults(mod, monkeypatch):
    monkeypatch.setenv("FIGMENT_API_KEY", "fig-test-123")
    resource = mod.FigmentResource(api_key_env_var="FIGMENT_API_KEY")
    assert resource.base_url == "https://api.figment.io"


def test_base_url_override(mod, monkeypatch):
    monkeypatch.setenv("FIGMENT_API_KEY", "fig-test-123")
    resource = mod.FigmentResource(
        api_key_env_var="FIGMENT_API_KEY",
        base_url="https://api.custom.figment.io",
    )
    assert resource.base_url == "https://api.custom.figment.io"


def test_get_client_missing_env_var_raises(mod, monkeypatch):
    monkeypatch.delenv("FIGMENT_API_KEY", raising=False)
    resource = mod.FigmentResource(api_key_env_var="FIGMENT_API_KEY")

    with pytest.raises(ValueError, match="FIGMENT_API_KEY"):
        resource.get_client()


def test_resource_component_build_defs_registers_resource(mod, monkeypatch):
    monkeypatch.setenv("FIGMENT_API_KEY", "fig-test-123")
    component = mod.FigmentResourceComponent(
        resource_key="figment_resource",
        api_key_env_var="FIGMENT_API_KEY",
    )
    defs = component.build_defs(context=None)
    assert "figment_resource" in defs.resources
    assert isinstance(defs.resources["figment_resource"], mod.FigmentResource)
