"""An unreachable spec_url (or missing/unparseable spec_path) must not crash the whole code
location: build_defs runs eagerly on every code-server reload, so an uncaught exception there
would take down every OTHER component sharing that location too -- the same bug class this repo
fixed this session for terraform_cloud_asset/coalesce_workspace/polytomic_asset/etc. build_defs
now catches the fetch failure and returns an empty Definitions() with a warning log instead.
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeLog:
    def __init__(self):
        self.warnings = []

    def warning(self, msg):
        self.warnings.append(msg)


class _FakeContext:
    def __init__(self):
        self.log = _FakeLog()


def test_unreachable_spec_url_returns_empty_defs_not_a_crash(mod, monkeypatch):
    def _raise(*a, **k):
        raise ConnectionError("no route to host")

    monkeypatch.setattr(mod, "_fetch_spec", _raise)
    component = mod.OpenApiToolServerComponent(spec_url="https://unreachable.example.com/openapi.json")
    context = _FakeContext()

    defs = component.build_defs(context)

    assert isinstance(defs, dg.Definitions)
    assert not defs.assets
    assert context.log.warnings


def test_unreachable_spec_url_safe_with_no_context_log(mod, monkeypatch):
    """context=None (this repo's own test convention for build_defs) must not itself crash --
    `hasattr(context, "log")` guards the warning call."""
    def _raise(*a, **k):
        raise ConnectionError("no route to host")

    monkeypatch.setattr(mod, "_fetch_spec", _raise)
    component = mod.OpenApiToolServerComponent(spec_url="https://unreachable.example.com/openapi.json")

    defs = component.build_defs(None)

    assert isinstance(defs, dg.Definitions)
    assert not defs.assets
