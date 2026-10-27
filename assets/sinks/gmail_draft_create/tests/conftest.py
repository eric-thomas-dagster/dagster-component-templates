"""Shared test helpers for GmailDraftCreateComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real Gmail API call is never
made -- `requests.post` (the one external boundary) is monkeypatched
wholesale in tests, while everything this component actually owns
(upstream resolution, template rendering, MIME construction, base64url
encoding, validation, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "gmail_draft_create_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, value):
    @dg.asset(name=name)
    def _upstream():
        return value
    return _upstream


class FakeResponse:
    """Stands in for `requests.Response` -- only the attributes this
    component actually reads (`status_code`, `.json()`, `.text`)."""

    def __init__(self, status_code: int, json_data=None, text: str = ""):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text or str(json_data)

    def json(self):
        return self._json_data


def metadata_for(result, asset_name: str) -> dict:
    """Output(metadata=...) / MaterializeResult(metadata=...) carry their
    data on the materialization event, not the step output -- read the
    event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
