"""Shared test helpers for PandaDocDocumentCreateComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `requests` network path is
never exercised -- `_call_pandadoc_api` (the one external API boundary,
covering create + poll + send) is monkeypatched wholesale in tests, while
everything this component actually owns (dual source resolution,
template rendering, recipient/tokens construction, validation, per-row
failure isolation, metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "pandadoc_document_create_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakePandaDocResource:
    """Stands in for the real PandaDocResource -- its methods are never
    actually invoked since `_call_pandadoc_api` is monkeypatched wholesale
    in tests."""

    def create_document(self, **kwargs):
        return {"id": "fake-doc-id", "status": "document.uploaded"}

    def wait_until_draft(self, document_id, **kwargs):
        return "document.draft"

    def send_document(self, document_id, **kwargs):
        return {"id": document_id, "status": "document.sent"}


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
