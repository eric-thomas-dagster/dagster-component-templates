"""Shared test helpers for NotionBriefPageComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The real Notion API call is never made --
`NotionResource.create_page` (the one external, network boundary) is
monkeypatched on a fake resource instance, never an unscoped reassignment of a
shared module-level object, while everything this component actually owns
(block building, title templating, default-sections fallback, markdown-body
splitting, receipt recording) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "notion_brief_page_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_context_receipt_store_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parents[4] / "resources" / "context_receipt_store"
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "context_receipt_store_component_for_notion_brief_page_tests", component_py
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


class FakeNotionResource:
    """Stands in for the real NotionResource -- only `create_page` is faked."""

    def __init__(self, page_id: str = "page-abc123", url: str = "https://notion.so/page-abc123"):
        self.page_id = page_id
        self.url = url
        self.calls = []

    def create_page(self, parent, properties, children=None, icon=None, cover=None):
        self.calls.append(
            {"parent": parent, "properties": properties, "children": children, "icon": icon, "cover": cover}
        )
        return {"id": self.page_id, "url": self.url}


def metadata_for(result, asset_name: str) -> dict:
    """MaterializeResult(metadata=...) with no `value=` carries its data on
    the materialization event, not the step output -- output_for_node()
    returns a sentinel for these, so read the event's metadata instead."""
    raw = result.asset_materializations_for_node(asset_name)[0].metadata
    out = {}
    for k, v in raw.items():
        out[k] = v.value if hasattr(v, "value") else v
    return out
