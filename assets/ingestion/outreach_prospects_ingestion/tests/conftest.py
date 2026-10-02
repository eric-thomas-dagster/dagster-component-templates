"""Shared test helpers for OutreachProspectsIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The one external, paid-API boundary --
`OutreachResource.get()` -- is replaced by a minimal fake that serves
hand-built JSON:API pages; everything this component actually owns (JSON:API
attribute flattening, cursor-following pagination, partitions_def
construction, preview metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "outreach_prospects_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeOutreachResource:
    """Stands in for the real OutreachResource. `.get(path, params=None)`
    serves pre-built JSON:API response dicts from `pages`, keyed by call
    index for the first call and then by the `links.next` URL for
    subsequent calls -- mirroring how the real resource is called (first
    call passes `path="prospects"` + params, later calls pass the full
    `links.next` URL with no params)."""

    def __init__(self, pages):
        # pages: list of response dicts, served in order regardless of the
        # path/URL passed in (the fake doesn't need to parse cursor URLs --
        # it just hands out the next page in sequence).
        self._pages = list(pages)
        self.calls = []

    def get(self, path, params=None):
        self.calls.append({"path": path, "params": params})
        if not self._pages:
            return {"data": [], "links": {}}
        return self._pages.pop(0)


def make_prospect(id_, **attributes):
    return {"id": id_, "type": "prospect", "attributes": attributes, "relationships": {}}
