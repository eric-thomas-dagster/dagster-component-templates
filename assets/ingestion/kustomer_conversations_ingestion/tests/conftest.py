"""Shared test helpers for KustomerConversationsIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The only thing mocked is the Kustomer HTTP call
(`KustomerResource.post`) -- everything else (partitions_def construction,
the page-walking loop, JSON:API attribute flattening, DataFrame building,
preview metadata) runs for real against a fake resource object.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "kustomer_conversations_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeKustomerResource:
    """Stands in for KustomerResource. `pages` is a list of response bodies
    returned in order, one per call to `.post()`. Records every call
    (including the request body) for pagination/filter assertions."""

    def __init__(self, pages):
        self._pages = list(pages)
        self.calls = []

    def post(self, path, json=None):
        self.calls.append({"path": path, "json": json})
        if not self._pages:
            return {"data": []}
        return self._pages.pop(0)


def make_conversation_object(conv_id, created_at, **extra):
    attributes = {"conversation_created_at": created_at, "conversation_status": "open"}
    attributes.update(extra)
    return {"id": conv_id, "type": "conversation", "attributes": attributes}
