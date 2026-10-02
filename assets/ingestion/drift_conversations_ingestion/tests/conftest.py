"""Shared test helpers for DriftConversationsIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The only thing mocked is the Drift HTTP call
(`DriftResource.get`) -- everything else (partitions_def construction, the
page_token-walking loop, the client-side date filter, per-conversation
message fetch/flatten, DataFrame building, preview metadata) runs for real
against a fake resource object.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "drift_conversations_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeDriftResource:
    """Stands in for DriftResource. `responses` maps a path prefix
    ('conversations/list' or 'conversations/<id>/messages') to a list of
    response bodies returned in order for calls to that path. Records every
    call for pagination assertions."""

    def __init__(self, list_pages=None, messages_by_conv=None):
        self._list_pages = list(list_pages or [])
        # messages_by_conv: {conv_id: [page1_body, page2_body, ...]}
        self._messages_by_conv = {k: list(v) for k, v in (messages_by_conv or {}).items()}
        self.calls = []

    def get(self, path, params=None):
        self.calls.append({"path": path, "params": dict(params or {})})
        if path == "conversations/list":
            if not self._list_pages:
                return {"data": [], "pagination": {}}
            return self._list_pages.pop(0)
        if path.startswith("conversations/") and path.endswith("/messages"):
            conv_id = path.split("/")[1]
            # ids may be passed as int in the DataFrame but formatted as str in path
            key = conv_id
            if key not in self._messages_by_conv:
                try:
                    key = int(conv_id)
                except ValueError:
                    pass
            pages = self._messages_by_conv.get(key, [])
            if not pages:
                return {"messages": [], "pagination": {}}
            return pages.pop(0)
        raise AssertionError(f"Unexpected path requested: {path}")


def make_conversation(conv_id, created_at_ms, **extra):
    c = {
        "id": conv_id,
        "status": "open",
        "createdAt": created_at_ms,
        "updatedAt": created_at_ms,
        "inboxId": 1,
        "contactId": 42,
    }
    c.update(extra)
    return c
