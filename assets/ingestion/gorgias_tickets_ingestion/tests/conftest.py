"""Shared test helpers for GorgiasTicketsIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The only thing mocked is the Gorgias HTTP call
(`GorgiasResource.get`) -- everything else (partitions_def construction,
cursor-walking loop, client-side date filter, DataFrame building, preview
metadata) runs for real against a fake resource object.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "gorgias_tickets_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeGorgiasResource:
    """Stands in for GorgiasResource. `pages` is a list of response bodies
    returned in order, one per call to `.get()`. Records every call for
    pagination assertions."""

    def __init__(self, pages):
        self._pages = list(pages)
        self.calls = []

    def get(self, path, params=None):
        self.calls.append({"path": path, "params": dict(params or {})})
        if not self._pages:
            return {"data": [], "meta": {}}
        return self._pages.pop(0)


def make_ticket(ticket_id, created_datetime, **extra):
    t = {
        "id": ticket_id,
        "created_datetime": created_datetime,
        "subject": f"Ticket {ticket_id}",
        "status": "open",
    }
    t.update(extra)
    return t
