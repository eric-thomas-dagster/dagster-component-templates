"""Shared test helpers for SalesloftPeopleIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The one external, paid-API boundary --
`SalesloftResource.get()` -- is replaced by a minimal fake that serves
hand-built `{data, metadata}` pages; everything this component actually
owns (page-number pagination, partitions_def construction, filter-param
translation, limit enforcement, preview metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "salesloft_people_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeSalesloftResource:
    """Stands in for the real SalesloftResource. `.get(path, params=None)`
    returns the page matching the requested `params["page"]`, keyed by
    page number -- mirroring how the real resource is called (every call
    passes the same `path="people.json"`, with `params["page"]` advancing)."""

    def __init__(self, pages_by_number):
        # pages_by_number: {1: response_dict, 2: response_dict, ...}
        self._pages_by_number = dict(pages_by_number)
        self.calls = []

    def get(self, path, params=None):
        self.calls.append({"path": path, "params": dict(params) if params else None})
        page_num = (params or {}).get("page", 1)
        return self._pages_by_number.get(page_num, {"data": [], "metadata": {}})


def make_person(id_, **fields):
    row = {"id": id_}
    row.update(fields)
    return row
