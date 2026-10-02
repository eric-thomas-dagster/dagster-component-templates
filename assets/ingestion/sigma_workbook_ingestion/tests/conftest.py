"""Shared test helpers for SigmaWorkbookIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent
package to be pip-installed. The one external, paid-API boundary --
`SigmaResource.get()` -- is replaced by a minimal fake that serves
hand-built `{entries, nextPage}` pages; everything this component actually
owns (workbook/element pagination, input-table filtering, partitions_def
construction, limit enforcement, preview metadata) is exercised for real.
"""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "sigma_workbook_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeSigmaResource:
    """Stands in for the real SigmaResource. `.get(path, params=None)`
    looks up a canned response keyed by (path, page), where `page` comes
    from `params.get("page")` (None for the first page) -- mirroring how
    the real resource is called: `v2/workbooks` for the top-level list,
    then `v2/workbooks/{id}/elements` per workbook."""

    def __init__(self, responses_by_path_and_page):
        # responses_by_path_and_page: {(path, page_or_None): response_dict}
        self._responses = dict(responses_by_path_and_page)
        self.calls = []

    def get(self, path, params=None):
        self.calls.append({"path": path, "params": dict(params) if params else None})
        page = (params or {}).get("page")
        return self._responses.get((path, page), {"entries": [], "nextPage": None})


def make_workbook(workbook_id, **fields):
    row = {"workbookId": workbook_id}
    row.update(fields)
    return row


def make_element(element_id, type_, **fields):
    row = {"elementId": element_id, "type": type_}
    row.update(fields)
    return row
