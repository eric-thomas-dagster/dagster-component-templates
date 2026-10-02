"""Shared test helpers for ZohoCrmRecordUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real `ZohoCrmResource` (and its
HTTP/OAuth boundary) is never imported -- a minimal fake resource stands in
for the one external, paid-API boundary (`.upsert()`), matching the
convention of mocking only the external call while exercising all of this
component's own logic (dual source resolution, key-field validation,
chunking, metadata counting) for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "zoho_crm_record_upsert_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_upstream_asset(name: str, df):
    """A fresh @asset closure per call -- needed because materializing the
    same function object twice under different DataFrames in one test file
    would otherwise share the same closed-over `df`."""
    @dg.asset(name=name)
    def _upstream():
        return df
    return _upstream


class FakeZohoCrmResource:
    """Stands in for the real ZohoCrmResource -- `.upsert()` records every
    call and returns a scripted response shaped like Zoho's real API:
    one {code, duplicate_field, action, status, message, details} entry
    per input record, in the same order.

    By default every record succeeds and is reported as an "insert" --
    tests override `response_for_chunk` to script updates / errors.
    """

    def __init__(self, response_for_chunk=None):
        self.upsert_calls = []
        self._response_for_chunk = response_for_chunk

    def upsert(self, module_api_name, records, duplicate_check_fields=None):
        call_index = len(self.upsert_calls)
        self.upsert_calls.append({
            "module_api_name": module_api_name,
            "records": records,
            "duplicate_check_fields": duplicate_check_fields,
        })
        if self._response_for_chunk is not None:
            return self._response_for_chunk(call_index, records)
        return [
            {
                "code": "SUCCESS",
                "duplicate_field": None,
                "action": "insert",
                "status": "success",
                "message": "record added",
                "details": {"id": f"id-{call_index}-{i}"},
            }
            for i in range(len(records))
        ]
