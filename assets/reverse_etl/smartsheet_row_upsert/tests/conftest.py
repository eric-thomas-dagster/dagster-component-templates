"""Shared test helpers for SmartsheetRowUpsertComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed. The real Smartsheet HTTP API is never
called -- a minimal fake resource stands in for the one external boundary
(`SmartsheetResource.upsert_rows_by_column`), matching the convention of
mocking only the external call while exercising all of this component's
own logic (dual source resolution, validation, row-building, blank-key
skipping, batch capping, metadata) for real.
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "smartsheet_row_upsert_component", component_py
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


class FakeSmartsheetResource:
    """Stands in for `SmartsheetResource` -- implements only the one method
    the sink calls (`upsert_rows_by_column`), tracking every call so tests
    can assert on exactly what the component sent.

    `existing_rows_by_key` seeds which key values are treated as already
    present on the sheet (mimicking a prior `get_sheet` fetch finding a
    matching row) -- those route to "updated", everything else to "created".
    Set `raise_on_upsert` to an exception instance to simulate the resource
    (and hence the underlying HTTP call) failing.
    """

    def __init__(self, existing_rows_by_key=None, raise_on_upsert=None):
        self.existing_rows_by_key = dict(existing_rows_by_key or {})
        self.raise_on_upsert = raise_on_upsert
        self.upsert_calls = []
        self._next_row_id = 1000

    def upsert_rows_by_column(self, sheet_id, key_column_title, rows_as_dicts):
        self.upsert_calls.append(
            {
                "sheet_id": sheet_id,
                "key_column_title": key_column_title,
                "rows": rows_as_dicts,
            }
        )
        if self.raise_on_upsert is not None:
            raise self.raise_on_upsert

        created = []
        updated = []
        for row_dict in rows_as_dicts:
            key_value = row_dict.get(key_column_title)
            key_str = str(key_value)
            existing_id = self.existing_rows_by_key.get(key_str)
            if existing_id is not None:
                updated.append({"id": existing_id, "cells": row_dict})
            else:
                self._next_row_id += 1
                new_id = self._next_row_id
                self.existing_rows_by_key[key_str] = new_id
                created.append({"id": new_id, "cells": row_dict})
        return {"created": created, "updated": updated}
