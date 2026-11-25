"""Shared test helpers for DataframeToHttpComponent (self-contained; loads component.py
via importlib so tests don't require the parent package to be pip-installed).

`requests` is a real dependency (not faked/injected) -- the external boundary mocked here
is `requests.Session.request` itself (monkeypatched per-test), matching this repo's "mock
only the paid/external call" convention. The component runs for real under dg.materialize().
"""
import importlib.util
import pathlib
from types import ModuleType

import dagster as dg
import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dataframe_to_http_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=None, headers=None):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text if text is not None else ("" if json_data is None else "")
        self.headers = headers or ({"content-type": "application/json"} if json_data is not None else {})

    def json(self):
        if self._json_data is None:
            raise ValueError("no JSON body")
        return self._json_data


def source_asset(name="rows_src", rows=None):
    """A plain (unpartitioned) upstream asset returning a small DataFrame."""
    rows = rows if rows is not None else [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    @dg.asset(name=name)
    def _up() -> pd.DataFrame:
        return pd.DataFrame(rows)

    return _up
