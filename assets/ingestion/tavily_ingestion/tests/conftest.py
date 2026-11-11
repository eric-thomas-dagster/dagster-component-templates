"""Shared test helpers for TavilyIngestionComponent.

`dlt.pipeline(...)` is faked (this repo's "mock only the paid call" convention) so
`.run()` never consumes the resource generator by itself -- the real Tavily HTTP call
only happens in tests that explicitly iterate the captured resource (and those tests
mock `requests.post`, never calling the real API). Structural tests (hash_columns,
exclude_columns, max_rows_per_resource, on_schema_change) never iterate the resource,
so no HTTP mocking is needed for them at all -- same convention as
activecampaign_ingestion/tests/conftest.py.
"""
import importlib.util
import pathlib
import sys
from contextlib import contextmanager
from types import ModuleType
from typing import Any, List, Optional

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("tavily_ingestion_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Registered in sys.modules (not just module_from_spec'd) BEFORE exec: dlt's
    # @dlt.resource decorator uses inspect.getmodule(fn) at decoration time (real dlt
    # 1.x behavior, confirmed by the AttributeError it raises without this), which
    # resolves via sys.modules[fn.__module__] -- a module built but never registered
    # there means that lookup returns None and dlt crashes trying to read module.__name__.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, df: Optional[pd.DataFrame] = None):
        self._df = df if df is not None else pd.DataFrame()

    def df(self):
        return self._df


class FakeSqlClient:
    def __init__(self, df: Optional[pd.DataFrame] = None):
        self._df = df

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @contextmanager
    def execute_query(self, sql: str):
        yield FakeCursor(self._df)


class FakePipeline:
    def __init__(
        self,
        readback_df: Optional[pd.DataFrame] = None,
        consume: bool = False,
        **pipeline_kwargs,
    ):
        self.pipeline_kwargs = pipeline_kwargs
        self.run_calls: List[Any] = []
        self.captured_rows: Optional[List[dict]] = None
        self._readback_df = readback_df
        self._consume = consume

    def run(self, source):
        self.run_calls.append(source)
        if self._consume:
            # Iterate the resource generator NOW, while the real AssetExecutionContext
            # driving it (and its context.log.info calls) is still live -- iterating it
            # later, after dg.materialize() has returned and the run has torn down,
            # crashes trying to write to the now-gone run's event log.
            self.captured_rows = list(source)
        return f"fake-load-info(resources={len(self.run_calls)})"

    def sql_client(self):
        return FakeSqlClient(self._readback_df)


def install_fake_dlt(
    monkeypatch, mod, readback_df: Optional[pd.DataFrame] = None, consume: bool = False
) -> FakePipeline:
    """Fakes dlt.pipeline(...) so the asset body never touches a real DuckDB file.

    `consume=False` (default): `.run()` never iterates the resource generator at all --
    use this for structural tests (hash_columns/exclude_columns/max_rows_per_resource/
    on_schema_change) that only inspect the resource's pipe steps / schema_contract, so
    no Tavily request is made and `requests.post` needs no mocking.

    `consume=True`: `.run()` eagerly iterates the resource into `fake_pipeline.
    captured_rows`, for tests that need the actual landed rows -- pair with
    `install_fake_tavily_search` to mock the real Tavily HTTP call.
    """
    fake_pipeline = FakePipeline(readback_df=readback_df, consume=consume)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    return fake_pipeline


def install_fake_tavily_search(monkeypatch, responder):
    """Fakes `requests.post` for the real Tavily `/search` call. `responder(url, headers,
    payload)` returns the JSON body the fake response should carry."""
    import requests

    class _Resp:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            pass

        def json(self):
            return self._payload

    def _fake_post(url, headers=None, json=None, timeout=None):
        return _Resp(responder(url, headers, json))

    monkeypatch.setattr(requests, "post", _fake_post)
