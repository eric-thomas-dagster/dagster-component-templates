"""Shared test helpers for ChargifyIngestionComponent.

`dlt.sources.rest_api.rest_api_source` is a real, installed dlt-core module (not a
verified/vendored source), so nothing needs to be faked to build it -- only
`dlt.pipeline(...)` is faked (this repo's "mock only the paid call" convention):
`.run()` never iterates the resource generators, so no real HTTP request is ever made.
"""
import importlib.util
import pathlib
from contextlib import contextmanager
from typing import Any, List, Optional
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "chargify_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    """No actual query-back result needed for these tests -- empty but
    well-shaped (an empty DataFrame has the same .description/.fetchall()
    surface a real cursor would)."""

    @property
    def description(self):
        return []

    def fetchall(self):
        return []


class FakeSqlClient:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @contextmanager
    def execute_query(self, sql: str):
        yield FakeCursor()


class FakePipeline:
    def __init__(self, **pipeline_kwargs):
        self.pipeline_kwargs = pipeline_kwargs
        self.run_calls: List[Any] = []

    def run(self, source):
        self.run_calls.append(source)
        return f"fake-load-info(resources={len(self.run_calls)})"

    def sql_client(self):
        return FakeSqlClient()


def install_fake_dlt(monkeypatch, mod):
    """Fakes dlt.pipeline(...) so the asset body never touches a real DuckDB file or
    makes a real HTTP request."""
    fake_pipeline = FakePipeline()

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    return fake_pipeline
