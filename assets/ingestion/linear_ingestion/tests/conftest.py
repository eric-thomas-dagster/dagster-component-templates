"""Shared test helpers for LinearIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed (this repo's "committed test convention": conftest.py importlib loader +
real files, mock only the paid call).

This component's one real external-network boundary is `requests.post` against Linear's
GraphQL API -- that is what `install_fake_requests_post` mocks. `dlt.pipeline(...)` itself
is faked (same convention as sitemap_crawler_ingestion/firecrawl_ingestion's own conftest.py):
dlt 1.30.0's duckdb destination rejects a bare `credentials=":memory:"` string, so an
in-memory real pipeline isn't viable here. Faking only `dlt.pipeline(...)` still exercises
the REAL `@dlt.resource` object and its REAL `.apply_hints()`, so `FakePipeline.run_calls`
can be iterated in a test to see the real write_disposition/primary_key/hash_columns/
exclude_columns/max_rows_per_resource/on_schema_change output, without touching a real
destination.
"""
import importlib.util
import pathlib
import sys
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Dict, List, Optional

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location("linear_ingestion_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    """Stands in for requests.Response."""

    def __init__(self, json_body=None, status_code=200, raise_exc=None):
        self._json_body = json_body or {}
        self.status_code = status_code
        self._raise_exc = raise_exc

    def json(self):
        return self._json_body

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"{self.status_code} error")


def install_fake_requests_post(monkeypatch, mod, post):
    """`post` is a callable with the same signature as requests.post."""
    import requests
    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(mod.requests, "post", post)


class FakeCursor:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def df(self) -> pd.DataFrame:
        return self._df


class FakeSqlClient:
    """`table_rows` maps table name -> the DataFrame that table "contains"; the
    information_schema lookup returns exactly those table names."""

    def __init__(self, table_rows: Dict[str, pd.DataFrame]):
        self.table_rows = table_rows
        self.queries: List[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @contextmanager
    def execute_query(self, sql: str):
        self.queries.append(sql)
        if "information_schema.tables" in sql:
            yield FakeCursor(pd.DataFrame({"table_name": list(self.table_rows.keys())}))
            return
        for table_name, df in self.table_rows.items():
            if sql.strip().endswith(f".{table_name}"):
                yield FakeCursor(df)
                return
        yield FakeCursor(pd.DataFrame())


class FakePipeline:
    """Stands in for `dlt.pipeline(...)`. Records the resource(s) passed to `.run()`
    (`run_calls`) without touching a real DuckDB file or the network."""

    def __init__(self, table_rows: Optional[Dict[str, pd.DataFrame]] = None, **pipeline_kwargs):
        self.pipeline_kwargs = pipeline_kwargs
        self.run_calls: List[Any] = []
        self._table_rows = table_rows or {}
        self.sql_client_instance = FakeSqlClient(self._table_rows)

    def run(self, source):
        self.run_calls.append(source)
        return f"fake-load-info(resources={len(self.run_calls)})"

    def sql_client(self):
        return self.sql_client_instance


def install_fake_dlt(monkeypatch, mod, table_rows: Optional[Dict[str, pd.DataFrame]] = None):
    fake_pipeline = FakePipeline(table_rows=table_rows)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    return fake_pipeline
