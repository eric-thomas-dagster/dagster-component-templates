"""Shared test helpers for FirecrawlIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed (this repo's "committed test convention": conftest.py importlib loader +
real files, mock only the paid call).

This component's one real external-network boundary is `requests.post`/`requests.get`
against the Firecrawl API (`/v2/scrape`, `/v2/crawl`, `GET /v2/crawl/{id}`, and the
`next` pagination URL it returns) -- that is what `install_fake_requests` mocks.

Unlike jina_reader_ingestion's tests (which run a real dlt pipeline against an
in-memory DuckDB destination), this component's tests fake `dlt.pipeline(...)` itself
(same convention as mongodb_ingestion/netlify_ingestion's conftest.py): dlt 1.30.0's
duckdb destination outright rejects a bare `credentials=":memory:"` string (raises
`InvalidInMemoryDuckdbCredentials`, confirmed empirically against this repo's installed
dlt version), so an in-memory real pipeline isn't viable here. Faking only
`dlt.pipeline(...)` still exercises the REAL `@dlt.resource` object and its REAL
`.apply_hints()`/`.add_map()`/`.add_limit()` (dlt resources apply their own pipe steps
when iterated directly, with no pipeline/destination involved at all -- verified
empirically: `list(resource)` after `resource.add_map(fn)` runs `fn` over every row) --
so `install_fake_dlt`'s returned `FakePipeline.run_calls[0]` can be iterated in a test
to see the REAL hash_columns/exclude_columns/max_rows_per_resource/on_schema_change
output, without touching a real destination.

Because the `@dlt.resource`-decorated function (`_pages_resource`) is defined INSIDE
`firecrawl_ingestion_asset`, not at module level, dlt's decorator internals
(`dlt.common.reflection.spec.spec_from_signature`) resolve its defining module via
`sys.modules[fn.__module__]`, which is `None` unless the loaded module is registered
there first (same gotcha mongodb_ingestion's conftest.py documents for its inline
`@dlt.source`/`@dlt.resource`).
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
    spec = importlib.util.spec_from_file_location("firecrawl_ingestion_component", component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    """Stands in for requests.Response."""

    def __init__(self, json_body=None, status_code=200, raise_exc=None):
        self._json_body = json_body
        self.status_code = status_code
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code} error")

    def json(self):
        return self._json_body


def install_fake_requests(monkeypatch, mod, post=None, get=None):
    """`post`/`get` are callables with the same signature as requests.post/requests.get.
    Patches both the real `requests` module and `mod.requests` (the same module object,
    since the component imports `requests` at module scope -- patching one reaches both,
    but patching both matches this repo's existing convention)."""
    import requests

    if post is not None:
        monkeypatch.setattr(requests, "post", post)
        monkeypatch.setattr(mod.requests, "post", post)
    if get is not None:
        monkeypatch.setattr(requests, "get", get)
        monkeypatch.setattr(mod.requests, "get", get)


class FakeCursor:
    """Stands in for the DBAPI cursor dlt's sql_client().execute_query() yields;
    `.df()` is the only surface this component touches."""

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def df(self) -> pd.DataFrame:
        return self._df


class FakeSqlClient:
    """Stands in for `pipeline.sql_client()`. `table_rows` maps table name -> the
    DataFrame that table "contains"; the information_schema lookup returns exactly
    those table names."""

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
    """Stands in for `dlt.pipeline(...)`. Records the kwargs it was built with and the
    resource(s) passed to `.run()` (`run_calls`), without touching a real DuckDB file
    or the network. `run_calls[-1]` holds the REAL dlt resource list this component
    built -- iterate it (`list(resource)`) to see hash_columns/exclude_columns/
    max_rows_per_resource actually applied."""

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
    """Monkeypatches `dlt.pipeline` on the loaded component module so `build_defs(...)`'s
    asset body never touches a real DuckDB file. Returns the `FakePipeline` instance."""
    fake_pipeline = FakePipeline(table_rows=table_rows)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    return fake_pipeline
