"""Shared test helpers for AmperityIngestionComponent.

Loads component.py directly via importlib so tests don't require the
parent package to be pip-installed (this repo's committed-test
convention: conftest.py importlib loader + real files, mock only the
paid call).

This component's one "paid call" is the dlt pipeline run against the real
Amperity REST API (`dlt.pipeline(...).run(rest_api_source(config))`) plus
the follow-up `pipeline.sql_client()` query-back used to assemble the
output DataFrame. Both are mocked wholesale via `FakeDltPipeline` /
`FakeSqlClient` below; everything this component actually owns --
resource-config construction (`_build_resources_config`), the tenant-
subdomain base URL, bearer-auth config shape, destination/staging
resolution, non-SQL-destination handling, and the per-resource DataFrame
combination + metadata shape -- is exercised for real.
"""
import importlib.util
import pathlib
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Dict, List, Optional

import pandas as pd

# Ensures `dlt.sources.rest_api` is bound as an attribute of the `dlt`
# package (not just importable by dotted path) before `install_fake_dlt`
# below reaches for `mod.dlt.sources.rest_api` -- the component itself only
# imports this submodule lazily, inside the asset body, at call time.
import dlt.sources.rest_api  # noqa: F401


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "amperity_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    """Stands in for the DBAPI cursor dlt's sql_client().execute_query()
    yields; `.df()` is the only surface this component touches."""

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def df(self) -> pd.DataFrame:
        return self._df


class FakeSqlClient:
    """Stands in for `pipeline.sql_client()`. `table_rows` maps table name
    -> the DataFrame that table "contains"; the information_schema lookup
    returns exactly those table names."""

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
        raise AssertionError(f"FakeSqlClient: unexpected query {sql!r}")


class FakePipeline:
    """Stands in for `dlt.pipeline(...)`. Records the kwargs it was built
    with and the source(s) passed to `.run()`, without touching a real
    DuckDB file or the network."""

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
    """Monkeypatches `dlt.pipeline` and
    `dlt.sources.rest_api.rest_api_source` on the loaded component module
    so `build_defs(...)`'s asset body never touches the network or a real
    DuckDB file. Returns the `FakePipeline` instance and a `captured`
    dict holding the exact `config` passed to `rest_api_source`, so tests
    can assert on resource shape / auth / params without any real HTTP.
    """
    captured: Dict[str, Any] = {}
    fake_pipeline = FakePipeline(table_rows=table_rows)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    def fake_rest_api_source(config):
        captured["config"] = config
        return object()

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    monkeypatch.setattr(
        mod.dlt.sources.rest_api, "rest_api_source", fake_rest_api_source, raising=False
    )
    return fake_pipeline, captured
