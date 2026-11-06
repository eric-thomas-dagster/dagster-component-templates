"""Shared test helpers for DltLoadCollectionComponent.

Unlike a vendor source (zendesk_support, salesforce_source, ...), all three
of this component's source-building calls -- `dlt.sources.sql_database.sql_table`,
`dlt.sources.rest_api.rest_api_source`, `dlt.sources.filesystem.filesystem` --
are REAL, built into the `dlt` package itself (not a verified/vendored
source requiring `dlt init`), so nothing needs to be faked at that layer.
Only `dlt.pipeline(...)` is faked (this repo's "mock only the paid call"
convention) -- `.run()` never iterates the resource generators, so
`sql_table(..., defer_table_reflect=True)` never opens a real DB
connection, `rest_api_source(...)` never issues a real HTTP request, and
`filesystem(...)` never lists a real bucket.
"""
import importlib.util
import pathlib
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Dict, List, Optional

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "dlt_load_collection_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeCursor:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    @property
    def description(self):
        return [(col,) for col in self._df.columns]

    def fetchall(self):
        return list(self._df.itertuples(index=False, name=None))


class FakeSqlClient:
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
        for table_name, df in self.table_rows.items():
            if sql.strip().endswith(f".{table_name}"):
                yield FakeCursor(df)
                return
        yield FakeCursor(pd.DataFrame())


class FakePipeline:
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
    """Fakes dlt.pipeline(...) so the asset body never touches a real DuckDB
    file, DB connection, HTTP request, or bucket listing."""
    fake_pipeline = FakePipeline(table_rows=table_rows)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    return fake_pipeline
