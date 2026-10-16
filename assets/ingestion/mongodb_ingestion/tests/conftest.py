"""Shared test helpers for MongoDBIngestionComponent.

Unlike the other 23 cosmetic-partitioning components, this one builds an
INLINE dlt source (`_mongodb_source`) using the real `@dlt.source`/
`@dlt.resource` decorators -- only `dlt.pipeline(...)` needs faking (same
"mock only the paid call" convention). `pymongo` is imported eagerly at
the top of the asset body but MongoClient is never actually instantiated
in these tests (pipeline.run() is faked and never iterates the resource
generators that would open a real connection) -- pymongo isn't installed
in this repo's own dev environment, so a minimal fake module is injected
into sys.modules (same lazy-optional-dependency convention as other
components this session) purely so the import succeeds.
"""
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Dict, List, Optional

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "mongodb_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # This component applies the real @dlt.source/@dlt.resource decorators
    # directly to functions DEFINED INSIDE it (an inline dlt source) --
    # dlt's decorator internals resolve the defining module via
    # sys.modules[fn.__module__], which is None unless the loaded module is
    # registered here first (unlike every other conftest.py in this repo,
    # which fake the dlt source call itself and never hit this path).
    sys.modules[spec.name] = mod
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
    fake_pipeline = FakePipeline(table_rows=table_rows)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    if "pymongo" not in sys.modules:
        fake_pymongo = types.ModuleType("pymongo")
        fake_pymongo.MongoClient = object
        monkeypatch.setitem(sys.modules, "pymongo", fake_pymongo)

    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    return fake_pipeline
