"""Shared test helpers for GoogleAdsIngestionComponent.

`dlt.sources.google_ads` is a dlt VERIFIED source -- not part of the pip-installed
`dlt` package itself, normally vendored into a consuming project via
`dlt init`. Not present in this repo's own dev environment, so a fake
module is injected into sys.modules (same lazy-optional-dependency
convention as azure-storage-blob/google-cloud-storage/paramiko elsewhere
in this repo) rather than mocked via unittest.mock.patch.

The dlt pipeline run + sql_client() query-back are faked wholesale (this
repo's "mock only the paid call" convention) -- everything this component
actually owns (destination/staging resolution, resource dispatch,
DataFrame concatenation, partition filtering, preview metadata) is
exercised for real.
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
        "google_ads_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Permissive:
    """Generic duck-typed stand-in for a dlt source object -- every
    attribute access or call returns another permissive object, so
    whatever this component does to the source object (attribute-based
    resource selection, .with_resources(*names), .ads.bind(...), etc.)
    succeeds without needing to replicate the real dlt source's actual
    shape."""

    def __getattr__(self, name):
        return self

    def __call__(self, *args, **kwargs):
        return self


class FakeCursor:
    """Stands in for the DBAPI cursor `pipeline.sql_client().execute_query()`
    yields -- `.description` (column name tuples) and `.fetchall()` (row
    tuples) are the only surface this component touches."""

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
    """Fakes dlt.pipeline(...) and dlt.sources.google_ads.{google_ads} so the
    asset body never touches the network or a real DuckDB file."""
    fake_pipeline = FakePipeline(table_rows=table_rows)

    def fake_pipeline_factory(**kwargs):
        fake_pipeline.pipeline_kwargs = kwargs
        return fake_pipeline

    fake_mod = types.ModuleType("dlt.sources.google_ads")
    fake_mod.google_ads = lambda *a, **kw: _Permissive()
    monkeypatch.setitem(sys.modules, "dlt.sources.google_ads", fake_mod)
    monkeypatch.setattr(mod.dlt, "pipeline", fake_pipeline_factory)
    return fake_pipeline
