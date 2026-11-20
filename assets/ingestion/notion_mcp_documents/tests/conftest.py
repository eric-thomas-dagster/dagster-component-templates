import importlib.util
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

_COMPONENT_PATH = Path(__file__).resolve().parent.parent / "component.py"


def load_component_module():
    spec = importlib.util.spec_from_file_location("notion_mcp_documents_component", _COMPONENT_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod  # dlt's @dlt.resource reflection needs module.__name__ resolvable via sys.modules
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return load_component_module()


class FakeCursor:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def df(self) -> pd.DataFrame:
        return self._df


class FakeSqlClient:
    def __init__(self, table_rows: Dict[str, pd.DataFrame]):
        self.table_rows = table_rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    @contextmanager
    def execute_query(self, sql: str):
        for table_name, df in self.table_rows.items():
            if sql.strip().endswith(f".{table_name}"):
                yield FakeCursor(df)
                return
        yield FakeCursor(pd.DataFrame())


class FakePipeline:
    """Stands in for `dlt.pipeline(...)` -- avoids touching a real DuckDB file. `run_calls[-1]`
    holds the REAL dlt resource the component built, same pattern as notion_documents's own
    conftest.py."""

    def __init__(self, table_rows: Optional[Dict[str, pd.DataFrame]] = None, **pipeline_kwargs):
        self.pipeline_kwargs = pipeline_kwargs
        self.run_calls: List[Any] = []
        self.sql_client_instance = FakeSqlClient(table_rows or {})

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
