"""Loads component.py by file path so tests don't depend on package install."""
import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd
import pytest



def load_component_module():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("_mssql_io_manager_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def mod():
    return load_component_module()


class Backend:
    """Real sqlite via sqlalchemy; the manager's own create_engine / to_sql / read_sql_table run."""

    def __init__(self, mod, tmp_path, monkeypatch):
        import sqlalchemy as sa

        self.sa = sa
        self.dbo = tmp_path / 'dbo.db'
        self.url = f"sqlite:///{tmp_path / 'main.db'}"
        self.schema = "dbo"
        real = sa.create_engine
        attach = True

        def _engine(url, *a, **k):
            eng = real(url, *a, **k)
            if attach:
                @sa.event.listens_for(eng, "connect")
                def _on_connect(dbapi_conn, _):
                    dbapi_conn.execute(f"ATTACH DATABASE '{tmp_path / 'dbo.db'}' AS dbo")
            return eng

        monkeypatch.setattr(mod, "create_engine", _engine)
        self.manager = mod.MSSQLIOManager(connection_url=self.url, schema_name="dbo")

    @staticmethod
    def _name(asset, key):
        return asset if key is None else f"{asset}_{key}"

    def has(self, asset, key):
        eng = self.sa.create_engine(self.url)
        try:
            with eng.connect() as c:
                c.exec_driver_sql(f"ATTACH DATABASE '{self.dbo}' AS dbo")
                return self.sa.inspect(c).has_table(self._name(asset, key), schema=self.schema)
        finally:
            eng.dispose()

    def read(self, asset, key):
        eng = self.sa.create_engine(self.url)
        try:
            with eng.connect() as c:
                c.exec_driver_sql(f"ATTACH DATABASE '{self.dbo}' AS dbo")
                return pd.read_sql_table(self._name(asset, key), c, schema=self.schema)
        finally:
            eng.dispose()


@pytest.fixture
def be(mod, tmp_path, monkeypatch):
    return Backend(mod, tmp_path, monkeypatch)
