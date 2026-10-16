"""Shared test helpers for GoogleSheetsIngestionComponent.

Unlike the other cosmetic-partitioning components, this one calls the
Google Sheets API directly (not dlt) and -- with no `destination` set, the
default -- never touches dlt at all, returning the fetched DataFrame
straight from the API response. Mocks ONLY
`google.oauth2.service_account.Credentials.from_service_account_info`
(the real `google-auth` package IS installed here, so this is patched
directly rather than faked via sys.modules) and
`googleapiclient.discovery.build` (the real `google-api-python-client`
package is NOT installed in this repo's own dev environment, so a minimal
fake module is injected into sys.modules instead, same convention as
other components this session).
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType
from typing import Dict, List


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "google_sheets_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeValuesCall:
    def __init__(self, sheet_data: Dict[str, List[list]]):
        self._sheet_data = sheet_data
        self._range = None

    def get(self, spreadsheetId, range):
        self._range = range
        return self

    def execute(self):
        return {"values": self._sheet_data.get(self._range, [])}


class _FakeSpreadsheets:
    def __init__(self, sheet_data):
        self._values = _FakeValuesCall(sheet_data)

    def values(self):
        return self._values


class FakeSheetsService:
    """Maps a sheet/range name to canned [header, *rows] values, matching
    the real Sheets API's `spreadsheets().values().get(...).execute()`
    response shape: `{"values": [[...], [...]]}`."""

    def __init__(self, sheet_data: Dict[str, List[list]]):
        self._spreadsheets = _FakeSpreadsheets(sheet_data)

    def spreadsheets(self):
        return self._spreadsheets


def install_fake_google_sheets(monkeypatch, sheet_data: Dict[str, List[list]]):
    from google.oauth2 import service_account

    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        classmethod(lambda cls, info, scopes=None: object()),
    )

    fake_discovery = types.ModuleType("googleapiclient.discovery")
    fake_discovery.build = lambda *a, **kw: FakeSheetsService(sheet_data)
    fake_googleapiclient = types.ModuleType("googleapiclient")
    fake_googleapiclient.discovery = fake_discovery
    monkeypatch.setitem(sys.modules, "googleapiclient", fake_googleapiclient)
    monkeypatch.setitem(sys.modules, "googleapiclient.discovery", fake_discovery)
