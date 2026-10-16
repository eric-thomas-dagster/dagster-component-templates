"""Test helpers for DataframeToStarRocksComponent: loads component.py via importlib; only the external service is faked."""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType

import pandas as pd


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("dataframe_to_starrocks_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def install_fake_requests_put(monkeypatch):
    """Patch requests.put (the external HTTP call); returns the recorded-calls list."""
    import requests

    calls = []

    def fake_put(url, data=None, headers=None, auth=None, **kw):
        calls.append({"url": url, "data": data, "headers": headers, "auth": auth, **kw})
        return FakeResponse({"Status": "Success", "NumberLoadedRows": 3, "NumberFilteredRows": 0,
                             "NumberTotalRows": 3, "LoadTimeMs": 7})

    monkeypatch.setattr(requests, "put", fake_put)
    return calls


class FakeFEResource:
    """Same attributes the real resource exposes to the sink: host/username/password/ssl/http_port."""

    def __init__(self, host, username, password, ssl=False, http_port=8030):
        self.host, self.username, self.password = host, username, password
        self.ssl, self.http_port = ssl, http_port
