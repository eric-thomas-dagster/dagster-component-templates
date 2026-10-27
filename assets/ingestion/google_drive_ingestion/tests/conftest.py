"""Shared test helpers for GoogleDriveIngestionComponent.

Same convention as google_sheets_ingestion's tests: `google-auth` IS really
installed here, so `service_account.Credentials.from_service_account_info` is
patched directly via monkeypatch.setattr; `google-api-python-client` is NOT
installed in this repo's own dev environment, so a minimal fake
`googleapiclient` module (covering `.discovery.build` and
`.http.MediaIoBaseDownload`, both of which this component imports) is injected
into `sys.modules` instead.
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType
from typing import Dict, List, Optional


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "google_drive_ingestion_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeExecute:
    def __init__(self, result: dict):
        self._result = result

    def execute(self):
        return self._result


class _FakeMediaRequest:
    """Stands in for the real `svc.files().get_media(fileId=...)` request object --
    `_FakeMediaIoBaseDownload` reads the bytes straight off it."""

    def __init__(self, data: bytes):
        self.media_data = data


class _FakeMediaIoBaseDownload:
    """Stands in for googleapiclient.http.MediaIoBaseDownload: writes the fake
    request's bytes into the caller's io.BytesIO buffer in one "chunk"."""

    def __init__(self, buf, request):
        self._buf = buf
        self._data = request.media_data

    def next_chunk(self):
        self._buf.write(self._data)
        return None, True


class _FakeFilesCall:
    def __init__(self, files: List[dict], media_by_id: Dict[str, bytes]):
        self._files = files
        self._media_by_id = media_by_id

    def list(self, q=None, fields=None, pageSize=None, pageToken=None):
        return _FakeExecute({"files": self._files, "nextPageToken": None})

    def get_media(self, fileId):
        return _FakeMediaRequest(self._media_by_id.get(fileId, b""))


class FakeDriveService:
    """Maps a canned `files.list` response plus a fileId->bytes map (for
    `files.get_media`), matching the real Drive v3 API's shapes."""

    def __init__(self, files: List[dict], media_by_id: Optional[Dict[str, bytes]] = None):
        self._files_call = _FakeFilesCall(files, media_by_id or {})

    def files(self):
        return self._files_call


def install_fake_google_drive(monkeypatch, files: List[dict], media_by_id: Optional[Dict[str, bytes]] = None):
    from google.oauth2 import service_account

    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        classmethod(lambda cls, info, scopes=None: object()),
    )

    fake_discovery = types.ModuleType("googleapiclient.discovery")
    fake_discovery.build = lambda *a, **kw: FakeDriveService(files, media_by_id)

    fake_http = types.ModuleType("googleapiclient.http")
    fake_http.MediaIoBaseDownload = _FakeMediaIoBaseDownload

    fake_googleapiclient = types.ModuleType("googleapiclient")
    fake_googleapiclient.discovery = fake_discovery
    fake_googleapiclient.http = fake_http

    monkeypatch.setitem(sys.modules, "googleapiclient", fake_googleapiclient)
    monkeypatch.setitem(sys.modules, "googleapiclient.discovery", fake_discovery)
    monkeypatch.setitem(sys.modules, "googleapiclient.http", fake_http)
