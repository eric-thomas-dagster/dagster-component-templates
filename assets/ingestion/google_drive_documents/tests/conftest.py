"""Shared test helpers for GoogleDriveDocumentsComponent.

`install_fake_google_drive` mocks only the Drive API network boundary:
`service_account.Credentials.from_service_account_info`, `googleapiclient.discovery.build`, and
`googleapiclient.http.MediaIoBaseDownload` (google-auth / google-api-python-client ARE really
installed in this environment, so these are patched directly on the real modules via
monkeypatch.setattr -- same mechanism google_docs_extractor's own tests use via
unittest.mock.patch). The component's own extraction logic (`_extract_text_from_bytes`) always
runs for real against real fixture bytes -- nothing about parsing is faked.
"""
import importlib.util
import pathlib
import re
from types import ModuleType
from typing import Dict, List, Optional, Tuple


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    spec = importlib.util.spec_from_file_location(
        "google_drive_documents_component", component_py
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeExecute:
    def __init__(self, result):
        self._result = result

    def execute(self):
        return self._result


class _FakeMediaRequest:
    """Stands in for `svc.files().get_media(fileId=...)` -- `_FakeMediaIoBaseDownload` reads the
    bytes straight off it."""

    def __init__(self, data: bytes):
        self.media_data = data


class _FakeMediaIoBaseDownload:
    """Stands in for googleapiclient.http.MediaIoBaseDownload: writes the fake request's bytes
    into the caller's io.BytesIO buffer in one "chunk"."""

    def __init__(self, buf, request):
        self._buf = buf
        self._data = request.media_data

    def next_chunk(self):
        self._buf.write(self._data)
        return None, True


class _FakeFilesCall:
    def __init__(
        self,
        files_by_folder: Dict[str, List[dict]],
        export_by_id: Dict[Tuple[str, str], bytes],
        media_by_id: Dict[str, bytes],
    ):
        self._files_by_folder = files_by_folder
        self._export_by_id = export_by_id
        self._media_by_id = media_by_id

    def list(self, q=None, fields=None, pageSize=None, pageToken=None):
        m = re.search(r"'([^']+)' in parents", q or "")
        folder_id = m.group(1) if m else None
        files = self._files_by_folder.get(folder_id, [])
        return _FakeExecute({"files": files, "nextPageToken": None})

    def export(self, fileId, mimeType):
        return _FakeExecute(self._export_by_id.get((fileId, mimeType), b""))

    def get_media(self, fileId):
        return _FakeMediaRequest(self._media_by_id.get(fileId, b""))


class FakeDriveService:
    """Maps canned `files.list` responses (keyed by parent folder id, matching the real Drive v3
    API's `'<id>' in parents` query shape), `files.export` responses (keyed by (fileId,
    exportMimeType)), and `files.get_media` responses (keyed by fileId)."""

    def __init__(
        self,
        files_by_folder: Dict[str, List[dict]],
        export_by_id: Optional[Dict[Tuple[str, str], bytes]] = None,
        media_by_id: Optional[Dict[str, bytes]] = None,
    ):
        self._files_call = _FakeFilesCall(files_by_folder, export_by_id or {}, media_by_id or {})

    def files(self):
        return self._files_call


def install_fake_google_drive(
    monkeypatch,
    files_by_folder: Dict[str, List[dict]],
    export_by_id: Optional[Dict[Tuple[str, str], bytes]] = None,
    media_by_id: Optional[Dict[str, bytes]] = None,
):
    from google.oauth2 import service_account
    import googleapiclient.discovery as discovery
    import googleapiclient.http as http_mod

    monkeypatch.setattr(
        service_account.Credentials,
        "from_service_account_info",
        classmethod(lambda cls, info, scopes=None: object()),
    )
    fake_service = FakeDriveService(files_by_folder, export_by_id, media_by_id)
    monkeypatch.setattr(discovery, "build", lambda *a, **kw: fake_service)
    monkeypatch.setattr(http_mod, "MediaIoBaseDownload", _FakeMediaIoBaseDownload)
