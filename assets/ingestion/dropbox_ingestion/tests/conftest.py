"""Shared test helpers for DropboxIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed (this repo's "committed test convention": conftest.py importlib loader +
real files, mock only the paid call).

Like jina_reader_ingestion's tests, the real dlt pipeline runs against a real
(file-based, tmp_path-isolated) DuckDB destination -- only `requests.post` (the two
calls that would otherwise hit the real Dropbox API: the OAuth2 token exchange and
`files/list_folder`/`files/list_folder/continue`) is mocked. dlt's bare `duckdb`
destination (no explicit credentials -- this component's own default when
`destination` is unset) resolves to a real on-disk DuckDB file, not a pure in-memory
connection, which is exactly how the "rerun doubles row_count" bug this fix addresses
shows up in practice. The `_isolated_dlt_dir` autouse fixture below redirects dlt's
pipelines/working directory into a per-test `tmp_path` (via `DLT_DATA_DIR`) so tests
never touch or pollute the real `~/.dlt/pipelines/` directory (a GLOBAL cache keyed by
pipeline_name, independent of cwd/tmp_path), and so pipeline state never bleeds between
test functions that happen to reuse the same `asset_name`.
"""
import importlib.util
import pathlib
import shutil
from types import ModuleType

import pytest


@pytest.fixture(autouse=True)
def _isolated_dlt_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    # Belt-and-suspenders: DLT_DATA_DIR covers fresh test processes, but reset any
    # stale global pipeline-sync state too, in case it was ever created outside this
    # fixture (e.g. a local interactive run against the real default "duckdb" target).
    shutil.rmtree(pathlib.Path.home() / ".dlt" / "pipelines" / "test_asset_pipeline", ignore_errors=True)
    yield


def load_component_module() -> ModuleType:
    """dlt's `@dlt.resource` decorator inspects the decorated function's module via
    `sys.modules` (to build its SPEC base) -- it must be registered there, not just
    held as a local reference, or dlt crashes with `module.__name__` on None."""
    import sys

    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    module_name = "dropbox_ingestion_component"
    spec = importlib.util.spec_from_file_location(module_name, component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    """Stands in for requests.Response."""

    def __init__(self, json_body=None, status_code=200, raise_exc=None):
        self._json_body = json_body if json_body is not None else {}
        self.status_code = status_code
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code} error")

    def json(self):
        return self._json_body


def install_fake_dropbox_api(monkeypatch, mod, list_folder_pages, access_token="fake-access-token"):
    """Fakes both real Dropbox API calls this component makes via `requests.post`:
    the OAuth2 refresh-token exchange (`oauth2/token`) and the cursor-paginated
    `files/list_folder` / `files/list_folder/continue` walk.

    `list_folder_pages`: a list of raw Dropbox `/files/list_folder` response dicts
    (each `{"entries": [...], "cursor": ..., "has_more": bool}`), consumed in order --
    the first element answers the initial `files/list_folder` call, subsequent
    elements answer successive `files/list_folder/continue` calls. Calling this again
    (e.g. between two materializations in the same test) resets the page sequence, so
    a second run can be fed a different listing.
    """
    import requests

    pages = list(list_folder_pages)
    state = {"next_page": 0}

    def _fake_post(url, json=None, data=None, headers=None, timeout=None):
        if url.endswith("/oauth2/token"):
            return FakeResponse(json_body={"access_token": access_token})
        if url.endswith("/files/list_folder") or url.endswith("/files/list_folder/continue"):
            idx = state["next_page"]
            state["next_page"] += 1
            if idx >= len(pages):
                return FakeResponse(json_body={"entries": [], "cursor": None, "has_more": False})
            return FakeResponse(json_body=pages[idx])
        raise AssertionError(f"no fake response registered for POST {url!r}")

    monkeypatch.setattr(requests, "post", _fake_post)
    # The component imports `requests` at module scope -- same module object already in
    # sys.modules, so patching the real `requests.post` here reaches it too.
    monkeypatch.setattr(mod.requests, "post", _fake_post)
    return state
