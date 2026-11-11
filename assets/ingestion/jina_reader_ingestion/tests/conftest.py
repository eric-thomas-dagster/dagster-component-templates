"""Shared test helpers for JinaReaderIngestionComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed (this repo's "committed test convention": conftest.py importlib loader +
real files).

This component's only external-network boundary is `requests.get("https://r.jina.ai/...")`
inside `_fetch_jina_page` -- that is the one thing tests mock. Everything else (the dlt
pipeline, an in-memory DuckDB destination, hash_columns/exclude_columns/max_rows_per_resource/
on_schema_change, destination read-back, DataFrame/metadata assembly) runs for real through
`dagster.materialize()`, exercising the real dlt code paths rather than a hand-rolled fake.
dlt's bare `duckdb` destination (no explicit credentials -- this component's own default
when `destination` is unset) resolves to a real on-disk DuckDB file under dlt's pipelines
working directory, not a pure in-memory connection (confirmed empirically: passing the
string `":memory:"` as `destination_credentials_url` raises dlt's own
`InvalidInMemoryDuckdbCredentials` -- dlt's in-memory mode requires an actual
`duckdb.connect(":memory:")` connection OBJECT passed to the destination factory, which
this component's `_resolve_destination()` has no code path for). The `_isolated_dlt_dir`
autouse fixture below redirects dlt's pipelines/working directory into a per-test
`tmp_path` (via `DLT_DATA_DIR`) so tests never touch or pollute the real
`~/.dlt/pipelines/` directory, and so pipeline state never bleeds between test functions
that happen to reuse the same `asset_name`.
"""
import importlib.util
import pathlib
from types import ModuleType

import pytest


@pytest.fixture(autouse=True)
def _isolated_dlt_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    yield


def load_component_module() -> ModuleType:
    """dlt's `@dlt.resource` decorator inspects the decorated function's module via
    `sys.modules` (to build its SPEC base) -- it must be registered there, not just
    held as a local reference, or dlt crashes with `module.__name__` on None."""
    import sys

    here = pathlib.Path(__file__).resolve().parent.parent
    component_py = here / "component.py"
    module_name = "jina_reader_ingestion_component"
    spec = importlib.util.spec_from_file_location(module_name, component_py)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


class FakeResponse:
    """Stands in for requests.Response. `json_body=None` simulates a non-JSON body
    (resp.json() raises ValueError), falling back to `.text`."""

    def __init__(self, json_body=None, text="", status_code=200, raise_exc=None):
        self._json_body = json_body
        self.text = text
        self.status_code = status_code
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code} error")

    def json(self):
        if self._json_body is None:
            raise ValueError("no JSON body")
        return self._json_body


def install_fake_requests_get(monkeypatch, mod, responses_by_url, default=None):
    """`responses_by_url`: {url: FakeResponse} (keyed on the exact URL passed to
    `_fetch_jina_page`, i.e. the Jina *target* URL, not the r.jina.ai wrapper URL).
    `default`: FakeResponse (or a callable raising) used for any URL not in the map."""
    import requests

    def _fake_get(full_url, headers=None, timeout=None):
        target = full_url[len("https://r.jina.ai/"):]
        if target in responses_by_url:
            resp = responses_by_url[target]
        elif default is not None:
            resp = default
        else:
            raise AssertionError(f"no fake response registered for {target!r}")
        if callable(resp):
            return resp(headers=headers, timeout=timeout)
        return resp

    monkeypatch.setattr(requests, "get", _fake_get)
    # The component imports `requests` at module scope -- same module object already in
    # sys.modules, so patching the real `requests.get` here reaches it too.
    monkeypatch.setattr(mod.requests, "get", _fake_get)
