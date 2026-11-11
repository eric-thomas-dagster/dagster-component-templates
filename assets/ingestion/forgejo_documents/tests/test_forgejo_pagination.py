"""Real-pagination test for ForgejoDocumentsComponent's 'issues'/'pulls' fetch.

Mocks ONLY `requests.Session.send` and lets the real `requests.utils.
parse_header_links`-based Link-header parsing drive `_forgejo_list_paginated`'s
pagination loop end to end, through a real dlt pipeline run against in-memory
DuckDB -- per this repo's "mock only the paid/external call" convention (mirrors
gitlab_documents/tests/test_gitlab_pagination.py, the sibling component's
equivalent test, and this repo's established GitHub/GitLab/Forgejo Link-header
pagination pattern). Confirmed from Forgejo's own prose API-usage docs
(forgejo.org/docs/.../user/api-usage/): "the Link header is returned with the
next, previous, and last page links if there are more than one pages."
"""
import importlib.util
import json
import pathlib
import sys

import dagster as dg
import pytest
import requests

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("forgejo_documents_component_pagination", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
# See test_forgejo_documents.py for why this must happen before exec_module (dlt's
# @dlt.resource reflection needs sys.modules[fn.__module__] to resolve).
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

ForgejoDocumentsComponent = _mod.ForgejoDocumentsComponent


@pytest.fixture(autouse=True)
def _isolate_duckdb_pipeline_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")


def _forgejo_calls(calls):
    return [c for c in calls if "forgejo.example.com" in c]


def _fake_send_two_pages(page_1_items, page_2_items, next_url):
    calls = []

    def _send(self, prepared_request, **kwargs):
        calls.append(prepared_request.url)
        resp = requests.Response()
        resp.status_code = 200
        resp.url = prepared_request.url
        resp.request = prepared_request
        if len(calls) == 1:
            resp.headers["Link"] = f'<{next_url}>; rel="next"'
            resp._content = json.dumps(page_1_items).encode()
        else:
            resp._content = json.dumps(page_2_items).encode()
        return resp

    return _send, calls


def test_issues_pagination_follows_link_header_across_two_pages(monkeypatch):
    page_1 = [{"id": 1, "number": 1, "title": "one", "body": "b1", "html_url": "u1", "updated_at": "2024-01-01T00:00:00Z"}]
    page_2 = [{"id": 2, "number": 2, "title": "two", "body": "b2", "html_url": "u2", "updated_at": "2024-01-02T00:00:00Z"}]
    next_url = "https://forgejo.example.com/api/v1/repos/acme/widgets/issues?page=2&limit=1&type=issues"
    fake_send, calls = _fake_send_two_pages(page_1, page_2, next_url)

    component = ForgejoDocumentsComponent(
        asset_name="pg_two_pages",
        access_token_env_var="FJ_TOKEN",
        owner="acme",
        repo="widgets",
        resources="issues",
        per_page=1,
        api_base_url="https://forgejo.example.com/api/v1",
    )
    monkeypatch.setenv("FJ_TOKEN", "fj-abc123")
    asset_def = component.build_defs(context=None).assets[0]

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", fake_send)
        result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert result.success
    forgejo_calls = _forgejo_calls(calls)
    assert len(forgejo_calls) == 2  # exactly two HTTP requests -- one per page
    assert forgejo_calls[1] == next_url  # the second request used Forgejo's own next-page Link URL verbatim

    df = result.output_for_node(asset_def.op.name)
    assert sorted(df["doc_id"].tolist()) == ["1", "2"]  # rows from BOTH pages landed


def test_no_link_header_on_first_page_stops_after_one_request(monkeypatch):
    calls = []

    def _send(self, prepared_request, **kwargs):
        calls.append(prepared_request.url)
        resp = requests.Response()
        resp.status_code = 200
        resp.url = prepared_request.url
        resp.request = prepared_request
        resp._content = json.dumps([{"id": 1, "number": 1, "title": "t", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]).encode()
        return resp

    component = ForgejoDocumentsComponent(
        asset_name="pg_single_page",
        owner="acme",
        repo="widgets",
        resources="issues",
        api_base_url="https://forgejo.example.com/api/v1",
    )
    asset_def = component.build_defs(context=None).assets[0]

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _send)
        result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert result.success
    assert len(_forgejo_calls(calls)) == 1
    df = result.output_for_node(asset_def.op.name)
    assert list(df["doc_id"]) == ["1"]


def test_token_header_is_sent_on_every_request(monkeypatch):
    monkeypatch.setenv("FJ_TOKEN", "fj-secret-value")
    seen_headers = []

    def _send(self, prepared_request, **kwargs):
        seen_headers.append(dict(prepared_request.headers))
        resp = requests.Response()
        resp.status_code = 200
        resp.url = prepared_request.url
        resp.request = prepared_request
        resp._content = b"[]"
        return resp

    component = ForgejoDocumentsComponent(
        asset_name="pg_token_header",
        access_token_env_var="FJ_TOKEN",
        owner="acme",
        repo="widgets",
        resources="issues",
        api_base_url="https://forgejo.example.com/api/v1",
    )
    asset_def = component.build_defs(context=None).assets[0]

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _send)
        result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert result.success
    # Forgejo's own documented convention: the literal word "token" before the value --
    # NOT GitHub's "Bearer" scheme, NOT GitLab's dedicated "PRIVATE-TOKEN" header.
    assert seen_headers[0]["Authorization"] == "token fj-secret-value"
