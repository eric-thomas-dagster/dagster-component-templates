"""Real-pagination test for GitlabIngestionComponent.

Unlike test_gitlab_ingestion.py (which fakes dlt.sources.rest_api.rest_api_source
entirely to inspect the REST API config dict), this file mocks ONLY the actual HTTP
call (`requests.Session.send`) and lets the real dlt RESTClient + HeaderLinkPaginator
+ an in-memory DuckDB pipeline run end-to-end -- per this repo's "mock only the
paid/external call" testing convention.

GitLab's pagination (confirmed against docs.gitlab.com/ee/api/rest/, 2026-11): the
default offset-pagination response carries a standard RFC-5988 `Link` header with
`rel="next"` giving the next page's full URL -- the same mechanism GitHub's API
uses. dlt's `HeaderLinkPaginator` is built exactly for this and is what
GitlabIngestionComponent wires up (see component.py's `_build_pipeline_config`).
This test serves two pages (a Link header on the first response, none on the
second) and asserts: (a) exactly two HTTP requests were made, (b) the second
request's URL is the exact next-page URL GitLab's Link header specified, and (c)
every row across both pages lands in the materialized DataFrame.
"""
import importlib.util
import json
import pathlib
from typing import List

import dagster as dg
import pytest
import requests

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("gitlab_ingestion_component_pagination", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

GitlabIngestionComponent = _mod.GitlabIngestionComponent


@pytest.fixture(autouse=True)
def _isolate_duckdb_pipeline_dir(tmp_path, monkeypatch):
    """The real dlt pipeline run in this file writes a DuckDB file to the current
    working directory, AND dlt tracks each named pipeline's state/schema in a
    persistent directory OUTSIDE the cwd (~/.dlt/pipelines/<pipeline_name> by default)
    that survives across test runs -- real bug caught while writing this file: two
    tests sharing one asset_name (hence one pipeline_name) silently accumulated rows
    across runs via dlt's default 'append' write disposition. Fixed two ways, belt
    and suspenders: each test below uses its OWN asset_name (so no pipeline_name is
    ever shared), and this fixture also chdirs into a pytest tmp_path (for the DuckDB
    file) and points DLT_DATA_DIR at a fresh tmp_path subdirectory (for pipeline
    state) so a run never touches ~/.dlt at all. Also disables dlt's own telemetry
    ping (it otherwise shares the same monkeypatched requests.Session.send and would
    pollute the captured call list with unrelated https://telemetry.scalevector.ai
    requests)."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")


def _gitlab_calls(calls):
    """Filter out dlt's own telemetry pings, keeping only requests made to the GitLab API."""
    return [c for c in calls if "gitlab.example.com" in c]


def _fake_send_two_pages(page_1_rows, page_2_rows, next_url):
    """Returns a `requests.Session.send` replacement serving `page_1_rows` with a Link:
    rel="next" header pointing at `next_url`, then `page_2_rows` with no Link header
    (signaling the paginator to stop).

    Real bug caught here: this used to decide "page 1 vs page 2" by raw call
    position (`len(calls) == 1`) -- but dlt sends its own telemetry ping over this
    same monkeypatched `requests.Session.send`, and whether that ping lands BEFORE
    or after the real GitLab request depends on dlt's own internal timing, which in
    turn depends on what else already ran earlier in the same pytest process (e.g.
    a real dlt pipeline in another test file). When the ping won the race, the fake
    Link header landed on the telemetry response instead of the first real GitLab
    page, so the actual first GitLab request was served "page 2" content with no
    Link header -- the paginator correctly stopped after one page, because nothing
    ever told it there was a next one. Counting only requests to gitlab.example.com
    (same filter `_gitlab_calls` already applies for assertions) makes the fake
    immune to how many other requests interleave, in whatever order."""
    calls = []
    gitlab_calls: List[str] = []

    def _send(self, prepared_request, **kwargs):
        calls.append(prepared_request.url)
        resp = requests.Response()
        resp.status_code = 200
        resp.url = prepared_request.url
        resp.request = prepared_request
        if "gitlab.example.com" not in prepared_request.url:
            resp._content = b"{}"  # dlt's telemetry ping (or anything else unrelated) -- ignored
            return resp
        gitlab_calls.append(prepared_request.url)
        if len(gitlab_calls) == 1:
            resp.headers["Link"] = f'<{next_url}>; rel="next"'
            resp._content = json.dumps(page_1_rows).encode()
        else:
            resp._content = json.dumps(page_2_rows).encode()
        return resp

    return _send, calls


def test_header_link_paginator_follows_rel_next_across_two_pages(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")

    page_1 = [{"iid": 1, "title": "one"}, {"iid": 2, "title": "two"}]
    page_2 = [{"iid": 3, "title": "three"}]
    next_url = "https://gitlab.example.com/api/v4/projects/my-group%2Fproj/issues?page=2&per_page=2"
    fake_send, calls = _fake_send_two_pages(page_1, page_2, next_url)

    component = GitlabIngestionComponent(
        asset_name="pg_two_pages",
        access_token_env_var="GL_TOKEN",
        project_id="my-group/proj",
        tables="issues",
        per_page=2,
        base_url="https://gitlab.example.com/api/v4",
    )
    asset_def = component.build_defs(context=None).assets[0]

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", fake_send)
        result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert result.success
    gitlab_calls = _gitlab_calls(calls)
    assert len(gitlab_calls) == 2  # exactly two HTTP requests -- one per page
    assert gitlab_calls[1] == next_url  # the second request used GitLab's own next-page Link URL verbatim
    assert "PRIVATE-TOKEN" not in gitlab_calls[0]  # token goes in a header, never the URL

    df = result.output_for_node(asset_def.op.name)
    assert sorted(df["iid"].tolist()) == [1, 2, 3]  # rows from BOTH pages landed


def test_private_token_header_is_sent_on_every_request(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-secret-value")

    page_1 = [{"iid": 1}]
    fake_send, calls = _fake_send_two_pages(page_1, [], "https://gitlab.example.com/api/v4/projects/1/issues?page=2")
    seen_headers = []

    def _send_capturing(self, prepared_request, **kwargs):
        # Same bug class as _fake_send_two_pages above: only record headers for the
        # real GitLab calls -- dlt's own telemetry ping goes through this same
        # monkeypatched send and has no PRIVATE-TOKEN header at all, so letting it
        # land in seen_headers[0] would check the wrong request entirely.
        if "gitlab.example.com" in prepared_request.url:
            seen_headers.append(dict(prepared_request.headers))
        return fake_send(self, prepared_request, **kwargs)

    component = GitlabIngestionComponent(
        asset_name="pg_token_header",
        access_token_env_var="GL_TOKEN",
        project_id="1",
        tables="issues",
        base_url="https://gitlab.example.com/api/v4",
    )
    asset_def = component.build_defs(context=None).assets[0]

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _send_capturing)
        result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert result.success
    assert seen_headers[0]["PRIVATE-TOKEN"] == "glpat-secret-value"


def test_no_link_header_on_first_page_stops_after_one_request(monkeypatch):
    monkeypatch.setenv("GL_TOKEN", "glpat-abc123")
    calls = []

    def _send(self, prepared_request, **kwargs):
        calls.append(prepared_request.url)
        resp = requests.Response()
        resp.status_code = 200
        resp.url = prepared_request.url
        resp.request = prepared_request
        resp._content = json.dumps([{"iid": 1}]).encode()
        return resp

    component = GitlabIngestionComponent(
        asset_name="pg_single_page",
        access_token_env_var="GL_TOKEN",
        project_id="1",
        tables="issues",
        base_url="https://gitlab.example.com/api/v4",
    )
    asset_def = component.build_defs(context=None).assets[0]

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _send)
        result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert result.success
    assert len(_gitlab_calls(calls)) == 1
    df = result.output_for_node(asset_def.op.name)
    assert list(df["iid"]) == [1]
