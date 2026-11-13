"""Tests for GithubDocumentsComponent.

Mocks ONLY `requests.Session.send` (the actual HTTP call) -- per this repo's "mock
only the paid/external call" testing convention (see gitlab_ingestion/tests/
test_gitlab_pagination.py) -- and lets the real `requests.Session.get` build the
PreparedRequest, the real dlt pipeline run against in-memory DuckDB, and the real
Dagster asset materialization happen end to end.

Real API details this component is built against (docs.github.com/en/rest, verified
2026-11): `GET git/trees/{ref}?recursive=1` returns a `tree` array of
`{path, type, ...}` (`type=='blob'` is a file); `GET contents/{path}?ref=...` returns
base64-wrapped `content`; `GET issues?state=all` returns both issues and PRs from one
endpoint, PRs tagged by a `pull_request` key; pagination is the standard RFC-5988
`Link` header (`rel="next"`), parsed natively by `requests` into `response.links`.
"""
import base64
import importlib.util
import json
import pathlib
import sys
import urllib.parse

import dagster as dg
import dlt
import pandas as pd
import pytest
import requests

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("github_documents_component", _COMPONENT_PY)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
# Registering in sys.modules BEFORE exec_module is required: the component's
# @dlt.resource-decorated functions are defined inside nested closures (not at module
# level), and dlt's decorator internals (dlt.common.reflection.spec.spec_from_signature)
# resolve the defining module via sys.modules[fn.__module__] -- None (crashing with
# AttributeError: 'NoneType' object has no attribute '__name__') unless this is set
# first (same gotcha firecrawl_ingestion/tests/conftest.py documents).
sys.modules[_spec.name] = _mod
_spec.loader.exec_module(_mod)

GithubDocumentsComponent = _mod.GithubDocumentsComponent


@pytest.fixture(autouse=True)
def _isolate_duckdb_pipeline_dir(tmp_path, monkeypatch):
    """Same isolation gitlab_ingestion/tests/test_gitlab_pagination.py uses: a real dlt
    pipeline run writes a DuckDB file + persistent pipeline state outside the cwd by
    default (~/.dlt/pipelines/<name>) -- chdir into a fresh tmp_path and redirect
    DLT_DATA_DIR so no test run ever touches ~/.dlt or accumulates rows across runs
    sharing a pipeline_name. Also disables dlt's own telemetry ping so it doesn't
    show up as an unexpected call against our mocked Session.send."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")


def _build_component(**overrides):
    defaults = dict(asset_name="test_asset", owner="acme", repo="widgets")
    defaults.update(overrides)
    return GithubDocumentsComponent(**defaults)


def _resp(url, json_body=None, text_body=None, headers=None, status=200):
    r = requests.Response()
    r.status_code = status
    r.url = url
    if json_body is not None:
        r._content = json.dumps(json_body).encode()
    elif text_body is not None:
        r._content = text_body.encode()
    else:
        r._content = b"{}"
    if headers:
        r.headers.update(headers)
    return r


class _FakeGithubBackend:
    """Routes requests.Session.send by URL shape to canned tree/contents/issues
    responses, and records every URL for assertions (pagination counts, etc.)."""

    def __init__(self, tree_entries=None, file_contents=None, issue_pages=None, truncated=False):
        self.tree_entries = tree_entries or []
        self.file_contents = file_contents or {}
        self.issue_pages = issue_pages if issue_pages is not None else [[]]
        self.truncated = truncated
        self.calls = []
        self._issues_page_idx = 0

    def send(self, prepared_request, **kwargs):
        # Bound method assigned directly onto requests.Session.send (see call sites below) --
        # `self` here is the _FakeGithubBackend instance, not the Session, since a bound
        # method isn't re-descriptor-bound when assigned as a class attribute.
        url = prepared_request.url
        self.calls.append(url)
        if "/git/trees/" in url:
            return _resp(url, json_body={"tree": self.tree_entries, "truncated": self.truncated})
        if "/contents/" in url:
            path = urllib.parse.unquote(url.split("/contents/", 1)[1].split("?", 1)[0])
            content = self.file_contents.get(path)
            if content is None:
                return _resp(url, json_body={}, status=404)
            b64 = base64.b64encode(content.encode()).decode()
            b64 = b64[:8] + "\n" + b64[8:]  # mimic GitHub's real newline-wrapped base64
            return _resp(url, json_body={
                "content": b64,
                "encoding": "base64",
                "html_url": f"https://github.com/acme/widgets/blob/main/{path}",
                "sha": "deadbeef",
                "size": len(content),
            })
        if "/issues" in url:
            idx = min(self._issues_page_idx, len(self.issue_pages) - 1)
            items = self.issue_pages[idx]
            headers = {}
            if idx < len(self.issue_pages) - 1:
                next_url = f"https://api.github.com/repos/acme/widgets/issues?page={idx + 2}"
                headers["Link"] = f'<{next_url}>; rel="next"'
            self._issues_page_idx += 1
            return _resp(url, json_body=items, headers=headers)
        raise AssertionError(f"unexpected URL in test backend: {url}")


def _materialize(component, selection=None, partition_key=None):
    asset_def = component.build_defs(context=None).assets[0]
    kwargs = {"resources": {"io_manager": dg.InMemoryIOManager()}}
    if selection is not None:
        kwargs["selection"] = selection
    if partition_key is not None:
        kwargs["partition_key"] = partition_key
    result = dg.materialize([asset_def], **kwargs)
    return asset_def, result


# --- build_defs / validation -------------------------------------------------


def test_build_defs_does_not_raise_with_no_token():
    component = _build_component(access_token_env_var=None)
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_missing_token_env_var_fails_the_run_not_the_location(monkeypatch):
    """access_token_env_var is set but the env var itself is unset -- this must fail at
    materialize time (a clear KeyError naming the var), not at Definitions-load time."""
    backend = _FakeGithubBackend()
    component = _build_component(access_token_env_var="GH_TOKEN_NOT_SET", resources="issues")
    asset_def = component.build_defs(context=None).assets[0]  # must not raise

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        with pytest.raises(Exception) as exc_info:
            dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "GH_TOKEN_NOT_SET" in str(exc_info.value)


def test_blank_resources_raises_a_clear_error():
    component = _build_component(resources="  ,  ")
    with pytest.raises(ValueError, match="'resources' must list at least one"):
        component.build_defs(context=None)


def test_unknown_resource_raises_a_clear_error():
    component = _build_component(resources="files,bogus")
    with pytest.raises(ValueError, match="unknown entries"):
        component.build_defs(context=None)


def test_table_name_map_with_an_unknown_resource_raises_a_clear_error():
    component = _build_component(resources="files", table_name_map={"issues": "raw_issues"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)


# --- 'files': tree walk + content fetch + extension/path_prefix filtering ----


def test_file_tree_walk_filters_by_extension_and_builds_contract_rows(monkeypatch):
    backend = _FakeGithubBackend(
        tree_entries=[
            {"path": "README.md", "type": "blob", "sha": "a"},
            {"path": "src/main.py", "type": "blob", "sha": "b"},  # filtered: wrong extension
            {"path": "docs", "type": "tree", "sha": "c"},          # filtered: a directory
            {"path": "docs/guide.md", "type": "blob", "sha": "d"},
        ],
        file_contents={
            "README.md": "# Widgets\n\nReal content.",
            "docs/guide.md": "# Guide",
        },
    )
    component = _build_component(resources="files", file_extensions=".md")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert set(df["doc_id"]) == {"README.md", "docs/guide.md"}  # .py and the directory excluded

    readme = df[df["doc_id"] == "README.md"].iloc[0]
    assert readme["content"] == "# Widgets\n\nReal content."
    assert readme["title"] == "README.md"
    assert readme["source_type"] == "github_file"
    assert readme["url"] == "https://github.com/acme/widgets/blob/main/README.md"
    import hashlib
    assert readme["content_hash"] == hashlib.sha256(readme["content"].encode("utf-8")).hexdigest()

    # every document-source-contract column is present
    for col in ("doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"):
        assert col in df.columns


def test_path_prefix_scopes_the_file_walk(monkeypatch):
    backend = _FakeGithubBackend(
        tree_entries=[
            {"path": "README.md", "type": "blob"},
            {"path": "docs/guide.md", "type": "blob"},
        ],
        file_contents={"docs/guide.md": "guide text"},
    )
    component = _build_component(resources="files", file_extensions=".md", path_prefix="docs/")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert list(df["doc_id"]) == ["docs/guide.md"]


# --- 'issues'/'prs': split from one fetch, body extraction -------------------


def test_issue_and_pr_are_split_from_one_fetch(monkeypatch):
    items = [
        {"id": 1, "number": 10, "title": "Bug report", "body": "It crashes", "html_url": "https://github.com/acme/widgets/issues/10", "updated_at": "2024-01-01T00:00:00Z", "state": "open"},
        {"id": 2, "number": 11, "title": "Fix the bug", "body": "This fixes it", "html_url": "https://github.com/acme/widgets/pull/11", "updated_at": "2024-01-02T00:00:00Z", "state": "open", "pull_request": {"url": "..."}},
    ]
    backend = _FakeGithubBackend(issue_pages=[items])
    component = _build_component(resources="issues,prs")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    issues_calls = [c for c in backend.calls if "/issues" in c]
    assert len(issues_calls) == 1  # one endpoint fetch covers both 'issues' and 'prs'

    df = result.output_for_node(asset_def.op.name)
    assert set(df["source_type"]) == {"github_issue", "github_pr"}
    issue_row = df[df["source_type"] == "github_issue"].iloc[0]
    pr_row = df[df["source_type"] == "github_pr"].iloc[0]
    assert issue_row["content"] == "It crashes"
    assert issue_row["title"] == "Bug report"
    assert pr_row["content"] == "This fixes it"
    assert pr_row["doc_id"] == "2"


def test_row_from_github_issue_handles_null_body():
    row, is_pr = _mod._row_from_github_issue({"id": 5, "title": "t", "body": None, "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"})
    assert row["content"] == ""
    assert is_pr is False


# --- asset_shape: single vs multi_asset --------------------------------------


def test_asset_shape_defaults_to_single():
    component = _build_component(resources="files,issues")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_single_shape_unions_resources_tagged_by_resource_column(monkeypatch):
    backend = _FakeGithubBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "title": "i", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
    )
    component = _build_component(resources="files,issues")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert dict(df.groupby("_resource")["doc_id"].count()) == {"files": 1, "issues": 1}


def test_multi_asset_shape_selecting_files_never_fetches_issues(monkeypatch):
    """The core multi_asset requirement: selecting just 'files' must not even
    request the issues endpoint."""
    backend = _FakeGithubBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
    )
    component = _build_component(resources="files,issues", asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_files"), dg.AssetKey("test_asset_issues")}

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        result = dg.materialize(
            [asset_def],
            resources={"io_manager": dg.InMemoryIOManager()},
            selection=[dg.AssetKey("test_asset_files")],
        )
    assert result.success
    assert not any("/issues" in c for c in backend.calls)

    files_df = result.output_for_node(asset_def.op.name, output_name="test_asset_files")
    assert list(files_df["doc_id"]) == ["README.md"]


# --- partitioning: cosmetic-partitioning regression --------------------------


def test_partition_date_column_actually_filters_the_dataframe(monkeypatch):
    backend = _FakeGithubBackend(issue_pages=[[
        {"id": 1, "title": "old", "body": "x", "html_url": "u1", "updated_at": "2023-12-01T00:00:00Z"},
        {"id": 2, "title": "new", "body": "y", "html_url": "u2", "updated_at": "2024-01-01T10:00:00Z"},
    ]])
    component = _build_component(
        resources="issues",
        partition_type="daily",
        partition_start="2024-01-01",
        partition_date_column="updated_at",
    )

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component, partition_key="2024-01-01")
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert list(df["doc_id"]) == ["2"]  # only the 2024-01-01 row, not the 2023-12-01 one


# --- write_disposition: resource hints (real dlt schema introspection) -------
#
# This component's own tests run a REAL dlt pipeline against a real (tmp_path-isolated,
# file-backed) DuckDB destination, not a faked dlt.pipeline() -- so `.apply_hints()` is
# verified here by re-attaching a fresh `dlt.pipeline()` handle (same pipeline_name/
# dataset_name) after materialization and reading the resulting table schema straight out
# of dlt's own local pipeline state, rather than inspecting a captured DltResource object.


def _reattached_table_schema(asset_name, dataset_name, table_name):
    pipeline = dlt.pipeline(pipeline_name=f"{asset_name}_pipeline", destination="duckdb", dataset_name=dataset_name)
    return pipeline.default_schema.tables[table_name]


def test_default_write_disposition_is_replace_on_every_resource_table(monkeypatch):
    backend = _FakeGithubBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "title": "i", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
    )
    component = _build_component(resources="files,issues", asset_name="wd_replace")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    for table_name in ("files", "issues"):
        table = _reattached_table_schema("wd_replace", "wd_replace", table_name)
        assert table.get("write_disposition") == "replace"
        assert table["columns"]["doc_id"].get("primary_key") is not True

    md = result.asset_materializations_for_node("wd_replace")[0].metadata
    assert md["write_disposition"].value == "replace"
    assert md["load_id"].value  # a real dlt load id string, not null


def test_merge_write_disposition_sets_primary_key_on_doc_id_on_every_resource_table(monkeypatch):
    backend = _FakeGithubBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "title": "i", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
    )
    component = _build_component(resources="files,issues", asset_name="wd_merge", write_disposition="merge")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    for table_name in ("files", "issues"):
        table = _reattached_table_schema("wd_merge", "wd_merge", table_name)
        assert table.get("write_disposition") == "merge"
        assert table["columns"]["doc_id"]["primary_key"] is True

    assert result.asset_materializations_for_node("wd_merge")[0].metadata["write_disposition"].value == "merge"


def test_append_write_disposition_is_explicit_opt_in(monkeypatch):
    backend = _FakeGithubBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
    )
    component = _build_component(resources="files", asset_name="wd_append", write_disposition="append")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    table = _reattached_table_schema("wd_append", "wd_append", "files")
    assert table.get("write_disposition") == "append"
    assert table["columns"]["doc_id"].get("primary_key") is not True
    assert result.asset_materializations_for_node("wd_append")[0].metadata["write_disposition"].value == "append"


# --- _dedupe_by_doc_id: pure-function safety net -----------------------------


def test_dedupe_by_doc_id_keeps_latest_updated_at():
    df = pd.DataFrame([
        {"doc_id": "a", "updated_at": "2024-01-01T00:00:00Z", "content": "old"},
        {"doc_id": "a", "updated_at": "2024-02-01T00:00:00Z", "content": "new"},
        {"doc_id": "b", "updated_at": "2024-01-01T00:00:00Z", "content": "b"},
    ])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("doc_id").loc["a", "content"] == "new"


def test_dedupe_by_doc_id_no_doc_id_column_is_a_no_op():
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 2


def test_dedupe_by_doc_id_empty_df_is_a_no_op():
    df = pd.DataFrame(columns=["doc_id", "content"])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 0


def test_dedupe_by_doc_id_without_updated_at_column_still_dedupes():
    df = pd.DataFrame([{"doc_id": "x", "v": 1}, {"doc_id": "x", "v": 2}])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 1


# --- real dlt + real (file-backed) duckdb: the actual regression this was fixing -----
# Every test in this file already runs a real dlt.pipeline() against a real DuckDB file
# (the component's own default destination), isolated per-test by the autouse
# `_isolate_duckdb_pipeline_dir` fixture (chdir + DLT_DATA_DIR), which also relocates
# dlt's own global pipeline-sync state (normally ~/.dlt/pipelines/<name>/) under
# tmp_path -- confirmed directly: dlt.pipeline(...).pipelines_dir resolves under
# DLT_DATA_DIR, so no extra ~/.dlt teardown is needed here (unlike a component whose
# tests fake dlt.pipeline() entirely and only drop into real dlt for one or two
# dedicated rerun tests).


def test_rerun_with_default_write_disposition_does_not_duplicate_rows(monkeypatch):
    backend = _FakeGithubBackend(issue_pages=[[
        {"id": 1, "title": "one", "body": "b1", "html_url": "u1", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": 2, "title": "two", "body": "b2", "html_url": "u2", "updated_at": "2024-01-02T00:00:00Z"},
    ]])
    component = _build_component(resources="issues", asset_name="rerun_replace")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        _, result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node("rerun_replace")[0].metadata
    assert md1["row_count"].value == 2
    assert md1["duplicate_rows_removed_total"].value == 0

    # A second, independent materialization against the SAME destination (same
    # pipeline_name/dataset_name, same on-disk duckdb file) -- this is the exact bug:
    # dlt's own default write_disposition ('append') would make this 4, not 2.
    backend._issues_page_idx = 0
    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        _, result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node("rerun_replace")[0].metadata
    assert md2["row_count"].value == 2  # NOT 4
    assert md2["duplicate_rows_removed_total"].value == 0


def test_rerun_with_append_write_disposition_accumulates_but_dedupe_safety_net_still_catches_it(monkeypatch):
    """'append' is a real, working opt-in: the destination table really does accumulate
    every run's rows (unlike 'replace'/'merge'). The per-row dedupe safety net -- applied
    regardless of write_disposition -- still catches it before the DataFrame/metadata is
    returned, so row_count stays correct; duplicate_rows_removed is what actually reveals
    that 'append' let the destination table accumulate underneath."""
    backend = _FakeGithubBackend(issue_pages=[[
        {"id": 1, "title": "one", "body": "b1", "html_url": "u1", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": 2, "title": "two", "body": "b2", "html_url": "u2", "updated_at": "2024-01-02T00:00:00Z"},
    ]])
    component = _build_component(resources="issues", asset_name="rerun_append", write_disposition="append")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        _, result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node("rerun_append")[0].metadata
    assert md1["row_count"].value == 2
    assert md1["duplicate_rows_removed_total"].value == 0

    backend._issues_page_idx = 0
    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        _, result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node("rerun_append")[0].metadata
    assert md2["row_count"].value == 2  # the dedupe safety net still returns 2, not 4...
    assert md2["duplicate_rows_removed_total"].value == 2  # ...but reveals the destination had accumulated to 4
