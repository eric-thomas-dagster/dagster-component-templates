"""Tests for GitlabDocumentsComponent.

Mocks ONLY `requests.Session.send` (the actual HTTP call) -- per this repo's "mock
only the paid/external call" testing convention (see gitlab_ingestion/tests/
test_gitlab_pagination.py, this component's structural sibling) -- and lets the
real `requests.Session.get` build the PreparedRequest, the real dlt pipeline run
against in-memory DuckDB, and the real Dagster asset materialization happen end
to end.

Real API details this component is built against (docs.gitlab.com/ee/api/,
verified 2026-11): `GET repository/tree?recursive=true` returns an array of
`{path, type, ...}` (`type=='blob'` is a file); `GET repository/files/:file_path/
raw?ref=...` returns raw bytes directly (file_path URL-encoded, '/' -> '%2F');
`GET issues` / `GET merge_requests` return `description`/`title`/`web_url`/
`updated_at`; pagination is the standard RFC-5988 `Link` header (`rel="next"`),
parsed natively by `requests` into `response.links` -- the same mechanism
gitlab_ingestion.py's dlt HeaderLinkPaginator drives.
"""
import importlib.util
import json
import pathlib
import sys
import urllib.parse

import dagster as dg
import dlt
import pytest
import requests

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("gitlab_documents_component", _COMPONENT_PY)
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

GitlabDocumentsComponent = _mod.GitlabDocumentsComponent


@pytest.fixture(autouse=True)
def _isolate_duckdb_pipeline_dir(tmp_path, monkeypatch):
    """Same isolation gitlab_ingestion/tests/test_gitlab_pagination.py uses: a real dlt
    pipeline run writes a DuckDB file + persistent pipeline state outside the cwd by
    default -- chdir into a fresh tmp_path and redirect DLT_DATA_DIR so no test run
    ever touches ~/.dlt or accumulates rows across runs sharing a pipeline_name."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")


def _build_component(**overrides):
    defaults = dict(asset_name="test_asset", project_id="my-group/widgets")
    defaults.update(overrides)
    return GitlabDocumentsComponent(**defaults)


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


class _FakeGitlabBackend:
    """Routes requests.Session.send by URL shape to canned tree/raw-file/issues/MR
    responses, and records every URL for assertions (pagination counts, encoding)."""

    def __init__(self, tree_entries=None, file_contents=None, issue_pages=None, mr_pages=None):
        self.tree_entries = tree_entries or []
        self.file_contents = file_contents or {}
        self.issue_pages = issue_pages if issue_pages is not None else [[]]
        self.mr_pages = mr_pages if mr_pages is not None else [[]]
        self.calls = []
        self._issues_page_idx = 0
        self._mr_page_idx = 0

    def send(self, prepared_request, **kwargs):
        # Bound method assigned directly onto requests.Session.send -- `self` here is
        # the _FakeGitlabBackend instance (see call sites below).
        url = prepared_request.url
        self.calls.append(url)
        if "/repository/tree" in url:
            return _resp(url, json_body=self.tree_entries)
        if "/repository/files/" in url and url.endswith("/raw") or "/raw?" in url:
            encoded = url.split("/repository/files/", 1)[1].split("/raw", 1)[0]
            path = urllib.parse.unquote(encoded)
            content = self.file_contents.get(path)
            if content is None:
                return _resp(url, json_body={"message": "404 File Not Found"}, status=404)
            return _resp(url, text_body=content)
        if "/merge_requests" in url:
            idx = min(self._mr_page_idx, len(self.mr_pages) - 1)
            items = self.mr_pages[idx]
            headers = {}
            if idx < len(self.mr_pages) - 1:
                next_url = f"https://gitlab.example.com/api/v4/projects/my-group%2Fwidgets/merge_requests?page={idx + 2}"
                headers["Link"] = f'<{next_url}>; rel="next"'
            self._mr_page_idx += 1
            return _resp(url, json_body=items, headers=headers)
        if "/issues" in url:
            idx = min(self._issues_page_idx, len(self.issue_pages) - 1)
            items = self.issue_pages[idx]
            headers = {}
            if idx < len(self.issue_pages) - 1:
                next_url = f"https://gitlab.example.com/api/v4/projects/my-group%2Fwidgets/issues?page={idx + 2}"
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


def _attached_table_schema(asset_name, table_name):
    """Re-attaches the REAL dlt pipeline the component just ran (same pipeline_name, same
    working dir -- the autouse fixture's chdir()/DLT_DATA_DIR already scope this to the
    current test), and returns that table's schema dict -- this is how write_disposition/
    primary_key hints wired onto a dlt resource are verified against a real dlt run (there
    is no FakePipeline here to record .run_calls against; dlt.pipeline() itself is real)."""
    pipeline = dlt.pipeline(pipeline_name=f"{asset_name}_pipeline", destination="duckdb")
    return pipeline.default_schema.get_table(table_name)


# --- build_defs / validation -------------------------------------------------


def test_build_defs_does_not_raise_with_no_token():
    component = _build_component(access_token_env_var=None)
    defs = component.build_defs(context=None)
    assert len(defs.assets) == 1


def test_missing_token_env_var_fails_the_run_not_the_location(monkeypatch):
    """access_token_env_var is set but the env var itself is unset -- this must fail at
    materialize time (a clear KeyError naming the var), not at Definitions-load time."""
    backend = _FakeGitlabBackend()
    component = _build_component(access_token_env_var="GL_TOKEN_NOT_SET", resources="issues")
    asset_def = component.build_defs(context=None).assets[0]  # must not raise

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        with pytest.raises(Exception) as exc_info:
            dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "GL_TOKEN_NOT_SET" in str(exc_info.value)


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


# --- 'files': tree walk + raw content fetch + extension/path_prefix filtering -


def test_file_tree_walk_filters_by_extension_and_builds_contract_rows(monkeypatch):
    backend = _FakeGitlabBackend(
        tree_entries=[
            {"path": "README.md", "type": "blob"},
            {"path": "src/main.py", "type": "blob"},  # filtered: wrong extension
            {"path": "docs", "type": "tree"},          # filtered: a directory
            {"path": "docs/guide.md", "type": "blob"},
        ],
        file_contents={
            "README.md": "# Widgets\n\nReal content.",
            "docs/guide.md": "# Guide",
        },
    )
    component = _build_component(resources="files", file_extensions=".md", base_url="https://gitlab.example.com/api/v4")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert set(df["doc_id"]) == {"README.md", "docs/guide.md"}  # .py and the directory excluded

    readme = df[df["doc_id"] == "README.md"].iloc[0]
    assert readme["content"] == "# Widgets\n\nReal content."
    assert readme["title"] == "README.md"
    assert readme["source_type"] == "gitlab_file"
    # project_id is a namespace path ('my-group/widgets') -> a real blob-view URL is built
    assert readme["url"] == "https://gitlab.example.com/my-group/widgets/-/blob/HEAD/README.md"
    import hashlib
    assert readme["content_hash"] == hashlib.sha256(readme["content"].encode("utf-8")).hexdigest()

    for col in ("doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"):
        assert col in df.columns

    # file_path was URL-encoded in the raw-content request ('/' -> '%2F')
    assert any("docs%2Fguide.md" in c for c in backend.calls)


def test_path_prefix_scopes_the_file_walk(monkeypatch):
    backend = _FakeGitlabBackend(
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


def test_numeric_project_id_falls_back_to_the_raw_api_url(monkeypatch):
    """No namespace path to build a web blob-view URL from -> url falls back to the
    raw API endpoint itself (a real, fetchable URL, just not the UI page)."""
    backend = _FakeGitlabBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hi"},
    )
    component = _build_component(project_id="42", resources="files", file_extensions=".md")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert df.iloc[0]["url"] == "https://gitlab.com/api/v4/projects/42/repository/files/README.md/raw"


# --- 'issues'/'merge_requests': body extraction ------------------------------


def test_issue_and_merge_request_rows(monkeypatch):
    issues = [{"id": 1, "iid": 5, "title": "Bug", "description": "It crashes", "web_url": "https://gitlab.example.com/g/p/-/issues/5", "updated_at": "2024-01-01T00:00:00Z", "state": "opened"}]
    mrs = [{"id": 2, "iid": 7, "title": "Fix bug", "description": "This fixes it", "web_url": "https://gitlab.example.com/g/p/-/merge_requests/7", "updated_at": "2024-01-02T00:00:00Z", "state": "opened"}]
    backend = _FakeGitlabBackend(issue_pages=[issues], mr_pages=[mrs])
    component = _build_component(resources="issues,merge_requests")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert set(df["source_type"]) == {"gitlab_issue", "gitlab_merge_request"}
    issue_row = df[df["source_type"] == "gitlab_issue"].iloc[0]
    mr_row = df[df["source_type"] == "gitlab_merge_request"].iloc[0]
    assert issue_row["content"] == "It crashes"
    assert issue_row["url"] == "https://gitlab.example.com/g/p/-/issues/5"
    assert mr_row["content"] == "This fixes it"
    assert mr_row["doc_id"] == "2"


def test_row_from_gitlab_item_handles_null_description():
    row = _mod._row_from_gitlab_item({"id": 9, "title": "t", "description": None, "web_url": "u", "updated_at": "2024-01-01T00:00:00Z"}, "gitlab_issue")
    assert row["content"] == ""


# --- asset_shape: single vs multi_asset --------------------------------------


def test_asset_shape_defaults_to_single():
    component = _build_component(resources="files,issues")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_single_shape_unions_resources_tagged_by_resource_column(monkeypatch):
    backend = _FakeGitlabBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "iid": 1, "title": "i", "description": "b", "web_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
    )
    component = _build_component(resources="files,issues", file_extensions=".md")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert dict(df.groupby("_resource")["doc_id"].count()) == {"files": 1, "issues": 1}


def test_multi_asset_shape_selecting_files_never_fetches_issues(monkeypatch):
    """The core multi_asset requirement: selecting just 'files' must not even
    request the issues endpoint."""
    backend = _FakeGitlabBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
    )
    component = _build_component(resources="files,issues", asset_shape="multi_asset", file_extensions=".md")
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
    backend = _FakeGitlabBackend(issue_pages=[[
        {"id": 1, "iid": 1, "title": "old", "description": "x", "web_url": "u1", "updated_at": "2023-12-01T00:00:00Z"},
        {"id": 2, "iid": 2, "title": "new", "description": "y", "web_url": "u2", "updated_at": "2024-01-01T10:00:00Z"},
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


# --- write_disposition: wired onto every table via the real dlt pipeline -----


def _three_resource_backend():
    return _FakeGitlabBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "iid": 1, "title": "i", "description": "b", "web_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
        mr_pages=[[{"id": 2, "iid": 2, "title": "m", "description": "c", "web_url": "u2", "updated_at": "2024-01-02T00:00:00Z"}]],
    )


def test_default_write_disposition_is_replace_on_every_table(monkeypatch):
    backend = _three_resource_backend()
    component = _build_component(resources="files,issues,merge_requests", file_extensions=".md", asset_name="wd_replace_asset")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    metadata = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert metadata["write_disposition"].value == "replace"
    assert metadata["load_id"].value  # a real load id, not null

    for table_name in ("files", "issues", "merge_requests"):
        schema = _attached_table_schema("wd_replace_asset", table_name)
        assert schema["write_disposition"] == "replace"
        assert not schema["columns"]["doc_id"].get("primary_key")


def test_merge_write_disposition_sets_primary_key_on_every_table(monkeypatch):
    backend = _three_resource_backend()
    component = _build_component(
        resources="files,issues,merge_requests", file_extensions=".md",
        asset_name="wd_merge_asset", write_disposition="merge",
    )

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    metadata = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert metadata["write_disposition"].value == "merge"

    for table_name in ("files", "issues", "merge_requests"):
        schema = _attached_table_schema("wd_merge_asset", table_name)
        assert schema["write_disposition"] == "merge"
        assert schema["columns"]["doc_id"]["primary_key"] is True


def test_append_write_disposition_is_explicit_opt_in(monkeypatch):
    backend = _three_resource_backend()
    component = _build_component(
        resources="files,issues,merge_requests", file_extensions=".md",
        asset_name="wd_append_asset", write_disposition="append",
    )

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    metadata = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert metadata["write_disposition"].value == "append"

    for table_name in ("files", "issues", "merge_requests"):
        schema = _attached_table_schema("wd_append_asset", table_name)
        assert schema["write_disposition"] == "append"
        assert not schema["columns"]["doc_id"].get("primary_key")


def test_multi_asset_shape_also_wires_write_disposition_per_table(monkeypatch):
    """The two call sites (single vs multi_asset asset_shape) share the one
    _make_dlt_resources() helper -- this is a thin check that the multi_asset path gets
    the same hints, not a re-test of every mode."""
    backend = _three_resource_backend()
    component = _build_component(
        resources="files,issues", file_extensions=".md",
        asset_name="wd_multi_asset", asset_shape="multi_asset", write_disposition="merge",
    )
    asset_def = component.build_defs(context=None).assets[0]

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success

    for table_name in ("files", "issues"):
        schema = _attached_table_schema("wd_multi_asset", table_name)
        assert schema["write_disposition"] == "merge"
        assert schema["columns"]["doc_id"]["primary_key"] is True


# --- _dedupe_by_doc_id: pure-function safety net ------------------------------


def test_dedupe_by_doc_id_keeps_latest_updated_at():
    import pandas as pd

    df = pd.DataFrame([
        {"doc_id": "1", "updated_at": "2024-01-01T00:00:00Z", "content": "old"},
        {"doc_id": "1", "updated_at": "2024-02-01T00:00:00Z", "content": "new"},
        {"doc_id": "2", "updated_at": "2024-01-01T00:00:00Z", "content": "other"},
    ])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("doc_id").loc["1", "content"] == "new"


def test_dedupe_by_doc_id_no_doc_id_column_is_a_no_op():
    import pandas as pd

    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 2


def test_dedupe_by_doc_id_empty_dataframe_is_a_no_op():
    import pandas as pd

    df = pd.DataFrame(columns=["doc_id", "content"])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 0


def test_dedupe_by_doc_id_without_updated_at_column_still_dedupes():
    import pandas as pd

    df = pd.DataFrame([{"doc_id": "x", "v": 1}, {"doc_id": "x", "v": 2}])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 1


# --- real dlt + real (file-backed) duckdb: the actual regression this was fixing ------
# Re-materializing the SAME component instance twice in one test re-runs the real dlt
# pipeline against the same on-disk duckdb file (the autouse fixture's chdir()/
# DLT_DATA_DIR scope this to the test's own tmp_path, so nothing leaks across tests or
# into ~/.dlt) -- this is what actually proves row_count doesn't silently double on a
# rerun, which a test that fakes dlt.pipeline() entirely cannot show (a fake never
# persists anything between two separate runs).


def test_rerun_with_default_write_disposition_does_not_duplicate_rows(monkeypatch):
    backend = _FakeGitlabBackend(issue_pages=[[
        {"id": 1, "iid": 1, "title": "one", "description": "b1", "web_url": "u1", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": 2, "iid": 2, "title": "two", "description": "b2", "web_url": "u2", "updated_at": "2024-01-02T00:00:00Z"},
    ]])
    component = _build_component(resources="issues", asset_name="rerun_replace_asset")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        _, result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node("rerun_replace_asset")[0].metadata
    assert md1["rows_issues"].value == 2
    assert md1["duplicate_rows_removed"].value == 0

    backend2 = _FakeGitlabBackend(issue_pages=[[
        {"id": 1, "iid": 1, "title": "one", "description": "b1", "web_url": "u1", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": 2, "iid": 2, "title": "two", "description": "b2", "web_url": "u2", "updated_at": "2024-01-02T00:00:00Z"},
    ]])
    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend2.send)
        _, result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node("rerun_replace_asset")[0].metadata
    assert md2["rows_issues"].value == 2  # NOT 4 -- this is the exact bug being fixed
    assert md2["row_count"].value == 2
    assert md2["duplicate_rows_removed"].value == 0


def test_rerun_with_append_write_disposition_accumulates_but_the_dataframe_output_stays_correct(monkeypatch):
    """'append' is a real, working opt-in: the destination table really does accumulate
    every run's rows (unlike 'replace'/'merge'). But the per-row dedupe safety net (applied
    regardless of write_disposition) still catches it before the DataFrame is returned, so
    row_count stays correct either way -- duplicate_rows_removed is what reveals that
    'append' is accumulating underneath."""
    rows = [
        {"id": 1, "iid": 1, "title": "one", "description": "b1", "web_url": "u1", "updated_at": "2024-01-01T00:00:00Z"},
        {"id": 2, "iid": 2, "title": "two", "description": "b2", "web_url": "u2", "updated_at": "2024-01-02T00:00:00Z"},
    ]
    component = _build_component(resources="issues", asset_name="rerun_append_asset", write_disposition="append")

    backend1 = _FakeGitlabBackend(issue_pages=[rows])
    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend1.send)
        _, result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node("rerun_append_asset")[0].metadata
    assert md1["row_count"].value == 2
    assert md1["duplicate_rows_removed"].value == 0

    backend2 = _FakeGitlabBackend(issue_pages=[rows])
    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend2.send)
        _, result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node("rerun_append_asset")[0].metadata
    assert md2["row_count"].value == 2  # the safety net still returns the current 2 rows...
    assert md2["duplicate_rows_removed"].value == 2  # ...but reports the destination accumulated to 4
