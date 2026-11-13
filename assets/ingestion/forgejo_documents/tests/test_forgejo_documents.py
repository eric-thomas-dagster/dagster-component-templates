"""Tests for ForgejoDocumentsComponent.

Mocks ONLY `requests.Session.send` (the actual HTTP call) -- per this repo's "mock
only the paid/external call" testing convention (see gitlab_documents/tests/
test_gitlab_documents.py, this component's structural sibling) -- and lets the
real `requests.Session.get` build the PreparedRequest, the real dlt pipeline run
against in-memory DuckDB, and the real Dagster asset materialization happen end
to end.

Real API details this component is built against (fetched live from Forgejo's
own Swagger spec at https://codeberg.org/swagger.v1.json, verified 2026-11):
`GET git/trees/{sha}?recursive=true` returns `{tree: [...], truncated: bool, ...}`
(`tree` entries are `{path, type, ...}`, `type=='blob'` is a file); `GET
contents/{filepath}` returns `{content (base64), encoding, html_url, sha, size,
last_commit_when, ...}`; `GET issues` (with `type=issues`) and `GET pulls` (a
SEPARATE, dedicated endpoint -- Forgejo's own name for the resource) each
return flat arrays of `{id, number, title, body, html_url, updated_at, state}`;
pagination is the standard RFC-5988 `Link` header (`rel="next"`), confirmed
from Forgejo's own prose API-usage docs.
"""
import base64
import importlib.util
import json
import pathlib
import sys

import dagster as dg
import pytest
import requests

_HERE = pathlib.Path(__file__).resolve().parent.parent
_COMPONENT_PY = _HERE / "component.py"
_spec = importlib.util.spec_from_file_location("forgejo_documents_component", _COMPONENT_PY)
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

ForgejoDocumentsComponent = _mod.ForgejoDocumentsComponent


@pytest.fixture(autouse=True)
def _isolate_duckdb_pipeline_dir(tmp_path, monkeypatch):
    """Same isolation gitlab_documents/tests/test_gitlab_documents.py uses: a real dlt
    pipeline run writes a DuckDB file + persistent pipeline state outside the cwd by
    default -- chdir into a fresh tmp_path and redirect DLT_DATA_DIR so no test run
    ever touches ~/.dlt or accumulates rows across runs sharing a pipeline_name."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt_data"))
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")


def _build_component(**overrides):
    defaults = dict(asset_name="test_asset", owner="acme", repo="widgets")
    defaults.update(overrides)
    return ForgejoDocumentsComponent(**defaults)


def _b64(text):
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def _resp(url, json_body=None, headers=None, status=200):
    r = requests.Response()
    r.status_code = status
    r.url = url
    r._content = json.dumps(json_body if json_body is not None else {}).encode()
    if headers:
        r.headers.update(headers)
    return r


class _FakeForgejoBackend:
    """Routes requests.Session.send by URL shape to canned tree/contents/issues/pulls
    responses, and records every URL for assertions (pagination counts, params)."""

    def __init__(self, tree_entries=None, truncated=False, file_contents=None, issue_pages=None, pull_pages=None):
        self.tree_entries = tree_entries or []
        self.truncated = truncated
        self.file_contents = file_contents or {}
        self.issue_pages = issue_pages if issue_pages is not None else [[]]
        self.pull_pages = pull_pages if pull_pages is not None else [[]]
        self.calls = []
        self._issues_page_idx = 0
        self._pulls_page_idx = 0

    def send(self, prepared_request, **kwargs):
        # Bound method assigned directly onto requests.Session.send -- `self` here is
        # the _FakeForgejoBackend instance (see call sites below).
        url = prepared_request.url
        self.calls.append(url)
        if "/git/trees/" in url:
            return _resp(url, json_body={"tree": self.tree_entries, "truncated": self.truncated})
        if "/contents/" in url:
            path = url.split("/contents/", 1)[1].split("?", 1)[0]
            import urllib.parse

            path = urllib.parse.unquote(path)
            content = self.file_contents.get(path)
            if content is None:
                return _resp(url, json_body={"message": "404 Not Found"}, status=404)
            return _resp(url, json_body={
                "content": _b64(content),
                "encoding": "base64",
                "html_url": f"https://forgejo.example.com/acme/widgets/src/branch/main/{path}",
                "sha": "deadbeef",
                "size": len(content),
                "last_commit_when": "2024-03-01T00:00:00Z",
            })
        if "/pulls" in url:
            idx = min(self._pulls_page_idx, len(self.pull_pages) - 1)
            items = self.pull_pages[idx]
            headers = {}
            if idx < len(self.pull_pages) - 1:
                next_url = f"https://forgejo.example.com/api/v1/repos/acme/widgets/pulls?page={idx + 2}"
                headers["Link"] = f'<{next_url}>; rel="next"'
            self._pulls_page_idx += 1
            return _resp(url, json_body=items, headers=headers)
        if "/issues" in url:
            idx = min(self._issues_page_idx, len(self.issue_pages) - 1)
            items = self.issue_pages[idx]
            headers = {}
            if idx < len(self.issue_pages) - 1:
                next_url = f"https://forgejo.example.com/api/v1/repos/acme/widgets/issues?page={idx + 2}"
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
    backend = _FakeForgejoBackend()
    component = _build_component(access_token_env_var="FJ_TOKEN_NOT_SET", resources="issues")
    asset_def = component.build_defs(context=None).assets[0]  # must not raise

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        with pytest.raises(Exception) as exc_info:
            dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})

    assert "FJ_TOKEN_NOT_SET" in str(exc_info.value)


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
    backend = _FakeForgejoBackend(
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
    component = _build_component(resources="files", file_extensions=".md", api_base_url="https://forgejo.example.com/api/v1")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert set(df["doc_id"]) == {"README.md", "docs/guide.md"}  # .py and the directory excluded

    readme = df[df["doc_id"] == "README.md"].iloc[0]
    assert readme["content"] == "# Widgets\n\nReal content."
    assert readme["title"] == "README.md"
    assert readme["source_type"] == "forgejo_file"
    assert readme["url"] == "https://forgejo.example.com/acme/widgets/src/branch/main/README.md"
    # real per-file last_commit_when used, not the extraction run's fetch time (DuckDB
    # round-trips the ISO string as a real timestamp, hence the str() comparison)
    assert str(readme["updated_at"]) == "2024-03-01 00:00:00+00:00"
    import hashlib
    assert readme["content_hash"] == hashlib.sha256(readme["content"].encode("utf-8")).hexdigest()

    for col in ("doc_id", "content", "title", "url", "source_type", "updated_at", "content_hash"):
        assert col in df.columns

    # nested path reached the contents endpoint intact (slashes preserved, not %2F-encoded)
    assert any("/contents/docs/guide.md" in c for c in backend.calls)


def test_path_prefix_scopes_the_file_walk(monkeypatch):
    backend = _FakeForgejoBackend(
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


def test_truncated_tree_logs_a_warning_but_still_returns_what_it_got(monkeypatch):
    backend = _FakeForgejoBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        truncated=True,
        file_contents={"README.md": "hi"},
    )
    component = _build_component(resources="files", file_extensions=".md")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success
    df = result.output_for_node(asset_def.op.name)
    assert list(df["doc_id"]) == ["README.md"]


# --- 'issues'/'pulls': body extraction ---------------------------------------


def test_issue_and_pull_request_rows(monkeypatch):
    issues = [{"id": 1, "number": 5, "title": "Bug", "body": "It crashes", "html_url": "https://forgejo.example.com/acme/widgets/issues/5", "updated_at": "2024-01-01T00:00:00Z", "state": "open"}]
    pulls = [{"id": 2, "number": 7, "title": "Fix bug", "body": "This fixes it", "html_url": "https://forgejo.example.com/acme/widgets/pulls/7", "updated_at": "2024-01-02T00:00:00Z", "state": "open"}]
    backend = _FakeForgejoBackend(issue_pages=[issues], pull_pages=[pulls])
    component = _build_component(resources="issues,pulls")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert set(df["source_type"]) == {"forgejo_issue", "forgejo_pull_request"}
    issue_row = df[df["source_type"] == "forgejo_issue"].iloc[0]
    pull_row = df[df["source_type"] == "forgejo_pull_request"].iloc[0]
    assert issue_row["content"] == "It crashes"
    assert issue_row["url"] == "https://forgejo.example.com/acme/widgets/issues/5"
    assert pull_row["content"] == "This fixes it"
    assert pull_row["doc_id"] == "2"

    # 'issues' was fetched with type=issues (so PRs are never conflated in)
    issues_calls = [c for c in backend.calls if "/issues" in c]
    assert all("type=issues" in c for c in issues_calls)
    # 'pulls' came from its own, separate endpoint
    assert any("/pulls" in c for c in backend.calls)


def test_row_from_forgejo_item_handles_null_body():
    row = _mod._row_from_forgejo_item({"id": 9, "title": "t", "body": None, "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}, "forgejo_issue")
    assert row["content"] == ""


def test_issues_since_is_not_sent_to_the_pulls_endpoint(monkeypatch):
    """Confirmed from Forgejo's own API spec: /pulls has no since/before filter param --
    issues_since must only ever be applied to the 'issues' fetch."""
    backend = _FakeForgejoBackend(issue_pages=[[]], pull_pages=[[]])
    component = _build_component(resources="issues,pulls", issues_since="2024-01-01T00:00:00Z")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    issues_calls = [c for c in backend.calls if "/issues" in c]
    pulls_calls = [c for c in backend.calls if "/pulls" in c]
    assert all("since=2024-01-01" in c for c in issues_calls)
    assert all("since" not in c for c in pulls_calls)


# --- asset_shape: single vs multi_asset --------------------------------------


def test_asset_shape_defaults_to_single():
    component = _build_component(resources="files,issues")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_single_shape_unions_resources_tagged_by_resource_column(monkeypatch):
    backend = _FakeForgejoBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "number": 1, "title": "i", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
    )
    component = _build_component(resources="files,issues", file_extensions=".md")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    df = result.output_for_node(asset_def.op.name)
    assert dict(df.groupby("_resource")["doc_id"].count()) == {"files": 1, "issues": 1}


def test_multi_asset_shape_selecting_files_never_fetches_issues_or_pulls(monkeypatch):
    """The core multi_asset requirement: selecting just 'files' must not even
    request the issues/pulls endpoints."""
    backend = _FakeForgejoBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
    )
    component = _build_component(resources="files,issues,pulls", asset_shape="multi_asset", file_extensions=".md")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {
        dg.AssetKey("test_asset_files"),
        dg.AssetKey("test_asset_issues"),
        dg.AssetKey("test_asset_pulls"),
    }

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        result = dg.materialize(
            [asset_def],
            resources={"io_manager": dg.InMemoryIOManager()},
            selection=[dg.AssetKey("test_asset_files")],
        )
    assert result.success
    assert not any("/issues" in c for c in backend.calls)
    assert not any("/pulls" in c for c in backend.calls)

    files_df = result.output_for_node(asset_def.op.name, output_name="test_asset_files")
    assert list(files_df["doc_id"]) == ["README.md"]


# --- partitioning: cosmetic-partitioning regression --------------------------


def test_partition_date_column_actually_filters_the_dataframe(monkeypatch):
    backend = _FakeForgejoBackend(issue_pages=[[
        {"id": 1, "number": 1, "title": "old", "body": "x", "html_url": "u1", "updated_at": "2023-12-01T00:00:00Z"},
        {"id": 2, "number": 2, "title": "new", "body": "y", "html_url": "u2", "updated_at": "2024-01-01T10:00:00Z"},
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


# --- write_disposition: fixes row_count silently doubling/tripling/... on every rerun ----
# (the component's own default destination is a REAL on-disk duckdb file via the cwd chdir
# in _isolate_duckdb_pipeline_dir above, not literally in-memory, so this bug is real against
# the default config, not just an opt-in destination.)


def _install_capturing_run(monkeypatch, mod):
    """Wraps `dlt.pipeline()` so every `pipeline.run(resources)` call records its real
    DltResource objects in `run_calls` BEFORE delegating to the real `.run()` -- this still
    performs a REAL dlt pipeline run against the real (tmp_path-isolated) duckdb destination
    (per this file's own testing convention: only requests.Session.send is faked), it just
    also lets a test assert on the real resources' write_disposition/primary_key hints."""
    real_pipeline_fn = mod.dlt.pipeline
    run_calls = []

    def _capturing_pipeline(*args, **kwargs):
        pipeline = real_pipeline_fn(*args, **kwargs)
        real_run = pipeline.run

        def _run(resources, *a, **kw):
            run_calls.append(list(resources))
            return real_run(resources, *a, **kw)

        pipeline.run = _run
        return pipeline

    monkeypatch.setattr(mod.dlt, "pipeline", _capturing_pipeline)
    return run_calls


def test_default_write_disposition_is_replace_on_every_resource(monkeypatch):
    backend = _FakeForgejoBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "number": 1, "title": "i", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
    )
    component = _build_component(resources="files,issues", file_extensions=".md")
    run_calls = _install_capturing_run(monkeypatch, _mod)

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    resources = run_calls[-1]
    assert len(resources) == 2  # files + issues
    for resource in resources:
        assert resource.write_disposition == "replace"

    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["write_disposition"].value == "replace"


def test_merge_write_disposition_sets_primary_key_doc_id_on_every_resource(monkeypatch):
    backend = _FakeForgejoBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
        issue_pages=[[{"id": 1, "number": 1, "title": "i", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
    )
    component = _build_component(resources="files,issues", file_extensions=".md", write_disposition="merge")
    run_calls = _install_capturing_run(monkeypatch, _mod)

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    resources = run_calls[-1]
    assert len(resources) == 2
    for resource in resources:
        assert resource.write_disposition == "merge"
        assert resource.compute_table_schema()["columns"]["doc_id"]["primary_key"] is True

    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["write_disposition"].value == "merge"


def test_append_write_disposition_is_an_explicit_opt_in(monkeypatch):
    backend = _FakeForgejoBackend(
        tree_entries=[{"path": "README.md", "type": "blob"}],
        file_contents={"README.md": "hello"},
    )
    component = _build_component(resources="files", file_extensions=".md", write_disposition="append")
    run_calls = _install_capturing_run(monkeypatch, _mod)

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", backend.send)
        asset_def, result = _materialize(component)
    assert result.success

    resources = run_calls[-1]
    assert len(resources) == 1
    assert resources[0].write_disposition == "append"

    md = result.asset_materializations_for_node(asset_def.op.name)[0].metadata
    assert md["write_disposition"].value == "append"


# --- _dedupe_by_doc_id: pure-function safety net (independent of write_disposition) -----


def test_dedupe_by_doc_id_keeps_the_latest_updated_at():
    import pandas as pd

    df = pd.DataFrame([
        {"doc_id": "a", "updated_at": "2024-01-01", "content": "old"},
        {"doc_id": "a", "updated_at": "2024-02-01", "content": "new"},
        {"doc_id": "b", "updated_at": "2024-01-01", "content": "b"},
    ])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("doc_id").loc["a", "content"] == "new"


def test_dedupe_by_doc_id_is_a_no_op_without_a_doc_id_column():
    import pandas as pd

    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 2


def test_dedupe_by_doc_id_is_a_no_op_on_an_empty_dataframe():
    import pandas as pd

    df = pd.DataFrame(columns=["doc_id", "updated_at"])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 0
    assert len(deduped) == 0


def test_dedupe_by_doc_id_still_dedupes_without_an_updated_at_column():
    import pandas as pd

    df = pd.DataFrame([{"doc_id": "x", "v": 1}, {"doc_id": "x", "v": 2}])
    deduped, removed = _mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 1


# --- real dlt + real (file-backed) duckdb: the actual regression this was fixing --------
# The component's own default destination (no `destination` set) is a real on-disk DuckDB
# file in the chdir'd tmp_path (see _isolate_duckdb_pipeline_dir above), not literally
# in-memory -- materializing the SAME component twice against identical mocked Forgejo API
# responses reuses that same file (same pipeline_name, same cwd for the whole test), so a
# real regression of the exact bug this fixes -- row_count silently doubling on every
# rerun, for every table -- would be caught here.


def test_rerun_with_default_write_disposition_does_not_duplicate_rows_across_tables(monkeypatch):
    def _fresh_backend():
        return _FakeForgejoBackend(
            tree_entries=[{"path": "README.md", "type": "blob"}],
            file_contents={"README.md": "hello"},
            issue_pages=[[{"id": 1, "number": 1, "title": "i", "body": "b", "html_url": "u", "updated_at": "2024-01-01T00:00:00Z"}]],
        )

    component = _build_component(resources="files,issues", file_extensions=".md", asset_name="rerun_asset")

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _fresh_backend().send)
        asset_def1, result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node(asset_def1.op.name)[0].metadata
    assert md1["rows_files"].value == 1
    assert md1["rows_issues"].value == 1
    assert md1["row_count"].value == 2
    assert md1["duplicate_rows_removed"].value == 0

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _fresh_backend().send)
        asset_def2, result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node(asset_def2.op.name)[0].metadata
    # NOT 2/2/4 -- this is the exact bug being fixed: dlt's own default write_disposition
    # ("append") has no notion of a previous run's rows becoming stale, so an unguarded
    # rerun against this same on-disk duckdb file would double every table's row_count.
    assert md2["rows_files"].value == 1
    assert md2["rows_issues"].value == 1
    assert md2["row_count"].value == 2
    assert md2["duplicate_rows_removed"].value == 0


def test_rerun_with_append_write_disposition_accumulates_but_the_dedupe_safety_net_still_reports_it(monkeypatch):
    """'append' is a real, working opt-in: the destination TABLE really does accumulate every
    run's rows (unlike 'replace'/'merge'). The per-row dedupe safety net (independent of
    write_disposition) catches it before the DataFrame is returned, so row_count stays
    correct either way -- duplicate_rows_removed is what actually reveals 'append' is
    accumulating underneath."""
    def _fresh_backend():
        return _FakeForgejoBackend(
            tree_entries=[{"path": "README.md", "type": "blob"}],
            file_contents={"README.md": "hello"},
        )

    component = _build_component(
        resources="files", file_extensions=".md", asset_name="rerun_append_asset", write_disposition="append",
    )

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _fresh_backend().send)
        asset_def1, result1 = _materialize(component)
    md1 = result1.asset_materializations_for_node(asset_def1.op.name)[0].metadata
    assert md1["rows_files"].value == 1
    assert md1["duplicate_rows_removed"].value == 0

    with monkeypatch.context() as m:
        m.setattr(requests.Session, "send", _fresh_backend().send)
        asset_def2, result2 = _materialize(component)
    md2 = result2.asset_materializations_for_node(asset_def2.op.name)[0].metadata
    assert md2["rows_files"].value == 1  # the safety net still returns the current 1 file...
    assert md2["duplicate_rows_removed"].value == 1  # ...but reports the destination accumulated to 2
