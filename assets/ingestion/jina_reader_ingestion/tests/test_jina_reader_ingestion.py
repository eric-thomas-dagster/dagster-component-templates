"""End-to-end tests for JinaReaderIngestionComponent, running the real dlt pipeline
against a real (file-based, tmp_path-isolated) DuckDB destination with only
`requests.get` (the one call that would otherwise hit the real r.jina.ai API) mocked.
See conftest.py for why `destination_credentials_url` is pinned to `tmp_path` rather
than left at this component's bare "duckdb" default (which would otherwise write a
real `.duckdb` file into the current working directory -- the repo root under pytest).
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import FakeResponse, install_fake_requests_get, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _build_component(mod, tmp_path, **overrides):
    defaults = dict(
        asset_name="test_asset",
        urls="https://example.com/a,https://example.com/b",
        include_preview_metadata=False,
        destination="duckdb",
        destination_credentials_url=str(tmp_path / "test.duckdb"),
    )
    defaults.update(overrides)
    return mod.JinaReaderIngestionComponent(**defaults)


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


def test_multi_url_fetch_returns_one_row_per_url(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {
            "https://example.com/a": FakeResponse(json_body={
                "data": {"url": "https://example.com/a", "title": "Page A", "content": "# A\n\nhello"}
            }),
            "https://example.com/b": FakeResponse(json_body={
                "data": {"url": "https://example.com/b", "title": "Page B", "content": "# B\n\nworld"}
            }),
        },
    )
    component = _build_component(mod, tmp_path)
    result = _materialize(component)
    assert result.success

    df = result.output_for_node("test_asset")
    assert len(df) == 2
    assert set(df["url"]) == {"https://example.com/a", "https://example.com/b"}
    assert set(df["title"]) == {"Page A", "Page B"}
    assert df["error"].isna().all()
    assert all("hello" in t or "world" in t for t in df["text"])

    meta = {k: v.value for k, v in result.asset_materializations_for_node("test_asset")[0].metadata.items()}
    assert meta["urls_requested"] == 2
    assert meta["urls_failed"] == 0
    assert meta["urls_succeeded"] == 2


def test_emits_the_document_source_contract_columns(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {
            "https://example.com/a": FakeResponse(json_body={
                "data": {"url": "https://example.com/a", "title": "Page A", "content": "# A\n\nhello"}
            }),
        },
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a")
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    row = df.iloc[0]
    assert row["doc_id"] == "https://example.com/a"
    assert "hello" in row["content"]
    assert row["source_type"] == "jina_reader"
    assert row["updated_at"]
    assert row["content_hash"]


def test_non_json_response_falls_back_to_raw_text(mod, monkeypatch, tmp_path):
    """Accept: application/json can be ignored by some front-end proxy -- the
    component must not crash, and should keep the raw markdown as `text`."""
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body=None, text="# Raw Markdown\n\nplain body")},
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a")
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert df.iloc[0]["text"] == "# Raw Markdown\n\nplain body"
    assert df.iloc[0]["title"] is None


def test_partial_failure_across_urls_does_not_crash_the_run(mod, monkeypatch, tmp_path):
    """One bad URL must not fail the whole run -- it lands as a row with `error` set
    instead, and the good URL's row is still present."""
    def _boom(**kwargs):
        raise ConnectionError("DNS resolution failed")

    install_fake_requests_get(
        monkeypatch, mod,
        {
            "https://example.com/a": FakeResponse(json_body={
                "data": {"url": "https://example.com/a", "title": "Page A", "content": "ok"}
            }),
        },
        default=_boom,
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a,https://bad.invalid/page")
    result = _materialize(component)
    assert result.success

    df = result.output_for_node("test_asset")
    assert len(df) == 2
    good = df[df["url"] == "https://example.com/a"].iloc[0]
    bad = df[df["url"] == "https://bad.invalid/page"].iloc[0]
    assert good["error"] is None or pd.isna(good["error"])
    assert "DNS resolution failed" in bad["error"]
    assert pd.isna(bad["text"])

    meta = {k: v.value for k, v in result.asset_materializations_for_node("test_asset")[0].metadata.items()}
    assert meta["urls_failed"] == 1
    assert meta["urls_succeeded"] == 1


def test_malformed_url_is_treated_as_a_fetch_failure_not_a_crash(mod, monkeypatch, tmp_path):
    """A malformed URL (no scheme) makes `requests.get` raise -- same per-URL
    catch-and-record path as any other fetch failure."""
    import requests

    def _fake_get(full_url, headers=None, timeout=None):
        raise requests.exceptions.MissingSchema(f"Invalid URL {full_url!r}: No scheme supplied")

    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(mod.requests, "get", _fake_get)

    component = _build_component(mod, tmp_path, urls="not-a-valid-url")
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 1
    assert "No scheme supplied" in df.iloc[0]["error"]


def test_unauthenticated_request_works_without_an_api_key(mod, monkeypatch, tmp_path):
    """Jina Reader's documented auth model: the API key is optional (anonymous
    requests work at a lower rate limit). api_key_env_var pointed at an unset env
    var must not raise or block the request."""
    captured_headers = {}

    def _fake_get(full_url, headers=None, timeout=None):
        captured_headers.update(headers or {})
        return FakeResponse(json_body={"data": {"content": "ok"}})

    import requests
    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(mod.requests, "get", _fake_get)

    monkeypatch.delenv("JINA_API_KEY", raising=False)
    component = _build_component(mod, tmp_path, urls="https://example.com/a", api_key_env_var="JINA_API_KEY")
    result = _materialize(component)
    assert result.success
    assert "Authorization" not in captured_headers


def test_api_key_sent_as_bearer_header_when_env_var_is_set(mod, monkeypatch, tmp_path):
    captured_headers = {}

    def _fake_get(full_url, headers=None, timeout=None):
        captured_headers.update(headers or {})
        return FakeResponse(json_body={"data": {"content": "ok"}})

    import requests
    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(mod.requests, "get", _fake_get)

    monkeypatch.setenv("JINA_API_KEY", "secret-key-123")
    component = _build_component(mod, tmp_path, urls="https://example.com/a", api_key_env_var="JINA_API_KEY")
    result = _materialize(component)
    assert result.success
    assert captured_headers["Authorization"] == "Bearer secret-key-123"


def test_real_jina_request_options_sent_as_documented_headers(mod, monkeypatch, tmp_path):
    captured_headers = {}

    def _fake_get(full_url, headers=None, timeout=None):
        captured_headers.update(headers or {})
        return FakeResponse(json_body={"data": {"content": "ok"}})

    import requests
    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(mod.requests, "get", _fake_get)

    component = _build_component(
        mod, tmp_path, urls="https://example.com/a",
        target_selector="article", wait_for_selector="#content",
        request_timeout_seconds=30, with_generated_alt=True,
    )
    result = _materialize(component)
    assert result.success
    assert captured_headers["X-Target-Selector"] == "article"
    assert captured_headers["X-Wait-For-Selector"] == "#content"
    assert captured_headers["X-Timeout"] == "30"
    assert captured_headers["X-With-Generated-Alt"] == "true"
    assert captured_headers["Accept"] == "application/json"


def test_hash_columns_pseudonymizes_before_landing_in_the_destination(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={
            "data": {"url": "https://example.com/a", "title": "Secret Title", "content": "body"}
        })},
    )
    component = _build_component(
        mod, tmp_path, urls="https://example.com/a",
        hash_columns=["title"], hash_salt="pepper",
    )
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    expected = hashlib.sha256(("Secret Title" + "pepper").encode()).hexdigest()
    assert df.iloc[0]["title"] == expected


def test_hash_columns_without_salt_raises(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={"data": {"content": "x"}})},
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a", hash_columns=["title"])
    with pytest.raises(Exception):
        _materialize(component)


def test_exclude_columns_drops_an_unhinted_column_entirely(mod, monkeypatch, tmp_path):
    """`fetched_at` has no explicit dlt column hint (unlike title/text/error, which
    are hinted so they survive an all-null load) -- excluding it removes the column
    from the destination schema entirely, same as any other ingestion component."""
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={
            "data": {"url": "https://example.com/a", "title": "T", "content": "body"}
        })},
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a", exclude_columns=["fetched_at"])
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert "fetched_at" not in df.columns


def test_exclude_columns_on_a_hinted_column_nulls_values_but_keeps_the_column(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={
            "data": {"url": "https://example.com/a", "title": "T", "content": "body"}
        })},
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a", exclude_columns=["title"])
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert "title" in df.columns
    assert df.iloc[0]["title"] is None or pd.isna(df.iloc[0]["title"])


def test_max_rows_per_resource_caps_rows_landed(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {
            "https://example.com/a": FakeResponse(json_body={"data": {"content": "a"}}),
            "https://example.com/b": FakeResponse(json_body={"data": {"content": "b"}}),
            "https://example.com/c": FakeResponse(json_body={"data": {"content": "c"}}),
        },
    )
    component = _build_component(
        mod, tmp_path, urls="https://example.com/a,https://example.com/b,https://example.com/c",
        max_rows_per_resource=1,
    )
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 1


def test_on_schema_change_default_leaves_schema_contract_unset(mod, monkeypatch, tmp_path):
    """Row shape here is fixed (url/title/text/fetched_at/error), not derived
    dynamically from the upstream API like a multi-table connector -- so this is
    verified at the resource-hint level (same thing dlt itself checks before
    accepting/rejecting a load), not via a real schema-violating second load."""
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={"data": {"content": "a"}})},
    )
    captured = {}
    real_pipeline = mod.dlt.pipeline

    def _capturing_pipeline(**kw):
        p = real_pipeline(**kw)
        real_run = p.run

        def _run(resources, *a, **k):
            captured["resources"] = resources
            return real_run(resources, *a, **k)

        p.run = _run
        return p

    monkeypatch.setattr(mod.dlt, "pipeline", _capturing_pipeline)

    component = _build_component(mod, tmp_path, urls="https://example.com/a")
    result = _materialize(component)
    assert result.success
    assert captured["resources"][0].compute_table_schema().get("schema_contract") is None


def test_on_schema_change_block_all_sets_freeze_contract_hint(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={"data": {"content": "a"}})},
    )
    captured = {}
    real_pipeline = mod.dlt.pipeline

    def _capturing_pipeline(**kw):
        p = real_pipeline(**kw)
        real_run = p.run

        def _run(resources, *a, **k):
            captured["resources"] = resources
            return real_run(resources, *a, **k)

        p.run = _run
        return p

    monkeypatch.setattr(mod.dlt, "pipeline", _capturing_pipeline)

    component = _build_component(mod, tmp_path, urls="https://example.com/a", on_schema_change="block_all")
    result = _materialize(component)
    assert result.success
    assert captured["resources"][0].compute_table_schema()["schema_contract"] == {
        "tables": "evolve", "columns": "freeze", "data_type": "freeze",
    }


def test_destination_persist_only_emits_materialize_result(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={"data": {"content": "a"}})},
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a", persist_only=True)
    result = _materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node("test_asset")
    assert len(mats) == 1
    assert mats[0].metadata["dataset_name"].value == "test_asset"


def test_filesystem_destination_without_persist_only_warns_and_still_succeeds(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={"data": {"content": "a"}})},
    )
    fs_dir = tmp_path / "fs_out"
    fs_dir.mkdir()
    component = _build_component(
        mod, tmp_path, urls="https://example.com/a",
        destination="filesystem", destination_credentials_url=None,
        bucket_url=f"file://{fs_dir}",
    )
    result = _materialize(component)
    assert result.success
    mats = result.asset_materializations_for_node("test_asset")
    assert mats[0].metadata["destination"].value == "filesystem"


# --- write_disposition: fixing "re-run doubles row_count" -------------------------------

def test_default_write_disposition_is_replace_and_rerun_does_not_duplicate_rows(mod, monkeypatch, tmp_path):
    """This component's OWN previous hard-coded `write_disposition="append"` is the exact
    bug this fixes: re-running against the same URLs used to double row_count every time."""
    install_fake_requests_get(
        monkeypatch, mod,
        {
            "https://example.com/a": FakeResponse(json_body={"data": {"url": "https://example.com/a", "title": "A", "content": "hello"}}),
            "https://example.com/b": FakeResponse(json_body={"data": {"url": "https://example.com/b", "title": "B", "content": "world"}}),
        },
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a,https://example.com/b")

    result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node("test_asset")[0].metadata
    assert md1["row_count"].value == 2
    assert md1["write_disposition"].value == "replace"
    assert md1["duplicate_rows_removed"].value == 0

    result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node("test_asset")[0].metadata
    assert md2["row_count"].value == 2  # NOT 4 -- this is the exact bug being fixed


def test_merge_write_disposition_sets_primary_key_and_keeps_prior_rows_not_refetched(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={"data": {"url": "https://example.com/a", "title": "A", "content": "hello"}})},
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a", write_disposition="merge")
    result1 = _materialize(component)
    assert result1.success
    assert result1.asset_materializations_for_node("test_asset")[0].metadata["row_count"].value == 1

    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/b": FakeResponse(json_body={"data": {"url": "https://example.com/b", "title": "B", "content": "world"}})},
    )
    component2 = _build_component(mod, tmp_path, urls="https://example.com/b", write_disposition="merge")
    result2 = _materialize(component2)
    assert result2.success
    md2 = result2.asset_materializations_for_node("test_asset")[0].metadata
    assert md2["write_disposition"].value == "merge"
    assert md2["row_count"].value == 2  # merge keeps "a" from the first run alongside new "b"


def test_append_write_disposition_accumulates_at_destination_but_dedupe_safety_net_catches_it(mod, monkeypatch, tmp_path):
    install_fake_requests_get(
        monkeypatch, mod,
        {"https://example.com/a": FakeResponse(json_body={"data": {"url": "https://example.com/a", "title": "A", "content": "hello"}})},
    )
    component = _build_component(mod, tmp_path, urls="https://example.com/a", write_disposition="append")
    result1 = _materialize(component)
    md1 = result1.asset_materializations_for_node("test_asset")[0].metadata
    assert md1["row_count"].value == 1
    assert md1["duplicate_rows_removed"].value == 0

    result2 = _materialize(component)
    md2 = result2.asset_materializations_for_node("test_asset")[0].metadata
    assert md2["row_count"].value == 1  # safety net still returns the current 1 page...
    assert md2["duplicate_rows_removed"].value == 1  # ...but reveals the destination accumulated to 2


def test_dedupe_by_doc_id_keeps_latest_updated_at(mod):
    df = pd.DataFrame([
        {"doc_id": "https://a", "updated_at": "2024-01-01", "content": "old"},
        {"doc_id": "https://a", "updated_at": "2024-02-01", "content": "new"},
        {"doc_id": "https://b", "updated_at": "2024-01-01", "content": "b"},
    ])
    deduped, removed = mod._dedupe_by_doc_id(df)
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("doc_id").loc["https://a", "content"] == "new"
