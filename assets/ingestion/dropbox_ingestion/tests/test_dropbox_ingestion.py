"""End-to-end tests for DropboxIngestionComponent, running the real dlt pipeline against
a real (file-based, tmp_path-isolated) DuckDB destination with only `requests.post` (the
OAuth2 token exchange and `files/list_folder`/`files/list_folder/continue`) mocked. See
conftest.py for why `destination_credentials_url` is pinned to `tmp_path`, and for the
`~/.dlt/pipelines/` global-cache isolation.

Covers the real, previously-shipped bug this fixes: `pipeline.run(dlt_resources)` ran
with no `write_disposition` set, so dlt's own default ("append") made every rerun
against the same folder listing double row_count forever -- even against this
component's own default on-disk DuckDB destination (a real file, not in-memory).
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dropbox_api, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _entry(id_, name, server_modified, client_modified=None, tag="file"):
    return {
        ".tag": tag,
        "name": name,
        "path_lower": f"/{name.lower()}",
        "path_display": f"/{name}",
        "id": id_,
        "client_modified": client_modified or server_modified,
        "server_modified": server_modified,
        "rev": "0059" + id_[-4:],
        "size": 1024,
    }


def _build_component(mod, tmp_path, **overrides):
    defaults = dict(
        asset_name="test_asset",
        client_id="fake-client-id",
        client_secret="fake-client-secret",
        refresh_token="fake-refresh-token",
        folder_path="",
        include_preview_metadata=False,
        destination="duckdb",
        destination_credentials_url=str(tmp_path / "test.duckdb"),
    )
    defaults.update(overrides)
    return mod.DropboxIngestionComponent(**defaults)


def _materialize(component):
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})


# --- write_disposition: fixing "re-run doubles row_count" -------------------------------

def test_default_write_disposition_is_replace_and_rerun_does_not_duplicate_rows(mod, monkeypatch, tmp_path):
    """This component's OWN previous no-write_disposition call is the exact bug this
    fixes (dlt's own default is 'append'): re-running against the same folder listing
    used to double row_count every time, even against the default on-disk DuckDB file."""
    install_fake_dropbox_api(
        monkeypatch, mod,
        [{"entries": [
            _entry("id:aaa1", "a.txt", "2024-01-01T00:00:00Z"),
            _entry("id:bbb2", "b.txt", "2024-01-01T00:00:00Z"),
        ], "cursor": None, "has_more": False}],
    )
    component = _build_component(mod, tmp_path)

    result1 = _materialize(component)
    assert result1.success
    md1 = result1.asset_materializations_for_node("test_asset")[0].metadata
    assert md1["row_count"].value == 2
    assert md1["write_disposition"].value == "replace"
    assert md1["duplicate_rows_removed"].value == 0

    install_fake_dropbox_api(
        monkeypatch, mod,
        [{"entries": [
            _entry("id:aaa1", "a.txt", "2024-01-01T00:00:00Z"),
            _entry("id:bbb2", "b.txt", "2024-01-01T00:00:00Z"),
        ], "cursor": None, "has_more": False}],
    )
    result2 = _materialize(component)
    assert result2.success
    md2 = result2.asset_materializations_for_node("test_asset")[0].metadata
    assert md2["row_count"].value == 2  # NOT 4 -- this is the exact bug being fixed


def test_merge_write_disposition_sets_primary_key_and_keeps_prior_rows_not_relisted(mod, monkeypatch, tmp_path):
    install_fake_dropbox_api(
        monkeypatch, mod,
        [{"entries": [_entry("id:aaa1", "a.txt", "2024-01-01T00:00:00Z")], "cursor": None, "has_more": False}],
    )
    component = _build_component(mod, tmp_path, write_disposition="merge")
    result1 = _materialize(component)
    assert result1.success
    assert result1.asset_materializations_for_node("test_asset")[0].metadata["row_count"].value == 1

    install_fake_dropbox_api(
        monkeypatch, mod,
        [{"entries": [_entry("id:bbb2", "b.txt", "2024-01-01T00:00:00Z")], "cursor": None, "has_more": False}],
    )
    component2 = _build_component(mod, tmp_path, write_disposition="merge")
    result2 = _materialize(component2)
    assert result2.success
    md2 = result2.asset_materializations_for_node("test_asset")[0].metadata
    assert md2["write_disposition"].value == "merge"
    assert md2["row_count"].value == 2  # merge keeps "aaa1" from the first run alongside new "bbb2"


def test_append_write_disposition_accumulates_at_destination_but_dedupe_safety_net_catches_it(mod, monkeypatch, tmp_path):
    install_fake_dropbox_api(
        monkeypatch, mod,
        [{"entries": [_entry("id:aaa1", "a.txt", "2024-01-01T00:00:00Z")], "cursor": None, "has_more": False}],
    )
    component = _build_component(mod, tmp_path, write_disposition="append")
    result1 = _materialize(component)
    md1 = result1.asset_materializations_for_node("test_asset")[0].metadata
    assert md1["row_count"].value == 1
    assert md1["duplicate_rows_removed"].value == 0

    install_fake_dropbox_api(
        monkeypatch, mod,
        [{"entries": [_entry("id:aaa1", "a.txt", "2024-01-01T00:00:00Z")], "cursor": None, "has_more": False}],
    )
    result2 = _materialize(component)
    md2 = result2.asset_materializations_for_node("test_asset")[0].metadata
    assert md2["row_count"].value == 1  # safety net still returns the current 1 entry...
    assert md2["duplicate_rows_removed"].value == 1  # ...but reveals the destination accumulated to 2


def test_pagination_via_cursor_continue_is_followed(mod, monkeypatch, tmp_path):
    install_fake_dropbox_api(
        monkeypatch, mod,
        [
            {"entries": [_entry("id:aaa1", "a.txt", "2024-01-01T00:00:00Z")], "cursor": "cursor-1", "has_more": True},
            {"entries": [_entry("id:bbb2", "b.txt", "2024-01-01T00:00:00Z")], "cursor": None, "has_more": False},
        ],
    )
    component = _build_component(mod, tmp_path)
    result = _materialize(component)
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 2
    assert set(df["id"]) == {"id:aaa1", "id:bbb2"}


def test_load_id_is_present_in_metadata(mod, monkeypatch, tmp_path):
    install_fake_dropbox_api(
        monkeypatch, mod,
        [{"entries": [_entry("id:aaa1", "a.txt", "2024-01-01T00:00:00Z")], "cursor": None, "has_more": False}],
    )
    component = _build_component(mod, tmp_path)
    result = _materialize(component)
    md = result.asset_materializations_for_node("test_asset")[0].metadata
    assert md["load_id"].value  # a real dlt load id string, not null


# --- _dedupe_by_key: pure-function unit tests -------------------------------------------

def test_dedupe_by_key_keeps_latest_order_col(mod):
    df = pd.DataFrame([
        {"id": "id:aaa1", "server_modified": "2024-01-01T00:00:00Z", "name": "old"},
        {"id": "id:aaa1", "server_modified": "2024-02-01T00:00:00Z", "name": "new"},
        {"id": "id:bbb2", "server_modified": "2024-01-01T00:00:00Z", "name": "b"},
    ])
    deduped, removed = mod._dedupe_by_key(df, "id", "server_modified")
    assert removed == 1
    assert len(deduped) == 2
    assert deduped.set_index("id").loc["id:aaa1", "name"] == "new"


def test_dedupe_by_key_is_noop_when_key_col_absent(mod):
    df = pd.DataFrame([{"name": "a"}, {"name": "b"}])
    deduped, removed = mod._dedupe_by_key(df, "id", "server_modified")
    assert removed == 0
    assert len(deduped) == 2


def test_dedupe_by_key_is_noop_on_empty_dataframe(mod):
    df = pd.DataFrame(columns=["id", "server_modified"])
    deduped, removed = mod._dedupe_by_key(df, "id", "server_modified")
    assert removed == 0
    assert len(deduped) == 0


def test_dedupe_by_key_without_order_col_still_dedupes(mod):
    df = pd.DataFrame([
        {"id": "id:aaa1", "name": "first"},
        {"id": "id:aaa1", "name": "second"},
    ])
    deduped, removed = mod._dedupe_by_key(df, "id")
    assert removed == 1
    assert len(deduped) == 1
    assert deduped.iloc[0]["name"] == "second"
