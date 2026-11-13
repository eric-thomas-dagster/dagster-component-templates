"""Committed tests for migrate_asset_metadata.py's pure logic (no network I/O mocked --
there's nothing to mock: parse_asset_key/extract_metadata_value/group_latest_per_partition/
build_metadata_entry_fragment/_is_scalar_field are all plain functions over plain data).

The GraphQL/REST network calls themselves (graphql(), report_asset_materialization_rest(),
introspect_metadata_entry_scalar_fields(), fetch_all_materializations()) are NOT exercised here
-- this is a standalone customer-facing script (zero deps beyond PyYAML, no `dagster` import,
same family as cli/sync_catalog_views.py), not a component with a committed real-service test
convention, and there's no free/local Dagster+ deployment to test against."""
import importlib.util
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent


def _load_mod():
    path = REPO_ROOT / "cli" / "migrate_asset_metadata" / "migrate_asset_metadata.py"
    spec = importlib.util.spec_from_file_location("migrate_asset_metadata_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def mod():
    return _load_mod()


# ─── parse_asset_key ────────────────────────────────────────────────────────

def test_parse_asset_key_splits_on_slash(mod):
    assert mod.parse_asset_key("a/b/c") == ["a", "b", "c"]


def test_parse_asset_key_single_segment(mod):
    assert mod.parse_asset_key("my_asset") == ["my_asset"]


def test_parse_asset_key_ignores_leading_trailing_slashes(mod):
    assert mod.parse_asset_key("/a/b/") == ["a", "b"]


# ─── _is_scalar_field / build_metadata_entry_fragment ──────────────────────

def test_is_scalar_field_true_for_plain_scalar(mod):
    assert mod._is_scalar_field({"name": "text", "type": {"kind": "SCALAR", "ofType": None}})


def test_is_scalar_field_true_for_non_null_scalar(mod):
    assert mod._is_scalar_field({"name": "text", "type": {"kind": "NON_NULL", "ofType": {"kind": "SCALAR", "name": "String"}}})


def test_is_scalar_field_false_for_object_type(mod):
    assert not mod._is_scalar_field({"name": "table", "type": {"kind": "OBJECT", "ofType": None}})


def test_is_scalar_field_true_for_enum(mod):
    assert mod._is_scalar_field({"name": "status", "type": {"kind": "ENUM", "ofType": None}})


def test_build_metadata_entry_fragment_includes_typename_and_label_once_plus_each_type(mod):
    fragment = mod.build_metadata_entry_fragment({
        "TextMetadataEntry": ["text"],
        "IntMetadataEntry": ["intValue", "intRepr"],
    })
    assert fragment.count("__typename") == 1
    assert fragment.count("label") == 1
    assert "... on TextMetadataEntry { text }" in fragment
    assert "... on IntMetadataEntry { intValue intRepr }" in fragment


def test_build_metadata_entry_fragment_empty_input_still_has_typename_and_label(mod):
    fragment = mod.build_metadata_entry_fragment({})
    assert "__typename" in fragment and "label" in fragment


# ─── extract_metadata_value ─────────────────────────────────────────────────

def test_extract_metadata_value_prefers_priority_field(mod):
    entry = {"__typename": "TextMetadataEntry", "label": "rows", "text": "hello", "boolValue": None}
    assert mod.extract_metadata_value(entry) == "hello"


def test_extract_metadata_value_falls_back_to_first_present_scalar(mod):
    # A hypothetical/future entry type not in the priority list at all.
    entry = {"__typename": "FutureMetadataEntry", "label": "x", "someNewField": 42}
    assert mod.extract_metadata_value(entry) == 42


def test_extract_metadata_value_returns_none_when_nothing_usable(mod):
    entry = {"__typename": "TableMetadataEntry", "label": "schema"}
    assert mod.extract_metadata_value(entry) is None


def test_extract_metadata_value_skips_typename_and_label_in_fallback(mod):
    entry = {"__typename": "X", "label": "y"}
    assert mod.extract_metadata_value(entry) is None


# ─── group_latest_per_partition ─────────────────────────────────────────────

def test_group_latest_per_partition_keeps_highest_timestamp_per_partition(mod):
    events = [
        {"partition": "2026-01-01", "timestamp": 100.0},
        {"partition": "2026-01-01", "timestamp": 200.0},
        {"partition": "2026-01-02", "timestamp": 150.0},
    ]
    out = mod.group_latest_per_partition(events)
    assert out["2026-01-01"]["timestamp"] == 200.0
    assert out["2026-01-02"]["timestamp"] == 150.0
    assert len(out) == 2


def test_group_latest_per_partition_unpartitioned_uses_none_key(mod):
    events = [{"partition": None, "timestamp": 10.0}, {"partition": None, "timestamp": 20.0}]
    out = mod.group_latest_per_partition(events)
    assert len(out) == 1
    assert out[None]["timestamp"] == 20.0


def test_group_latest_per_partition_empty_input(mod):
    assert mod.group_latest_per_partition([]) == {}


# ─── CLI safety rails ────────────────────────────────────────────────────────

def test_migrate_refuses_to_write_without_explicit_yes(mod, monkeypatch):
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", "agent:fake")
    args = mod.argparse.Namespace(
        token_env="DAGSTER_CLOUD_API_TOKEN", dry_run=False, yes=False,
        deployment_url="https://acme.dagster.cloud/prod",
        manifest=None, source_asset_key="a", dest_asset_key="b",
    )
    with pytest.raises(SystemExit):
        mod.cmd_migrate(args)


def test_migrate_dry_run_end_to_end_with_mocked_network(mod, monkeypatch, capsys):
    """Exercises the real cmd_migrate -> _migrate_one -> _events_to_report wiring together, with
    only the actual network calls (introspection + materialization fetch) mocked -- everything
    else (grouping by partition, extracting metadata values, dry-run reporting) runs for real."""
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", "agent:fake")
    monkeypatch.setattr(mod, "introspect_metadata_entry_scalar_fields", lambda *a, **k: {"TextMetadataEntry": ["text"]})

    events = [
        {"partition": "p1", "timestamp": 100.0, "runId": "r1",
         "metadataEntries": [{"__typename": "TextMetadataEntry", "label": "source", "text": "s3://old"}]},
        {"partition": "p1", "timestamp": 200.0, "runId": "r2",
         "metadataEntries": [{"__typename": "TextMetadataEntry", "label": "source", "text": "s3://new"}]},
        {"partition": "p2", "timestamp": 150.0, "runId": "r3",
         "metadataEntries": [{"__typename": "TableMetadataEntry", "label": "preview"}]},  # no scalar value -> skipped
    ]
    monkeypatch.setattr(mod, "fetch_all_materializations", lambda *a, **k: events)
    report_calls = []
    monkeypatch.setattr(mod, "report_asset_materialization_rest", lambda *a, **k: report_calls.append((a, k)))

    args = mod.argparse.Namespace(
        token_env="DAGSTER_CLOUD_API_TOKEN", dry_run=True, yes=False,
        deployment_url="https://acme.dagster.cloud/prod",
        manifest=None, source_asset_key="old/key", dest_asset_key="new/key",
        all_events=False, limit=None,
    )
    rc = mod.cmd_migrate(args)
    assert rc == 0
    assert report_calls == []  # dry-run never calls the write endpoint
    out = capsys.readouterr().err
    assert "p1" in out and "p2" in out
    assert "Would report 2 event(s)" in out  # latest-per-partition: p1's newest (200.0) + p2's only event


def test_migrate_refuses_identical_source_and_dest_keys(mod, monkeypatch):
    monkeypatch.setattr(mod, "introspect_metadata_entry_scalar_fields", lambda *a, **k: {})
    monkeypatch.setenv("DAGSTER_CLOUD_API_TOKEN", "agent:fake")
    args = mod.argparse.Namespace(
        token_env="DAGSTER_CLOUD_API_TOKEN", dry_run=True, yes=False,
        deployment_url="https://acme.dagster.cloud/prod",
        manifest=None, source_asset_key="same/key", dest_asset_key="same/key",
        all_events=False, limit=None,
    )
    with pytest.raises(SystemExit):
        mod.cmd_migrate(args)
