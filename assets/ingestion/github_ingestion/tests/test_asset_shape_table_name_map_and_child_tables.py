"""Tests for GitHubIngestionComponent's `asset_shape`/`table_name_map` (same rollout as
zendesk_ingestion/servicenow_ingestion) and the child-table merge fix.

Real pre-existing bug found while scoping this rollout: dlt's normalizer automatically splits
any nested JSON array field (e.g. an issue's `labels`, `assignees`) into a separate CHILD TABLE
linked back to the parent via `_dlt_parent_id` -> `_dlt_id` -- core dlt behavior, not specific to
GitHub. The old query-back code only ever did `SELECT * FROM {dataset_name}.{resource_name}` for
the literal top-level resource name, so the full nested data landed correctly in a real
destination (dlt handles that independently of this component), but the DataFrame Dagster got
back silently dropped every nested array field. Fixed via `_fetch_table_with_children`, which
lists and merges any `{resource}__*` child tables back onto the parent rows as list-of-dict
columns.

`repositories`/`owner` remain shared construction parameters, not a second subsettable axis --
every GitHub resource (issues, pull_requests, ...) already spans every configured repository in
one dlt resource call, so `asset_shape='multi_asset'` keys only by `resources`, exactly like
zendesk_ingestion.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import install_fake_dlt, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _make_component(mod, **overrides):
    kwargs = dict(
        asset_name="test_asset",
        access_token="tok",
        owner="acme",
        repositories=["repo1"],
        resources=["issues"],
    )
    kwargs.update(overrides)
    return mod.GitHubIngestionComponent(**kwargs)


def test_asset_shape_defaults_to_single(mod):
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset")}


def test_multi_asset_shape_produces_one_asset_per_resource(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "issues": pd.DataFrame({"id": [1, 2], "_dlt_id": ["a", "b"]}),
        "pull_requests": pd.DataFrame({"id": [9], "_dlt_id": ["c"]}),
    })
    component = _make_component(mod, resources=["issues", "pull_requests"], asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]
    assert set(asset_def.keys) == {dg.AssetKey("test_asset_issues"), dg.AssetKey("test_asset_pull_requests")}

    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    issues_df = result.output_for_node("test_asset", output_name="test_asset_issues")
    prs_df = result.output_for_node("test_asset", output_name="test_asset_pull_requests")
    assert list(issues_df["id"]) == [1, 2]
    assert list(prs_df["id"]) == [9]


def test_multi_asset_shape_selecting_one_resource_never_touches_the_other(mod, monkeypatch):
    fake_pipeline = install_fake_dlt(monkeypatch, mod, table_rows={
        "issues": pd.DataFrame({"id": [1], "_dlt_id": ["a"]}),
        "pull_requests": pd.DataFrame({"id": [9], "_dlt_id": ["c"]}),
    })
    component = _make_component(mod, resources=["issues", "pull_requests"], asset_shape="multi_asset")
    asset_def = component.build_defs(context=None).assets[0]

    result = dg.materialize(
        [asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        selection=[dg.AssetKey("test_asset_issues")],
    )
    assert result.success
    queries = fake_pipeline.sql_client_instance.queries
    assert any("issues" in q for q in queries)
    assert not any("pull_requests" in q for q in queries)


def test_table_name_map_overrides_destination_table_in_single_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_issues": pd.DataFrame({"id": [1, 2, 3], "_dlt_id": ["a", "b", "c"]})})
    component = _make_component(mod, table_name_map={"issues": "raw_issues"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert len(df) == 3


def test_table_name_map_overrides_destination_table_in_multi_asset_shape(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={"raw_issues": pd.DataFrame({"id": [1, 2, 3], "_dlt_id": ["a", "b", "c"]})})
    component = _make_component(mod, asset_shape="multi_asset", table_name_map={"issues": "raw_issues"})
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset", output_name="test_asset_issues")
    assert len(df) == 3


def test_table_name_map_with_unknown_resource_raises_a_clear_error(mod):
    component = _make_component(mod, table_name_map={"pull_requests": "raw_prs"})
    with pytest.raises(ValueError, match="table_name_map has key"):
        component.build_defs(context=None)


def test_child_tables_are_merged_back_onto_the_parent_as_list_columns(mod, monkeypatch):
    """Regression test for the real pre-existing bug: nested array fields (labels, assignees)
    get split into separate dlt child tables and were silently dropped from the DataFrame."""
    issues = pd.DataFrame({
        "id": [1, 2],
        "title": ["bug A", "bug B"],
        "_dlt_id": ["issue-1", "issue-2"],
    })
    labels = pd.DataFrame({
        "value": ["bug", "p1", "p2"],
        "_dlt_parent_id": ["issue-1", "issue-1", "issue-2"],
        "_dlt_list_idx": [0, 1, 0],
        "_dlt_id": ["lbl-1", "lbl-2", "lbl-3"],
    })
    install_fake_dlt(monkeypatch, mod, table_rows={
        "issues": issues,
        "issues__labels": labels,
    })
    component = _make_component(mod)
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")

    assert "labels" in df.columns
    row1 = df[df["id"] == 1].iloc[0]
    row2 = df[df["id"] == 2].iloc[0]
    assert [d["value"] for d in row1["labels"]] == ["bug", "p1"]
    assert [d["value"] for d in row2["labels"]] == ["p2"]
    # dlt bookkeeping columns must not leak into the merged child data.
    assert all("_dlt_parent_id" not in d and "_dlt_id" not in d for d in row1["labels"])


def test_resource_with_no_child_tables_is_unaffected(mod, monkeypatch):
    install_fake_dlt(monkeypatch, mod, table_rows={
        "pull_requests": pd.DataFrame({"id": [5], "_dlt_id": ["pr-1"]}),
    })
    component = _make_component(mod, resources=["pull_requests"])
    asset_def = component.build_defs(context=None).assets[0]
    result = dg.materialize([asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    df = result.output_for_node("test_asset")
    assert list(df["id"]) == [5]
