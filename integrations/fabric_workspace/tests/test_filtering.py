"""Real, committed tests for a gap found comparing fabric_workspace's
filtering against snowflake_workspace's (dagster-demo-factory component-
feedback): fabric_workspace only had filter_by_name_pattern/exclude_name_pattern
(one shared pattern for every item type, name only) -- no exact include_names
allow-list and no way to filter on a Fabric item's own `description` field,
even though discovery already stores it (just never reads it back for
filtering). Added `include_names` and `filter_by_description_pattern`,
replicating snowflake_workspace's own established
include_names/filter_by_comment_pattern precedence (exclude_name_pattern
always vetoes; otherwise an item is included if it satisfies ANY configured
inclusion criterion -- OR/union semantics, not AND), rather than inventing a
new filtering design.
"""
import importlib.util
import pathlib

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("fabric_workspace_filtering_under_test", _HERE / "component.py")
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

FabricWorkspaceComponent = _mod.FabricWorkspaceComponent
FabricResource = _mod.FabricResource


def _component(**overrides):
    attrs = dict(
        workspace=FabricResource(workspace_id="ws-1", tenant_id="t", client_id="c", client_secret="s"),
        import_pipelines=True,
    )
    attrs.update(overrides)
    return FabricWorkspaceComponent(**attrs)


def test_no_filters_includes_everything():
    comp = _component()
    assert comp._matches_filter("anything") is True


def test_exclude_name_pattern_vetoes_even_with_no_other_filters():
    comp = _component(exclude_name_pattern="_old$")
    assert comp._matches_filter("fuel_old") is False
    assert comp._matches_filter("fuel_new") is True


def test_filter_by_name_pattern_alone_is_a_hard_requirement():
    """Preserves the PRIOR behavior exactly: with only filter_by_name_pattern
    set, an item that doesn't match is excluded (not "included because no
    other criterion matched either")."""
    comp = _component(filter_by_name_pattern="^fuel_")
    assert comp._matches_filter("fuel_sales") is True
    assert comp._matches_filter("other_table") is False


def test_include_names_is_exact_and_case_insensitive():
    comp = _component(include_names=["Nightly_Ingest", "Other_Pipeline"])
    assert comp._matches_filter("nightly_ingest") is True
    assert comp._matches_filter("NIGHTLY_INGEST") is True
    assert comp._matches_filter("nightly_ingest_v2") is False  # exact match only, not a substring


def test_include_names_and_filter_by_name_pattern_are_unioned_not_anded():
    """An item matching EITHER criterion is included -- same OR/union
    semantics as snowflake_workspace's include_names/filter_by_name_pattern."""
    comp = _component(include_names=["special_one_off"], filter_by_name_pattern="^fuel_")
    assert comp._matches_filter("special_one_off") is True  # matches include_names only
    assert comp._matches_filter("fuel_sales") is True  # matches filter_by_name_pattern only
    assert comp._matches_filter("unrelated") is False  # matches neither


def test_exclude_name_pattern_vetoes_an_explicit_include_names_entry():
    comp = _component(include_names=["fuel_old"], exclude_name_pattern="_old$")
    assert comp._matches_filter("fuel_old") is False


def test_filter_by_description_pattern_matches_on_description_not_name():
    comp = _component(filter_by_description_pattern="nightly")
    assert comp._matches_filter("item_a", description="Runs nightly at 2am") is True
    assert comp._matches_filter("item_b", description="Ad-hoc backfill only") is False


def test_filter_by_description_pattern_is_case_insensitive():
    comp = _component(filter_by_description_pattern="NIGHTLY")
    assert comp._matches_filter("item_a", description="runs nightly") is True


def test_filter_by_description_pattern_with_no_description_excludes():
    comp = _component(filter_by_description_pattern="nightly")
    assert comp._matches_filter("item_a", description=None) is False


def test_description_is_read_from_the_real_item_during_discovery(tmp_path, monkeypatch):
    """Integration-level: write_state_to_path actually reads item['description']
    from the Fabric API response and threads it into _matches_filter -- not
    just that the method itself works in isolation."""
    import asyncio

    comp = _component(filter_by_description_pattern="nightly")
    fake_items = [
        {"id": "item-1", "displayName": "a", "type": "DataPipeline", "description": "runs nightly", "workspaceId": "ws-1"},
        {"id": "item-2", "displayName": "b", "type": "DataPipeline", "description": "ad-hoc only", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    state_path = tmp_path / "state.json"
    asyncio.run(comp.write_state_to_path(state_path))

    import json
    state = json.loads(state_path.read_text())
    names = {row["displayName"] for row in state["pipelines"]}
    assert names == {"a"}
