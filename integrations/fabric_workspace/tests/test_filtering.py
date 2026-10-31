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


def test_exclude_name_pattern_is_case_insensitive():
    """Component-feedback: an earlier version only made include_names/
    filter_by_description_pattern case-insensitive -- exclude_name_pattern
    silently failed to exclude 'fuel_price_transform' when configured as
    'FUEL'."""
    comp = _component(exclude_name_pattern="FUEL")
    assert comp._matches_filter("fuel_price_transform") is False


def test_filter_by_name_pattern_is_case_insensitive():
    comp = _component(filter_by_name_pattern="FUEL")
    assert comp._matches_filter("fuel_price_transform") is True


# ── V: warn when a filter matches nothing ──────────────────────────────────

def test_misspelled_include_names_entry_warns(tmp_path, monkeypatch, caplog):
    import asyncio
    import logging

    comp = _component(include_names=["nightly_ingest", "typo_name"])
    fake_items = [
        {"id": "item-1", "displayName": "nightly_ingest", "type": "DataPipeline", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    assert any("typo_name" in r.message for r in caplog.records)
    assert not any("nightly_ingest" in r.message and "matched no" in r.message for r in caplog.records)


def test_filter_by_name_pattern_matching_nothing_warns(tmp_path, monkeypatch, caplog):
    import asyncio
    import logging

    comp = _component(filter_by_name_pattern="^zzz_no_match_")
    fake_items = [
        {"id": "item-1", "displayName": "nightly_ingest", "type": "DataPipeline", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    assert any("filter_by_name_pattern" in r.message and "matched no" in r.message for r in caplog.records)


def test_filter_matching_everything_out_warns_once_more_loudly(tmp_path, monkeypatch, caplog):
    import asyncio
    import logging

    comp = _component(include_names=["typo_a", "typo_b"])
    fake_items = [
        {"id": "item-1", "displayName": "nightly_ingest", "type": "DataPipeline", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    assert any("excluded ALL" in r.message for r in caplog.records)


def test_no_filters_configured_never_warns(tmp_path, monkeypatch, caplog):
    import asyncio
    import logging

    comp = _component()
    fake_items = [
        {"id": "item-1", "displayName": "nightly_ingest", "type": "DataPipeline", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    assert not caplog.records


# ── X: warn on unknown item_* keys ──────────────────────────────────────────

def test_misspelled_item_partitions_key_warns(tmp_path, monkeypatch, caplog):
    import asyncio
    import logging

    comp = _component(item_partitions={"typo_name": {"partition_type": "daily", "partition_start": "2024-01-01"}})
    fake_items = [
        {"id": "item-1", "displayName": "nightly_ingest", "type": "DataPipeline", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    assert any("item_partitions" in r.message and "typo_name" in r.message for r in caplog.records)


def test_correct_item_partitions_key_does_not_warn(tmp_path, monkeypatch, caplog):
    import asyncio
    import logging

    comp = _component(item_partitions={"nightly_ingest": {"partition_type": "daily", "partition_start": "2024-01-01"}})
    fake_items = [
        {"id": "item-1", "displayName": "nightly_ingest", "type": "DataPipeline", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    assert not caplog.records


def test_misspelled_item_pools_item_parameters_item_execution_data_keys_all_warn(tmp_path, monkeypatch, caplog):
    import asyncio
    import logging

    comp = _component(
        item_pools={"typo_pools": "x"},
        item_parameters={"typo_params": {"a": 1}},
        item_execution_data={"typo_exec": {"a": 1}},
    )
    fake_items = [
        {"id": "item-1", "displayName": "nightly_ingest", "type": "DataPipeline", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    messages = [r.message for r in caplog.records]
    assert any("item_pools" in m and "typo_pools" in m for m in messages)
    assert any("item_parameters" in m and "typo_params" in m for m in messages)
    assert any("item_execution_data" in m and "typo_exec" in m for m in messages)


def test_item_setting_key_matching_a_non_runnable_item_still_warns(tmp_path, monkeypatch, caplog):
    """A Lakehouse/Warehouse/SemanticModel/Report name doesn't count as a
    valid item_* key -- those item kinds never get parameters/pools/
    partitions/execution_data."""
    import asyncio
    import logging

    comp = _component(item_parameters={"my_lakehouse": {"a": 1}})
    fake_items = [
        {"id": "item-1", "displayName": "my_lakehouse", "type": "Lakehouse", "workspaceId": "ws-1"},
    ]
    monkeypatch.setattr(comp, "_list_items", lambda: fake_items)

    with caplog.at_level(logging.WARNING):
        asyncio.run(comp.write_state_to_path(tmp_path / "state.json"))

    assert any("item_parameters" in r.message and "my_lakehouse" in r.message for r in caplog.records)


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
