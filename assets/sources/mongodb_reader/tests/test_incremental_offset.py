"""Committed tests for MongodbReaderComponent's offset_field incremental-read retrofit.

Real dg.materialize() end to end (same convention as test_resource_key.py); only the
external pymongo client is faked. The persisted-high-water-mark behavior is exercised by
reusing one DagsterInstance.ephemeral() across two consecutive materialize() calls, so the
second call's context.instance.get_latest_materialization_event() genuinely sees the first
call's stored metadata -- not a re-implementation of that lookup.
"""
import dagster as dg
import pytest

from .conftest import install_fake_pymongo, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake(monkeypatch):
    monkeypatch.setenv("MONGODB_URI", "mongodb://inline.example:27017")
    return install_fake_pymongo(monkeypatch)


def _component(mod, **kw):
    return mod.MongodbReaderComponent(
        asset_name="from_mongo", database="shop", collection="orders", **kw
    )


def test_offset_field_unset_keeps_original_behavior_unchanged(mod, fake):
    """No offset_field: single query, no instance lookup, no new_offset metadata key."""
    fake.preseed = {("shop", "orders"): [{"id": 1, "updated_at": 5}, {"id": 2, "updated_at": 10}]}
    source = _component(mod).build_defs(None).assets[0]
    result = dg.materialize([source])
    assert result.success
    mats = result.asset_materializations_for_node("from_mongo")
    assert mats[0].metadata["dagster/row_count"].value == 2
    assert "dagster_community_components/last_offset" not in mats[0].metadata


def test_offset_field_paginates_and_stores_high_water_mark(mod, fake):
    """page_size=2 over 5 docs: multiple pages in one run, final offset = max seen."""
    docs = [{"id": i, "updated_at": i} for i in range(1, 6)]  # updated_at 1..5
    fake.preseed = {("shop", "orders"): docs}
    source = _component(
        mod, offset_field="updated_at", initial_offset="0", page_size=2
    ).build_defs(None).assets[0]

    instance = dg.DagsterInstance.ephemeral()
    result = dg.materialize([source], instance=instance)
    assert result.success
    mats = result.asset_materializations_for_node("from_mongo")
    assert mats[0].metadata["dagster/row_count"].value == 5
    assert mats[0].metadata["dagster_community_components/last_offset"].value == "5"


def test_offset_field_resumes_from_stored_offset_on_next_run(mod, fake):
    """Second materialize() against the same instance only picks up rows newer than
    what the first run already stored as the high-water-mark."""
    source = _component(
        mod, offset_field="updated_at", initial_offset="0", page_size=10
    ).build_defs(None).assets[0]
    instance = dg.DagsterInstance.ephemeral()

    fake.preseed = {("shop", "orders"): [{"id": 1, "updated_at": 1}, {"id": 2, "updated_at": 2}]}
    result1 = dg.materialize([source], instance=instance)
    assert result1.success
    mats1 = result1.asset_materializations_for_node("from_mongo")
    assert mats1[0].metadata["dagster/row_count"].value == 2
    assert mats1[0].metadata["dagster_community_components/last_offset"].value == "2"

    # Simulate new rows having arrived in the real collection since the first run: the
    # component reconnects (constructs a fresh MongoClient) on every run just like a
    # real app would, so update the preseed the NEXT client will read from rather than
    # mutating the first client's now-discarded in-memory copy.
    fake.preseed = {
        ("shop", "orders"): [
            {"id": 1, "updated_at": 1},
            {"id": 2, "updated_at": 2},
            {"id": 3, "updated_at": 3},
        ]
    }

    result2 = dg.materialize([source], instance=instance)
    assert result2.success
    mats2 = result2.asset_materializations_for_node("from_mongo")
    assert mats2[0].metadata["dagster/row_count"].value == 1  # only the new row
    assert mats2[0].metadata["dagster_community_components/last_offset"].value == "3"


def test_offset_field_with_no_new_rows_reemits_same_offset(mod, fake):
    fake.preseed = {("shop", "orders"): [{"id": 1, "updated_at": 1}]}
    source = _component(
        mod, offset_field="updated_at", initial_offset="0", page_size=10
    ).build_defs(None).assets[0]
    instance = dg.DagsterInstance.ephemeral()
    result1 = dg.materialize([source], instance=instance)
    assert result1.success

    result2 = dg.materialize([source], instance=instance)
    assert result2.success
    mats2 = result2.asset_materializations_for_node("from_mongo")
    assert mats2[0].metadata["dagster/row_count"].value == 0
    assert mats2[0].metadata["dagster_community_components/last_offset"].value == "1"


def test_offset_field_and_partition_type_mutually_exclusive(mod, fake):
    comp = _component(
        mod,
        offset_field="updated_at",
        partition_type="daily",
        partition_start="2024-01-01",
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        comp.build_defs(None)
