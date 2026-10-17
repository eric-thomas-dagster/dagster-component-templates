import importlib.util
from datetime import datetime
from pathlib import Path

import dagster as dg
import pytest

ROOT = Path(__file__).parents[1]


def _load():
    spec = importlib.util.spec_from_file_location("ctx_receipt_lineage", ROOT / "component.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod():
    return _load()


DAILY = dg.DailyPartitionsDefinition("2026-10-01")
WEEKLY = dg.WeeklyPartitionsDefinition("2026-10-01")


@dg.asset(partitions_def=DAILY)
def tickets(): ...


@dg.asset(partitions_def=DAILY, deps=[tickets])
def ticket_chunks(): ...


@dg.asset(deps=[ticket_chunks])
def account_summary(): ...


@dg.asset(partitions_def=WEEKLY, deps=[ticket_chunks])
def weekly_digest(): ...


@dg.asset
def unrelated(): ...


GRAPH = dg.Definitions(
    assets=[tickets, ticket_chunks, account_summary, weekly_digest, unrelated]
).resolve_asset_graph()
NOW = datetime(2026, 10, 20)


def _r(store, asset, partition):
    return store.record_retrieval(
        context_name="c",
        items=[{"item_id": f"{asset}:{partition}", "source_asset_key": asset, "source_partition": partition}],
    )


def _seed(store):
    return {
        "chunks_08": _r(store, "ticket_chunks", "2026-10-08"),
        "chunks_09": _r(store, "ticket_chunks", "2026-10-09"),
        "summary": _r(store, "account_summary", None),
        "digest_wk": _r(store, "weekly_digest", "2026-10-04"),
        "unrelated": _r(store, "unrelated", None),
        "chunks_unknown": _r(store, "ticket_chunks", None),
    }


def test_recall_walks_lineage_and_translates_partitions(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "l.db"))
    ids = _seed(store)
    out = mod.impacted_receipts(store, GRAPH, "tickets", ["2026-10-08"], current_time=NOW)
    assert out["affected"] == {
        "tickets": ["2026-10-08"],
        "ticket_chunks": ["2026-10-08"],  # identity mapping
        "account_summary": None,  # unpartitioned child: whole asset
        "weekly_digest": ["2026-10-04"],  # daily -> weekly mapping
    }
    assert out["receipt_ids"] == sorted([ids["chunks_08"], ids["summary"], ids["digest_wk"]])
    assert out["partition_unknown"] == [ids["chunks_unknown"]]  # can't rule out, so surfaced
    found = out["receipt_ids"] + out["partition_unknown"]
    assert ids["chunks_09"] not in found
    assert ids["unrelated"] not in found


def test_recall_whole_asset_and_errors(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "m.db"))
    ids = _seed(store)
    out = mod.impacted_receipts(store, GRAPH, "tickets", current_time=NOW)
    assert ids["unrelated"] not in out["receipt_ids"]
    assert len(out["receipt_ids"]) == 5 and out["partition_unknown"] == []
    with pytest.raises(KeyError):
        mod.impacted_receipts(store, GRAPH, "nope")


def test_source_partition_round_trips_through_spec_events(mod, tmp_path):
    a = mod.ContextReceiptStore(str(tmp_path / "a.db"))
    rid = _r(a, "tickets", "2026-10-08")
    events = a.export_events(rid)
    assert events[0]["items"][0]["source_partition"] == "2026-10-08"
    b = mod.ContextReceiptStore(str(tmp_path / "b.db"))
    for e in events:
        b.ingest(e)
    assert b.get_receipt(rid)["items"][0]["source_partition"] == "2026-10-08"


@dg.asset(partitions_def=DAILY, name="docs")
def docs_partitioned(context: dg.AssetExecutionContext):
    version = {"2026-10-01": "v2", "2026-10-02": "v9"}[context.partition_key]
    return dg.MaterializeResult(data_version=dg.DataVersion(version))


def test_sensor_judges_staleness_per_partition_and_rebuilds_that_partition(mod, tmp_path):
    store = mod.ContextReceiptStore(str(tmp_path / "s.db"))
    instance = dg.DagsterInstance.ephemeral()
    for pk in ("2026-10-01", "2026-10-02"):
        dg.materialize([docs_partitioned], partition_key=pk, instance=instance)
    sensor = mod.build_usage_sensor(
        store, name="s", interval_seconds=60, rebuild_asset_keys=["docs"],
        stale_reads_threshold=2, prior_strength=1.0,
    )
    for _ in range(2):  # served v1 of partition 10-01, which is now v2: stale
        store.record_retrieval(context_name="c", items=[
            {"item_id": "x", "source_asset_key": "docs", "source_partition": "2026-10-01",
             "source_version": "v1"}])
    store.record_retrieval(context_name="c", items=[  # 10-02 is current: fresh
        {"item_id": "y", "source_asset_key": "docs", "source_partition": "2026-10-02",
         "source_version": "v9"}])
    result = sensor(dg.build_sensor_context(instance=instance))
    by_partition = {e.partition: e.metadata for e in result.asset_events}
    assert set(by_partition) == {"2026-10-01", "2026-10-02"}
    assert by_partition["2026-10-01"]["context_receipts/stale_reads"].value == 2
    assert by_partition["2026-10-02"]["context_receipts/stale_reads"].value == 0
    assert [(r.partition_key, r.asset_selection) for r in result.run_requests] == [
        ("2026-10-01", [dg.AssetKey("docs")])
    ]
