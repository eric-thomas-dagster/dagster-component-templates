"""asset_status / list_recent_failures / stale_assets report `last_failure` from the failure observation contract, REAL Dagster instance.

The failing asset is the real receipt_outcome_ingestion component (a missing feedback file), so the observation is exactly what a
shipped component writes; a real ASSET_FAILED_TO_MATERIALIZE event is simulated (OSS storages never keep one) to prove the
forward-compatible path wins when it exists."""
import csv
import importlib.util
import pathlib
import sys
from types import SimpleNamespace

import dagster as dg
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[4]


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


@pytest.fixture(scope="module")
def ing():
    return _load(ROOT / "assets" / "ai" / "receipt_outcome_ingestion" / "component.py", "ingestion_for_instance_tools_tests")


@pytest.fixture()
def env(ing, mod, tmp_path):
    store = _load(ROOT / "resources" / "context_receipt_store" / "component.py", "store_for_instance_tools_tests").ContextReceiptStore(
        str(tmp_path / "r.db"))
    rid = store.record_retrieval(context_name="c", dagster_asset_key="kb", items=[{"item_id": "c1", "source_asset_key": "kb", "rank": 0}])
    (tmp_path / "home").mkdir()
    inst = dg.DagsterInstance.local_temp(str(tmp_path / "home"))
    path = tmp_path / "fb.csv"
    mapping = {"receipt_id": "receipt_id", "signal": "reaction", "outcome_ref": "user", "value_map": {"good": "thumbs_up"}}
    c = ing.ReceiptOutcomeIngestionComponent(source={"kind": "file", "path": str(path)}, mapping=mapping)

    def run():
        return dg.materialize(c.build_defs(None).assets, resources={"context_receipts": store}, instance=inst, raise_on_error=False)

    def heal():
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["receipt_id", "reaction", "user"])
            w.writeheader()
            w.writerow({"receipt_id": rid, "reaction": "good", "user": "u1"})
    return SimpleNamespace(inst=inst, run=run, heal=heal, tools=mod.DagsterInstanceTools(inst, audit=lambda s: None))


KEY = "receipt_outcome_ingestion"


def test_asset_status_reports_last_failure_from_the_observation(env):
    r = env.run()
    assert not r.success
    s = env.tools.call("asset_status", {"asset_key": KEY})
    lf = s["last_failure"]
    assert lf["source"] == "failure_observation" and lf["reason"] == "FAILED_TO_MATERIALIZE" and lf["run_id"] == r.run_id
    assert lf["asset_key"] == KEY and lf["failed_step"] == "read_file" and lf["error_class"] and lf["error_message"] and lf["at"]
    assert lf["will_retry"] is None            # no retry policy: not knowable, omitted by the component
    assert s["latest_failed_run_since"]["run_id"] == r.run_id   # the run-status derivation is still there


def test_a_later_good_materialization_clears_last_failure(env):
    assert not env.run().success
    env.heal()
    assert env.run().success
    s = env.tools.call("asset_status", {"asset_key": KEY})
    assert s["last_failure"] is None and s["latest_failed_run_since"] is None


def test_the_next_failure_after_a_good_run_is_reported(env, tmp_path):
    env.heal()
    assert env.run().success
    (tmp_path / "fb.csv").unlink()
    r = env.run()
    assert not r.success
    assert env.tools.call("asset_status", {"asset_key": KEY})["last_failure"]["run_id"] == r.run_id


def test_list_recent_failures_carries_per_asset_failures(env):
    r = env.run()
    out = env.tools.call("list_recent_failures", {"hours": 1})
    run = next(x for x in out["runs"] if x["run_id"] == r.run_id)
    [af] = run["asset_failures"]
    assert af["asset_key"] == KEY and af["reason"] == "FAILED_TO_MATERIALIZE" and af["failed_step"] == "read_file" and af["error_class"]


def test_stale_assets_includes_last_failure(env):
    env.run()
    rows = env.tools.call("stale_assets", {"max_age_hours": 24})["assets"]
    row = next(x for x in rows if x["asset_key"] == KEY) if rows else None
    assert row is not None and row["last_failure"]["source"] == "failure_observation"


def test_a_real_asset_failed_to_materialize_event_is_preferred(env, mod, monkeypatch):
    r = env.run()
    from dagster._core.definitions.events import AssetMaterializationFailure, AssetMaterializationFailureReason, AssetMaterializationFailureType
    from dagster._core.events import AssetFailedToMaterializeData
    from dagster._core.utils import make_new_run_id  # noqa: F401
    fail = AssetMaterializationFailure(asset_key=dg.AssetKey(KEY), description="x", metadata={}, partition=None, tags={},
                                       failure_type=AssetMaterializationFailureType.FAILED, reason=AssetMaterializationFailureReason.RUN_TERMINATED,
                                       will_retry=False)
    data = AssetFailedToMaterializeData(asset_materialization_failure=fail)
    rec = SimpleNamespace(timestamp=9e9, run_id="run-xyz",
                          event_log_entry=SimpleNamespace(dagster_event=SimpleNamespace(event_specific_data=data)))
    real = env.inst.get_event_records

    def fake(filt, limit=None, ascending=False):
        if filt.event_type == dg.DagsterEventType.ASSET_FAILED_TO_MATERIALIZE:
            return [rec]
        return real(filt, limit=limit, ascending=ascending)
    monkeypatch.setattr(env.inst, "get_event_records", fake)
    lf = env.tools.call("asset_status", {"asset_key": KEY})["last_failure"]
    assert lf["source"] == "asset_failed_to_materialize_event" and lf["reason"] == "RUN_TERMINATED" and lf["run_id"] == "run-xyz"
    assert lf["will_retry"] is False
    assert r.run_id != "run-xyz"


def test_falls_back_to_the_failed_run_when_no_observation_exists(mod, world):
    s = mod.DagsterInstanceTools(world["instance"], audit=lambda x: None).call("asset_status", {"asset_key": "bad"})
    lf = s["last_failure"]
    assert lf["source"] == "failed_run" and lf["run_id"] == world["bad_run"] and lf["error_class"] == "ValueError" and lf["reason"] is None


def test_healthy_asset_has_no_last_failure(mod, world):
    s = mod.DagsterInstanceTools(world["instance"], audit=lambda x: None).call("asset_status", {"asset_key": "ok"})
    assert s["last_failure"] is None


def test_partition_healed_by_a_later_materialization_of_that_partition_only(mod, ing, tmp_path):
    (tmp_path / "home").mkdir()
    inst = dg.DagsterInstance.local_temp(str(tmp_path / "home"))
    state = {"fail": {"p1", "p2"}}

    @dg.asset(partitions_def=dg.StaticPartitionsDefinition(["p1", "p2"]))
    def part(context: dg.AssetExecutionContext):
        pk = context.partition_key
        try:
            if pk in state["fail"]:
                raise RuntimeError(f"boom {pk}")
        except Exception as exc:
            ing._fo_emit(context, exc, counter=ing._FO_COUNTER, asset_keys=[context.asset_key], failed_step="s")
            raise
    t = mod.DagsterInstanceTools(inst, audit=lambda s: None)
    assert not dg.materialize([part], instance=inst, partition_key="p1", raise_on_error=False).success
    assert not dg.materialize([part], instance=inst, partition_key="p2", raise_on_error=False).success
    assert t.call("asset_status", {"asset_key": "part"})["last_failure"]["partition"] == "p2"      # newest
    state["fail"] = {"p2"}
    assert dg.materialize([part], instance=inst, partition_key="p1").success                        # p1 heals, p2 still broken
    lf = t.call("asset_status", {"asset_key": "part"})["last_failure"]
    assert lf["partition"] == "p2" and "boom p2" in lf["error_message"]     # p1's later materialization must not hide p2's failure
    state["fail"] = set()
    assert dg.materialize([part], instance=inst, partition_key="p2").success
    assert t.call("asset_status", {"asset_key": "part"})["last_failure"] is None
