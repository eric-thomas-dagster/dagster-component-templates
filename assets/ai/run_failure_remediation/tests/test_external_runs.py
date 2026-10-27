"""Runs recorded by an external orchestrator (dagster-airlift's `run_storage.add_historical_run`) against a REAL temp-dir
DagsterInstance (OSS sqlite storage -- Dagster+ storage is NOT exercised).

The first two tests pin the OBSERVED Dagster behaviour so a future Dagster change is noticed:
  * a run written with status FAILURE straight into run storage emits NO event, so `run_failure_sensor` never fires for it;
  * airlift's actual sequence (add_historical_run NOT_STARTED, then `report_dagster_event` PIPELINE_START / PIPELINE_FAILURE)
    DOES move the run to FAILURE and DOES fire `run_failure_sensor` -- with `error=None`, so there is no error text.
"""
import json
import logging
from datetime import datetime, timedelta, timezone

import dagster as dg
import pytest
from dagster import DagsterEvent, DagsterRun, DagsterRunStatus, RunsFilter
from dagster._core.events import JobFailureData

from .conftest import load_agentic_module, load_component_module

logging.getLogger("dagster").setLevel(logging.CRITICAL)

AIRFLOW_TAGS = {"dagster-airlift/airflow-dag-id": "load_orders", "dagster/external_job_source": "airflow",
                "dagster-airlift/observation-run": "true"}


def ago(**kw):
    return datetime.now(timezone.utc) - timedelta(**kw)


def add_direct_failure(inst, run_id, job="airflow_load_orders", when=None, tags=None):
    """add_historical_run with status FAILURE and nothing else (what a naive external integration would do)."""
    inst.run_storage.add_historical_run(
        DagsterRun(run_id=run_id, job_name=job, status=DagsterRunStatus.FAILURE, tags=dict(tags if tags is not None else AIRFLOW_TAGS)),
        when or ago(hours=2))


def add_airlift_failure(inst, run_id, job="airflow_load_orders", when=None, tags=None):
    """Exactly what dagster_airlift's DagRunStarted + DagRunCompleted(failed) persist."""
    when = when or ago(hours=2)
    inst.run_storage.add_historical_run(
        DagsterRun(run_id=run_id, job_name=job, status=DagsterRunStatus.NOT_STARTED, tags=dict(tags if tags is not None else AIRFLOW_TAGS)), when)
    inst.report_dagster_event(run_id=run_id, dagster_event=DagsterEvent(event_type_value="PIPELINE_START", job_name=job),
                              timestamp=when.timestamp())
    inst.report_dagster_event(
        run_id=run_id, timestamp=(when + timedelta(minutes=5)).timestamp(),
        dagster_event=DagsterEvent(event_type_value="PIPELINE_FAILURE", job_name=job,
                                   event_specific_data=JobFailureData(error=None, failure_reason=None, first_step_failure_event=None)))


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def inst():
    with dg.instance_for_test() as i:
        yield i


class Env:
    def __init__(self, mod, tmp, inst, detect, **kw):
        self.tmp, self.inst = tmp, inst
        self.dirs = {k: tmp / k for k in ("evidence", "proposals", "approvals", "results")}

        @dg.asset(partitions_def=dg.DynamicPartitionsDefinition(name="run_failures"), group_name="triage")
        def run_triage():
            return 1

        @dg.asset
        def orders():
            return 1

        cfg = dict(evidence_dir=str(tmp / "evidence"), proposals_dir=str(tmp / "proposals"), approval_dir=str(tmp / "approvals"),
                   results_dir=str(tmp / "results"), triage_asset_selection="group:triage", cooldown_minutes=0,
                   default_status="RUNNING", remediation_jobs={"core": "key:orders"}, detect=detect)
        cfg.update(kw)
        self.comp = mod.RunFailureRemediationComponent(**cfg)
        self.defs = dg.Definitions.merge(self.comp.build_defs(None), dg.Definitions(assets=[run_triage, orders]))
        self.cursors = {}
        self.asensor = self.defs.get_sensor_def("run_remediation_approval_sensor")
        self.sensors = {n: self.defs.get_sensor_def(f"run_remediation_{n}") for n in ("failure_sensor", "failed_runs_sensor")
                        if any(s.name == f"run_remediation_{n}" for s in self.defs.sensors)}
        for name in self.sensors:
            self.tick(name)  # first tick arms / initializes the cursor

    def ctx(self, cursor=None):
        return dg.build_sensor_context(instance=self.inst, definitions=self.defs, cursor=cursor, instance_ref=self.inst.get_ref())

    def tick(self, name):
        res = self.sensors[name].evaluate_tick(self.ctx(self.cursors.get(name)))
        self.cursors[name] = res.cursor
        return res

    def requested(self, res):
        return [r.run_key for r in res.run_requests]

    def evidence(self, run_id):
        return json.loads((self.dirs["evidence"] / f"{run_id}.json").read_text())

    def write_proposal(self, run_id, actions):
        self.dirs["proposals"].mkdir(parents=True, exist_ok=True)
        (self.dirs["proposals"] / f"{run_id}.json").write_text(json.dumps(
            {"run_id": run_id, "summary": "airflow task failed", "root_cause": "upstream", "confidence": 0.9, "actions": actions}))

    def results(self, run_id):
        return json.loads((self.dirs["results"] / f"{run_id}.json").read_text())


# ═══ the observed Dagster behaviour (the question this feature answers) ═══

def test_observed_direct_failure_status_emits_no_event_and_run_failure_sensor_does_not_fire(inst):
    seen = []

    @dg.run_failure_sensor(name="plain", monitor_all_code_locations=True)
    def plain(context):
        seen.append(context.dagster_run.run_id)

    defs = dg.Definitions(sensors=[plain])
    mk = lambda c=None: dg.build_sensor_context(instance=inst, definitions=defs, cursor=c, instance_ref=inst.get_ref())  # noqa: E731
    cur = plain.evaluate_tick(mk()).cursor
    add_direct_failure(inst, "direct1")
    assert inst.get_run_by_id("direct1").status == DagsterRunStatus.FAILURE
    assert inst.all_logs("direct1") == []                                      # no RUN_FAILURE (or any) event
    plain.evaluate_tick(mk(cur))
    assert seen == []                                                          # so the event-driven sensor is blind to it
    # ...but the run RECORD is queryable
    recs = inst.get_run_records(RunsFilter(statuses=[DagsterRunStatus.FAILURE], updated_after=ago(hours=1)))
    assert [r.dagster_run.run_id for r in recs] == ["direct1"]


def test_observed_airlift_sequence_does_emit_run_failure_and_run_failure_sensor_fires(inst):
    seen = []

    @dg.run_failure_sensor(name="plain", monitor_all_code_locations=True)
    def plain(context):
        seen.append(context.dagster_run.run_id)

    defs = dg.Definitions(sensors=[plain])
    mk = lambda c=None: dg.build_sensor_context(instance=inst, definitions=defs, cursor=c, instance_ref=inst.get_ref())  # noqa: E731
    cur = plain.evaluate_tick(mk()).cursor
    add_airlift_failure(inst, "airlift1")
    assert inst.get_run_by_id("airlift1").status == DagsterRunStatus.FAILURE
    kinds = [e.dagster_event.event_type_value for e in inst.all_logs("airlift1") if e.dagster_event]
    assert kinds == ["PIPELINE_START", "PIPELINE_FAILURE"]
    cur = plain.evaluate_tick(mk(cur)).cursor
    assert seen == ["airlift1"]
    # persisted LATE with event timestamps from long ago: still fires (the event cursor is the event-log id, not the timestamp)
    add_airlift_failure(inst, "airlift_late", when=ago(hours=30))
    plain.evaluate_tick(mk(cur))
    assert seen == ["airlift1", "airlift_late"]


# ═══ detect: events (today's behaviour) ═══

def test_events_mode_ignores_a_direct_failure_but_handles_airlift_runs_with_limited_evidence(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "events")
    assert set(env.sensors) == {"failure_sensor"}
    add_direct_failure(inst, "direct1")
    assert env.requested(env.tick("failure_sensor")) == []
    assert not (env.dirs["evidence"] / "direct1.json").exists()
    add_airlift_failure(inst, "airlift1")
    assert env.requested(env.tick("failure_sensor")) == ["airlift1"]
    ev = env.evidence("airlift1")
    assert ev["source"] == "events" and ev["limited"] is True                # PIPELINE_FAILURE carries no error, no steps
    assert ev["external_source"] == "airflow" and ev["errors"] == []


# ═══ detect: runs ═══

def test_runs_mode_triages_a_direct_failure_once_with_run_record_evidence(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "runs")
    assert set(env.sensors) == {"failed_runs_sensor"}
    add_direct_failure(inst, "direct1", when=ago(hours=3))
    res = env.tick("failed_runs_sensor")
    assert env.requested(res) == ["direct1"]
    assert res.run_requests[0].partition_key == "direct1"
    assert res.dynamic_partitions_requests and list(res.dynamic_partitions_requests[0].partition_keys) == ["direct1"]
    ev = env.evidence("direct1")
    assert ev["source"] == "run_record" and ev["limited"] is True
    assert ev["job_name"] == "airflow_load_orders" and ev["status"] == "FAILURE"
    assert ev["external_source"] == "airflow" and ev["tags"]["dagster-airlift/airflow-dag-id"] == "load_orders"
    assert ev["failed_step_keys"] == [] and ev["errors"] == []
    # nothing again on later ticks
    assert env.requested(env.tick("failed_runs_sensor")) == []
    assert env.requested(env.tick("failed_runs_sensor")) == []


def test_runs_mode_also_sees_airlift_runs_and_skips_failures_that_predate_arming(mod, tmp_path, inst):
    add_direct_failure(inst, "old_before_arming")
    env = Env(mod, tmp_path, inst, "runs")                                    # arming marks it seen, does not triage it
    add_airlift_failure(inst, "airlift1")
    assert env.requested(env.tick("failed_runs_sensor")) == ["airlift1"]
    assert not (env.dirs["evidence"] / "old_before_arming.json").exists()


def test_runs_cursor_loses_nothing_with_identical_timestamps_and_late_persisted_runs(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "runs", runs_max_per_tick=2)
    same = ago(hours=1).replace(microsecond=0)
    for i in range(5):                                                         # identical create/update timestamps
        add_direct_failure(inst, f"same{i}", when=same)
    got = []
    for _ in range(2):
        got += env.requested(env.tick("failed_runs_sensor"))
    assert len(got) == 4                                                       # max 2 per tick, oldest (ts, run_id) first
    # a run persisted NOW whose timestamps are far older than ones already seen is not lost (an updated_timestamp high-water
    # mark would skip it) -- and neither is one that only becomes FAILURE after rows with a higher id were seen
    add_airlift_failure(inst, "late_old", when=ago(hours=20))
    inst.run_storage.add_historical_run(DagsterRun(run_id="pending_later", job_name="j", status=DagsterRunStatus.NOT_STARTED,
                                                   tags=dict(AIRFLOW_TAGS)), ago(hours=19))
    add_direct_failure(inst, "higher_id", when=ago(minutes=30))
    for _ in range(4):
        got += env.requested(env.tick("failed_runs_sensor"))
    inst.report_dagster_event(run_id="pending_later", dagster_event=DagsterEvent(event_type_value="PIPELINE_FAILURE", job_name="j"),
                              timestamp=ago(hours=18).timestamp())
    for _ in range(3):
        got += env.requested(env.tick("failed_runs_sensor"))
    assert sorted(got) == sorted([f"same{i}" for i in range(5)] + ["late_old", "higher_id", "pending_later"])
    assert len(got) == len(set(got))                                           # nothing doubled


def test_runs_mode_cursor_survives_rebuilding_the_definitions(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "runs")
    add_direct_failure(inst, "d1")
    assert env.requested(env.tick("failed_runs_sensor")) == ["d1"]
    env2 = Env.__new__(Env)
    env2.__dict__.update(env.__dict__)
    env2.defs = dg.Definitions.merge(env.comp.build_defs(None), dg.Definitions(assets=[]))
    env2.sensors = {"failed_runs_sensor": env2.defs.get_sensor_def("run_remediation_failed_runs_sensor")}
    assert env2.requested(env2.tick("failed_runs_sensor")) == []


def test_runs_mode_filters_only_tags_only_jobs_and_no_recursion(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "runs", only_tags={"dagster-airlift/airflow-dag-id": "*"}, ignore_jobs=["ignored_job"])
    add_direct_failure(inst, "keep")
    add_direct_failure(inst, "no_tag", tags={"team": "x"})
    add_direct_failure(inst, "ignored", job="ignored_job")
    add_direct_failure(inst, "triage_self", job="run_failure_triage_job")
    assert env.requested(env.tick("failed_runs_sensor")) == ["keep"]
    assert env.requested(env.tick("failed_runs_sensor")) == []                 # the filtered ones are decided, not re-judged forever

    env = Env(mod, tmp_path / "b", inst, "runs", only_tags={"dagster/external_job_source": "airflow"}, only_jobs=["airflow_load_orders"])
    add_direct_failure(inst, "other_job", job="airflow_other")
    add_direct_failure(inst, "wrong_value", job="airflow_load_orders", tags={"dagster/external_job_source": "dbt"})
    add_direct_failure(inst, "match2", job="airflow_load_orders")
    assert env.requested(env.tick("failed_runs_sensor")) == ["match2"]


def test_runs_mode_cooldown_and_rate_limit_apply_and_rate_limited_runs_wait(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "runs", cooldown_minutes=60)
    add_direct_failure(inst, "c1")
    add_direct_failure(inst, "c2")                                             # same job + signature: cooldown suppresses it
    assert env.requested(env.tick("failed_runs_sensor")) == ["c1"]
    assert env.requested(env.tick("failed_runs_sensor")) == []
    env = Env(mod, tmp_path / "r", inst, "runs", max_triage_per_hour=1, runs_lookback_hours=48)
    add_direct_failure(inst, "r1", job="job_a")
    add_direct_failure(inst, "r2", job="job_b")
    first = env.requested(env.tick("failed_runs_sensor"))
    assert len(first) == 1
    assert env.requested(env.tick("failed_runs_sensor")) == []                 # deferred (still undecided), not dropped
    from datetime import timedelta
    real_now = mod._now
    mod._now = lambda: real_now() + timedelta(minutes=61)                      # the hour passes
    second = env.requested(env.tick("failed_runs_sensor"))
    mod._now = real_now
    assert sorted(first + second) == ["r1", "r2"]


# ═══ detect: both ═══

def test_both_mode_never_triages_the_same_run_twice(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "both")
    assert set(env.sensors) == {"failure_sensor", "failed_runs_sensor"}
    add_airlift_failure(inst, "both1")                                         # seen by BOTH sensors
    add_direct_failure(inst, "only_runs")                                      # seen only by the record sensor
    ev = env.requested(env.tick("failure_sensor"))
    rr = env.requested(env.tick("failed_runs_sensor"))
    assert ev == ["both1"] and rr == ["only_runs"]
    for _ in range(2):
        assert env.requested(env.tick("failure_sensor")) == [] and env.requested(env.tick("failed_runs_sensor")) == []
    assert len(list((env.dirs["evidence"] / "_state" / "ratelimit").glob("*-triage.json"))) == 2


def test_both_mode_record_sensor_first_then_event_sensor_replays_the_same_run_key(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, "both")
    add_airlift_failure(inst, "both1")
    assert env.requested(env.tick("failed_runs_sensor")) == ["both1"]
    # the event sensor re-judges it, but it is the same run_key (the daemon dedupes it) and costs no extra rate-limit budget
    assert env.requested(env.tick("failure_sensor")) == ["both1"]
    assert len(list((env.dirs["evidence"] / "_state" / "ratelimit").glob("*-triage.json"))) == 1


# ═══ actions for runs of an external orchestrator ═══

def test_retry_and_rerun_are_rejected_for_external_runs_but_ticket_still_works(mod, tmp_path, inst):
    agentic = load_agentic_module()
    env = Env(mod, tmp_path, inst, "runs")
    add_direct_failure(inst, "ext1")
    env.tick("failed_runs_sensor")
    env.write_proposal("ext1", [{"kind": "retry_run", "params": {}, "reason": "try again"},
                                {"kind": "rerun_partition", "params": {}, "reason": "again"},
                                {"kind": "open_ticket", "params": {"title": "Airflow load_orders failed", "body": "see airflow", "severity": "high"},
                                 "reason": "needs a human"}])
    env.asensor.evaluate_tick(env.ctx())
    agentic.approve_pending(str(env.dirs["approvals"]), "ext1", approved=True, approver="alice", reason="ok")
    res = env.asensor.evaluate_tick(env.ctx())
    assert res.run_requests == []                                              # nothing to re-execute
    rec = env.results("ext1")
    by = {a["kind"]: a for a in rec["actions"]}
    for k in ("retry_run", "rerun_partition"):
        assert by[k]["status"] == "rejected"
        assert "external orchestrator (airflow)" in by[k]["reason"] and "no Dagster job to re-execute" in by[k]["reason"]
    assert by["open_ticket"]["status"] == "done"
    assert (env.dirs["results"] / "tickets" / "ext1-2.json").exists()


def test_config_validation(mod, tmp_path):
    with pytest.raises(ValueError, match="detect"):
        mod.RunFailureRemediationComponent(evidence_dir="e", proposals_dir="p", approval_dir="a", results_dir="r",
                                           triage_asset_selection="group:t", detect="sometimes").build_defs(None)
    c = mod.RunFailureRemediationComponent(evidence_dir="e", proposals_dir="p", approval_dir="a", results_dir="r",
                                           triage_asset_selection="group:t", only_tags=["a=b"])
    assert c.only_tags == {"a": "b"} and c.detect == "events"
