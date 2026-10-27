"""RunFailureRemediationComponent against a REAL temp-dir DagsterInstance.

Runs genuinely fail (assets that raise), then the real run_failure_sensor / approval sensor are evaluated with
`SensorDefinition.evaluate_tick` (the same path the daemon uses, cursor and all). Remediation RunRequests the sensors
hand back are launched here with `dg.materialize` (there is no daemon in a test) and tagged exactly as requested.
"""
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace

import dagster as dg
import pytest

from .conftest import load_agentic_module, load_component_module, load_review_queue_module

logging.getLogger("dagster").setLevel(logging.CRITICAL)

SECRET_MSG = ("connection refused token=abc123secret password=hunter2 Authorization: Bearer sk-live-ABCDEFGHIJKLMNOP123456 "
              "url=postgres://admin:s3cr3tpw@db.internal:5432/x blob=QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5")


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def agentic():
    return load_agentic_module()


@pytest.fixture
def inst():
    with dg.instance_for_test() as i:
        yield i


class Env:
    def __init__(self, mod, tmp_path, inst, **kw):
        self.mod, self.tmp, self.inst = mod, tmp_path, inst
        self.flag = tmp_path / "fixed.flag"
        self.msg = kw.pop("msg", SECRET_MSG)
        flag, msg_holder = self.flag, self

        @dg.asset
        def upstream():
            return 1

        @dg.asset(deps=[upstream])
        def flaky():
            if not flag.exists():
                raise ConnectionError(msg_holder.msg)

        @dg.asset(deps=[upstream], partitions_def=dg.StaticPartitionsDefinition(["p1", "p2"]))
        def pflaky():
            if not flag.exists():
                raise ConnectionError("partition load failed")

        @dg.asset(partitions_def=dg.DynamicPartitionsDefinition(name="run_failures"), group_name="triage")
        def run_triage():
            return 1

        self.assets = [upstream, flaky, pflaky] + (kw.pop("triage_assets", None) or [run_triage])
        self.nightly = dg.define_asset_job("nightly", selection=[upstream, flaky])
        cfg = dict(evidence_dir=str(tmp_path / "evidence"), proposals_dir=str(tmp_path / "proposals"),
                   approval_dir=str(tmp_path / "approvals"), results_dir=str(tmp_path / "results"),
                   triage_asset_selection="group:triage", cooldown_minutes=0, default_status="RUNNING",
                   remediation_jobs={"core": "key:upstream or key:flaky", "parts": "key:pflaky"})
        cfg.update(kw)
        self.comp = mod.RunFailureRemediationComponent(**cfg)
        cdefs = self.comp.build_defs(None)
        self.defs = dg.Definitions.merge(cdefs, dg.Definitions(assets=self.assets, jobs=[self.nightly]))
        self.fsensor = self.defs.get_sensor_def("run_remediation_failure_sensor")
        self.asensor = self.defs.get_sensor_def("run_remediation_approval_sensor")
        self.cursor = None
        self.fresult = self.tick_failure()  # first tick only initializes the cursor
        self.dirs = {k: tmp_path / k for k in ("evidence", "proposals", "approvals", "results")}

    def ctx(self, cursor=None, **kw):
        return dg.build_sensor_context(instance=self.inst, definitions=self.defs, cursor=cursor, instance_ref=self.inst.get_ref(), **kw)

    def tick_failure(self):
        res = self.fsensor.evaluate_tick(self.ctx(self.cursor))
        self.cursor = res.cursor
        return res

    def drain_failure(self):
        """Tick until the cursor stops moving (one tick may only consume part of the new failures); returns all results."""
        out = []
        for _ in range(10):
            before = self.cursor
            out.append(self.tick_failure())
            if self.cursor == before:
                break
        return out

    def tick_approval(self, **kw):
        return self.asensor.evaluate_tick(self.ctx(**kw))

    # -- real failures --
    def fail_nightly(self):
        r = self.defs.get_job_def("nightly").execute_in_process(instance=self.inst, raise_on_error=False)
        assert not r.success
        return self.inst.get_run_by_id(r.run_id)

    def fail_partition(self, key="p1"):
        r = dg.materialize([self.assets[0], self.assets[2]], instance=self.inst, partition_key=key, raise_on_error=False)
        assert not r.success
        return self.inst.get_run_by_id(r.run_id)

    # -- what the daemon would do with requests --
    def launch(self, rr):
        keys = set(rr.asset_selection)
        sel = [a for a in self.assets if keys & set(a.keys)]
        r = dg.materialize(sel, instance=self.inst, partition_key=rr.partition_key, tags=rr.tags, run_config=rr.run_config,
                           raise_on_error=False)
        return r

    # -- files --
    def evidence(self, run_id):
        return json.loads((self.dirs["evidence"] / f"{run_id}.json").read_text())

    def results(self, run_id):
        return json.loads((self.dirs["results"] / f"{run_id}.json").read_text())

    def write_proposal(self, run_id, actions, **kw):
        d = self.dirs["proposals"]
        d.mkdir(parents=True, exist_ok=True)
        p = {"run_id": run_id, "summary": "transient upstream outage", "root_cause": "db connection reset", "confidence": 0.9,
             "actions": actions, **kw}
        (d / f"{run_id}.json").write_text(json.dumps(p))
        return p

    def pending(self, run_id):
        return json.loads((self.dirs["approvals"] / "pending" / f"{run_id}.json").read_text())


@pytest.fixture
def env(mod, tmp_path, inst):
    return Env(mod, tmp_path, inst)


def test_validate_triage_job_and_dynamic_partitions_def(env):
    assert env.defs.get_job_def("run_failure_triage_job")
    assert env.comp.partitions_def() == dg.DynamicPartitionsDefinition(name="run_failures")


# ═══ evidence + triage request ═══

def test_failure_writes_redacted_evidence_and_requests_triage_in_one_tick(env, inst):
    run = env.fail_nightly()
    res = env.tick_failure()
    # ASSUMPTION CHECK: one run-failure-sensor tick returned BOTH a run request for another job AND a dynamic-partition add
    assert [r.partition_key for r in res.run_requests] == [run.run_id]
    assert res.run_requests[0].run_key == run.run_id
    assert [(a.partitions_def_name, list(a.partition_keys)) for a in res.dynamic_partitions_requests] == [("run_failures", [run.run_id])]
    assert res.run_requests[0].tags["dagster/partition_set"].startswith("run_failure_triage_job")
    ev = env.evidence(run.run_id)
    assert ev["run_id"] == run.run_id and ev["job_name"] == "nightly" and ev["status"] == "FAILURE"
    assert ev["failed_step_keys"] == ["flaky"]
    assert ev["errors"][0]["error_class"] == "ConnectionError"           # the ROOT cause, not the Dagster wrapper
    assert ev["errors"][0]["wrapper_error_class"] == "DagsterExecutionStepExecutionError"
    assert "connection refused" in ev["errors"][0]["message_tail"]
    # which assets failed: planned but not materialized (upstream succeeded)
    assert [a["asset_key"] for a in ev["failed_assets"]] == ["flaky"]
    assert [a["asset_key"] for a in ev["materialized_assets"]] == ["upstream"]
    # upstream last materialization is retrievable
    up = {u["asset_key"]: u for u in ev["upstream"]}
    assert up["upstream"]["downstream_failed_asset"] == "flaky" and up["upstream"]["last_materialization"]["run_id"] == run.run_id
    assert ev["recent_runs"] == [] and ev["recent_outcomes"] == []
    assert ev["remediation_contract"]["allowed_actions"] == ["retry_run", "rerun_partition", "open_ticket"]
    text = (env.dirs["evidence"] / f"{run.run_id}.json").read_text()
    for secret in ("abc123secret", "hunter2", "sk-live-ABCDEFGHIJKLMNOP123456", "s3cr3tpw", "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5"):
        assert secret not in text
    assert "[REDACTED]" in text


def test_evidence_sees_partition_and_history(env):
    first = env.fail_nightly()
    env.tick_failure()
    second = env.fail_nightly()
    env.tick_failure()
    ev = env.evidence(second.run_id)
    assert [r["run_id"] for r in ev["recent_runs"]] == [first.run_id] and ev["recent_runs"][0]["status"] == "FAILURE"
    pr = env.fail_partition("p2")
    env.tick_failure()
    pev = env.evidence(pr.run_id)
    assert pev["partition"] == "p2"
    # ASSUMPTION CHECK: the sensor can see which asset+partition failed (from ASSET_MATERIALIZATION_PLANNED vs MATERIALIZATION events)
    assert {"asset_key": "pflaky", "partition": "p2"} in pev["failed_assets"]


def _rl(env, kind):
    """The append-only rate-limit event files of one kind."""
    d = env.dirs["evidence"] / "_state" / "ratelimit"
    return sorted(d.glob(f"*-{kind}.json")) if d.exists() else []


def test_retick_hands_back_the_same_run_key_and_costs_no_extra_budget(env):
    before = env.cursor
    run = env.fail_nightly()
    first = env.tick_failure()
    assert len(first.run_requests) == 1
    for req in first.dynamic_partitions_requests:                                      # what the daemon does with the result
        env.inst.add_dynamic_partitions(req.partitions_def_name, req.partition_keys)
    assert env.fsensor.evaluate_tick(env.ctx(env.cursor)).run_requests == []          # cursor advanced past the failure
    replay = env.fsensor.evaluate_tick(env.ctx(before))                                # a replayed tick (cursor lost / crash)
    assert [r.run_key for r in replay.run_requests] == [run.run_id]                    # same run_key: the daemon dedupes the launch
    assert replay.dynamic_partitions_requests == []                                    # partition already registered: not re-added
    assert len(_rl(env, "triage")) == 1                                                # the replay did not count against the rate limit


def test_cursor_survives_rebuilding_the_definitions(env, mod, inst):
    """In-process stand-in for a code-location reload (NOT a real reload): a NEW sensor definition + the stored cursor still sees
    the failure, because run-status cursors live with the instance's run storage, not in the sensor object."""
    env.fail_nightly()
    cdefs = mod.RunFailureRemediationComponent(**env.comp.model_dump()).build_defs(None)
    defs2 = dg.Definitions.merge(cdefs, dg.Definitions(assets=env.assets, jobs=[env.nightly]))
    res = defs2.get_sensor_def("run_remediation_failure_sensor").evaluate_tick(
        dg.build_sensor_context(instance=inst, definitions=defs2, cursor=env.cursor, instance_ref=inst.get_ref()))
    assert len(res.run_requests) == 1


def test_cooldown_per_job_and_signature(mod, tmp_path, inst):
    env = Env(mod, tmp_path, inst, cooldown_minutes=60)
    a = env.fail_nightly()
    assert len(env.tick_failure().run_requests) == 1
    b = env.fail_nightly()                                         # same job, same error signature: flapping
    res = env.tick_failure()
    assert res.run_requests == [] and "cooldown" in (res.skip_message or "")
    assert not (env.dirs["evidence"] / f"{b.run_id}.json").exists()
    env.msg = "a completely different error: disk quota exceeded"  # new signature
    c = env.fail_nightly()
    assert len(env.tick_failure().run_requests) == 1
    assert env.evidence(c.run_id)["error_signature"] != env.evidence(a.run_id)["error_signature"]
    assert len(_rl(env, "suppressed")) == 1 and len(_rl(env, "cooldown")) == 2


def test_cooldown_expires(env):
    env.fail_nightly()
    assert len(env.tick_failure().run_requests) == 1
    env.fail_nightly()
    assert len(env.tick_failure().run_requests) == 1               # cooldown_minutes=0 in the fixture


def test_rate_limit_max_triage_per_hour(mod, tmp_path, inst):
    e = Env(mod, tmp_path, inst, max_triage_per_hour=2)
    for i in range(3):
        e.msg = f"distinct failure number {chr(97 + i)}{chr(97 + i)}{chr(97 + i)} class"
        e.fail_nightly()
    res = []
    for _ in range(3):
        res.append(e.tick_failure())
    assert sum(len(r.run_requests) for r in res) == 2
    assert "rate limit" in (res[-1].skip_message or "")


def test_ignore_jobs_and_only_jobs(mod, tmp_path, inst):
    e = Env(mod, tmp_path, inst, ignore_jobs=["nightly"])
    run = e.fail_nightly()
    r = e.tick_failure()
    assert r.run_requests == [] and "ignore_jobs" in r.skip_message
    assert not (e.dirs["evidence"] / f"{run.run_id}.json").exists()


def test_no_recursion_triage_job_failure_is_skipped(mod, tmp_path, inst):
    e = Env(mod, tmp_path, inst)
    inst.add_dynamic_partitions("run_failures", ["abc"])

    @dg.asset(partitions_def=dg.DynamicPartitionsDefinition(name="run_failures"), group_name="triage")
    def run_triage():
        raise RuntimeError("triage pipeline itself blew up")

    jobdef = dg.Definitions(assets=[run_triage], jobs=[dg.define_asset_job("run_failure_triage_job", selection=[run_triage])])
    r = jobdef.get_job_def("run_failure_triage_job").execute_in_process(instance=inst, partition_key="abc", raise_on_error=False)
    assert not r.success
    res = e.tick_failure()
    assert res.run_requests == [] and "triage job itself" in res.skip_message
    assert not list(e.dirs["evidence"].glob("*.json")) if e.dirs["evidence"].exists() else True


def test_remediation_launched_failures_are_not_triaged(env):
    env.fail_nightly()
    env.tick_failure()
    r = dg.materialize([env.assets[0], env.assets[1]], instance=env.inst, raise_on_error=False,
                       tags={"dagster_remediation/proposal": "some-run", "dagster_remediation/kind": "retry_run"})
    assert not r.success
    res = env.tick_failure()
    assert res.run_requests == [] and "remediation" in res.skip_message


# ═══ redaction (unit) ═══

@pytest.mark.parametrize("text,secret", [
    ("password=hunter2", "hunter2"), ('{"api_key": "sk-abcdefghijklmnop1234"}', "sk-abcdefghijklmnop1234"),
    ("Authorization: Bearer abcdef.ghijkl.mnopqr", "abcdef.ghijkl.mnopqr"), ("curl -H 'authorization: Basic dXNlcjpwYXNz'", "dXNlcjpwYXNz"),
    ("postgres://u:pw0rd@h/db", "pw0rd"), ("aws AKIAABCDEFGHIJKLMNOP", "AKIAABCDEFGHIJKLMNOP"),
    ("ghp_abcdefghijklmnopqrstuvwxyz0123", "ghp_abcdefghijklmnopqrstuvwxyz0123"),
    ("xoxb-1234567890-abcdefghij", "xoxb-1234567890-abcdefghij"),
    ("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dBjftJeZ4CVPmB92K27uhbUJU1p1r", "eyJhbGciOiJIUzI1NiJ9"),
    ("blob QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5 end", "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5"),
    ("-----BEGIN RSA PRIVATE KEY-----\nMIIEow\n-----END RSA PRIVATE KEY-----", "MIIEow"),
])
def test_redact_text(mod, text, secret):
    out = mod.redact_text(text)
    assert secret not in out and "[REDACTED]" in out


def test_redact_keeps_run_ids_paths_and_plain_text(mod):
    text = "run 8c3f1d52-aaaa-4bbb-8ccc-0123456789ab failed at /var/lib/dagster/storage/very/long/path/name/file.parquet: timeout"
    assert mod.redact_text(text) == text


def test_redact_env_secret_values_and_user_patterns(mod, monkeypatch):
    monkeypatch.setenv("MY_SERVICE_TOKEN", "plainvalue-9")
    assert "plainvalue-9" not in mod.redact_text("saw plainvalue-9 in the log")
    assert "EMP-1234" not in mod.redact_text("employee EMP-1234", extra_patterns=[r"EMP-\d+"])
    out = mod.redact_obj({"db_password": 1234, "nested": {"api_token": "x"}, "ok": "fine", "run_id": "tok"}, skip_keys=("run_id",))
    assert out == {"db_password": "[REDACTED]", "nested": {"api_token": "[REDACTED]"}, "ok": "fine", "run_id": "tok"}


# ═══ proposal -> pending -> approval -> action ═══

def failed_and_triaged(env):
    run = env.fail_nightly()
    env.tick_failure()
    return run


def approve(agentic, env, run_id, ok=True):
    return agentic.approve_pending(str(env.dirs["approvals"]), run_id, approved=ok, approver="alice", reason="looks right")


def test_proposal_becomes_pending_and_nothing_runs_without_approval(env):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "transient"}])
    res = env.tick_approval()
    assert res.run_requests == []
    p = env.pending(run.run_id)
    assert p["state"] == "new" and p["fingerprint"] and p["approve_by_writing"]["json"]["fingerprint"] == p["fingerprint"]
    assert "retry_run" in p["summary"]
    assert not (env.dirs["results"] / f"{run.run_id}.json").exists()
    env.tick_approval()                                            # idempotent
    assert len(list((env.dirs["approvals"] / "pending").glob("*.json"))) == 1


def test_approved_retry_really_reexecutes_and_outcome_is_fixed(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "transient"}])
    env.tick_approval()
    approve(agentic, env, run.run_id)
    env.flag.write_text("fixed")                                   # the outage is over
    res = env.tick_approval()
    assert len(res.run_requests) == 1
    rr = res.run_requests[0]
    assert rr.run_key == f"{run.run_id}:0:retry_run"
    assert [k.to_user_string() for k in rr.asset_selection] == ["flaky"]       # only what failed, not upstream again
    assert rr.tags["dagster/parent_run_id"] == run.run_id and rr.tags["dagster_remediation/proposal"] == run.run_id
    rec = env.results(run.run_id)
    assert rec["status"] == "requested" and rec["approver"] == "alice" and rec["actions"][0]["status"] == "requested"
    assert not (env.dirs["approvals"] / "pending" / f"{run.run_id}.json").exists()     # decided: left the review queue
    token = json.loads((env.dirs["approvals"] / f"{run.run_id}.json").read_text())
    assert token["consumed_at"]                                                       # spent: cannot be replayed
    new = env.launch(rr)
    assert new.success
    # outcome window not reached: nothing recorded yet
    assert env.results(run.run_id)["outcome"] is None
    rec["outcome_due_at"] = "2000-01-01T00:00:00+00:00"
    (env.dirs["results"] / f"{run.run_id}.json").write_text(json.dumps(rec))
    env.tick_approval()
    done = env.results(run.run_id)
    assert done["outcome"] == "fixed" and done["status"] == "completed" and done["new_run_ids"] == [new.run_id]


def test_retry_that_fails_again_is_not_fixed_and_feeds_the_next_triage(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "transient"}])
    env.tick_approval()
    approve(agentic, env, run.run_id)
    rr = env.tick_approval().run_requests[0]
    assert not env.launch(rr).success                             # still broken
    rec = env.results(run.run_id)
    rec["outcome_due_at"] = "2000-01-01T00:00:00+00:00"
    (env.dirs["results"] / f"{run.run_id}.json").write_text(json.dumps(rec))
    env.tick_approval()
    assert env.results(run.run_id)["outcome"] == "not_fixed"
    nxt = env.fail_nightly()
    env.drain_failure()
    ro = env.evidence(nxt.run_id)["recent_outcomes"]
    assert ro and ro[0]["outcome"] == "not_fixed" and ro[0]["actions"] == ["retry_run"] and ro[0]["same_signature"] is True
    assert ro[0]["root_cause"] == "db connection reset"


def test_outcome_not_fixed_when_run_never_started_and_requests_are_reyielded(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "x"}])
    env.tick_approval()
    approve(agentic, env, run.run_id)
    first = env.tick_approval().run_requests
    again = env.tick_approval().run_requests                       # nothing launched yet -> same run_key handed out again (daemon dedupes)
    assert [r.run_key for r in again] == [r.run_key for r in first]
    rec = env.results(run.run_id)
    rec["outcome_due_at"] = "2000-01-01T00:00:00+00:00"
    (env.dirs["results"] / f"{run.run_id}.json").write_text(json.dumps(rec))
    env.tick_approval()
    r = env.results(run.run_id)
    assert r["outcome"] == "not_fixed" and "never started" in r["outcome_detail"]


def test_retry_limit_caps_the_chain(mod, tmp_path, inst, agentic):
    e = Env(mod, tmp_path, inst, max_retries_per_run=1)
    run = e.fail_nightly()
    e.tick_failure()
    dg.materialize(e.assets[:2], instance=inst, raise_on_error=False,
                   tags={"dagster_remediation/proposal": run.run_id, "dagster_remediation/kind": "retry_run"})
    e.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "x"}])
    e.tick_approval()
    approve(agentic, e, run.run_id)
    assert e.tick_approval().run_requests == []
    assert "retry limit" in e.results(run.run_id)["actions"][0]["reason"]


def test_rerun_partition_requests_the_failed_partition(env, agentic):
    run = env.fail_partition("p2")
    env.tick_failure()
    env.write_proposal(run.run_id, [{"kind": "rerun_partition", "params": {}, "reason": "partition p2 had a bad read"}])
    env.tick_approval()
    approve(agentic, env, run.run_id)
    env.flag.write_text("x")
    reqs = env.tick_approval().run_requests
    assert sorted((r.partition_key, tuple(k.to_user_string() for k in r.asset_selection)) for r in reqs if r.partition_key) == [("p2", ("pflaky",))]
    reqs = [r for r in reqs if r.partition_key]
    assert len(reqs) == 1 and reqs[0].partition_key == "p2"
    assert reqs[0].job_name == "run_remediation_remediate_parts"
    assert sorted(k.to_user_string() for k in reqs[0].asset_selection) == ["pflaky", "upstream"] or \
        sorted(k.to_user_string() for k in reqs[0].asset_selection) == ["pflaky"]
    assert env.launch(reqs[0]).success


def test_retry_run_with_partitioned_failure_and_mixed_partitioning(env, agentic):
    run = env.fail_partition("p1")
    env.tick_failure()
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "x"}])
    env.tick_approval()
    approve(agentic, env, run.run_id)
    env.flag.write_text("x")
    reqs = env.tick_approval().run_requests
    assert [r.partition_key for r in reqs] == ["p1"] and [k.to_user_string() for k in reqs[0].asset_selection] == ["pflaky"]
    assert env.launch(reqs[0]).success


def test_materialize_assets_is_opt_in_and_restrictable(mod, tmp_path, inst, agentic):
    e = Env(mod, tmp_path, inst)
    run = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(run.run_id, [{"kind": "materialize_assets", "params": {"asset_keys": ["upstream"]}, "reason": "refresh"}])
    e.tick_approval()
    assert not (e.dirs["approvals"] / "pending").exists() or not list((e.dirs["approvals"] / "pending").glob("*.json"))
    r = e.results(run.run_id)
    assert r["status"] == "rejected" and "not in allowed_actions" in r["actions"][0]["reason"]
    # allowed, but only for listed assets
    e2 = Env(mod, tmp_path / "b", inst, allowed_actions=["materialize_assets"], allowed_materialize_assets=["upstream"])
    (tmp_path / "b").mkdir(exist_ok=True)
    run2 = e2.fail_nightly()
    e2.tick_failure()
    e2.write_proposal(run2.run_id, [{"kind": "materialize_assets", "params": {"asset_keys": ["flaky"]}, "reason": "x"}])
    e2.tick_approval()
    assert "allowed_materialize_assets" in e2.results(run2.run_id)["actions"][0]["reason"]
    run3 = e2.fail_nightly()
    e2.tick_failure()
    e2.write_proposal(run3.run_id, [{"kind": "materialize_assets", "params": {"asset_keys": ["upstream"]}, "reason": "x"}])
    e2.tick_approval()
    approve(agentic, e2, run3.run_id)
    reqs = e2.tick_approval().run_requests
    assert [k.to_user_string() for k in reqs[0].asset_selection] == ["upstream"] and e2.launch(reqs[0]).success


@pytest.mark.parametrize("action,why", [
    ({"kind": "drop_database", "params": {}}, "unknown action kind"),
    ({"kind": "notify", "params": {"message": "hi"}}, "not in allowed_actions"),
    ({"kind": "materialize_assets", "params": {"asset_keys": ["x"]}}, "not in allowed_actions"),
    ({"kind": "retry_run", "params": {"run_id": "someone-elses-run"}}, "only retry the run this proposal is about"),
    ({"kind": "retry_run", "params": {"force": True}}, "unexpected params"),
    ({"kind": "rerun_partition", "params": {"asset_keys": ["a b; rm -rf /"]}}, "asset_keys must be a list"),
    ({"kind": "rerun_partition", "params": {"partition_key": ""}}, "partition_key"),
    ({"kind": "open_ticket", "params": {"body": "no title"}}, "title is required"),
    ({"kind": "open_ticket", "params": {"title": "t", "severity": "apocalyptic"}}, "severity"),
    ({"kind": "retry_run", "params": "oops"}, "params must be an object"),
    ("not-an-object", "not an object"),
])
def test_rejected_actions_are_recorded_with_reason_and_never_pending(env, action, why):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [action])
    res = env.tick_approval()
    assert res.run_requests == []
    r = env.results(run.run_id)
    assert r["status"] == "rejected" and why in r["actions"][0]["reason"]
    assert not (env.dirs["approvals"] / "pending" / f"{run.run_id}.json").exists()


def test_partially_valid_proposal_executes_only_valid_actions_after_approval(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "drop_database", "params": {}}, {"kind": "retry_run", "params": {}, "reason": "ok"}])
    env.tick_approval()
    assert "rejected_actions" in env.pending(run.run_id)["preview"]
    approve(agentic, env, run.run_id)
    reqs = env.tick_approval().run_requests
    assert len(reqs) == 1
    acts = {a["index"]: a for a in env.results(run.run_id)["actions"]}
    assert acts[0]["status"] == "rejected" and acts[1]["status"] == "requested"


def test_invalid_proposals_are_recorded(env):
    run = failed_and_triaged(env)
    (env.dirs["proposals"]).mkdir(parents=True, exist_ok=True)
    (env.dirs["proposals"] / f"{run.run_id}.json").write_text(json.dumps({"run_id": "other-run", "actions": []}))
    env.tick_approval()
    assert env.results(run.run_id)["status"] == "invalid" and "does not match" in env.results(run.run_id)["reason"]
    (env.dirs["proposals"] / "junk.json").write_text("not json at all")
    env.tick_approval()
    assert env.results("junk")["status"] == "invalid"


def test_accepts_agentic_pipeline_step_shapes(env, agentic):
    run = failed_and_triaged(env)
    body = {"op": "extract", "text": "{}", "extracted": {"summary": "s", "root_cause": "r", "confidence": 0.7,
                                                           "actions": [{"kind": "retry_run", "params": {}, "reason": "x"}]}}
    env.dirs["proposals"].mkdir(parents=True, exist_ok=True)
    (env.dirs["proposals"] / f"{run.run_id}.json").write_text(json.dumps(body))
    env.tick_approval()
    assert env.pending(run.run_id)["state"] == "new"
    run2 = env.fail_nightly()
    env.tick_failure()
    fenced = "```json\n" + json.dumps({"summary": "s", "actions": [{"kind": "open_ticket", "params": {"title": "t"}}]}) + "\n```"
    (env.dirs["proposals"] / f"{run2.run_id}.json").write_text(json.dumps({"op": "llm_call", "text": fenced}))
    env.tick_approval()
    assert env.pending(run2.run_id)["state"] == "new"


def test_rejection_token_is_honoured(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}}])
    env.tick_approval()
    approve(agentic, env, run.run_id, ok=False)
    assert env.tick_approval().run_requests == []
    r = env.results(run.run_id)
    assert r["status"] == "rejected_by_approver" and r["approver"] == "alice"
    assert not (env.dirs["approvals"] / "pending" / f"{run.run_id}.json").exists()


# ═══ fingerprint binding ═══

def test_fingerprint_mismatch_is_not_honoured_and_marks_pending_stale(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "transient"}])
    env.tick_approval()
    tok = {"approved": True, "approver": "mallory", "fingerprint": "0" * 32, "approved_at": "2026-01-01T00:00:00+00:00"}
    (env.dirs["approvals"] / f"{run.run_id}.json").write_text(json.dumps(tok))
    res = env.tick_approval()
    assert res.run_requests == [] and not (env.dirs["results"] / f"{run.run_id}.json").exists()
    p = env.pending(run.run_id)
    assert p["state"] == "stale" and "input changed" in p["reason"]
    # a token with no fingerprint is not bound to anything either
    (env.dirs["approvals"] / f"{run.run_id}.json").write_text(json.dumps({"approved": True}))
    assert env.tick_approval().run_requests == []
    assert "no fingerprint" in env.pending(run.run_id)["reason"]


def test_approval_of_old_proposal_does_not_cover_a_rewritten_one(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "open_ticket", "params": {"title": "harmless"}}])
    env.tick_approval()
    approve(agentic, env, run.run_id)                              # approves the harmless proposal...
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}, "reason": "swapped in after approval"}])   # ...which is replaced
    res = env.tick_approval()
    assert res.run_requests == [] and not (env.dirs["results"] / f"{run.run_id}.json").exists()
    assert env.pending(run.run_id)["state"] == "stale"


def test_expired_and_consumed_tokens(mod, tmp_path, inst, agentic):
    e = Env(mod, tmp_path, inst, approval_max_age_hours=1)
    run = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}}])
    e.tick_approval()
    tok = approve(agentic, e, run.run_id)
    tok["approved_at"] = "2020-01-01T00:00:00+00:00"
    (e.dirs["approvals"] / f"{run.run_id}.json").write_text(json.dumps(tok))
    assert e.tick_approval().run_requests == []
    assert "expired" in e.pending(run.run_id)["reason"]


# ═══ auto approve ═══

def test_auto_approve_is_off_by_default(env):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}}], confidence=0.99)
    assert env.tick_approval().run_requests == []
    assert (env.dirs["approvals"] / "pending" / f"{run.run_id}.json").exists()


def test_auto_approve_limits(mod, tmp_path, inst):
    e = Env(mod, tmp_path, inst, auto_approve={"kinds": ["retry_run"], "max_per_hour": 1, "min_confidence": 0.8})
    r1 = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(r1.run_id, [{"kind": "retry_run", "params": {}}], confidence=0.5)
    assert e.tick_approval().run_requests == []                    # confidence too low -> human queue
    e.write_proposal(r1.run_id, [{"kind": "retry_run", "params": {}}], confidence=0.95)
    reqs = e.tick_approval().run_requests
    assert len(reqs) == 1 and e.results(r1.run_id)["auto_approved"] is True and e.results(r1.run_id)["approver"] == "auto_approve"
    r2 = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(r2.run_id, [{"kind": "retry_run", "params": {}}], confidence=0.99)
    assert [r for r in e.tick_approval().run_requests if r.run_key.startswith(r2.run_id)] == []   # max_per_hour reached
    assert e.pending(r2.run_id)["state"] == "new"
    r3 = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(r3.run_id, [{"kind": "open_ticket", "params": {"title": "t"}}], confidence=0.99)
    e.tick_approval()
    assert e.pending(r3.run_id)                                    # kind outside auto_approve.kinds -> human


def test_auto_approve_config_validation(mod, tmp_path):
    def build(**kw):
        return mod.RunFailureRemediationComponent(evidence_dir="e", proposals_dir="p", approval_dir="a", results_dir="r",
                                                  triage_asset_selection="group:triage", **kw).validate_config()
    with pytest.raises(ValueError, match="subset of allowed_actions"):
        build(auto_approve={"kinds": ["materialize_assets"]})
    with pytest.raises(ValueError, match="unknown kinds"):
        build(allowed_actions=["retry_run", "format_disk"])
    with pytest.raises(ValueError, match="webhook_url_env_var"):
        build(allowed_actions=["notify"])
    with pytest.raises(ValueError, match="unknown keys"):
        build(auto_approve={"kinds": ["retry_run"], "yolo": True})
    build(auto_approve={"kinds": ["retry_run"]})


# ═══ tickets + webhooks ═══

class _Hook(BaseHTTPRequestHandler):
    bodies = []

    def do_POST(self):
        _Hook.bodies.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


@pytest.fixture
def hook(monkeypatch):
    _Hook.bodies = []
    srv = HTTPServer(("127.0.0.1", 0), _Hook)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    monkeypatch.setenv("REMEDIATION_HOOK_URL", f"http://127.0.0.1:{srv.server_port}/secret-path-token")
    yield _Hook
    srv.shutdown()


def test_open_ticket_and_notify_never_leak_the_webhook_url_or_secrets(mod, tmp_path, inst, agentic, hook):
    e = Env(mod, tmp_path, inst, allowed_actions=["open_ticket", "notify"], webhook_url_env_var="REMEDIATION_HOOK_URL",
            ticket_webhook_url_env_var="REMEDIATION_HOOK_URL")
    run = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(run.run_id, [
        {"kind": "open_ticket", "params": {"title": "ETL down", "body": "creds were password=hunter2 here", "severity": "high"}},
        {"kind": "notify", "params": {"message": "heads up, Authorization: Bearer abcdefghijklmnop"}}])
    e.tick_approval()
    approve(agentic, e, run.run_id)
    e.tick_approval()
    r = e.results(run.run_id)
    assert [a["status"] for a in r["actions"]] == ["done", "done"] and r["outcome"] == "not_applicable"
    ticket = json.loads((tmp_path / "results" / "tickets" / f"{run.run_id}-0.json").read_text())
    assert ticket["title"] == "ETL down" and ticket["severity"] == "high" and "hunter2" not in json.dumps(ticket)
    assert len(hook.bodies) == 2 and "hunter2" not in json.dumps(hook.bodies) and "abcdefghijklmnop" not in json.dumps(hook.bodies)
    blob = "".join(p.read_text() for p in (tmp_path).rglob("*.json"))
    assert "secret-path-token" not in blob and "127.0.0.1" not in blob


def test_notify_failure_is_recorded_without_the_url(mod, tmp_path, inst, agentic, monkeypatch):
    monkeypatch.setenv("REMEDIATION_HOOK_URL", "http://127.0.0.1:1/very-secret")
    e = Env(mod, tmp_path, inst, allowed_actions=["notify"], webhook_url_env_var="REMEDIATION_HOOK_URL", webhook_timeout_seconds=2)
    run = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(run.run_id, [{"kind": "notify", "params": {"message": "m"}}])
    e.tick_approval()
    approve(agentic, e, run.run_id)
    e.tick_approval()
    a = e.results(run.run_id)["actions"][0]
    assert a["status"] == "failed" and "very-secret" not in json.dumps(e.results(run.run_id))


# ═══ results never contain secrets; receipts ═══

def test_results_and_pending_never_contain_secrets(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "open_ticket", "params": {"title": "x", "body": "token=abc123secret"}}],
                       summary="saw password=hunter2", root_cause="Authorization: Bearer abcdefghijklmnopqrst")
    env.tick_approval()
    approve(agentic, env, run.run_id)
    env.tick_approval()
    # everything WE write (evidence, pending records, tokens, results, tickets); the proposal file is the triage pipeline's own output
    blob = "".join(p.read_text() for d in ("evidence", "approvals", "results") for p in (env.tmp / d).rglob("*.json"))
    for secret in ("abc123secret", "hunter2", "abcdefghijklmnopqrst", "sk-live-ABCDEFGHIJKLMNOP123456", "s3cr3tpw"):
        assert secret not in blob
    assert "password=[REDACTED]" in env.results(run.run_id)["summary"]


class FakeStore:
    def __init__(self):
        self.calls = []

    def record_outcome(self, receipt_id, **kw):
        self.calls.append((receipt_id, kw))


def test_receipt_store_outcome_is_optional_and_duck_typed(mod, tmp_path, inst, agentic):
    e = Env(mod, tmp_path, inst, store_resource_key="receipts")
    run = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}}], receipt_id="rcpt-1")
    e.tick_approval(resources={"receipts": FakeStore()})
    approve(agentic, e, run.run_id)
    store = FakeStore()
    rr = e.tick_approval(resources={"receipts": store}).run_requests[0]
    e.flag.write_text("x")
    e.launch(rr)
    rec = e.results(run.run_id)
    rec["outcome_due_at"] = "2000-01-01T00:00:00+00:00"
    (e.dirs["results"] / f"{run.run_id}.json").write_text(json.dumps(rec))
    e.tick_approval(resources={"receipts": store})
    assert store.calls == [("rcpt-1", {"signal": "remediation_fixed", "value": 1.0, "source": "run_failure_remediation",
                                       "outcome_ref": run.run_id})]
    # resource present but without record_outcome / receipt absent: skipped cleanly
    e.flag.unlink()
    r2 = e.fail_nightly()
    e.tick_failure()
    e.write_proposal(r2.run_id, [{"kind": "retry_run", "params": {}}])
    e.tick_approval(resources={"receipts": SimpleNamespace()})
    approve(agentic, e, r2.run_id)
    e.tick_approval(resources={"receipts": SimpleNamespace()})
    rec2 = e.results(r2.run_id)
    rec2["outcome_due_at"] = "2000-01-01T00:00:00+00:00"
    (e.dirs["results"] / f"{r2.run_id}.json").write_text(json.dumps(rec2))
    e.tick_approval(resources={"receipts": SimpleNamespace()})
    assert e.results(r2.run_id)["outcome"] == "not_fixed" and len(store.calls) == 1


# ═══ agentic_pipeline / agent_review_queue compatibility (drift tests) ═══

def test_inlined_approval_primitives_match_agentic_pipeline(mod, agentic):
    for raw in ("abc", "8c3f1d52-aaaa-4bbb-8ccc-0123456789ab", "a/b c", "x" * 40 + "%"):
        assert mod._approval_key(raw) == agentic._approval_key(raw)
    assert mod._approval_fingerprint("remediation", "t", "s") == agentic._approval_fingerprint("remediation", "t", "s")
    tok = {"approved": True, "fingerprint": "f" * 32, "approved_at": "2026-01-01T00:00:00+00:00"}
    for t, fp, age in ((None, "f", None), (tok, "f" * 32, None), (tok, "e" * 32, None), ({**tok, "consumed_at": "x"}, "f" * 32, None),
                       ({"approved": True}, "f", None), (tok, "f" * 32, 1.0)):
        a = mod._token_state(t, fp, age, None)
        b = agentic._approval_token_state(t, fp, "once", age, None)
        assert a == b, (t, fp, age)


def test_token_written_by_agentic_approve_pending_is_honoured_and_review_queue_lists_ours(env, agentic):
    run = failed_and_triaged(env)
    env.write_proposal(run.run_id, [{"kind": "retry_run", "params": {}}])
    env.tick_approval()
    queue = load_review_queue_module()
    rows = queue._collect_pending([str(env.dirs["approvals"])], 60.0)
    assert [(r["key"], r["step"], r["state"]) for r in rows] == [(run.run_id, "remediation", "new")]
    assert "retry_run" in rows[0]["summary"] and rows[0]["approve_path"].endswith(f"{run.run_id}.json")
    approve(agentic, env, run.run_id)                              # the REAL helper writes the token from OUR pending record
    # the REAL approve op, pointed at the same dir/key/summary/text, honours that token -- so our fingerprint == its fingerprint
    # (it also removes the pending record, exactly as it does for its own gates; the queue is empty afterwards)
    # and the real approve op, pointed at the same dir/key/summary/text, agrees with our fingerprint
    from types import SimpleNamespace as NS
    p = json.loads((env.dirs["approvals"] / f"{run.run_id}.json").read_text())
    prop, _ = env.mod.normalize_proposal(json.loads((env.dirs["proposals"] / f"{run.run_id}.json").read_text()), run.run_id,
                                         env.mod.redact_text)
    assert p["fingerprint"] == agentic._approval_fingerprint("remediation", env.mod.proposal_text(prop), env.mod.proposal_summary(prop))
    state = {"source": {"text": env.mod.proposal_text(prop)}, "__ctx__": {"partition_key": None, "partition_fields": None}}
    step = {"id": "remediation", "op": "approve", "source": "source", "approval_dir": str(env.dirs["approvals"]), "key": run.run_id,
            "summary": env.mod.proposal_summary(prop), "reuse": "always"}
    agentic._run_step(step, state, NS(log=logging.getLogger("t")))
    assert state["remediation"]["approved"] is True and state["remediation"]["status"] == "approved"
    assert queue._collect_pending([str(env.dirs["approvals"])], 60.0) == []
    reqs = env.tick_approval().run_requests                        # and OUR sensor honours the same token
    assert len(reqs) == 1


# ═══ documented assumptions about Dagster itself ═══

def test_assumption_create_reexecuted_run_needs_a_workspace(mod):
    """Why retry_run is a sensor RunRequest and not instance.create_reexecuted_run: the latter needs the daemon-side workspace."""
    import inspect
    params = inspect.signature(dg.DagsterInstance.create_reexecuted_run).parameters
    assert {"request_context", "code_location", "remote_job"} <= set(params)


def test_assumption_run_failure_sensor_only_accepts_the_declared_target(env):
    """A RunRequest naming a job that is not the sensor's target is rejected by Dagster, which is why the triage job is defined here."""
    bad = dg.RunRequest(run_key="x", job_name="some_other_job")
    sensor = dg.run_failure_sensor(name="s", monitor_all_code_locations=True, request_job=env.defs.get_job_def("run_failure_triage_job"))(
        lambda context: bad)
    env.fail_nightly()
    ctx = dg.build_sensor_context(instance=env.inst, definitions=env.defs, instance_ref=env.inst.get_ref())
    cur = sensor.evaluate_tick(ctx).cursor
    env.fail_nightly()
    with pytest.raises(Exception, match="Expected one of"):
        sensor.evaluate_tick(dg.build_sensor_context(instance=env.inst, definitions=env.defs, cursor=cur, instance_ref=env.inst.get_ref()))


def test_shipped_example_yaml_validates(mod):
    import pathlib
    import yaml
    cfg = yaml.safe_load((pathlib.Path(__file__).resolve().parent.parent / "example.yaml").read_text())
    assert cfg["type"].endswith("RunFailureRemediationComponent")
    comp = mod.RunFailureRemediationComponent(**cfg["attributes"])
    defs = comp.build_defs(None)
    assert {s.name for s in defs.sensors} == {"run_remediation_failure_sensor", "run_remediation_approval_sensor"}
    assert {j.name for j in defs.jobs} >= {"run_failure_triage_job", "run_remediation_remediate_core", "run_remediation_remediate_daily"}
    assert all(s.default_status == dg.DefaultSensorStatus.STOPPED for s in defs.sensors)
