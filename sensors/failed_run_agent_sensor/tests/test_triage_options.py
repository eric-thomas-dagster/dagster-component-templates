"""Real gap reported from production (Hooli feedback): 121 failed runs in a week (~5/day),
mostly two jobs failing at random, would have each started the agent -- there was no way to
collapse a dynamically-mapped step's N parallel failures into one signature, no daily ceiling
independent of the per-signature cooldown, no way to avoid re-triaging every retry of the same
underlying failure, and no way to run this sensor at all without a shared-disk evidence_dir.

Tests:
- dynamic-mapping-suffix stripping in the signature (process_chunk[4] == process_chunk[9])
- cooldown_hours taking precedence over cooldown_minutes
- max_runs_per_day as an independent global daily ceiling
- the retry rule (dagster/root_run_id lineage, not signature matching)
- the no-evidence_dir run-history-only mode (no state file at all)
"""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def inst():
    with dg.instance_for_test() as i:
        yield i


class FakeSlackClient:
    def __init__(self):
        self.posted = []

    def chat_postMessage(self, channel, text):
        ts = f"{len(self.posted) + 1}.000"
        self.posted.append({"channel": channel, "text": text, "ts": ts})
        return {"ok": True, "ts": ts, "channel": channel}


@dg.asset
def upstream():
    return 1


@dg.op(out=dg.DynamicOut())
def fan_out(context):
    for i in range(2):
        yield dg.DynamicOutput(i, mapping_key=str(i))


@dg.op
def process_chunk(context, i):
    raise ConnectionError("connection refused to db.internal")


@dg.job
def mapped_job():
    fan_out().map(process_chunk)


@dg.asset(deps=[upstream])
def flaky():
    raise ConnectionError("connection refused to db.internal")


nightly = dg.define_asset_job("nightly", selection=[upstream, flaky])


@dg.asset
def bug_fixer():
    return 1


def _env(mod, monkeypatch, slack, tmp_path=None, **overrides):
    kwargs = dict(
        sensor_name="failed_run_agent_sensor", asset_keys=["bug_fixer"], slack_channel="C1",
        cooldown_minutes=0, default_status="RUNNING",
    )
    if tmp_path is not None:
        kwargs["evidence_dir"] = str(tmp_path / "evidence")
    kwargs.update(overrides)
    comp = mod.FailedRunAgentSensorComponent(**kwargs)
    monkeypatch.setattr(mod, "_get_slack_client", lambda token_env_var: slack)
    cdefs = comp.build_defs(None)
    defs = dg.Definitions.merge(
        cdefs, dg.Definitions(assets=[upstream, flaky, bug_fixer], jobs=[nightly, mapped_job]),
    )
    return defs.get_sensor_def(kwargs["sensor_name"]), defs


def _fail(inst, defs, job_name="nightly", tags=None):
    r = defs.get_job_def(job_name).execute_in_process(instance=inst, raise_on_error=False, tags=tags or {})
    assert not r.success
    return inst.get_run_by_id(r.run_id)


def _tick(sensor_def, inst, defs, cursor=None):
    return sensor_def.evaluate_tick(dg.build_sensor_context(instance=inst, definitions=defs, cursor=cursor, instance_ref=inst.get_ref()))


def _launch(defs, inst, run_request, sensor_name="failed_run_agent_sensor"):
    """Actually submit a tick's RunRequest as a real run -- the same thing the daemon does after
    getting one back -- so its tags (eng/error_signature, eng/sensor_name, eng/retry_chain_root)
    become queryable run history for the NEXT tick's cooldown/rate-limit/retry-rule checks.
    (A fresh run_id, not run_request.run_key -- that's a dedup key the daemon maps to its OWN
    generated run_id, not literally reused as one; it also collides here with the original
    failed run's own id.)"""
    return defs.get_job_def(f"{sensor_name}_job").execute_in_process(instance=inst, tags=run_request.tags)


def _primed(mod, monkeypatch, inst, slack, tmp_path=None, **overrides):
    sensor_def, defs = _env(mod, monkeypatch, slack, tmp_path=tmp_path, **overrides)
    primed = _tick(sensor_def, inst, defs)
    return sensor_def, defs, primed.cursor


# ── dynamic-mapping suffix stripping ─────────────────────────────────────────────────────────

def test_dynamic_mapping_suffixes_collapse_to_one_signature(mod):
    sig_a = mod.error_signature("loc", "job", "ConnectionError", "boom", ["process_chunk[4]"])
    sig_b = mod.error_signature("loc", "job", "ConnectionError", "boom", ["process_chunk[9]"])
    assert sig_a == sig_b


def test_different_steps_still_get_different_signatures(mod):
    sig_a = mod.error_signature("loc", "job", "ConnectionError", "boom", ["process_chunk[4]"])
    sig_b = mod.error_signature("loc", "job", "ConnectionError", "boom", ["other_step[4]"])
    assert sig_a != sig_b


def test_a_mapped_step_s_parallel_failures_share_one_cooldown(mod, monkeypatch, inst, tmp_path):
    """The real-world shape: a dynamic-output step fails across several mapping keys in one run.
    Those should all collapse into ONE signature (not hash-exploded by key), so a second run with
    the same mapped-step failure is suppressed by cooldown."""
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, monkeypatch, inst, slack, tmp_path=tmp_path, cooldown_minutes=60, asset_keys=["bug_fixer"])
    _fail(inst, defs, job_name="mapped_job")
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1

    _fail(inst, defs, job_name="mapped_job")
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []


# ── cooldown_hours precedence ────────────────────────────────────────────────────────────────

def test_cooldown_hours_takes_precedence_over_cooldown_minutes(mod, monkeypatch, inst, tmp_path):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(
        mod, monkeypatch, inst, slack, tmp_path=tmp_path, cooldown_minutes=0, cooldown_hours=168,
    )
    _fail(inst, defs)
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1

    _fail(inst, defs)
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []  # suppressed by the 168h cooldown, not the 0-minute one


# ── max_runs_per_day (global, independent of max_triage_per_hour) ───────────────────────────

def test_max_runs_per_day_caps_globally_across_signatures(mod, monkeypatch, inst, tmp_path):
    """Two DIFFERENT signatures (different jobs), each individually under max_triage_per_hour,
    still hit the shared daily cap on the second one."""
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(
        mod, monkeypatch, inst, slack, tmp_path=tmp_path, cooldown_minutes=0,
        max_triage_per_hour=10, max_runs_per_day=1,
    )
    _fail(inst, defs, job_name="nightly")
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1

    _fail(inst, defs, job_name="mapped_job")  # different job/signature, would pass max_triage_per_hour alone
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []


# ── retry rule (dagster/root_run_id lineage) ─────────────────────────────────────────────────

def test_a_retry_of_an_already_triaged_run_is_skipped(mod, monkeypatch, inst, tmp_path):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, monkeypatch, inst, slack, tmp_path=tmp_path, cooldown_minutes=0)
    original = _fail(inst, defs)
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1
    _launch(defs, inst, first.run_requests[0])

    # A different error message (so cooldown/signature matching alone would NOT suppress it) --
    # only the retry-lineage tag should catch this.
    retry = defs.get_job_def("nightly").execute_in_process(
        instance=inst, raise_on_error=False,
        tags={"dagster/root_run_id": original.run_id, "dagster/parent_run_id": original.run_id},
    )
    assert not retry.success
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []


def test_an_untriaged_retry_is_still_analyzed_and_becomes_the_chain_s_triage_point(mod, monkeypatch, inst, tmp_path):
    """The original run's own failure was never seen by this sensor (e.g. it started after the
    original failed) -- the retry itself should still get triaged, and its own RunRequest should
    carry eng/retry_chain_root so a LATER retry of the same chain is suppressed."""
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, monkeypatch, inst, slack, tmp_path=tmp_path, cooldown_minutes=0)

    fake_original_run_id = "11111111-1111-1111-1111-111111111111"
    retry = defs.get_job_def("nightly").execute_in_process(
        instance=inst, raise_on_error=False, tags={"dagster/root_run_id": fake_original_run_id},
    )
    assert not retry.success
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1
    assert first.run_requests[0].tags["eng/retry_chain_root"] == fake_original_run_id
    _launch(defs, inst, first.run_requests[0])

    # A second retry of the SAME chain is now suppressed.
    retry2 = defs.get_job_def("nightly").execute_in_process(
        instance=inst, raise_on_error=False, tags={"dagster/root_run_id": fake_original_run_id},
    )
    assert not retry2.success
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []


def test_use_retry_rule_false_disables_the_lineage_check(mod, monkeypatch, inst, tmp_path):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(
        mod, monkeypatch, inst, slack, tmp_path=tmp_path, cooldown_minutes=0, use_retry_rule=False,
    )
    original = _fail(inst, defs)
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1
    assert "eng/retry_chain_root" not in first.run_requests[0].tags

    retry = defs.get_job_def("nightly").execute_in_process(
        instance=inst, raise_on_error=False, tags={"dagster/root_run_id": original.run_id},
    )
    assert not retry.success
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert len(second.run_requests) == 1  # not suppressed -- the rule is off


# ── no evidence_dir: pure run-history mode, no state file at all ────────────────────────────

def test_no_evidence_dir_still_posts_and_triages_a_real_failure(mod, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, monkeypatch, inst, slack, cooldown_minutes=0)
    run = _fail(inst, defs)
    res = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(res.run_requests) == 1
    assert res.run_requests[0].tags["eng/failed_run_id"] == run.run_id
    assert len(slack.posted) == 1


def test_no_evidence_dir_cooldown_is_read_from_run_history_not_a_file(mod, monkeypatch, inst, tmp_path):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(mod, monkeypatch, inst, slack, cooldown_minutes=60)
    _fail(inst, defs)
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1
    _launch(defs, inst, first.run_requests[0])

    _fail(inst, defs)
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []
    assert len(slack.posted) == 1
    # No evidence directory ever created -- this is the whole point.
    assert not (tmp_path / "evidence").exists()


def test_no_evidence_dir_max_runs_per_day_is_read_from_run_history(mod, monkeypatch, inst):
    slack = FakeSlackClient()
    sensor_def, defs, cursor = _primed(
        mod, monkeypatch, inst, slack, cooldown_minutes=0, max_runs_per_day=1,
    )
    _fail(inst, defs, job_name="nightly")
    first = _tick(sensor_def, inst, defs, cursor=cursor)
    assert len(first.run_requests) == 1
    _launch(defs, inst, first.run_requests[0])

    _fail(inst, defs, job_name="mapped_job")
    second = _tick(sensor_def, inst, defs, cursor=first.cursor)
    assert second.run_requests == []


def test_random_temp_dirs_do_not_change_the_signature(mod):
    a = mod.error_signature("loc", "job", "FileNotFoundError", "No such file: '/tmp/tmpab12cd/out.csv'", ["s"])
    b = mod.error_signature("loc", "job", "FileNotFoundError", "No such file: '/tmp/tmpzz99yy/out.csv'", ["s"])
    c = mod.error_signature("loc", "job", "FileNotFoundError", "No such file: '/tmp/tmpzz99yy/other.csv'", ["s"])
    assert a == b
    assert a != c  # the file name still distinguishes failures


def test_a_different_error_still_gets_a_different_signature(mod):
    a = mod.error_signature("loc", "job", "KeyError", "KeyError: 'discount'", ["s"])
    b = mod.error_signature("loc", "job", "KeyError", "KeyError: 'price'", ["s"])
    assert a != b
