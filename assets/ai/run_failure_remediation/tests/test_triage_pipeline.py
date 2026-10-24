"""examples/triage_pipeline.yaml is a real agentic_pipeline config: it validates, and run end to end (only the model call faked)
it consumes the evidence our failure sensor wrote and writes a proposal our approval sensor accepts."""
import json
import pathlib
import sys
from types import SimpleNamespace

import pytest
import yaml

from .conftest import FakeLiteLLM
from .test_remediation import Env  # noqa: F401
from .test_remediation import agentic, inst, mod  # noqa: F401  (fixtures)

EXAMPLE = pathlib.Path(__file__).resolve().parent.parent / "examples" / "triage_pipeline.yaml"


@pytest.fixture
def fake(monkeypatch):
    f = FakeLiteLLM()
    monkeypatch.setitem(sys.modules, "litellm", f)
    return f


def triage_component(agentic, tmp_path):
    text = EXAMPLE.read_text().replace("./evidence", str(tmp_path / "evidence")).replace("./proposals", str(tmp_path / "proposals"))
    return agentic.AgenticPipelineComponent(**yaml.safe_load(text)["attributes"])


def test_example_validates_and_matches_the_component_contract(agentic, mod, tmp_path):
    cfg = yaml.safe_load(EXAMPLE.read_text())["attributes"]
    assets = list(triage_component(agentic, tmp_path).build_defs(None).assets)
    assert assets and {a.group_names_by_key[k] for a in assets for k in a.keys} == {"triage"}
    comp = mod.RunFailureRemediationComponent(evidence_dir="e", proposals_dir="p", approval_dir="a", results_dir="r",
                                              triage_asset_selection="group:triage")
    assert cfg["dynamic_partition_name"] == comp.dynamic_partitions_name                  # same dynamic partitions def
    assert cfg["source"]["path"] == "./evidence/{partition_key}.json"                      # <evidence_dir>/<run_id>.json
    assert cfg["outputs"]["json_sinks"][0]["path"] == "./proposals/{partition_key}.json"   # <proposals_dir>/<run_id>.json
    kinds = cfg["steps"][0]["output_schema"]["properties"]["actions"]["items"]["properties"]["kind"]["enum"]
    assert set(kinds) <= set(mod.DEFAULT_ALLOWED_ACTIONS)                                  # only offers what is allowed by default


def test_failure_to_evidence_to_triage_to_proposal_to_pending_to_retry(agentic, mod, tmp_path, inst, fake, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-not-a-real-key")   # the triage step reads its key from the env; CI has none (the model call itself is faked)
    triage = triage_component(agentic, tmp_path).build_defs(None)
    env = Env(mod, tmp_path, inst, triage_assets=list(triage.assets))
    run = env.fail_nightly()
    res = env.tick_failure()
    for req in res.dynamic_partitions_requests:
        inst.add_dynamic_partitions(req.partitions_def_name, req.partition_keys)
    proposal = {"summary": "db connection reset", "root_cause": "ConnectionError talking to the warehouse", "confidence": 0.9,
                "actions": [{"kind": "retry_run", "params": {}, "reason": "transient network error"}]}
    fake.script.append(("", [SimpleNamespace(id="c1", function=SimpleNamespace(name="extract_data", arguments=json.dumps(proposal)))]))
    rr = res.run_requests[0]
    out = env.defs.get_job_def("run_failure_triage_job").execute_in_process(instance=inst, partition_key=rr.partition_key, tags=rr.tags)
    assert out.success
    seen = fake.calls[0]["messages"][-1]["content"]
    ev = env.evidence(run.run_id)
    assert run.run_id in seen and ev["errors"][0]["error_class"] == "ConnectionError"     # the model saw OUR evidence
    assert "abc123secret" not in seen and "hunter2" not in seen
    written = json.loads((tmp_path / "proposals" / f"{run.run_id}.json").read_text())
    assert written["extracted"]["actions"][0]["kind"] == "retry_run"                       # agentic_pipeline's json_sink shape
    env.tick_approval()
    p = env.pending(run.run_id)
    assert p["state"] == "new" and "retry_run" in p["summary"]
    agentic.approve_pending(str(env.dirs["approvals"]), run.run_id, approver="alice")
    env.flag.write_text("fixed")
    reqs = env.tick_approval().run_requests
    assert [r.run_key for r in reqs] == [f"{run.run_id}:0:retry_run"] and env.launch(reqs[0]).success
