"""A frozen extraction pipeline end to end: extractor + job + folder sensor, configured exactly as Dagster Designer configures them.

Files arrive in a folder -> the sensor launches one run per batch -> the extractor reads the folder, flags documents that need a
person, moves finished files out of the way, and reports the share that passed. Only the model call is faked.
"""
import importlib.util
import json
import os
import sys
import time
import types
from pathlib import Path

import dagster as dg
import pytest

ROOT = Path(__file__).resolve().parent.parent
ASSET = "invoice_fields"
JOB = f"{ASSET}_job"
FIELDS = ["invoice_number", "vendor", "total_amount", "tax", "line_items"]


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def litellm_by_marker(monkeypatch):
    answers = {}

    def completion(model, messages, api_key=None, **kwargs):
        prompt = messages[0]["content"]
        text = prompt if isinstance(prompt, str) else prompt[0]["text"]
        for marker, reply in answers.items():
            if marker in text:
                return types.SimpleNamespace(choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=json.dumps(reply)))])
        raise AssertionError(f"no canned answer for {text[:60]!r}")

    module = types.ModuleType("litellm")
    module.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", module)
    return answers


def _invoice(n, **over):
    return {"invoice_number": f"INV-{n}", "vendor": "ACME", "total_amount": 110, "tax": 10,
            "line_items": [{"amount": 60}, {"amount": 40}], **over}


def _defs(tmp_path):
    inbox, done = tmp_path / "inbox", tmp_path / "inbox" / "_processed"
    inbox.mkdir()
    extractor = _load("assets/ai/structured_document_extractor", "ext").StructuredDocumentExtractorComponent(
        asset_name=ASSET, path=str(inbox / "*.txt"), document_type="custom", output_fields=FIELDS,
        post_process="move", post_process_dir=str(done),
        validation={
            "required_fields": ["invoice_number", "vendor"],
            "reconcile": {"total_field": "total_amount", "items_field": "line_items", "add_fields": ["tax"], "tolerance": 0.01},
            "min_pass_rate": 0.6,
        },
    )
    job = _load("schedules/asset_job", "job").AssetJobComponent(job_name=JOB, asset_keys=[ASSET])
    sensor = _load("sensors/filesystem_monitor", "sensor").FilesystemMonitorSensorComponent(
        sensor_name=f"{ASSET}_new_files", directory_path=str(inbox), file_pattern=r"(?i).*\.(?:txt)$",
        job_name=JOB, minimum_interval_seconds=30, default_status="running", batch_into_one_run=True,
    )
    defs = dg.Definitions.merge(*(c.build_defs(None) for c in (extractor, job, sensor)))  # positional: the argument is named differently per component
    return defs, inbox, done


def _drop(inbox, name, text, age):
    p = inbox / f"{name}.txt"
    p.write_text(text)
    t = time.time() - age
    os.utime(p, (t, t))


def test_files_arrive_get_checked_and_leave_the_inbox_exactly_once(litellm_by_marker, tmp_path):
    litellm_by_marker.update({
        "alpha": _invoice(1), "bravo": _invoice(2, vendor=None), "charlie": _invoice(3, total_amount=999),
        "delta": _invoice(4),
    })
    defs, inbox, done = _defs(tmp_path)
    sensor = list(defs.sensors)[0]
    job = defs.resolve_job_def(JOB)

    with dg.DagsterInstance.ephemeral() as instance:
        def tick(cursor=None):
            return sensor(dg.build_sensor_context(instance=instance, cursor=cursor))

        # three invoices land together: ONE run, not three
        for i, name in enumerate(("alpha", "bravo", "charlie")):
            _drop(inbox, name, name, age=300 - i)
        first = tick()
        assert len(first.run_requests) == 1

        run = job.execute_in_process(instance=instance)
        assert run.success
        df = run.output_for_node(ASSET)
        status = dict(zip(df["path"].map(lambda p: Path(p).stem), df["_validation_status"]))
        assert status == {"alpha": "ok", "bravo": "needs_review", "charlie": "needs_review"}
        issues = dict(zip(df["path"].map(lambda p: Path(p).stem), df["_validation_issues"]))
        assert issues["bravo"] == "missing:vendor" and issues["charlie"].startswith("totals_mismatch")

        (check,) = [c for c in run.get_asset_check_evaluations() if c.check_name == "extraction_quality"]
        assert check.passed is False  # 1 of 3 passed, 0.6 needed
        assert {r["source"].rsplit("/", 1)[-1] for r in check.metadata["review"].value} == {"bravo.txt", "charlie.txt"}

        # finished files left the inbox, so the next run can't read them twice
        assert sorted(p.name for p in inbox.glob("*.txt")) == []
        assert sorted(p.name for p in done.glob("*.txt")) == ["alpha.txt", "bravo.txt", "charlie.txt"]

        # nothing new: the sensor stays quiet
        assert not tick(first.cursor).run_requests

        # a fourth invoice arrives: one more run, which sees only it
        _drop(inbox, "delta", "delta", age=5)
        second = tick(first.cursor)
        assert len(second.run_requests) == 1
        run2 = job.execute_in_process(instance=instance)
        df2 = run2.output_for_node(ASSET)
        assert [Path(p).stem for p in df2["path"]] == ["delta"] and list(df2["_validation_status"]) == ["ok"]
        (check2,) = [c for c in run2.get_asset_check_evaluations() if c.check_name == "extraction_quality"]
        assert check2.passed is True
