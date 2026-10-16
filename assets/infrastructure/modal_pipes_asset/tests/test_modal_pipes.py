"""modal_pipes_asset partition handling / backfill_policy.

Only the `modal` CLI (network + Modal account) is faked: a tiny executable on
PATH that opens a REAL dagster_pipes context, records the partition key it was
launched with, and reports a real materialization back through Pipes. The
Pipes subprocess client, command building and dg.materialize() are all real.
"""
import os
import stat
import sys

import dagster as dg
import pytest

from .conftest import load_component_module

FAKE_MODAL = """#!{python}
import os, sys
from dagster_pipes import open_dagster_pipes
with open_dagster_pipes() as pipes:
    pk = os.environ.get("DAGSTER_PARTITION_KEY", "<none>")
    with open(os.environ["FAKE_MODAL_LOG"], "a") as f:
        f.write(pk + "|" + " ".join(sys.argv[1:]) + "\\n")
    pipes.report_asset_materialization(metadata={{"echo": pk}})
"""


@pytest.fixture()
def modal_log(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    exe = bin_dir / "modal"
    exe.write_text(FAKE_MODAL.format(python=sys.executable))
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "modal.log"
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_MODAL_LOG", str(log))
    return log


def _component(**kw):
    mod = load_component_module()
    return mod.ModalPipesAssetComponent(
        asset_name="modal_out", app_file="app.py", function_name="fn",
        partition_type="daily", partition_start="2024-01-01", **kw,
    )


def _run(component, partition_key=None, tags=None):
    return dg.materialize(
        component.build_defs(None).assets, partition_key=partition_key, tags=tags or {},
        resources={"pipes_subprocess_client": dg.PipesSubprocessClient()},
    )


def test_single_partition_passes_key(modal_log):
    r = _run(_component(), partition_key="2024-01-03")
    assert r.success
    assert modal_log.read_text().splitlines() == ["2024-01-03|run app.py::fn --partition_key 2024-01-03"]


def test_backfill_policy_wiring():
    assert _component(backfill_policy="single_run").build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    assert _component().build_defs(None).assets[0].backfill_policy is None


def test_single_run_backfill_launches_modal_once_per_partition(modal_log):
    """Before the fix has_partition_key was False for a range, so Modal was
    launched ONCE with no partition key at all."""
    r = _run(_component(backfill_policy="single_run"), tags={
        "dagster/asset_partition_range_start": "2024-01-02",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert [l.split("|")[0] for l in modal_log.read_text().splitlines()] == ["2024-01-02", "2024-01-03", "2024-01-04"]
    md = r.get_asset_materialization_events()[0].materialization.metadata
    assert {k: v.value for k, v in md.items() if k.endswith("/echo")} == {
        "2024-01-02/echo": "2024-01-02", "2024-01-03/echo": "2024-01-03", "2024-01-04/echo": "2024-01-04"}
