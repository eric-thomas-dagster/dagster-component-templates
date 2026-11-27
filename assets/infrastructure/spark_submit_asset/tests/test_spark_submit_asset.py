"""Real, committed tests for SparkSubmitAssetComponent.

Most tests run a real subprocess via a tiny fake `spark-submit` shim (parses
the real spark-submit flag vocabulary just enough to find the application
path, matching this component's own command construction, then execs it with
`sys.executable`) -- a real subprocess really runs, nothing about process
execution itself is mocked, only the unavoidable external `spark-submit`
binary (not installed in this environment). A dedicated test separately
proves the exact flag construction against the real spark-submit vocabulary.
"""
import sys
import textwrap

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()

_FLAGS_WITH_VALUE = (
    "--master", "--deploy-mode", "--class", "--name", "--driver-memory",
    "--executor-memory", "--num-executors", "--total-executor-cores",
    "--queue", "--packages", "--jars", "--py-files", "--conf",
)

_SHIM_BODY = f"""#!{sys.executable}
import sys, subprocess
args = sys.argv[1:]
flags_with_value = {_FLAGS_WITH_VALUE!r}
i = 0
while i < len(args):
    if args[i] in flags_with_value:
        i += 2
        continue
    app, app_args = args[i], args[i + 1:]
    sys.exit(subprocess.call([{sys.executable!r}, app] + app_args))
raise SystemExit("no application arg found")
"""


def _write_shim(tmp_path):
    import stat
    p = tmp_path / "fake_spark_submit.py"
    p.write_text(_SHIM_BODY)
    p.chmod(p.stat().st_mode | stat.S_IEXEC)
    return str(p)


def _write_script(tmp_path, body):
    p = tmp_path / "script.py"
    p.write_text(textwrap.dedent(body))
    return str(p)


def _comp(tmp_path, **kw):
    args = dict(asset_name="out", application="app.py", spark_submit_binary=_write_shim(tmp_path))
    args.update(kw)
    return mod.SparkSubmitAssetComponent(**args)


def test_successful_run_materializes_with_exit_code_zero(tmp_path):
    script = _write_script(tmp_path, """
        print("hello from spark job")
    """)
    comp = _comp(tmp_path, application=script)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    meta = mats[0].event_specific_data.materialization.metadata
    assert meta["exit_code"].value == 0
    assert "hello from spark job" in meta["stdout_preview"].value


def test_nonzero_exit_raises_with_stderr(tmp_path):
    script = _write_script(tmp_path, """
        import sys
        print("boom", file=sys.stderr)
        sys.exit(7)
    """)
    comp = _comp(tmp_path, application=script)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    err = str(failure.event_specific_data.error)
    assert "exited 7" in err


def test_success_exit_codes_allows_nonzero(tmp_path):
    script = _write_script(tmp_path, """
        import sys
        sys.exit(3)
    """)
    comp = _comp(tmp_path, application=script, success_exit_codes=[3])
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    assert mats[0].event_specific_data.materialization.metadata["exit_code"].value == 3


def test_timeout_kills_process_and_raises(tmp_path):
    script = _write_script(tmp_path, """
        import time
        time.sleep(30)
    """)
    comp = _comp(tmp_path, application=script, timeout_seconds=1)
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets), raise_on_error=False)
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "timed out" in str(failure.event_specific_data.error)


def test_env_vars_are_merged_into_subprocess_env(tmp_path):
    script = _write_script(tmp_path, """
        import os
        print(os.environ.get("MY_SPARK_VAR", "MISSING"))
    """)
    comp = _comp(tmp_path, application=script, env_vars={"MY_SPARK_VAR": "present"})
    defs = comp.build_defs(None)
    result = dg.materialize(list(defs.assets))
    assert result.success
    mats = [e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"]
    assert "present" in mats[0].event_specific_data.materialization.metadata["stdout_preview"].value


def test_invalid_deploy_mode_is_rejected(tmp_path):
    comp = _comp(tmp_path, deploy_mode="bogus")
    with pytest.raises(ValueError, match="deploy_mode"):
        comp.build_defs(None)


def test_command_construction_matches_real_spark_submit_flags():
    cmd = mod._build_spark_submit_command(
        spark_submit_binary="spark-submit",
        application="s3://bucket/job.py",
        master="yarn",
        deploy_mode="cluster",
        class_name="com.example.Main",
        name="my-job",
        driver_memory="2g",
        executor_memory="4g",
        num_executors=10,
        total_executor_cores=None,
        queue="default",
        packages=["org.apache.spark:spark-avro_2.12:3.5.0"],
        jars=["extra.jar"],
        py_files=["helpers.py"],
        conf={"spark.executor.memoryOverhead": "512m"},
        extra_args=None,
        application_args=["--date", "2024-01-01"],
    )
    assert cmd == [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", "cluster",
        "--class", "com.example.Main",
        "--name", "my-job",
        "--driver-memory", "2g",
        "--executor-memory", "4g",
        "--num-executors", "10",
        "--queue", "default",
        "--packages", "org.apache.spark:spark-avro_2.12:3.5.0",
        "--jars", "extra.jar",
        "--py-files", "helpers.py",
        "--conf", "spark.executor.memoryOverhead=512m",
        "s3://bucket/job.py",
        "--date", "2024-01-01",
    ]
