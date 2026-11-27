"""Real, committed tests for SparkSubmitResource.submit.

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
    "--exclude-packages", "--repositories", "--files", "--archives",
    "--driver-class-path", "--executor-cores", "--properties-file",
)

_FLAGS_NO_VALUE = ("--verbose",)

_SHIM_BODY = f"""#!{sys.executable}
import sys, subprocess
args = sys.argv[1:]
flags_with_value = {_FLAGS_WITH_VALUE!r}
flags_no_value = {_FLAGS_NO_VALUE!r}
i = 0
while i < len(args):
    if args[i] in flags_with_value:
        i += 2
        continue
    if args[i] in flags_no_value:
        i += 1
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


def _resource(tmp_path):
    return mod.SparkSubmitResource(spark_submit_binary=_write_shim(tmp_path))


def test_successful_run_returns_exit_code_zero(tmp_path):
    script = _write_script(tmp_path, """
        print("hello from spark job")
    """)
    result = _resource(tmp_path).submit(application=script)
    assert result["exit_code"] == 0
    assert "hello from spark job" in result["stdout_preview"]


def test_nonzero_exit_raises_with_stderr(tmp_path):
    script = _write_script(tmp_path, """
        import sys
        print("boom", file=sys.stderr)
        sys.exit(7)
    """)
    with pytest.raises(dg.Failure, match="exited 7"):
        _resource(tmp_path).submit(application=script)


def test_success_exit_codes_allows_nonzero(tmp_path):
    script = _write_script(tmp_path, """
        import sys
        sys.exit(3)
    """)
    result = _resource(tmp_path).submit(application=script, success_exit_codes=[3])
    assert result["exit_code"] == 3


def test_timeout_kills_process_and_raises(tmp_path):
    script = _write_script(tmp_path, """
        import time
        time.sleep(30)
    """)
    with pytest.raises(dg.Failure, match="timed out"):
        _resource(tmp_path).submit(application=script, timeout_seconds=1)


def test_env_vars_are_merged_into_subprocess_env(tmp_path):
    script = _write_script(tmp_path, """
        import os
        print(os.environ.get("MY_SPARK_VAR", "MISSING"))
    """)
    result = _resource(tmp_path).submit(application=script, env_vars={"MY_SPARK_VAR": "present"})
    assert "present" in result["stdout_preview"]


def test_invalid_deploy_mode_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="deploy_mode"):
        _resource(tmp_path).submit(application="app.py", deploy_mode="bogus")


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


def test_new_flags_construction_matches_real_spark_submit_help():
    cmd = mod._build_spark_submit_command(
        spark_submit_binary="spark-submit",
        application="job.py",
        master=None, deploy_mode=None, class_name=None, name=None,
        driver_memory=None, executor_memory=None, num_executors=None,
        total_executor_cores=None, queue=None, packages=None, jars=None,
        py_files=None, conf=None, extra_args=None, application_args=None,
        exclude_packages=["com.foo:bar"],
        repositories=["https://repo.example.com/maven2"],
        files=["data.csv", "lookup.json"],
        archives=["bundle.zip"],
        driver_class_path="/opt/extra/classes",
        executor_cores=4,
        verbose=True,
        properties_file="/etc/spark/spark-defaults.conf",
    )
    assert cmd == [
        "spark-submit",
        "--executor-cores", "4",
        "--exclude-packages", "com.foo:bar",
        "--repositories", "https://repo.example.com/maven2",
        "--files", "data.csv,lookup.json",
        "--archives", "bundle.zip",
        "--driver-class-path", "/opt/extra/classes",
        "--properties-file", "/etc/spark/spark-defaults.conf",
        "--verbose",
        "job.py",
    ]


def test_submit_runs_real_subprocess_with_new_flags(tmp_path):
    script = _write_script(tmp_path, """
        print("ran with new flags")
    """)
    result = _resource(tmp_path).submit(
        application=script,
        exclude_packages=["com.foo:bar"],
        repositories=["https://repo.example.com/maven2"],
        files=["a.txt"],
        archives=["b.zip"],
        driver_class_path="/opt/classes",
        executor_cores=2,
        verbose=True,
        properties_file="/etc/spark-defaults.conf",
    )
    assert result["exit_code"] == 0
    assert "ran with new flags" in result["stdout_preview"]


def test_component_registers_resource():
    component = mod.SparkSubmitResourceComponent()
    defs = component.build_defs(None)
    assert isinstance(defs.resources["spark_submit_resource"], mod.SparkSubmitResource)
