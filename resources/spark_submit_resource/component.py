"""SparkSubmitResourceComponent.

Register a reusable `SparkSubmitResource` exposing `submit(...)` -- the
Airflow `SparkSubmitOperator` equivalent for a Spark cluster reached by a
local `spark-submit` install (standalone master URL, YARN, Kubernetes master
URL, or `local[*]`), exposed as an injectable resource method rather than
baked into a single-purpose asset component.

Builds the real `spark-submit` command line (`--master`, `--deploy-mode`,
`--class`, `--conf key=value` per entry, `--packages`/`--jars`/`--py-files`
as comma-joined lists, then the application path and its args), matching
`spark-submit --help`'s documented flags -- not guessed. Streams stdout/
stderr through the parent process and raises `dg.Failure` with the captured
stderr on a non-zero exit code (unless the code is in `success_exit_codes`)
or a timeout.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _build_spark_submit_command(
    spark_submit_binary: str, application: str, master: Optional[str], deploy_mode: Optional[str],
    class_name: Optional[str], name: Optional[str], driver_memory: Optional[str], executor_memory: Optional[str],
    num_executors: Optional[int], total_executor_cores: Optional[int], queue: Optional[str],
    packages: Optional[List[str]], jars: Optional[List[str]], py_files: Optional[List[str]],
    conf: Optional[Dict[str, str]], extra_args: Optional[List[str]], application_args: Optional[List[str]],
) -> List[str]:
    cmd: List[str] = [spark_submit_binary]
    if master:
        cmd += ["--master", master]
    if deploy_mode:
        cmd += ["--deploy-mode", deploy_mode]
    if class_name:
        cmd += ["--class", class_name]
    if name:
        cmd += ["--name", name]
    if driver_memory:
        cmd += ["--driver-memory", driver_memory]
    if executor_memory:
        cmd += ["--executor-memory", executor_memory]
    if num_executors is not None:
        cmd += ["--num-executors", str(num_executors)]
    if total_executor_cores is not None:
        cmd += ["--total-executor-cores", str(total_executor_cores)]
    if queue:
        cmd += ["--queue", queue]
    if packages:
        cmd += ["--packages", ",".join(packages)]
    if jars:
        cmd += ["--jars", ",".join(jars)]
    if py_files:
        cmd += ["--py-files", ",".join(py_files)]
    for k, v in (conf or {}).items():
        cmd += ["--conf", f"{k}={v}"]
    cmd += list(extra_args or [])
    cmd.append(application)
    cmd += list(application_args or [])
    return cmd


class SparkSubmitResource(dg.ConfigurableResource):
    """Run `spark-submit` via subprocess -- the Airflow `SparkSubmitOperator` equivalent.

    Requires a `spark-submit` binary on PATH (or `spark_submit_binary` pointed
    at one). Injectable into any op or asset you write.
    """

    spark_submit_binary: str = "spark-submit"

    def submit(
        self,
        application: str,
        master: Optional[str] = None,
        deploy_mode: Optional[str] = None,
        class_name: Optional[str] = None,
        name: Optional[str] = None,
        driver_memory: Optional[str] = None,
        executor_memory: Optional[str] = None,
        num_executors: Optional[int] = None,
        total_executor_cores: Optional[int] = None,
        queue: Optional[str] = None,
        packages: Optional[List[str]] = None,
        jars: Optional[List[str]] = None,
        py_files: Optional[List[str]] = None,
        conf: Optional[Dict[str, str]] = None,
        extra_args: Optional[List[str]] = None,
        application_args: Optional[List[str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        success_exit_codes: Optional[List[int]] = None,
        log=None,
    ) -> Dict[str, Any]:
        """Run spark-submit and return {"exit_code", "command", "stdout_preview"}.

        Raises dg.Failure on a non-zero exit code (unless it's in
        success_exit_codes) or on a timeout.
        """
        import os
        import subprocess
        import sys
        import threading

        if deploy_mode is not None and deploy_mode not in ("client", "cluster"):
            raise ValueError(f"deploy_mode must be 'client' or 'cluster', got {deploy_mode!r}")

        cmd = _build_spark_submit_command(
            self.spark_submit_binary, application, master, deploy_mode,
            class_name, name, driver_memory, executor_memory,
            num_executors, total_executor_cores, queue,
            packages, jars, py_files, conf, extra_args, application_args,
        )
        merged_env = {**os.environ, **(env_vars or {})}
        if log:
            log.info(f"spark_submit_resource: $ {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd, env=merged_env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )

        timed_out = {"flag": False}
        timer = None
        if timeout_seconds is not None:
            def _kill():
                timed_out["flag"] = True
                proc.kill()
            timer = threading.Timer(timeout_seconds, _kill)
            timer.start()

        try:
            stdout_lines = []
            for line in proc.stdout or []:
                sys.stdout.write(line)
                sys.stdout.flush()
                stdout_lines.append(line)
            stderr_data = (proc.stderr.read() if proc.stderr else "") or ""
            if stderr_data:
                sys.stderr.write(stderr_data)
                sys.stderr.flush()
            proc.wait()
        finally:
            if timer:
                timer.cancel()

        if timed_out["flag"]:
            raise dg.Failure(
                description=f"spark-submit timed out after {timeout_seconds}s and was killed.",
                metadata={"stderr": dg.MetadataValue.text(stderr_data[-2000:])},
            )

        success_codes = set(success_exit_codes or []) | {0}
        if proc.returncode not in success_codes:
            raise dg.Failure(
                description=f"spark-submit exited {proc.returncode}",
                metadata={"stderr": dg.MetadataValue.text(stderr_data[-2000:])},
            )

        return {
            "exit_code": proc.returncode,
            "command": " ".join(cmd),
            "stdout_preview": "".join(stdout_lines)[-2000:],
        }


class SparkSubmitResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a SparkSubmitResource (run spark-submit as a reusable resource method) for use by other components."""

    resource_key: str = Field(default="spark_submit_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    spark_submit_binary: str = Field(default="spark-submit", description="Path to the spark-submit binary. Defaults to 'spark-submit' on PATH.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Spark Submit Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = SparkSubmitResource(spark_submit_binary=self.spark_submit_binary)
        return dg.Definitions(resources={self.resource_key: resource})
