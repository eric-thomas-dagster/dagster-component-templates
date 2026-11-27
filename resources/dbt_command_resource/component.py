"""DbtCommandResource component.

Reusable `dbt` CLI execution -- an Airflow Operator is reusable action code,
closer to a Dagster `ConfigurableResource` than an asset. Call `.run(...)`
from your own `@dg.asset`/`@dg.op` body when you want this wired into your
pipeline (with your own partitioning, retries, freshness policy, pool,
schedule); this component only registers the resource, it does not
materialize anything on its own. See `resources/http_resource` for the same
pattern.

Runs `dbt <command> --project-dir <dir> [--profiles-dir ...] [--target ...]
[--select ...] [--exclude ...] [--full-refresh] [--vars ...]` via
`subprocess.run`. Exit code -> raises `dagster.Failure` on non-zero. Best-
effort: if the command produced a `target/run_results.json` (every
`dbt run|build|test|snapshot|seed` invocation does), its per-node `status`
counts are returned -- `pass`/`error`/`skip`/`warn` node counts and total
elapsed seconds -- without requiring a full manifest-based asset graph.
"""
import json
import os
import subprocess
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


def _parse_run_results(project_dir: str, target_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """Best-effort parse of dbt's own `target/run_results.json` artifact (written by every
    `dbt run|build|test|snapshot|seed` invocation). Returns a summary dict of per-status node
    counts + total elapsed seconds, or None if the file doesn't exist or isn't parseable --
    never raises, since this is metadata enrichment, not a correctness requirement."""
    candidate = os.path.join(project_dir, target_path or "target", "run_results.json")
    try:
        with open(candidate) as f:
            data = json.load(f)
    except (OSError, ValueError):
        return None
    results = data.get("results") or []
    counts: Dict[str, int] = {}
    for node in results:
        status = str(node.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
    elapsed = data.get("elapsed_time")
    return {
        "node_count": len(results),
        "status_counts": counts,
        "elapsed_seconds": float(elapsed) if elapsed is not None else None,
    }


class DbtCommandResource(dg.ConfigurableResource):
    """Run an arbitrary `dbt` CLI command. Raises `dagster.Failure` on a non-zero exit code."""

    project_dir: str = Field(description="Path to the dbt project root (where dbt_project.yml lives).")
    profiles_dir: Optional[str] = Field(default=None, description="Path to profiles.yml (default: ~/.dbt).")

    def run(
        self,
        command: Literal["run", "build", "test", "snapshot", "seed", "source"] = "run",
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        full_refresh: bool = False,
        target: Optional[str] = None,
        vars: Optional[dict] = None,
        extra_args: Optional[List[str]] = None,
        cwd: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
        timeout_seconds: Optional[int] = None,
        dry_run: bool = False,
        parse_run_results: bool = True,
        target_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run `dbt <command>`. 'source' runs `dbt source freshness`. Returns
        {exit_code, dry_run, command, stdout, stderr, dbt_node_count,
        dbt_status_counts, dbt_elapsed_seconds}. Raises `dagster.Failure` on a
        non-zero exit code or timeout (never on dry_run, which skips the subprocess)."""
        resolved_cwd = cwd or self.project_dir

        cmd = ["dbt", command, "--project-dir", self.project_dir]
        if self.profiles_dir:
            cmd += ["--profiles-dir", self.profiles_dir]
        if target:
            cmd += ["--target", target]
        if select:
            cmd += ["--select", select]
        if exclude:
            cmd += ["--exclude", exclude]
        if full_refresh:
            cmd.append("--full-refresh")
        if vars:
            cmd += ["--vars", json.dumps(vars)]
        cmd += extra_args or []

        if dry_run:
            return {"dry_run": True, "command": " ".join(cmd), "exit_code": None, "stdout": "", "stderr": ""}

        merged_env = {**os.environ, **(env_vars or {})}
        try:
            r = subprocess.run(
                cmd, cwd=resolved_cwd, env=merged_env,
                capture_output=True, text=True, timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as e:
            raise dg.Failure(
                description=f"dbt {command} timed out after {timeout_seconds}s.",
                metadata={"stderr": dg.MetadataValue.text((e.stderr or "")[:2000] if e.stderr else "")},
            ) from e

        result: Dict[str, Any] = {
            "exit_code": r.returncode,
            "dry_run": False,
            "command": " ".join(cmd),
            "stdout": r.stdout or "",
            "stderr": r.stderr or "",
        }
        if parse_run_results:
            summary = _parse_run_results(resolved_cwd, target_path)
            if summary is not None:
                result["dbt_node_count"] = summary["node_count"]
                result["dbt_status_counts"] = summary["status_counts"]
                result["dbt_elapsed_seconds"] = summary["elapsed_seconds"]

        if r.returncode != 0:
            raise dg.Failure(
                description=f"dbt {command} failed, exit={r.returncode}",
                metadata={
                    "exit_code": dg.MetadataValue.int(r.returncode),
                    "command": dg.MetadataValue.text(result["command"]),
                    "stderr": dg.MetadataValue.text(result["stderr"][:2000]),
                },
            )
        return result


class DbtCommandResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `DbtCommandResource` for use by any op/asset in this code location.

    Call `context.resources.<resource_key>.run(command="build", select="my_model")` from your
    own asset/op body -- this is the reusable unit (the Airflow-Operator-equivalent action);
    wrap it in your own `@dg.asset` only when you want the run to participate in the Dagster
    asset graph (deps, partitioning, retries, freshness policy, pool, schedule), the same
    relationship `shell_command_asset` has to a full per-step pipeline component. For a
    per-dbt-model asset graph use `assets/dbt/enriched_dbt_project`; for a non-asset job/op use
    `jobs/dbt_run_job`; for triggering an existing dbt Cloud job use `jobs/dbt_cloud_trigger_job`
    or `assets/dbt/enriched_dbt_cloud_workspace`.
    """

    resource_key: str = Field(default="dbt_command_resource", description="Key used to register this resource. Other components/assets reference it via resource_key.")
    project_dir: str = Field(description="Path to the dbt project root (where dbt_project.yml lives).")
    profiles_dir: Optional[str] = Field(default=None, description="Path to profiles.yml (default: ~/.dbt).")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dbt Command Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DbtCommandResource(project_dir=self.project_dir, profiles_dir=self.profiles_dir)
        return dg.Definitions(resources={self.resource_key: resource})
