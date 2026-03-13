"""Pulumi Asset Component.

Deploys a Pulumi stack as the first step in a Dagster asset graph. Downstream
data assets declare `deps: [asset_name]` and will not run until the stack
has been deployed successfully.

Uses subprocess to call the Pulumi CLI — no Pulumi Automation API SDK required.
"""

import json
import os
import subprocess
from typing import Optional

import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
from pydantic import Field


def _run(cmd: list[str], cwd: str, extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command, streaming logs and raising on failure."""
    env = {**os.environ, **extra_env}
    log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        log.info(result.stdout)
    if result.stderr:
        log.info(result.stderr)
    return result


def _run_checked(cmd: list[str], cwd: str, extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command and raise on non-zero exit code."""
    result = _run(cmd, cwd, extra_env, log)
    if result.returncode != 0:
        raise Exception(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
            f"stderr: {result.stderr}\nstdout: {result.stdout}"
        )
    return result


class PulumiAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy a Pulumi stack as a Dagster asset.

    Runs `pulumi up` (or `destroy` / `preview`) against a Pulumi stack and
    surfaces the result as a Dagster asset materialization. Stack outputs and
    resource change counts are recorded as asset metadata so they are visible in
    the Dagster UI without leaving the pipeline.

    Designed to run as the *first step* in an asset graph so that downstream
    data assets can declare a dependency on provisioned cloud infrastructure:

    ```yaml
    # downstream asset
    deps:
      - provision_cloud_resources
    ```

    Example:
        ```yaml
        type: dagster_component_templates.PulumiAssetComponent
        attributes:
          asset_name: provision_cloud_resources
          stack_name: my-org/data-platform/production
          working_dir: "{{ project_root }}/infrastructure"
          operation: up
          refresh: true
          group_name: infrastructure
          description: Provision S3 buckets and IAM roles before pipeline runs
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    stack_name: str = Field(
        description=(
            "Fully qualified Pulumi stack name: 'org/project/stack' for Pulumi Cloud, "
            "or just 'stack' for local backends."
        )
    )
    working_dir: str = Field(description="Directory containing Pulumi.yaml")
    operation: str = Field(
        default="up",
        description="Pulumi operation to perform: 'up', 'destroy', or 'preview'.",
    )
    expect_no_changes: bool = Field(
        default=False,
        description=(
            "Fail the asset if the operation detects any resource changes. "
            "Useful for drift-detection pipelines where the stack should be stable."
        ),
    )
    refresh: bool = Field(
        default=False,
        description="Run 'pulumi refresh' before up/destroy to reconcile state with actual cloud resources.",
    )
    target_resources: Optional[list[str]] = Field(
        default=None,
        description="Limit the operation to specific resource URNs via --target.",
    )
    config: Optional[dict[str, str]] = Field(
        default=None,
        description="Config key-value pairs passed as --config key=value to the Pulumi command.",
    )
    secrets_provider: Optional[str] = Field(
        default=None,
        description="Secrets provider URL passed as --secrets-provider (e.g. awskms://..., azurekeyvault://...).",
    )
    pulumi_bin: str = Field(
        default="pulumi",
        description="Path to the pulumi binary (default: 'pulumi', assumed to be on PATH).",
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Extra environment variables for the Pulumi subprocess "
            "(e.g. AWS_PROFILE, PULUMI_ACCESS_TOKEN)."
        ),
    )
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of what this Pulumi stack provisions.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream Dagster asset keys this asset depends on (e.g. ['raw_orders', 'schema/orders']).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=self.asset_name,
            description=self.description or f"Deploy Pulumi stack {self.stack_name}",
            group_name=self.group_name,
            kinds={"pulumi"},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def pulumi_asset(context: AssetExecutionContext) -> MaterializeResult:
            extra_env: dict[str, str] = _self.env_vars or {}
            pulumi = _self.pulumi_bin
            cwd = _self.working_dir

            # --- 1. Refresh (optional) ---
            if _self.refresh and _self.operation in ("up", "destroy"):
                _run_checked(
                    [pulumi, "refresh", "--yes", "--stack", _self.stack_name],
                    cwd=cwd,
                    extra_env=extra_env,
                    log=context.log,
                )

            # --- 2. Build shared flags ---
            config_flags: list[str] = []
            for k, v in (_self.config or {}).items():
                config_flags += ["--config", f"{k}={v}"]

            target_flags: list[str] = []
            for urn in (_self.target_resources or []):
                target_flags += ["--target", urn]

            secrets_flags: list[str] = []
            if _self.secrets_provider:
                secrets_flags = ["--secrets-provider", _self.secrets_provider]

            # --- 3. Run operation ---
            if _self.operation == "preview":
                op_cmd = (
                    [pulumi, "preview", "--stack", _self.stack_name, "--json"]
                    + config_flags
                    + target_flags
                    + secrets_flags
                )
            elif _self.operation == "destroy":
                op_cmd = (
                    [pulumi, "destroy", "--yes", "--stack", _self.stack_name, "--json"]
                    + config_flags
                    + target_flags
                    + secrets_flags
                )
            else:
                # default: up
                op_cmd = (
                    [pulumi, "up", "--yes", "--stack", _self.stack_name, "--json"]
                    + config_flags
                    + target_flags
                    + secrets_flags
                )

            op_result = _run_checked(
                op_cmd,
                cwd=cwd,
                extra_env=extra_env,
                log=context.log,
            )

            # --- 4. Parse JSON output ---
            # Pulumi --json emits one JSON object per line (streaming events).
            # The final summary object has keys: "result", "steps", "outputs".
            # We look for the last line that is a valid JSON object with "result".
            summary: dict = {}
            resource_changes: dict = {}
            outputs: dict = {}

            for line in reversed(op_result.stdout.splitlines()):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict) and "result" in obj:
                        summary = obj
                        break
                except json.JSONDecodeError:
                    continue

            if summary:
                result_status = summary.get("result", "unknown")
                outputs = summary.get("outputs", {})
                # Pulumi resource change counts live in the summary under
                # "resourceChanges" (newer) or inside individual step objects.
                resource_changes = summary.get("resourceChanges", {})

                if result_status == "failed":
                    error_msg = summary.get("diagnostics", [{}])
                    raise Exception(
                        f"Pulumi {_self.operation} failed for stack {_self.stack_name}. "
                        f"diagnostics: {error_msg}"
                    )
            else:
                # Non-JSON or preview mode — best-effort
                result_status = "succeeded" if op_result.returncode == 0 else "failed"
                context.log.warning(
                    "Could not find a JSON summary object in Pulumi output. "
                    "Metadata will be minimal."
                )

            # --- 5. expect_no_changes enforcement ---
            if _self.expect_no_changes:
                changed_count = sum(
                    v for k, v in resource_changes.items() if k != "same"
                )
                if changed_count > 0:
                    raise Exception(
                        f"expect_no_changes=True but Pulumi detected changes: {resource_changes}. "
                        f"Stack: {_self.stack_name}"
                    )

            # --- 6. Return metadata ---
            return MaterializeResult(
                metadata={
                    "stack": dg.MetadataValue.text(_self.stack_name),
                    "operation": dg.MetadataValue.text(_self.operation),
                    "result": dg.MetadataValue.text(result_status),
                    "outputs": dg.MetadataValue.json(outputs),
                    "resource_changes": dg.MetadataValue.json(resource_changes),
                    "working_dir": dg.MetadataValue.text(cwd),
                }
            )

        return dg.Definitions(assets=[pulumi_asset])
