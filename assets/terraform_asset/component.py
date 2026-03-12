"""Terraform Asset Component.

Provisions infrastructure by running Terraform as the first step in a Dagster
asset graph. Downstream data assets declare `deps: [infrastructure]` and will
not run until Terraform has applied successfully.

Uses subprocess to call the Terraform CLI — no Python Terraform SDK required.
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


class TerraformAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Provision infrastructure with Terraform as a Dagster asset.

    Runs `terraform init` (optional), `terraform plan`, and `terraform apply`
    (or `terraform destroy`) in sequence. Terraform outputs are parsed as JSON
    and surfaced as Dagster asset metadata, making them available to downstream
    assets.

    Designed to run as the *first step* in an asset graph so that downstream
    data assets can declare a dependency on provisioned infrastructure:

    ```yaml
    # downstream asset
    deps:
      - provision_data_infrastructure
    ```

    Example:
        ```yaml
        type: dagster_component_templates.TerraformAssetComponent
        attributes:
          asset_name: provision_data_infrastructure
          working_dir: "{{ project_root }}/terraform/data_platform"
          workspace: production
          var_files:
            - vars/production.tfvars
          group_name: infrastructure
          description: Provision S3 buckets, IAM roles, and RDS instance before pipeline runs
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    working_dir: str = Field(description="Path to the Terraform root module directory")
    workspace: Optional[str] = Field(
        default=None,
        description="Terraform workspace to select before running. Uses the default workspace if None.",
    )
    var_files: Optional[list[str]] = Field(
        default=None,
        description="List of .tfvars files passed as -var-file arguments",
    )
    vars: Optional[dict[str, str]] = Field(
        default=None,
        description="Inline variable overrides passed as -var key=value",
    )
    targets: Optional[list[str]] = Field(
        default=None,
        description="Limit apply/plan scope to specific resources via -target",
    )
    terraform_bin: str = Field(
        default="terraform",
        description="Path to the terraform binary (default: 'terraform', assumed to be on PATH)",
    )
    init_on_run: bool = Field(
        default=True,
        description="Run 'terraform init -input=false' before plan/apply. Safe to always enable.",
    )
    plan_only: bool = Field(
        default=False,
        description="Only run 'terraform plan'; do not apply. Useful for CI drift detection.",
    )
    destroy: bool = Field(
        default=False,
        description="Run 'terraform destroy' instead of apply",
    )
    parallelism: int = Field(
        default=10,
        description="Number of concurrent operations (-parallelism flag)",
    )
    auto_approve: bool = Field(
        default=True,
        description="Pass -auto-approve to apply/destroy. Set False to require manual approval.",
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description="Extra environment variables for the Terraform subprocess (e.g. AWS_PROFILE)",
    )
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of what this Terraform module provisions",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream Dagster asset keys this asset depends on (e.g. ['raw_orders', 'schema/orders'])",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=self.asset_name,
            description=self.description or f"Provision infrastructure via Terraform in {self.working_dir}",
            group_name=self.group_name,
            kinds={"terraform"},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def terraform_asset(context: AssetExecutionContext) -> MaterializeResult:
            extra_env: dict[str, str] = _self.env_vars or {}
            tf = _self.terraform_bin
            cwd = _self.working_dir

            # --- 1. Workspace selection ---
            if _self.workspace:
                # Try to select; if the workspace doesn't exist, create it then select.
                select_result = _run(
                    [tf, "workspace", "select", _self.workspace],
                    cwd=cwd,
                    extra_env=extra_env,
                    log=context.log,
                )
                if select_result.returncode != 0:
                    context.log.info(
                        f"Workspace '{_self.workspace}' not found — creating it."
                    )
                    _run_checked(
                        [tf, "workspace", "new", _self.workspace],
                        cwd=cwd,
                        extra_env=extra_env,
                        log=context.log,
                    )

            # --- 2. Init ---
            if _self.init_on_run:
                _run_checked(
                    [tf, "init", "-input=false"],
                    cwd=cwd,
                    extra_env=extra_env,
                    log=context.log,
                )

            # --- 3. Build shared flags ---
            var_flags: list[str] = []
            for vf in (_self.var_files or []):
                var_flags += ["-var-file", vf]
            for k, v in (_self.vars or {}).items():
                var_flags += ["-var", f"{k}={v}"]

            target_flags: list[str] = []
            for t in (_self.targets or []):
                target_flags += ["-target", t]

            # --- 4. Plan ---
            plan_cmd = (
                [tf, "plan", "-out=tfplan", "-input=false", "-detailed-exitcode"]
                + [f"-parallelism={_self.parallelism}"]
                + var_flags
                + target_flags
            )
            plan_result = _run(
                plan_cmd,
                cwd=cwd,
                extra_env=extra_env,
                log=context.log,
            )

            # exit codes: 0 = no changes, 2 = changes present, 1 = error
            if plan_result.returncode == 1:
                raise Exception(
                    f"terraform plan failed.\nstderr: {plan_result.stderr}\nstdout: {plan_result.stdout}"
                )

            changes_present = plan_result.returncode == 2
            context.log.info(
                f"terraform plan exit code {plan_result.returncode} — "
                f"{'changes present' if changes_present else 'no changes'}"
            )

            # --- 5. Apply / Destroy ---
            if not _self.plan_only and changes_present:
                if _self.destroy:
                    destroy_cmd = [tf, "destroy", "-input=false", f"-parallelism={_self.parallelism}"]
                    if _self.auto_approve:
                        destroy_cmd.append("-auto-approve")
                    destroy_cmd += var_flags + target_flags
                    _run_checked(
                        destroy_cmd,
                        cwd=cwd,
                        extra_env=extra_env,
                        log=context.log,
                    )
                else:
                    _run_checked(
                        [tf, "apply", "tfplan"],
                        cwd=cwd,
                        extra_env=extra_env,
                        log=context.log,
                    )
            elif _self.plan_only:
                context.log.info("plan_only=True — skipping apply.")
            else:
                context.log.info("No changes detected — skipping apply.")

            # --- 6. Capture outputs ---
            outputs: dict = {}
            try:
                output_result = _run_checked(
                    [tf, "output", "-json"],
                    cwd=cwd,
                    extra_env=extra_env,
                    log=context.log,
                )
                raw_outputs = json.loads(output_result.stdout)
                # Flatten: extract the `value` field from each output object
                outputs = {k: v.get("value", v) for k, v in raw_outputs.items()}
            except Exception as exc:
                context.log.warning(f"Could not parse terraform outputs: {exc}")

            return MaterializeResult(
                metadata={
                    "outputs": dg.MetadataValue.json(outputs),
                    "working_dir": dg.MetadataValue.text(cwd),
                    "workspace": dg.MetadataValue.text(_self.workspace or "default"),
                    "plan_only": dg.MetadataValue.bool(_self.plan_only),
                    "changes_applied": dg.MetadataValue.bool(
                        not _self.plan_only and changes_present
                    ),
                }
            )

        return dg.Definitions(assets=[terraform_asset])
