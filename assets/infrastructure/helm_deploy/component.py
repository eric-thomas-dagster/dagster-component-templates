"""Helm Deploy Asset Component.

Deploys or upgrades a Helm chart as the first step in a Dagster asset graph.
Downstream assets declare `deps: [asset_name]` and will not run until the
release has been deployed and all pods are ready.

Uses subprocess to call the Helm CLI — no Python Helm SDK required.

Typical use case: deploy an ML model serving container before running
inference assets, or provision a database Helm chart before data loading assets.
"""

import json
import os
import subprocess
from typing import Optional

import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult
from pydantic import Field


def _run(cmd: list[str], extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command, streaming logs."""
    env = {**os.environ, **extra_env}
    log.info(f"Running: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        log.info(result.stdout)
    if result.stderr:
        log.info(result.stderr)
    return result


def _run_checked(cmd: list[str], extra_env: dict[str, str], log) -> subprocess.CompletedProcess:
    """Run a subprocess command and raise on non-zero exit code."""
    result = _run(cmd, extra_env, log)
    if result.returncode != 0:
        raise Exception(
            f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
            f"stderr: {result.stderr}\nstdout: {result.stdout}"
        )
    return result


class HelmDeployComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy or upgrade a Helm chart as a Dagster asset.

    Runs `helm upgrade --install` against a chart (from a repository, local
    path, or OCI registry) and surfaces the release status as a Dagster asset
    materialization. Release metadata — chart version, app version, deployed
    namespace, and release status — are recorded as asset metadata.

    Designed to run as the *first step* in an asset graph so that downstream
    assets can declare a dependency on a live Kubernetes workload:

    ```yaml
    # downstream inference asset
    deps:
      - deploy_ml_serving
    ```

    Example:
        ```yaml
        type: dagster_component_templates.HelmDeployComponent
        attributes:
          asset_name: deploy_ml_serving
          release_name: ml-serving
          chart: my-org/ml-serving
          repo_url: https://charts.my-company.com
          repo_name: my-org
          namespace: ml-serving
          set_values:
            image.tag: "latest"
            replicas: "3"
          group_name: infrastructure
          description: Deploy ML model serving before inference pipeline runs
        ```
    """

    asset_name: str = Field(description="Dagster asset name")
    release_name: str = Field(description="Helm release name")
    chart: str = Field(
        description=(
            "Chart reference: repository alias + name (e.g. 'my-org/ml-serving'), "
            "local path (e.g. './charts/ml-serving'), "
            "or OCI reference (e.g. 'oci://registry.example.com/charts/ml-serving')."
        )
    )
    namespace: str = Field(
        default="default",
        description="Kubernetes namespace to deploy into.",
    )
    create_namespace: bool = Field(
        default=True,
        description="Create the namespace if it does not already exist (--create-namespace).",
    )
    values_files: Optional[list[str]] = Field(
        default=None,
        description="List of values.yaml files passed as -f to helm upgrade.",
    )
    set_values: Optional[dict[str, str]] = Field(
        default=None,
        description="Key-value pairs passed as --set. Values are coerced to their YAML type by Helm.",
    )
    set_string_values: Optional[dict[str, str]] = Field(
        default=None,
        description="Key-value pairs passed as --set-string. Values are always treated as strings.",
    )
    version: Optional[str] = Field(
        default=None,
        description="Pin a specific chart version (--version). Omit to use the latest.",
    )
    repo_url: Optional[str] = Field(
        default=None,
        description=(
            "Helm repository URL to add before installing "
            "(e.g. https://charts.my-company.com). "
            "Required when using a repo alias in `chart` and the repo is not already added."
        ),
    )
    repo_name: Optional[str] = Field(
        default=None,
        description=(
            "Name to register the repository as (e.g. 'my-org'). "
            "Must match the alias prefix in `chart` when using a repository reference."
        ),
    )
    kube_context: Optional[str] = Field(
        default=None,
        description="Kubernetes context to use (--kube-context). Uses the current context if omitted.",
    )
    kubeconfig: Optional[str] = Field(
        default=None,
        description="Path to a kubeconfig file (--kubeconfig). Uses ~/.kube/config if omitted.",
    )
    atomic: bool = Field(
        default=True,
        description=(
            "Roll back automatically on failure (--atomic). "
            "Ensures the release never gets stuck in a broken state."
        ),
    )
    wait: bool = Field(
        default=True,
        description="Wait for all pods and resources to be ready before considering the deploy successful (--wait).",
    )
    timeout: str = Field(
        default="10m",
        description="Maximum time to wait for resources to become ready (--timeout). Format: '10m', '300s', etc.",
    )
    dry_run: bool = Field(
        default=False,
        description="Perform a dry run — render templates and validate but do not apply (--dry-run).",
    )
    helm_bin: str = Field(
        default="helm",
        description="Path to the helm binary (default: 'helm', assumed to be on PATH).",
    )
    env_vars: Optional[dict[str, str]] = Field(
        default=None,
        description=(
            "Extra environment variables for the Helm subprocess "
            "(e.g. KUBECONFIG, HELM_KUBEAPISERVER, cloud provider auth vars)."
        ),
    )
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of what this Helm release deploys.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream Dagster asset keys this asset depends on (e.g. ['raw_orders', 'schema/orders']).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset(
            name=self.asset_name,
            description=self.description or f"Deploy Helm release {self.release_name} ({self.chart})",
            group_name=self.group_name,
            kinds={"helm", "kubernetes"},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def helm_deploy(context: AssetExecutionContext) -> MaterializeResult:
            extra_env: dict[str, str] = _self.env_vars or {}
            helm = _self.helm_bin

            # Shared kubeconfig/context flags used across all helm commands
            kube_flags: list[str] = []
            if _self.kube_context:
                kube_flags += ["--kube-context", _self.kube_context]
            if _self.kubeconfig:
                kube_flags += ["--kubeconfig", _self.kubeconfig]

            # --- 1. Add Helm repository (if configured) ---
            if _self.repo_url and _self.repo_name:
                _run_checked(
                    [helm, "repo", "add", _self.repo_name, _self.repo_url],
                    extra_env=extra_env,
                    log=context.log,
                )
                _run_checked(
                    [helm, "repo", "update"],
                    extra_env=extra_env,
                    log=context.log,
                )

            # --- 2. Build upgrade flags ---
            upgrade_flags: list[str] = ["--output", "json"]

            if _self.create_namespace:
                upgrade_flags.append("--create-namespace")
            if _self.atomic:
                upgrade_flags.append("--atomic")
            if _self.wait:
                upgrade_flags.append("--wait")
            if _self.dry_run:
                upgrade_flags.append("--dry-run")
            if _self.version:
                upgrade_flags += ["--version", _self.version]

            upgrade_flags += ["--timeout", _self.timeout]

            for vf in (_self.values_files or []):
                upgrade_flags += ["-f", vf]
            for k, v in (_self.set_values or {}).items():
                upgrade_flags += ["--set", f"{k}={v}"]
            for k, v in (_self.set_string_values or {}).items():
                upgrade_flags += ["--set-string", f"{k}={v}"]

            # --- 3. Run helm upgrade --install ---
            upgrade_cmd = (
                [
                    helm, "upgrade", "--install",
                    _self.release_name, _self.chart,
                    "--namespace", _self.namespace,
                ]
                + upgrade_flags
                + kube_flags
            )

            upgrade_result = _run_checked(
                upgrade_cmd,
                extra_env=extra_env,
                log=context.log,
            )

            # --- 4. Parse upgrade JSON output ---
            release_info: dict = {}
            chart_version = "unknown"
            app_version = "unknown"
            deploy_status = "unknown"

            try:
                release_info = json.loads(upgrade_result.stdout)
                chart_meta = release_info.get("chart", {}).get("metadata", {})
                chart_version = chart_meta.get("version", "unknown")
                app_version = chart_meta.get("appVersion", "unknown")
                deploy_status = release_info.get("info", {}).get("status", "unknown")
            except (json.JSONDecodeError, AttributeError):
                context.log.warning(
                    "Could not parse helm upgrade JSON output. Metadata will be minimal."
                )

            # --- 5. Fetch deployed resources via helm status ---
            resources: dict = {}
            try:
                status_result = _run_checked(
                    [
                        helm, "status", _self.release_name,
                        "-n", _self.namespace,
                        "--output", "json",
                    ] + kube_flags,
                    extra_env=extra_env,
                    log=context.log,
                )
                status_data = json.loads(status_result.stdout)
                deploy_status = status_data.get("info", {}).get("status", deploy_status)
                # Resource summary is nested under info.resources in newer Helm versions
                resources = status_data.get("info", {}).get("resources", {})
            except Exception as exc:
                context.log.warning(f"Could not fetch helm status: {exc}")

            # --- 6. Return metadata ---
            return MaterializeResult(
                metadata={
                    "release": dg.MetadataValue.text(_self.release_name),
                    "chart": dg.MetadataValue.text(_self.chart),
                    "chart_version": dg.MetadataValue.text(chart_version),
                    "app_version": dg.MetadataValue.text(app_version),
                    "namespace": dg.MetadataValue.text(_self.namespace),
                    "status": dg.MetadataValue.text(deploy_status),
                    "dry_run": dg.MetadataValue.bool(_self.dry_run),
                    "resources": dg.MetadataValue.json(resources),
                }
            )

        return dg.Definitions(assets=[helm_deploy])
