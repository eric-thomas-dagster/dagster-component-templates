"""Databricks Asset Bundle Component.

Creates Dagster assets from Databricks Asset Bundle YAML configuration files.
Each task in the bundle becomes a materializable asset with proper dependencies.
"""

import json
import subprocess
from typing import Optional, List, Dict, Any, Sequence
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetSpec,
    multi_asset,
    MaterializeResult,
    ConfigurableResource,
    Resolvable,
    Model,
)
from pydantic import Field


class DatabricksWorkspaceResource(ConfigurableResource):
    """Resource for interacting with Databricks workspace."""

    host: str = Field(description="Databricks workspace URL")
    token: str = Field(description="Databricks personal access token")

    def get_client(self) -> WorkspaceClient:
        """Get Databricks workspace client."""
        return WorkspaceClient(host=self.host, token=self.token)

    def submit_and_poll(
        self,
        context: AssetExecutionContext,
        task_key: str,
        task_config: Dict[str, Any],
        cluster_config: Optional[Dict[str, Any]] = None,
        libraries: Optional[List[Dict[str, Any]]] = None,
    ) -> MaterializeResult:
        """Submit a Databricks job and poll until completion."""
        client = self.get_client()

        # Build job task configuration
        task_params: Dict[str, Any] = {
            "task_key": task_key,
        }

        # Add task type configuration
        if "notebook_task" in task_config:
            task_params["notebook_task"] = jobs.NotebookTask(**task_config["notebook_task"])
        elif "spark_python_task" in task_config:
            task_params["spark_python_task"] = jobs.SparkPythonTask(**task_config["spark_python_task"])
        elif "python_wheel_task" in task_config:
            task_params["python_wheel_task"] = jobs.PythonWheelTask(**task_config["python_wheel_task"])
        elif "spark_jar_task" in task_config:
            task_params["spark_jar_task"] = jobs.SparkJarTask(**task_config["spark_jar_task"])

        # Add cluster configuration
        if cluster_config:
            if "existing_cluster_id" in cluster_config:
                task_params["existing_cluster_id"] = cluster_config["existing_cluster_id"]
            elif "new_cluster" in cluster_config:
                task_params["new_cluster"] = jobs.ClusterSpec(**cluster_config["new_cluster"])

        # Add libraries
        if libraries:
            task_params["libraries"] = [jobs.Library(**lib) for lib in libraries]

        # Submit job
        context.log.info(f"Submitting Databricks task: {task_key}")
        run = client.jobs.submit(
            run_name=f"dagster_{context.run_id}_{task_key}",
            tasks=[jobs.SubmitTask(**task_params)],
        )

        context.log.info(f"Submitted run {run.run_id}, polling for completion...")

        # Poll until completion
        final_run = client.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)

        metadata = {
            "run_id": str(run.run_id),
            "run_url": final_run.run_page_url or "N/A",
            "state": str(final_run.state.life_cycle_state) if final_run.state else "UNKNOWN",
        }

        context.log.info(f"Task {task_key} completed with state: {metadata['state']}")

        return MaterializeResult(metadata=metadata)


class DatabricksAssetBundleComponent(Component, Model, Resolvable):
    """Component for creating Dagster assets from Databricks Asset Bundle configurations.

    Reads a databricks.yml bundle configuration file and creates a Dagster asset for each
    task defined in the bundle. Tasks are executed through the Databricks workspace when
    materialized.

    Example:
        ```yaml
        type: dagster_component_templates.DatabricksAssetBundleComponent
        attributes:
          databricks_config_path: databricks.yml
          workspace_host: https://dbc-abc123.cloud.databricks.com
          workspace_token: "{{ env('DATABRICKS_TOKEN') }}"
          asset_key_prefix: my_bundle
        ```
    """

    databricks_config_path: str = Field(
        description="Path to databricks.yml bundle configuration file (relative to project root)"
    )

    workspace_host: str = Field(description="Databricks workspace URL")

    workspace_token: str = Field(description="Databricks personal access token")

    asset_key_prefix: Optional[str] = Field(
        default=None,
        description="Optional prefix for all asset keys generated from bundle tasks",
    )

    group_name: Optional[str] = Field(
        default="databricks_bundle", description="Group name for all generated assets"
    )

    def _load_bundle_config(self, context: ComponentLoadContext) -> Dict[str, Any]:
        """Load and resolve the Databricks bundle configuration using Databricks CLI."""
        # Resolve path relative to project root
        config_path = Path(self.databricks_config_path)
        if not config_path.is_absolute():
            # Assume it's relative to the current working directory
            config_path = Path.cwd() / config_path

        if not config_path.exists():
            raise FileNotFoundError(f"Databricks config file not found: {config_path}")

        context.log.info(f"Loading Databricks bundle config from: {config_path}")

        # Use Databricks CLI to resolve the configuration
        # This resolves template variables like ${workspace.current_user.userName}
        try:
            result = subprocess.run(
                ["databricks", "bundle", "show", "config", "-c", str(config_path)],
                capture_output=True,
                text=True,
                check=True,
            )
            config = json.loads(result.stdout)
            return config
        except subprocess.CalledProcessError as e:
            context.log.warning(
                f"Failed to resolve bundle config with CLI: {e.stderr}. Falling back to raw YAML."
            )
            # Fallback: try to read the YAML directly without resolution
            import yaml

            with open(config_path) as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            context.log.warning(
                "Databricks CLI not found. Reading bundle config without variable resolution."
            )
            # Fallback: read YAML directly
            import yaml

            with open(config_path) as f:
                return yaml.safe_load(f)

    def _get_tasks_from_config(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract tasks from bundle configuration."""
        tasks = []

        # Bundle config structure: resources -> jobs -> {job_name} -> tasks
        resources = config.get("resources", {})
        jobs_config = resources.get("jobs", {})

        for job_name, job_spec in jobs_config.items():
            job_tasks = job_spec.get("tasks", [])
            for task in job_tasks:
                task_info = {
                    "job_name": job_name,
                    "task_key": task.get("task_key"),
                    "depends_on": task.get("depends_on", []),
                    "libraries": task.get("libraries", []),
                }

                # Extract task type configuration
                if "notebook_task" in task:
                    task_info["notebook_task"] = task["notebook_task"]
                elif "spark_python_task" in task:
                    task_info["spark_python_task"] = task["spark_python_task"]
                elif "python_wheel_task" in task:
                    task_info["python_wheel_task"] = task["python_wheel_task"]
                elif "spark_jar_task" in task:
                    task_info["spark_jar_task"] = task["spark_jar_task"]
                elif "run_job_task" in task:
                    task_info["run_job_task"] = task["run_job_task"]
                elif "condition_task" in task:
                    task_info["condition_task"] = task["condition_task"]

                # Extract cluster configuration
                if "existing_cluster_id" in task:
                    task_info["cluster_config"] = {"existing_cluster_id": task["existing_cluster_id"]}
                elif "new_cluster" in task:
                    task_info["cluster_config"] = {"new_cluster": task["new_cluster"]}

                tasks.append(task_info)

        return tasks

    def _get_asset_spec(self, task: Dict[str, Any]) -> AssetSpec:
        """Create an AssetSpec for a bundle task."""
        task_key = task["task_key"]

        # Build asset key
        if self.asset_key_prefix:
            asset_key = f"{self.asset_key_prefix}_{task_key}"
        else:
            asset_key = task_key

        # Sanitize asset key
        asset_key = asset_key.replace("-", "_").replace(".", "_").lower()

        # Build dependencies
        deps = []
        for dep in task.get("depends_on", []):
            dep_task_key = dep.get("task_key")
            if dep_task_key:
                if self.asset_key_prefix:
                    dep_key = f"{self.asset_key_prefix}_{dep_task_key}"
                else:
                    dep_key = dep_task_key
                dep_key = dep_key.replace("-", "_").replace(".", "_").lower()
                deps.append(dep_key)

        # Determine task type for metadata
        task_type = "unknown"
        if "notebook_task" in task:
            task_type = "notebook"
        elif "spark_python_task" in task:
            task_type = "spark_python"
        elif "python_wheel_task" in task:
            task_type = "python_wheel"
        elif "spark_jar_task" in task:
            task_type = "spark_jar"
        elif "run_job_task" in task:
            task_type = "run_job"
        elif "condition_task" in task:
            task_type = "condition"

        return AssetSpec(
            key=asset_key,
            group_name=self.group_name,
            description=f"Databricks bundle task: {task_key} (type: {task_type})",
            metadata={
                "task_key": task_key,
                "task_type": task_type,
                "job_name": task.get("job_name"),
                "bundle_path": self.databricks_config_path,
            },
            deps=deps if deps else None,
        )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from Databricks Asset Bundle configuration."""
        # Load bundle configuration
        config = self._load_bundle_config(context)

        # Extract tasks
        tasks = self._get_tasks_from_config(config)

        if not tasks:
            context.log.warning("No tasks found in Databricks bundle configuration")
            return Definitions(assets=[])

        context.log.info(f"Found {len(tasks)} tasks in bundle configuration")

        # Create asset specs
        asset_specs = [self._get_asset_spec(task) for task in tasks]

        # Create a mapping of asset key to task info for execution
        task_by_asset_key = {spec.key: task for spec, task in zip(asset_specs, tasks)}

        # Create the Databricks workspace resource
        databricks_resource = DatabricksWorkspaceResource(
            host=self.workspace_host, token=self.workspace_token
        )

        # Create multi-asset function
        @multi_asset(
            specs=asset_specs,
            name=f"{self.group_name}_tasks",
            required_resource_keys={"databricks"},
        )
        def databricks_bundle_assets(context: AssetExecutionContext) -> Sequence[MaterializeResult]:
            """Execute Databricks bundle tasks."""
            databricks = context.resources.databricks

            # Get selected assets to materialize
            selected_asset_keys = context.selected_asset_keys

            results = []
            for asset_key in selected_asset_keys:
                # Get task info
                task = task_by_asset_key.get(asset_key)
                if not task:
                    context.log.warning(f"No task found for asset: {asset_key}")
                    continue

                task_key = task["task_key"]

                # Build task config (excluding cluster and library info for now)
                task_config = {}
                for key in ["notebook_task", "spark_python_task", "python_wheel_task", "spark_jar_task"]:
                    if key in task:
                        task_config[key] = task[key]

                # Submit and poll
                result = databricks.submit_and_poll(
                    context=context,
                    task_key=task_key,
                    task_config=task_config,
                    cluster_config=task.get("cluster_config"),
                    libraries=task.get("libraries"),
                )

                results.append(result)

            return results

        return Definitions(
            assets=[databricks_bundle_assets],
            resources={"databricks": databricks_resource},
        )
