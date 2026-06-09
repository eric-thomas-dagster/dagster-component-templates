"""CloudRunJobTriggerAssetComponent — trigger a deployed Cloud Run job and wait.

Submits an `Execution` against an existing Cloud Run Jobs resource,
polls until it terminates, surfaces the execution result + log link
as Dagster metadata. Same shape as the rest of the run-and-poll
components in the registry (matillion_run_asset, rivery_run_asset,
precisely_run_asset, etc.).

Use the existing `google_cloud_run_jobs` integration component for
multi-job batch IMPORT (one Dagster asset per discovered Cloud Run
job). Use this single-job variant when you want one focused asset
that triggers one specific job with optional environment overrides.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class CloudRunJobTriggerAssetComponent(Component, Model, Resolvable):
    """Trigger a single deployed Cloud Run job and wait for it to finish."""

    asset_name: str = Field(description="Output asset name.")
    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    location: str = Field(default="us-central1", description="Cloud Run region (e.g. us-central1, europe-west1).")
    job_name: str = Field(description="Cloud Run job name (no `projects/.../jobs/` prefix).")

    # Per-execution overrides
    container_overrides: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "List of {name, args?, env?} per container override. "
            "name is the container name; env is [{name, value}, ...]; args is "
            "the container's command-line argv override."
        ),
    )
    task_count: Optional[int] = Field(default=None, description="Override task_count for this execution.")
    task_timeout_seconds: Optional[int] = Field(default=None, description="Override per-task timeout.")

    # Polling
    poll_interval_seconds: float = Field(default=5.0)
    timeout_seconds: float = Field(default=3600.0)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        job_name = self.job_name
        container_overrides = self.container_overrides or []
        task_count = self.task_count
        task_timeout_seconds = self.task_timeout_seconds
        poll_interval = self.poll_interval_seconds
        timeout_seconds = self.timeout_seconds

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Cloud Run job: projects/{project_id}/locations/{location}/jobs/{job_name}",
            group_name=self.group_name,
            kinds={"google", "cloud-run"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from google.cloud import run_v2
                from google.oauth2 import service_account
                from google.api_core.exceptions import NotFound, PermissionDenied
            except ImportError:
                raise ImportError("pip install google-cloud-run google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = run_v2.JobsClient(credentials=sa_creds)
            job_path = client.job_path(project_id, location, job_name)

            # Build Overrides if needed.
            overrides = None
            if container_overrides or task_count is not None or task_timeout_seconds is not None:
                co_list = []
                for co in container_overrides:
                    co_kwargs: Dict[str, Any] = {}
                    if "name" in co:
                        co_kwargs["name"] = co["name"]
                    if "args" in co:
                        co_kwargs["args"] = list(co["args"])
                    if "env" in co:
                        co_kwargs["env"] = [run_v2.EnvVar(name=e["name"], value=str(e.get("value", ""))) for e in co["env"]]
                    co_list.append(run_v2.RunJobRequest.Overrides.ContainerOverride(**co_kwargs))
                overrides_kwargs: Dict[str, Any] = {"container_overrides": co_list}
                if task_count is not None:
                    overrides_kwargs["task_count"] = task_count
                if task_timeout_seconds is not None:
                    from google.protobuf.duration_pb2 import Duration
                    d = Duration()
                    d.FromSeconds(task_timeout_seconds)
                    overrides_kwargs["timeout"] = d
                overrides = run_v2.RunJobRequest.Overrides(**overrides_kwargs)

            # Submit the run.
            request = run_v2.RunJobRequest(name=job_path, overrides=overrides) if overrides else run_v2.RunJobRequest(name=job_path)
            context.log.info(f"Triggering Cloud Run job {job_path}")
            try:
                op = client.run_job(request=request)
            except NotFound:
                context.log.error(
                    f"Cloud Run job {job_path!r} not found. Check job_name + location, or "
                    f"create the job first via `gcloud run jobs create {job_name} ...`"
                )
                raise
            except PermissionDenied:
                sa = creds_dict.get("client_email", "<sa>")
                context.log.error(
                    f"403 PERMISSION_DENIED. SA {sa!r} needs roles/run.invoker on the job, "
                    f"AND roles/iam.serviceAccountUser on the runtime SA the job uses."
                )
                raise

            # Wait on the long-running operation. The op completes when the
            # Execution resource is fully resolved; client wait_op + result
            # blocks until done.
            started_at = time.monotonic()
            while not op.done():
                if time.monotonic() - started_at > timeout_seconds:
                    raise TimeoutError(
                        f"Cloud Run job {job_path!r} did not finish within {timeout_seconds}s."
                    )
                time.sleep(poll_interval)
            execution = op.result()

            # Inspect the resulting Execution.
            row: Dict[str, Any] = {
                "execution_name":     execution.name,
                "job":                job_path,
                "succeeded_count":    int(getattr(execution, "succeeded_count", 0) or 0),
                "failed_count":       int(getattr(execution, "failed_count", 0) or 0),
                "running_count":      int(getattr(execution, "running_count", 0) or 0),
                "task_count":         int(getattr(execution, "task_count", 0) or 0),
                "completion_time":    execution.completion_time.isoformat() if getattr(execution, "completion_time", None) else None,
                "start_time":         execution.start_time.isoformat() if getattr(execution, "start_time", None) else None,
                "log_uri":            getattr(execution, "log_uri", None),
            }
            df = pd.DataFrame([row])

            md = {
                "execution_name":   MetadataValue.text(row["execution_name"]),
                "succeeded":        MetadataValue.int(row["succeeded_count"]),
                "failed":           MetadataValue.int(row["failed_count"]),
                "task_count":       MetadataValue.int(row["task_count"]),
            }
            if row["log_uri"]:
                md["log_uri"] = MetadataValue.url(row["log_uri"])

            # Fail-fast if any tasks failed.
            if row["failed_count"] > 0 and row["succeeded_count"] == 0:
                raise RuntimeError(
                    f"Cloud Run job {job_name!r} execution failed "
                    f"({row['failed_count']}/{row['task_count']} tasks). "
                    f"Logs: {row['log_uri']}"
                )

            return Output(value=df, metadata=md)

        return Definitions(assets=[_asset])
