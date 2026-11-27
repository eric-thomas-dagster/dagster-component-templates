"""DatabricksJobsResource component.

Reusable Databricks job run_now-and-wait logic -- an Airflow Operator is
reusable action code, closer to a Dagster `ConfigurableResource` than an
asset. Call `.run_now_and_wait(...)` from your own `@dg.asset`/`@dg.op` body
when you want this wired into your pipeline; this component only registers
the resource, it does not materialize anything on its own. See
`resources/http_resource` for the same pattern.

Real databricks-sdk API, verified directly against the installed
`databricks.sdk.service.jobs` module: `JobsAPI.run_now(job_id=..., ...) ->
Wait[Run]`, whose `.response` is a `RunNowResponse(run_id=...)`, followed by
`JobsAPI.wait_get_run_job_terminated_or_skipped(run_id=...) -> Run`. `Run.state`
is a `RunState(life_cycle_state, result_state, state_message)`; success is
`result_state == RunResultState.SUCCESS`.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class DatabricksJobsResource(dg.ConfigurableResource):
    """Trigger an existing Databricks job by `job_id` via `run_now` and wait for completion."""

    workspace_host: str = Field(description="Databricks workspace URL, e.g. 'https://dbc-abc123.cloud.databricks.com'.")
    workspace_token_env_var: str = Field(description="Env var holding a Databricks personal access token (or workspace-scoped token).")

    def _client(self):
        from databricks.sdk import WorkspaceClient

        token = dg.EnvVar(self.workspace_token_env_var).get_value()
        if token is None:
            raise KeyError(
                f"Environment variable {self.workspace_token_env_var!r} is not set. Set it in the "
                "environment of the Dagster code location (or point workspace_token_env_var at one that is)."
            )
        return WorkspaceClient(host=self.workspace_host, token=token)

    def run_now_and_wait(
        self,
        job_id: int,
        job_parameters: Optional[Dict[str, Any]] = None,
        notebook_params: Optional[Dict[str, str]] = None,
        python_params: Optional[List[str]] = None,
        idempotency_token: Optional[str] = None,
        poll_timeout_seconds: int = 3600,
        fail_on_non_success: bool = True,
    ) -> Dict[str, Any]:
        """Trigger `job_id` via `run_now`, wait for a terminal state, return
        {run_id, run_page_url, life_cycle_state, result_state, state_message}.
        Raises `dagster.Failure` on a non-SUCCESS `result_state` unless
        fail_on_non_success=False."""
        from datetime import timedelta

        client = self._client()

        run_now_kwargs: Dict[str, Any] = {"job_id": job_id}
        if job_parameters:
            run_now_kwargs["job_parameters"] = {str(k): str(v) for k, v in job_parameters.items()}
        if notebook_params:
            run_now_kwargs["notebook_params"] = {str(k): str(v) for k, v in notebook_params.items()}
        if python_params:
            run_now_kwargs["python_params"] = [str(v) for v in python_params]
        if idempotency_token:
            run_now_kwargs["idempotency_token"] = idempotency_token

        waiter = client.jobs.run_now(**run_now_kwargs)
        run_id = waiter.response.run_id

        final_run = client.jobs.wait_get_run_job_terminated_or_skipped(
            run_id=run_id, timeout=timedelta(seconds=poll_timeout_seconds)
        )

        state = final_run.state
        life_cycle_state = str(state.life_cycle_state) if state and state.life_cycle_state else "UNKNOWN"
        result_state = str(state.result_state) if state and state.result_state else None
        state_message = state.state_message if state else None

        result: Dict[str, Any] = {
            "run_id": run_id,
            "run_page_url": final_run.run_page_url,
            "life_cycle_state": life_cycle_state,
            "result_state": result_state,
            "state_message": state_message,
        }

        if fail_on_non_success and result_state not in (None, "RunResultState.SUCCESS", "SUCCESS"):
            raise dg.Failure(
                f"Databricks job {job_id} (run_id={run_id}) finished with "
                f"result_state={result_state!r}: {state_message}",
                metadata={"run_page_url": dg.MetadataValue.url(result["run_page_url"])} if result["run_page_url"] else None,
            )

        return result


class DatabricksJobsResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a `DatabricksJobsResource` for use by any op/asset in this code location.

    Call `context.resources.<resource_key>.run_now_and_wait(job_id=...)` from your own
    asset/op body -- this is the reusable Airflow-`DatabricksRunNowOperator`-equivalent
    action. Wrap it in your own `@dg.asset` only when you want the trigger itself to
    participate in the Dagster asset graph (deps, freshness, automation conditions).
    Distinct from `integrations/databricks_asset_bundle` (submits ad-hoc tasks defined
    inline from a `databricks.yml` bundle) and `integrations/databricks_clean_room_notebook_run`
    (ad-hoc Clean Rooms notebook submission) -- neither of those calls `run_now` against a
    pre-existing job.
    """

    resource_key: str = Field(default="databricks_jobs_resource", description="Key used to register this resource. Other components/assets reference it via resource_key.")
    workspace_host: str = Field(description="Databricks workspace URL, e.g. 'https://dbc-abc123.cloud.databricks.com'.")
    workspace_token_env_var: str = Field(description="Env var holding a Databricks personal access token (or workspace-scoped token).")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Databricks Jobs Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = DatabricksJobsResource(
            workspace_host=self.workspace_host,
            workspace_token_env_var=self.workspace_token_env_var,
        )
        return dg.Definitions(resources={self.resource_key: resource})
