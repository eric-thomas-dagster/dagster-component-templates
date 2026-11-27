"""DatabricksRunNowAssetComponent.

Triggers an EXISTING Databricks job by `job_id` via `jobs.run_now(...)` and
polls to completion -- the Airflow `DatabricksRunNowOperator` equivalent.
Distinct from `integrations/databricks_asset_bundle` (submits ad-hoc tasks
defined inline from a `databricks.yml` bundle) and
`integrations/databricks_clean_room_notebook_run` (ad-hoc Clean Rooms
notebook submission) -- neither of those calls `run_now` against a
pre-existing job.

Real databricks-sdk API, verified directly against the installed
`databricks.sdk.service.jobs` module: `JobsAPI.run_now(job_id=..., ...) ->
Wait[Run]`, whose `.response` is a `RunNowResponse(run_id=...)`, followed by
`JobsAPI.wait_get_run_job_terminated_or_skipped(run_id=...) -> Run` (same
two-step submit-then-wait shape `databricks_asset_bundle`'s own
`DatabricksWorkspaceResource.submit_and_poll` already uses). `Run.state` is a
`RunState(life_cycle_state, result_state, state_message)`; success is
`result_state == RunResultState.SUCCESS`.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


def _require_env(name: str) -> str:
    """Read an env var via dagster.EnvVar, with an actionable message when it is unset."""
    from dagster import EnvVar

    value = EnvVar(name).get_value()
    if value is None:
        raise KeyError(
            f"Environment variable {name!r} is not set. Set it in the environment of the "
            "Dagster code location (or point the *_env_var field at one that is)."
        )
    return value


def _make_databricks_client(host: str, token: str):
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(host=host, token=token)


def _run_now_and_wait(
    client,
    job_id: int,
    job_parameters: Optional[Dict[str, Any]],
    notebook_params: Optional[Dict[str, str]],
    python_params: Optional[List[str]],
    idempotency_token: Optional[str],
    poll_timeout_seconds: int,
) -> Dict[str, Any]:
    """Trigger `job_id` via `run_now`, wait for a terminal state, return a plain dict.
    Never raises on a non-success result_state -- that's the caller's call
    (fail_on_non_success)."""
    from datetime import timedelta

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

    return {
        "run_id": run_id,
        "run_page_url": final_run.run_page_url,
        "life_cycle_state": life_cycle_state,
        "result_state": result_state,
        "state_message": state_message,
    }


class DatabricksRunNowAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger an existing Databricks job by `job_id` and wait for completion.

    Calls the real `jobs.run_now` API against a job that already exists in
    the workspace (created via the Databricks UI, Terraform, or a bundle
    deploy) -- this is NOT for ad-hoc task submission; see
    `integrations/databricks_asset_bundle` for that.

    By default a non-`SUCCESS` `result_state` (`FAILED`, `TIMEDOUT`,
    `CANCELED`, etc.) fails the Dagster run. Set `fail_on_non_success: false`
    to materialize anyway with the result state surfaced in metadata.
    """

    asset_name: str = Field(description="Dagster asset key.")
    job_id: int = Field(description="Existing Databricks job id to trigger via run_now.")
    workspace_host: str = Field(description="Databricks workspace URL, e.g. 'https://dbc-abc123.cloud.databricks.com'.")
    workspace_token_env_var: str = Field(description="Env var holding a Databricks personal access token (or workspace-scoped token).")

    job_parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Named job parameters (the modern run_now API) -- values are coerced to strings. Mutually independent of notebook_params/python_params.",
    )
    notebook_params: Optional[Dict[str, str]] = Field(default=None, description="Notebook widget parameters, for jobs whose task is a notebook_task.")
    python_params: Optional[List[str]] = Field(default=None, description="Positional command-line parameters, for jobs whose task is a python/spark_python_task.")
    idempotency_token: Optional[str] = Field(default=None, description="An idempotent run_now request with the same token and job_id returns the already-running/completed run instead of starting a new one.")

    poll_timeout_seconds: int = Field(default=3600, ge=1, description="Maximum time to wait for the job run to reach a terminal state before this asset itself raises a timeout error.")
    fail_on_non_success: bool = Field(default=True, description="Raise if the job run's result_state isn't SUCCESS. False materializes anyway with the result state in metadata.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['databricks'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Databricks Run Now', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        _inferred_kinds = list(self.kinds or ["databricks"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Trigger Databricks job {self.job_id} via run_now and wait for completion.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
        )
        def _databricks_run_now_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            token = _require_env(self.workspace_token_env_var)
            client = _make_databricks_client(self.workspace_host, token)

            context.log.info(f"databricks_run_now_asset: triggering job_id={self.job_id} via run_now")
            result = _run_now_and_wait(
                client, self.job_id, self.job_parameters, self.notebook_params,
                self.python_params, self.idempotency_token, self.poll_timeout_seconds,
            )
            context.log.info(
                f"databricks_run_now_asset: run_id={result['run_id']} finished "
                f"(life_cycle_state={result['life_cycle_state']}, result_state={result['result_state']})"
            )

            if self.fail_on_non_success and result["result_state"] not in (None, "RunResultState.SUCCESS", "SUCCESS"):
                raise dg.Failure(
                    f"Databricks job {self.job_id} (run_id={result['run_id']}) finished with "
                    f"result_state={result['result_state']!r}: {result['state_message']}",
                    metadata={"run_page_url": dg.MetadataValue.url(result["run_page_url"])} if result["run_page_url"] else None,
                )

            metadata: Dict[str, Any] = {
                "job_id": dg.MetadataValue.int(self.job_id),
                "run_id": dg.MetadataValue.int(result["run_id"]),
                "life_cycle_state": dg.MetadataValue.text(result["life_cycle_state"]),
            }
            if result["result_state"] is not None:
                metadata["result_state"] = dg.MetadataValue.text(result["result_state"])
            if result["run_page_url"]:
                metadata["run_page_url"] = dg.MetadataValue.url(result["run_page_url"])
            if result["state_message"]:
                metadata["state_message"] = dg.MetadataValue.text(result["state_message"])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_databricks_run_now_asset])
