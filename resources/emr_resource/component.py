"""EmrResourceComponent.

Register a reusable `EmrResource` exposing `submit_step_and_wait(...)` -- the
Airflow `EmrAddStepsOperator` + `EmrStepSensor` equivalent as an injectable
resource method, callable from any op/asset you write, rather than baked
into a single-purpose asset component.

Real boto3 EMR API, verified against the botocore reference:
  - `add_job_flow_steps(JobFlowId=..., Steps=[{'Name', 'ActionOnFailure',
    'HadoopJarStep': {'Jar', 'MainClass', 'Args', 'Properties'}}])` ->
    `{'StepIds': [...]}`.
  - `describe_step(ClusterId=..., StepId=...)` ->
    `{'Step': {'Status': {'State', 'FailureDetails': {'Reason', 'Message',
    'LogFile'}, 'Timeline': {...}}}}`. States: PENDING, CANCEL_PENDING,
    RUNNING, COMPLETED, CANCELLED, FAILED, INTERRUPTED.
  - `list_clusters(ClusterStates=[...])` -> `{'Clusters': [{'Id', 'Name',
    ...}]}`, used to resolve `cluster_name` to a cluster id when `cluster_id`
    isn't given directly.

`jar` defaults to EMR's bundled `command-runner.jar`, the standard way to run
an arbitrary shell command (including `spark-submit`) as a step without a
custom JAR -- `args` then starts with the command itself, e.g.
`["spark-submit", "--deploy-mode", "cluster", "s3://bucket/job.py"]`.
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

_SUCCESS_STATES = {"COMPLETED"}
_FAILURE_STATES = {"CANCELLED", "FAILED", "INTERRUPTED"}


class _NullLog:
    def info(self, msg):
        pass


def _resolve_cluster_id(client, cluster_id: Optional[str], cluster_name: Optional[str]) -> str:
    if cluster_id:
        return cluster_id
    if not cluster_name:
        raise ValueError("Set either cluster_id or cluster_name.")
    matches = []
    paginator = client.get_paginator("list_clusters")
    for page in paginator.paginate(ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]):
        matches.extend(c for c in page["Clusters"] if c["Name"] == cluster_name)
    if not matches:
        raise RuntimeError(f"No active EMR cluster found with name {cluster_name!r}.")
    if len(matches) > 1:
        ids = ", ".join(c["Id"] for c in matches)
        raise RuntimeError(f"Multiple active EMR clusters named {cluster_name!r}: {ids}. Use cluster_id instead.")
    return matches[0]["Id"]


def _add_step(
    client, cluster_id: str, step_name: str, action_on_failure: str,
    jar: str, main_class: Optional[str], args: List[str], properties: Optional[Dict[str, str]],
) -> str:
    hadoop_jar_step: Dict[str, Any] = {"Jar": jar, "Args": list(args)}
    if main_class:
        hadoop_jar_step["MainClass"] = main_class
    if properties:
        hadoop_jar_step["Properties"] = [{"Key": k, "Value": v} for k, v in properties.items()]

    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{"Name": step_name, "ActionOnFailure": action_on_failure, "HadoopJarStep": hadoop_jar_step}],
    )
    return response["StepIds"][0]


def _describe_step(client, cluster_id: str, step_id: str) -> Dict[str, Any]:
    return client.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]


def _poll_until_terminal(client, cluster_id: str, step_id: str, log, poll_interval_seconds: float, timeout_seconds: int) -> Dict[str, Any]:
    import time
    deadline = time.monotonic() + timeout_seconds
    step = _describe_step(client, cluster_id, step_id)
    state = step["Status"]["State"]
    while state not in _SUCCESS_STATES and state not in _FAILURE_STATES:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"EMR step {step_id} on cluster {cluster_id} did not reach a terminal state within {timeout_seconds}s (last state={state!r}).")
        log.info(f"emr_resource: step {step_id} state={state!r}")
        time.sleep(poll_interval_seconds)
        step = _describe_step(client, cluster_id, step_id)
        state = step["Status"]["State"]
    return step


class EmrResource(dg.ConfigurableResource):
    """Thin wrapper around boto3's EMR client exposing submit_step_and_wait(...).

    Injectable into any op or asset you write -- not tied to a single
    pre-built asset shape.
    """

    region_name: Optional[str] = None
    profile_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None

    def _client(self):
        import boto3
        kwargs: Dict[str, Any] = {}
        if self.profile_name:
            kwargs["profile_name"] = self.profile_name
        if self.aws_access_key_id:
            kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        if self.aws_session_token:
            kwargs["aws_session_token"] = self.aws_session_token
        if self.region_name:
            kwargs["region_name"] = self.region_name
        session = boto3.Session(**kwargs)
        return session.client("emr")

    def submit_step_and_wait(
        self,
        cluster_id: Optional[str] = None,
        cluster_name: Optional[str] = None,
        step_name: str = "dagster-emr-step",
        action_on_failure: str = "CONTINUE",
        jar: str = "command-runner.jar",
        main_class: Optional[str] = None,
        args: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
        wait_for_completion: bool = True,
        poll_interval_seconds: float = 15.0,
        timeout_seconds: int = 3600,
        fail_on_step_failure: bool = True,
        log=None,
    ) -> Dict[str, Any]:
        """Add a step to an EMR cluster and (by default) poll it to a terminal state.

        Returns {"cluster_id", "step_id"} immediately when
        wait_for_completion=False, or {"cluster_id", "step_id", "state"} once
        terminal. Raises RuntimeError on a CANCELLED/FAILED/INTERRUPTED step
        unless fail_on_step_failure=False.
        """
        if not cluster_id and not cluster_name:
            raise ValueError("Set either cluster_id or cluster_name.")
        log = log or _NullLog()
        client = self._client()
        resolved_cluster_id = _resolve_cluster_id(client, cluster_id, cluster_name)

        step_id = _add_step(client, resolved_cluster_id, step_name, action_on_failure, jar, main_class, args or [], properties)
        log.info(f"emr_resource: added step_id={step_id} to cluster={resolved_cluster_id}")

        result: Dict[str, Any] = {"cluster_id": resolved_cluster_id, "step_id": step_id}
        if not wait_for_completion:
            return result

        step = _poll_until_terminal(client, resolved_cluster_id, step_id, log, poll_interval_seconds, timeout_seconds)
        state = step["Status"]["State"]
        result["state"] = state

        if state in _FAILURE_STATES and fail_on_step_failure:
            failure = step["Status"].get("FailureDetails") or {}
            raise RuntimeError(
                f"EMR step {step_id} on cluster {resolved_cluster_id} ended {state!r} instead of 'COMPLETED'. "
                f"Reason: {failure.get('Reason')!r} Message: {failure.get('Message')!r} LogFile: {failure.get('LogFile')!r}"
            )
        return result


class EmrResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an EmrResource (submit-step-and-wait on an AWS EMR cluster) for use by other components."""

    resource_key: str = Field(default="emr_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: Optional[str] = Field(default=None, description="AWS region, e.g. 'us-east-1'.")
    profile_name: Optional[str] = Field(default=None, description="Named AWS CLI/SDK profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.).")
    aws_access_key_id_env_var: Optional[str] = Field(default=None, description="Env var holding the AWS access key ID.")
    aws_secret_access_key_env_var: Optional[str] = Field(default=None, description="Env var holding the AWS secret access key.")
    aws_session_token_env_var: Optional[str] = Field(default=None, description="Env var holding a temporary AWS session token, paired with the access key/secret above.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='EMR Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = EmrResource(
            region_name=self.region_name,
            profile_name=self.profile_name,
            aws_access_key_id=dg.EnvVar(self.aws_access_key_id_env_var) if self.aws_access_key_id_env_var else None,
            aws_secret_access_key=dg.EnvVar(self.aws_secret_access_key_env_var) if self.aws_secret_access_key_env_var else None,
            aws_session_token=dg.EnvVar(self.aws_session_token_env_var) if self.aws_session_token_env_var else None,
        )
        return dg.Definitions(resources={self.resource_key: resource})
