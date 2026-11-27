"""Athena Resource component.

Reusable Amazon Athena query logic -- the Airflow `AthenaOperator`'s real
action, extracted as a `ConfigurableResource` so any op or asset in a user's
own code can call it directly, instead of being locked inside a
single-purpose asset. Real boto3 API, verified against the botocore
reference: `athena.start_query_execution(...)` -> `QueryExecutionId`, then
`athena.get_query_execution(QueryExecutionId=...)` ->
`QueryExecution.Status.State` in `QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED`.
"""
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class AthenaResource(dg.ConfigurableResource):
    """Run a SQL query against Amazon Athena and wait for a terminal state."""

    region_name: str
    aws_profile: Optional[str] = None

    def _client(self):
        import boto3
        session = boto3.Session(profile_name=self.aws_profile, region_name=self.region_name)
        return session.client("athena")

    def run_query_and_wait(
        self,
        query: str,
        database: Optional[str] = None,
        output_location: Optional[str] = None,
        workgroup: Optional[str] = None,
        poll_interval_seconds: float = 5.0,
        timeout_seconds: Optional[float] = 1800.0,
        log=None,
    ) -> Dict[str, Any]:
        """Start the query, poll to a terminal state, return a plain dict. Raises on FAILED/CANCELLED or timeout."""
        client = self._client()
        kwargs: Dict[str, Any] = {"QueryString": query}
        if database:
            kwargs["QueryExecutionContext"] = {"Database": database}
        if output_location:
            kwargs["ResultConfiguration"] = {"OutputLocation": output_location}
        if workgroup:
            kwargs["WorkGroup"] = workgroup

        start = client.start_query_execution(**kwargs)
        query_execution_id = start["QueryExecutionId"]
        if log:
            log.info(f"athena_resource: started QueryExecutionId={query_execution_id!r}")

        deadline = (time.monotonic() + timeout_seconds) if timeout_seconds else None
        while True:
            resp = client.get_query_execution(QueryExecutionId=query_execution_id)
            execution = resp["QueryExecution"]
            status = execution["Status"]
            state = status["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                statistics = execution.get("Statistics", {}) or {}
                result_config = execution.get("ResultConfiguration", {}) or {}
                result = {
                    "query_execution_id": query_execution_id,
                    "state": state,
                    "state_change_reason": status.get("StateChangeReason"),
                    "data_scanned_bytes": statistics.get("DataScannedInBytes"),
                    "engine_execution_time_ms": statistics.get("EngineExecutionTimeInMillis"),
                    "output_location": result_config.get("OutputLocation"),
                }
                if state != "SUCCEEDED":
                    raise RuntimeError(
                        f"Athena query {query_execution_id!r} ended in state {state!r}: "
                        f"{result['state_change_reason']}"
                    )
                return result
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Athena query {query_execution_id!r} did not reach a terminal state within "
                    f"{timeout_seconds}s (last state: {state!r})."
                )
            time.sleep(poll_interval_seconds)


class AthenaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register an AthenaResource for use by other components, ops, or assets.

    This is the Airflow `AthenaOperator`'s real action -- `start_query_execution`
    + a poll loop to a terminal state -- as a reusable resource rather than a
    dedicated asset. Call `run_query_and_wait(...)` from your own op or asset
    to run a query as a step in your pipeline; this component does not itself
    materialize anything.
    """

    resource_key: str = Field(default="athena_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: str = Field(description="AWS region, e.g. 'us-east-1'.")
    aws_profile: Optional[str] = Field(default=None, description="Named AWS profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.).")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Athena Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = AthenaResource(region_name=self.region_name, aws_profile=self.aws_profile)
        return dg.Definitions(resources={self.resource_key: resource})
