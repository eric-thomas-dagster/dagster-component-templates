"""Redshift Data API Resource component.

Reusable Amazon Redshift Data API logic -- the Airflow `RedshiftDataOperator`'s
real action, extracted as a `ConfigurableResource` so any op or asset in a
user's own code can call it directly. Distinct from `redshift_resource`
(a JDBC-style connection resource) -- this one calls the `redshift-data`
boto3 client's `execute_statement`/`describe_statement` HTTP API, no JDBC
driver or persistent connection required.
"""
import time
from typing import Any, Dict, Optional

import dagster as dg
from pydantic import Field


class RedshiftDataApiResource(dg.ConfigurableResource):
    """Run a SQL statement against Redshift via the Data API and wait for a terminal state.

    Connection: set EITHER `cluster_identifier` (a provisioned cluster --
    pair with `db_user` for temporary IAM credentials, or `secret_arn` for
    Secrets Manager auth) OR `workgroup_name` (Redshift Serverless --
    `secret_arn` optional there, since Serverless can authenticate via the
    caller's own IAM identity).
    """

    region_name: str
    aws_profile: Optional[str] = None

    def _client(self):
        import boto3
        session = boto3.Session(profile_name=self.aws_profile, region_name=self.region_name)
        return session.client("redshift-data")

    def execute_and_wait(
        self,
        sql: str,
        database: str,
        cluster_identifier: Optional[str] = None,
        db_user: Optional[str] = None,
        workgroup_name: Optional[str] = None,
        secret_arn: Optional[str] = None,
        poll_interval_seconds: float = 2.0,
        timeout_seconds: Optional[float] = 900.0,
        log=None,
    ) -> Dict[str, Any]:
        """Execute the statement, poll to a terminal state, return a plain dict. Raises on FAILED/ABORTED or timeout."""
        if bool(cluster_identifier) == bool(workgroup_name):
            raise ValueError(
                "Set exactly one of cluster_identifier (provisioned cluster) or "
                "workgroup_name (Redshift Serverless), not both and not neither."
            )
        if cluster_identifier and not (db_user or secret_arn):
            raise ValueError(
                "cluster_identifier requires either db_user (temporary IAM credentials) "
                "or secret_arn (Secrets Manager auth)."
            )

        client = self._client()
        kwargs: Dict[str, Any] = {"Database": database, "Sql": sql}
        if cluster_identifier:
            kwargs["ClusterIdentifier"] = cluster_identifier
            if db_user:
                kwargs["DbUser"] = db_user
        if workgroup_name:
            kwargs["WorkgroupName"] = workgroup_name
        if secret_arn:
            kwargs["SecretArn"] = secret_arn

        start = client.execute_statement(**kwargs)
        statement_id = start["Id"]
        if log:
            log.info(f"redshift_data_api_resource: started statement Id={statement_id!r}")

        deadline = (time.monotonic() + timeout_seconds) if timeout_seconds is not None else None
        while True:
            resp = client.describe_statement(Id=statement_id)
            status = resp["Status"]
            if status in ("FINISHED", "FAILED", "ABORTED"):
                result = {
                    "statement_id": statement_id,
                    "status": status,
                    "error": resp.get("Error"),
                    "result_rows": resp.get("ResultRows"),
                    "result_size": resp.get("ResultSize"),
                    "duration_ns": resp.get("Duration"),
                    "redshift_query_id": resp.get("RedshiftQueryId"),
                    "has_result_set": resp.get("HasResultSet"),
                }
                if status != "FINISHED":
                    raise RuntimeError(
                        f"Redshift Data API statement {statement_id!r} ended in status {status!r}: "
                        f"{result['error']}"
                    )
                return result
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Redshift Data API statement {statement_id!r} did not reach a terminal state within "
                    f"{timeout_seconds}s (last status: {status!r})."
                )
            time.sleep(poll_interval_seconds)


class RedshiftDataApiResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a RedshiftDataApiResource for use by other components, ops, or assets.

    This is the Airflow `RedshiftDataOperator`'s real action -- `execute_statement`
    + a poll loop to a terminal state via the Redshift Data API -- as a reusable
    resource rather than a dedicated asset. Call `execute_and_wait(...)` from
    your own op or asset to run a statement as a step in your pipeline; this
    component does not itself materialize anything.

    Does NOT fetch result rows (avoids pulling potentially large/sensitive
    query results through Dagster) -- only `result_rows`/`result_size` counts
    are returned.
    """

    resource_key: str = Field(default="redshift_data_api_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    region_name: str = Field(description="AWS region, e.g. 'us-east-1'.")
    aws_profile: Optional[str] = Field(default=None, description="Named AWS profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.).")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Redshift Data Api Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = RedshiftDataApiResource(region_name=self.region_name, aws_profile=self.aws_profile)
        return dg.Definitions(resources={self.resource_key: resource})
