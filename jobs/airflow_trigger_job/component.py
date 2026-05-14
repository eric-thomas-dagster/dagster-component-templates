"""AirflowTriggerJobComponent.

Trigger an Airflow DAG run via the Airflow REST API as a Dagster job.
"""

import os
from typing import Optional

import dagster as dg
from pydantic import Field


class AirflowTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger an Airflow DAG run via the Airflow REST API as a Dagster job."""

    job_name: str = Field(description="Dagster job name")
    schedule: Optional[str] = Field(default=None, description="Cron schedule (None = no schedule)")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags")

    airflow_url: str = Field(description="Airflow web URL (e.g. 'https://airflow.acme.com')")
    dag_id: str = Field(description="DAG to trigger")
    username_env: Optional[str] = Field(default="AIRFLOW_USER")
    password_env: Optional[str] = Field(default="AIRFLOW_PASSWORD")
    conf: Optional[dict] = Field(default=None, description="DAG run conf payload")
    wait_for_completion: bool = Field(default=False)
    poll_interval_seconds: int = Field(default=15)
    timeout_seconds: int = Field(default=3600)


    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.op(name=f"{self.job_name}_op")
        def _the_op(context: dg.OpExecutionContext):
            self = _self  # so body can use `self.<field>`
            import requests, os, time, datetime as dt
            auth = (os.environ[self.username_env], os.environ[self.password_env]) if self.username_env else None
            run_id = f"dagster_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
            body = {"dag_run_id": run_id}
            if self.conf:
                body["conf"] = self.conf
            r = requests.post(f"{self.airflow_url}/api/v1/dags/{self.dag_id}/dagRuns", json=body, auth=auth, timeout=60)
            if r.status_code >= 300:
                raise Exception(f"airflow trigger failed: {r.status_code} {r.text[:200]}")
            context.log.info(f"triggered {self.dag_id}/{run_id}")
            if self.wait_for_completion:
                deadline = time.time() + self.timeout_seconds
                while time.time() < deadline:
                    pr = requests.get(f"{self.airflow_url}/api/v1/dags/{self.dag_id}/dagRuns/{run_id}", auth=auth, timeout=30)
                    state = pr.json().get("state")
                    context.log.info(f"poll: state={state}")
                    if state in ("success", "failed"):
                        if state == "failed":
                            raise Exception(f"airflow run {run_id} failed")
                        return
                    time.sleep(self.poll_interval_seconds)
                raise Exception(f"timed out waiting for {run_id}")

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule, job=_the_job,
                default_status=dg.DefaultScheduleStatus.STOPPED if self.default_status.upper() == "STOPPED" else dg.DefaultScheduleStatus.RUNNING,
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
