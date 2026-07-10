"""QlikReplicateTaskTriggerJobComponent.

Trigger a Qlik Replicate task (start / resume / stop / reload) via the
Qlik Enterprise Manager REST API. Pair with `qlik_replicate_resource`
so credentials + base URL are centralized.
"""
import time
from typing import Optional

import dagster as dg
from pydantic import Field


class QlikReplicateTaskTriggerJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Qlik Replicate task via Enterprise Manager REST API.

    Actions supported (via the `action` field):
      - `run` — start / resume the task (default option: RESUME_PROCESSING)
      - `stop` — stop the task
      - `reload` — full reload (drops target, recreates, initial load)

    Example:

        ```yaml
        type: dagster_community_components.QlikReplicateTaskTriggerJobComponent
        attributes:
          job_name: reload_orders_cdc
          server: prod-replicate-01
          task: orders_sqlserver_to_snowflake
          action: reload
          resource_key: qlik_replicate_resource
          wait_for_completion: true
          poll_interval_seconds: 30
          timeout_seconds: 1800
          schedule: "0 3 * * *"
        ```
    """

    job_name: str = Field(description="Dagster job name")
    server: str = Field(description="Name of the Replicate server as registered in Enterprise Manager.")
    task: str = Field(description="Name of the Replicate task to control.")
    action: str = Field(default="run", description="run | stop | reload")
    run_option: str = Field(
        default="RESUME_PROCESSING",
        description=(
            "For action=run: option flag. Common values: RESUME_PROCESSING, "
            "RELOAD_TARGET, RESUME_PROCESSING_FROM_TIMESTAMP, RECOVER."
        ),
    )
    wait_for_completion: bool = Field(
        default=False,
        description=(
            "When true, the op polls until the task reaches a terminal state "
            "(STOPPED / ERROR). Useful for one-shot migrations. For continuous "
            "CDC tasks, leave false — CDC never 'completes'."
        ),
    )
    poll_interval_seconds: int = Field(default=30, description="Seconds between status polls when wait_for_completion=true.")
    timeout_seconds: int = Field(default=1800, description="Give up polling after this many seconds. Raises on timeout.")

    resource_key: str = Field(default="qlik_replicate_resource", description="Resource key to look up.")
    schedule: Optional[str] = Field(default=None, description="Optional cron schedule (None = no schedule).")
    default_status: str = Field(default="STOPPED", description="STOPPED | RUNNING for the schedule default state.")
    tags: Optional[dict] = Field(default=None, description="Dagster job tags.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.op(name=f"{self.job_name}_op", required_resource_keys=required_resource_keys)
        def _the_op(context: dg.OpExecutionContext):
            self = _self
            try:
                import requests
            except ImportError as e:
                raise Exception("requests library not installed") from e

            resource = getattr(context.resources, self.resource_key)
            session = requests.Session()
            session.verify = resource.verify_ssl
            headers = resource.get_auth_headers()

            login_body = resource.login_body()
            if login_body is not None:
                r = session.post(
                    f"{resource.api_base}/login",
                    json=login_body,
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                    timeout=30,
                )
                if r.status_code >= 300:
                    raise Exception(f"Qlik EM login failed: {r.status_code} {r.text[:200]}")

            task_url = resource.task_url(self.server, self.task)
            action_url = f"{task_url}?action={self.action}"
            if self.action == "run":
                action_url += f"&option={self.run_option}"

            r = session.post(action_url, headers=headers, timeout=60)
            if r.status_code >= 300:
                raise Exception(
                    f"Qlik EM task action failed: {r.status_code} {r.text[:200]} "
                    f"(server={self.server} task={self.task} action={self.action})"
                )
            context.log.info(
                f"Qlik Replicate: {self.action} sent to task={self.task} on server={self.server}"
            )

            if self.wait_for_completion:
                deadline = time.time() + self.timeout_seconds
                terminal_states = {"STOPPED", "ERROR"}
                last_state = None
                while time.time() < deadline:
                    time.sleep(self.poll_interval_seconds)
                    sr = session.get(task_url, headers=headers, timeout=30)
                    if sr.status_code >= 300:
                        context.log.warning(f"status poll {sr.status_code}: {sr.text[:200]}")
                        continue
                    body = sr.json() or {}
                    state = (body.get("task", {}) or {}).get("state") or body.get("state")
                    if state and state != last_state:
                        context.log.info(f"task state: {state}")
                        last_state = state
                    if state and state.upper() in terminal_states:
                        if state.upper() == "ERROR":
                            raise Exception(f"Qlik Replicate task ended in ERROR state (server={self.server} task={self.task})")
                        return
                raise Exception(
                    f"Qlik Replicate task did not reach terminal state within {self.timeout_seconds}s "
                    f"(last state={last_state}, server={self.server}, task={self.task})"
                )

        @dg.job(name=self.job_name, tags=self.tags or None)
        def _the_job():
            _the_op()

        defs_kwargs: dict = {"jobs": [_the_job]}
        if self.schedule:
            sched = dg.ScheduleDefinition(
                name=f"{self.job_name}_schedule",
                cron_schedule=self.schedule,
                job=_the_job,
                default_status=(
                    dg.DefaultScheduleStatus.STOPPED
                    if self.default_status.upper() == "STOPPED"
                    else dg.DefaultScheduleStatus.RUNNING
                ),
            )
            defs_kwargs["schedules"] = [sched]
        return dg.Definitions(**defs_kwargs)
