"""Qlik Replicate Task Metrics Ingestion Component.

Materialize per-task metrics from Qlik Replicate (via Enterprise Manager REST API)
as a DataFrame asset. Each materialization polls the API and emits one row per
task with its current state, latency, rows-replicated counter, and error count.

Common use cases:
  - Dashboard "how is CDC doing right now" — latency and throughput per source
  - Alerting inputs — feed error_count into downstream monitoring assets
  - SLO tracking — measure CDC lag against source-of-truth commit times
"""
from typing import List, Optional

import dagster as dg
from pydantic import Field


class QlikReplicateTaskMetricsIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit a DataFrame of per-task metrics from Qlik Replicate.

    One row per task per materialization. Columns:
      - server, task, state, stage
      - full_load_progress_percent
      - cdc_latency_seconds
      - source_transactions_committed, target_transactions_applied
      - source_records_processed, target_records_applied
      - error_count
      - last_state_change_time
      - polled_at

    Example:

        ```yaml
        type: dagster_community_components.QlikReplicateTaskMetricsIngestionComponent
        attributes:
          asset_key: qlik_task_metrics
          servers: [prod-replicate-01, prod-replicate-02]
          group_name: qlik_observability
          resource_key: qlik_replicate_resource
        ```

    Or scoped to a specific task list:

        ```yaml
        attributes:
          asset_key: orders_cdc_metrics
          servers: [prod-replicate-01]
          tasks: [orders_sqlserver_to_snowflake, customers_db2_to_snowflake]
          resource_key: qlik_replicate_resource
        ```
    """

    asset_key: str = Field(description="Asset key for the emitted DataFrame.")
    servers: List[str] = Field(description="Enterprise Manager server names to poll.")
    tasks: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional whitelist of task names. When set, only these tasks (across "
            "all `servers`) are included. Leave unset to include every task on "
            "every server."
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    resource_key: str = Field(default="qlik_replicate_resource", description="Resource key to look up.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.asset(
            name=_self.asset_key,
            group_name=_self.group_name,
            required_resource_keys=required_resource_keys,
            compute_kind="qlik_replicate",
        )
        def _the_asset(context: dg.AssetExecutionContext):
            import time
            try:
                import pandas as pd
                import requests
            except ImportError as e:
                raise Exception("pandas or requests library not installed") from e

            resource = getattr(context.resources, _self.resource_key)
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

            wanted_tasks = set(_self.tasks) if _self.tasks else None
            rows: list[dict] = []
            polled_at = time.time()

            for server_name in _self.servers:
                try:
                    lr = session.get(
                        f"{resource.server_url(server_name)}/tasks",
                        headers=headers,
                        timeout=60,
                    )
                    lr.raise_for_status()
                    task_list = (lr.json() or {}).get("taskList") or lr.json() or []
                except Exception as e:
                    context.log.warning(f"Failed to list tasks for server={server_name}: {e}")
                    continue

                if isinstance(task_list, dict):
                    task_list = task_list.get("tasks", [])

                for task_entry in task_list:
                    task_name = task_entry.get("name") if isinstance(task_entry, dict) else str(task_entry)
                    if not task_name:
                        continue
                    if wanted_tasks is not None and task_name not in wanted_tasks:
                        continue
                    try:
                        dr = session.get(
                            resource.task_url(server_name, task_name),
                            headers=headers,
                            timeout=30,
                        )
                        dr.raise_for_status()
                        detail = dr.json() or {}
                    except Exception as e:
                        context.log.warning(f"Detail fetch failed for {server_name}/{task_name}: {e}")
                        continue

                    task_obj = detail.get("task") or detail
                    stats = task_obj.get("cdc_event_counters") or task_obj.get("statistics") or {}
                    fl = task_obj.get("full_load_counters") or {}

                    rows.append({
                        "server": server_name,
                        "task": task_name,
                        "state": task_obj.get("state"),
                        "stage": task_obj.get("stage") or task_obj.get("current_stage"),
                        "full_load_progress_percent": fl.get("completed_percent"),
                        "cdc_latency_seconds": task_obj.get("cdc_latency_seconds") or stats.get("source_latency"),
                        "source_transactions_committed": stats.get("source_commits") or stats.get("source_transactions"),
                        "target_transactions_applied": stats.get("target_commits") or stats.get("target_transactions"),
                        "source_records_processed": stats.get("source_records") or fl.get("source_records"),
                        "target_records_applied": stats.get("target_records") or fl.get("target_records"),
                        "error_count": task_obj.get("error_count") or stats.get("errors") or 0,
                        "last_state_change_time": task_obj.get("last_state_change_time"),
                        "polled_at": polled_at,
                    })

            df = pd.DataFrame(rows)
            context.add_output_metadata({
                "row_count": len(df),
                "servers_polled": len(_self.servers),
                "wanted_tasks_filter": len(wanted_tasks) if wanted_tasks else 0,
            })
            return df

        return dg.Definitions(assets=[_the_asset])
