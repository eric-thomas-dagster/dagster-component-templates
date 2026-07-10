"""Qlik Compose Workflow Metrics Ingestion Component.

Emit a DataFrame with per-workflow metrics from Qlik Compose (state, last-run
duration, rows loaded, error count). One row per workflow per materialization.
Feed into BI dashboards or alert-input assets.
"""
from typing import List, Optional

import dagster as dg
from pydantic import Field


class QlikComposeWorkflowMetricsIngestionComponent(dg.Component, dg.Model, dg.Resolvable):
    """Emit per-workflow metrics from Qlik Compose.

    Example:

        ```yaml
        type: dagster_community_components.QlikComposeWorkflowMetricsIngestionComponent
        attributes:
          asset_key: qlik_compose_metrics
          projects: [FinanceDW, SalesDW]
          group_name: qlik_observability
          resource_key: qlik_compose_resource
        ```
    """

    asset_key: str = Field(description="Asset key for the emitted DataFrame.")
    projects: List[str] = Field(description="Compose projects to poll.")
    workflows: Optional[List[str]] = Field(default=None, description="Optional workflow whitelist across all listed projects.")
    group_name: Optional[str] = Field(default=None)
    resource_key: str = Field(default="qlik_compose_resource")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        required_resource_keys = {self.resource_key}

        @dg.asset(
            name=_self.asset_key,
            group_name=_self.group_name,
            required_resource_keys=required_resource_keys,
            compute_kind="qlik_compose",
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
                r = session.post(f"{resource.api_base}/login", json=login_body, timeout=30)
                if r.status_code >= 300:
                    raise Exception(f"Compose login failed: {r.status_code}")

            wanted_wfs = set(_self.workflows) if _self.workflows else None
            rows: list[dict] = []
            polled_at = time.time()

            for project in _self.projects:
                try:
                    lr = session.get(f"{resource.project_url(project)}/workflows", headers=headers, timeout=30)
                    lr.raise_for_status()
                    wf_list = (lr.json() or {}).get("workflows") or lr.json() or []
                except Exception as e:
                    context.log.warning(f"Failed to list workflows for {project}: {e}")
                    continue

                if isinstance(wf_list, dict):
                    wf_list = wf_list.get("value", [])

                for wf_entry in wf_list:
                    wf_name = wf_entry.get("name") if isinstance(wf_entry, dict) else str(wf_entry)
                    if not wf_name:
                        continue
                    if wanted_wfs is not None and wf_name not in wanted_wfs:
                        continue
                    try:
                        dr = session.get(resource.workflow_url(project, wf_name), headers=headers, timeout=30)
                        dr.raise_for_status()
                        detail = dr.json() or {}
                    except Exception as e:
                        context.log.warning(f"Detail fetch failed for {project}/{wf_name}: {e}")
                        continue

                    wf = detail.get("workflow") or detail
                    rows.append({
                        "project": project,
                        "workflow": wf_name,
                        "state": wf.get("state"),
                        "last_run_duration_seconds": wf.get("last_run_duration_seconds"),
                        "rows_loaded": wf.get("rows_loaded") or wf.get("total_rows"),
                        "error_count": wf.get("error_count") or 0,
                        "last_completion_time": wf.get("last_completion_time") or wf.get("last_run_time"),
                        "polled_at": polled_at,
                    })

            df = pd.DataFrame(rows)
            context.add_output_metadata({
                "row_count": len(df),
                "projects_polled": len(_self.projects),
            })
            return df

        return dg.Definitions(assets=[_the_asset])
