"""Sifflet Asset Check Component.

Triggers a Sifflet monitor run on demand via the Sifflet REST API,
polls for completion, and surfaces results as a Dagster asset check.

Sifflet owns the monitor logic; Dagster triggers execution and surfaces results —
the same pattern as Soda, Great Expectations, and Monte Carlo.

API: https://{tenant}.siffletdata.com/api/v1
"""
import time
from typing import Optional
import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check
from pydantic import Field


class SiffletCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Sifflet monitor run and surface results as a Dagster asset check.

    Calls the Sifflet API to run a specific monitor on demand, waits for it
    to complete, and returns pass/fail/warn as an AssetCheckResult.

    Example:
        ```yaml
        type: dagster_component_templates.SiffletCheckComponent
        attributes:
          asset_key: warehouse/orders
          sifflet_tenant_env_var: SIFFLET_TENANT
          api_token_env_var: SIFFLET_API_TOKEN
          monitor_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          severity: ERROR
        ```
    """

    asset_key: str = Field(description="Asset key to attach this check to")
    sifflet_tenant_env_var: str = Field(
        description="Env var with Sifflet tenant name (e.g. 'mycompany' from mycompany.siffletdata.com)"
    )
    api_token_env_var: str = Field(description="Env var with Sifflet API token")
    monitor_id: str = Field(
        description="UUID of the Sifflet monitor to run. Find in Sifflet UI: Monitor → Details → ID."
    )
    poll_interval_seconds: float = Field(default=5.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=300, description="Max seconds to wait for monitor run")
    severity: str = Field(default="ERROR", description="WARN or ERROR")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        asset_key = dg.AssetKey(self.asset_key.split("/"))
        severity = (
            AssetCheckSeverity.ERROR if self.severity.upper() == "ERROR"
            else AssetCheckSeverity.WARN
        )

        @asset_check(
            asset=asset_key,
            description=f"Run Sifflet monitor {self.monitor_id} and surface results",
        )
        def sifflet_check(context):
            import os
            try:
                import requests
            except ImportError:
                return AssetCheckResult(
                    passed=False, severity=severity,
                    metadata={"error": "requests not installed"},
                )

            tenant = os.environ.get(_self.sifflet_tenant_env_var, "")
            token = os.environ.get(_self.api_token_env_var, "")
            base_url = f"https://{tenant}.siffletdata.com/api/v1"
            headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}

            # Trigger the monitor run
            try:
                resp = requests.post(
                    f"{base_url}/monitors/{_self.monitor_id}/run",
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()
                run_data = resp.json()
                run_id = run_data.get("runId", run_data.get("id", ""))
            except Exception as e:
                return AssetCheckResult(
                    passed=False, severity=severity,
                    metadata={"error": f"Failed to trigger Sifflet monitor: {e}"},
                )

            context.log.info(f"Sifflet monitor triggered. run_id={run_id}")

            # Poll for completion
            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    status_resp = requests.get(
                        f"{base_url}/monitors/{_self.monitor_id}/runs/{run_id}",
                        headers=headers,
                        timeout=30,
                    )
                    status_resp.raise_for_status()
                    run_status = status_resp.json()
                    status = run_status.get("status", "").upper()
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status == "SUCCESS":
                    return AssetCheckResult(
                        passed=True,
                        severity=severity,
                        metadata={
                            "run_id": run_id,
                            "monitor_id": _self.monitor_id,
                            "status": status,
                            "checks_passed": run_status.get("checksPassed", 0),
                        },
                    )
                elif status in ("FAILED", "ERROR", "PARTIALLY_FAILED"):
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        metadata={
                            "run_id": run_id,
                            "monitor_id": _self.monitor_id,
                            "status": status,
                            "checks_failed": run_status.get("checksFailed", 0),
                            "checks_passed": run_status.get("checksPassed", 0),
                            "failure_details": run_status.get("failureDetails", ""),
                        },
                    )
                # RUNNING / PENDING — keep polling

            return AssetCheckResult(
                passed=False, severity=severity,
                metadata={"error": f"Timed out after {_self.timeout_seconds}s", "run_id": run_id},
            )

        return dg.Definitions(asset_checks=[sifflet_check])
