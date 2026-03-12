"""Acceldata Asset Check Component.

Triggers an Acceldata rule execution on demand via the Acceldata REST API,
polls for completion, and surfaces results as a Dagster asset check.

Acceldata owns the rule logic; Dagster triggers execution and surfaces results —
the same pattern as Soda, Great Expectations, Monte Carlo, and Sifflet.

API: https://{tenant}.acceldata.io/api
"""
import time
import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check
from pydantic import Field


class AcceldataCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger an Acceldata rule execution and surface results as a Dagster asset check.

    Calls the Acceldata API to run a specific data quality rule on demand, waits
    for it to complete, and returns pass/fail/warn as an AssetCheckResult.

    Example:
        ```yaml
        type: dagster_component_templates.AcceldataCheckComponent
        attributes:
          asset_key: warehouse/orders
          api_url_env_var: ACCELDATA_API_URL
          api_token_env_var: ACCELDATA_API_TOKEN
          rule_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          severity: ERROR
        ```
    """

    asset_key: str = Field(description="Asset key to attach this check to")
    api_url_env_var: str = Field(
        description="Env var with Acceldata API base URL (e.g. https://mycompany.acceldata.io)"
    )
    api_token_env_var: str = Field(description="Env var with Acceldata API token")
    rule_id: str = Field(
        description="ID of the Acceldata DQ rule to execute. Find in Acceldata UI: Rules → Rule Details → ID."
    )
    poll_interval_seconds: float = Field(default=5.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=300, description="Max seconds to wait for rule execution")
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
            description=f"Run Acceldata rule {self.rule_id} and surface results",
        )
        def acceldata_check(context):
            import os
            try:
                import requests
            except ImportError:
                return AssetCheckResult(
                    passed=False, severity=severity,
                    metadata={"error": "requests not installed"},
                )

            api_url = os.environ.get(_self.api_url_env_var, "").rstrip("/")
            token = os.environ.get(_self.api_token_env_var, "")
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            # Trigger the rule execution
            try:
                resp = requests.post(
                    f"{api_url}/api/v1/rules/{_self.rule_id}/execute",
                    headers=headers,
                    timeout=30,
                )
                resp.raise_for_status()
                exec_data = resp.json()
                execution_id = exec_data.get("executionId", exec_data.get("id", ""))
            except Exception as e:
                return AssetCheckResult(
                    passed=False, severity=severity,
                    metadata={"error": f"Failed to trigger Acceldata rule: {e}"},
                )

            context.log.info(f"Acceldata rule triggered. execution_id={execution_id}")

            # Poll for completion
            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    status_resp = requests.get(
                        f"{api_url}/api/v1/rules/{_self.rule_id}/executions/{execution_id}",
                        headers=headers,
                        timeout=30,
                    )
                    status_resp.raise_for_status()
                    exec_result = status_resp.json()
                    status = exec_result.get("status", "").upper()
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status in ("SUCCESS", "PASSED"):
                    return AssetCheckResult(
                        passed=True,
                        severity=severity,
                        metadata={
                            "execution_id": execution_id,
                            "rule_id": _self.rule_id,
                            "status": status,
                            "passed_count": exec_result.get("passedCount", 0),
                            "total_count": exec_result.get("totalCount", 0),
                        },
                    )
                elif status in ("FAILED", "ERROR", "VIOLATION"):
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        metadata={
                            "execution_id": execution_id,
                            "rule_id": _self.rule_id,
                            "status": status,
                            "failed_count": exec_result.get("failedCount", 0),
                            "passed_count": exec_result.get("passedCount", 0),
                            "total_count": exec_result.get("totalCount", 0),
                            "violation_details": exec_result.get("violationDetails", ""),
                        },
                    )
                # RUNNING / PENDING / QUEUED — keep polling

            return AssetCheckResult(
                passed=False, severity=severity,
                metadata={"error": f"Timed out after {_self.timeout_seconds}s", "execution_id": execution_id},
            )

        return dg.Definitions(asset_checks=[acceldata_check])
