"""Monte Carlo Asset Check Component.

Triggers a Monte Carlo rule execution on demand via the pycarlo SDK,
waits for the result, and surfaces it as a Dagster asset check.

Monte Carlo owns the rule logic; Dagster triggers execution and surfaces results —
the same pattern as Soda, Great Expectations, and the enhanced_data_quality_checks.

SDK: https://github.com/monte-carlo-data/pycarlo
"""
import time
from typing import Optional
import dagster as dg
from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check
from pydantic import Field


class MonteCarloCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Monte Carlo custom rule or monitor run and surface results as a Dagster asset check.

    Monte Carlo's Custom SQL Rules and Field Health monitors can be triggered
    on demand via the pycarlo SDK. This component triggers the rule, waits for
    it to complete, and returns pass/fail as an AssetCheckResult.

    Example:
        ```yaml
        type: dagster_component_templates.MonteCarloCheckComponent
        attributes:
          asset_key: warehouse/orders
          mcd_id_env_var: MCD_ID
          mcd_token_env_var: MCD_TOKEN
          rule_uuid: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          severity: ERROR
        ```
    """

    asset_key: str = Field(description="Asset key to attach this check to")
    mcd_id_env_var: str = Field(description="Env var with Monte Carlo API key ID")
    mcd_token_env_var: str = Field(description="Env var with Monte Carlo API token")
    rule_uuid: str = Field(
        description="UUID of the Monte Carlo Custom SQL Rule to execute. "
                    "Find in Monte Carlo UI: Monitors → Custom SQL Rule → Settings."
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
            description=f"Run Monte Carlo rule {self.rule_uuid} and surface results",
        )
        def monte_carlo_check(context):
            import os
            try:
                from pycarlo.core import Client, Mutation, Query
            except ImportError:
                return AssetCheckResult(
                    passed=False, severity=severity,
                    metadata={"error": "pycarlo not installed. Run: pip install pycarlo"},
                )

            mcd_id = os.environ.get(_self.mcd_id_env_var, "")
            mcd_token = os.environ.get(_self.mcd_token_env_var, "")

            try:
                client = Client(mcd_id=mcd_id, mcd_token=mcd_token)

                # Trigger the custom rule execution
                mutation = Mutation()
                mutation.trigger_custom_rule(rule_id=_self.rule_uuid).__fields__(
                    "execution_id", "status"
                )
                trigger_result = client(mutation)
                execution_id = trigger_result.trigger_custom_rule.execution_id
            except Exception as e:
                return AssetCheckResult(
                    passed=False, severity=severity,
                    metadata={"error": f"Failed to trigger Monte Carlo rule: {e}"},
                )

            context.log.info(f"Monte Carlo rule triggered. execution_id={execution_id}")

            # Poll for completion
            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    query = Query()
                    query.get_custom_rule_execution(
                        execution_id=execution_id
                    ).__fields__("execution_id", "status", "result", "breach_count", "error_message")
                    result = client(query)
                    exec_result = result.get_custom_rule_execution
                    status = (exec_result.status or "").lower()
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status in ("success", "passed", "no_breach"):
                    return AssetCheckResult(
                        passed=True,
                        severity=severity,
                        metadata={
                            "execution_id": execution_id,
                            "rule_uuid": _self.rule_uuid,
                            "status": status,
                            "breach_count": exec_result.breach_count or 0,
                        },
                    )
                elif status in ("breach", "failed", "error"):
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        metadata={
                            "execution_id": execution_id,
                            "rule_uuid": _self.rule_uuid,
                            "status": status,
                            "breach_count": exec_result.breach_count or 0,
                            "error_message": exec_result.error_message or "",
                        },
                    )
                # Still running — keep polling

            return AssetCheckResult(
                passed=False, severity=severity,
                metadata={"error": f"Timed out after {_self.timeout_seconds}s", "execution_id": execution_id},
            )

        return dg.Definitions(asset_checks=[monte_carlo_check])
