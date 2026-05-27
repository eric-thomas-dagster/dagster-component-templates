"""Precisely Data Integrity Suite — Data Quality asset checks.

Runs Data Quality rules defined in the Precisely Data Integrity Suite (DIS)
against a target asset and surfaces each rule's pass/fail as its own
Dagster asset check. Pre-resolves the rule set at build_defs time so each
rule is individually alertable, retriable, and visible in the catalog UI.

DIS Data Quality auth follows the documented OAuth2 client-credentials
flow against the global token endpoint
(``https://api.data.precisely.com/oauth/token``) and pulls rule executions
from the DQ service base path (``https://api.data.precisely.com/data360/dq``).
The runtime contract:

  1. Authenticate (client_id + client_secret → access_token, 1h TTL)
  2. GET /datasets/{dataset_id}/rules            (enumerate rule set)
  3. POST /datasets/{dataset_id}/rules/run       (execute, returns run_id)
  4. Poll GET /runs/{run_id}                     (wait until status=COMPLETE)
  5. GET /runs/{run_id}/results                  (per-rule pass/fail/score)

This is the modern DIS REST surface; the older Trillium DQ engine and
on-prem Data360 DQ+ have similar but not identical paths and are not
covered here.

Validation level: **code** — YAML loads, build_defs returns a
Definitions with N AssetCheckSpec objects, runtime path is untested
without a DIS tenant. Promotes to ``live`` once a customer pilot
exercises a real rule set end-to-end.
"""
import re
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field

DIS_DEFAULT_TOKEN_URL = "https://api.data.precisely.com/oauth/token"
DIS_DEFAULT_DQ_BASE = "https://api.data.precisely.com/data360/dq"


class PreciselyDISDQCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Wrap a Precisely DIS Data Quality rule set as N Dagster asset checks.

    Example:
        ```yaml
        type: dagster_community_components.PreciselyDISDQCheckComponent
        attributes:
          asset_key: warehouse/orders
          dataset_id: "ds-abc-123"
          rule_ids: ["rule-001", "rule-002", "rule-003"]
          client_id_env_var: PRECISELY_DIS_CLIENT_ID
          client_secret_env_var: PRECISELY_DIS_CLIENT_SECRET
        ```

    The DIS dataset is referenced by ``dataset_id`` (the DIS-side
    identifier, not the Dagster asset key). Each rule becomes its own
    Dagster asset check named ``dis_dq__{rule_id}`` on the target asset.
    """

    asset_key: str = Field(
        description=(
            "Dagster asset key the checks attach to (e.g. "
            "'warehouse/orders'). Use '/' separators for nested keys."
        ),
    )
    dataset_id: str = Field(
        description="DIS dataset id — the dataset the rules are bound to in DIS.",
    )
    rule_ids: List[str] = Field(
        description=(
            "Rule ids to execute. Pre-resolved at build_defs time so each "
            "becomes its own asset check (individually alertable + "
            "retriable). Get rule ids from the DIS UI or the "
            "GET /datasets/{dataset_id}/rules endpoint."
        ),
    )

    client_id_env_var: str = Field(
        default="PRECISELY_DIS_CLIENT_ID",
        description="Env var with DIS OAuth2 client_id",
    )
    client_secret_env_var: str = Field(
        default="PRECISELY_DIS_CLIENT_SECRET",
        description="Env var with DIS OAuth2 client_secret",
    )
    token_url: str = Field(
        default=DIS_DEFAULT_TOKEN_URL,
        description="OAuth2 token endpoint (override for non-global DIS tenants).",
    )
    dq_base_url: str = Field(
        default=DIS_DEFAULT_DQ_BASE,
        description="DIS Data Quality service base URL.",
    )

    poll_interval_seconds: int = Field(
        default=5,
        ge=1,
        description="Seconds between run-status polls while a DQ execution runs.",
    )
    timeout_seconds: int = Field(
        default=600,
        ge=10,
        description="Hard timeout (seconds) for a single DQ execution.",
    )
    severity: str = Field(
        default="ERROR",
        description="AssetCheckSeverity for failing checks: 'WARN' or 'ERROR'.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster import AssetCheckSeverity, AssetCheckSpec, asset_check, AssetCheckResult, AssetCheckExecutionContext, MetadataValue

        _self = self
        asset_key = dg.AssetKey(self.asset_key.split("/"))
        severity = AssetCheckSeverity[self.severity.upper()]

        def _make_check(rule_id: str):
            # DIS rule_ids commonly contain hyphens (e.g.
            # "rule-not-null-order_id"); Dagster check names must match
            # ^[A-Za-z0-9_]+$, so sanitize. The original rule_id is
            # still passed verbatim to the DIS API.
            safe_rule_id = re.sub(r"[^A-Za-z0-9_]+", "_", rule_id)
            check_name = f"dis_dq__{safe_rule_id}"

            @asset_check(name=check_name, asset=asset_key)
            def _check(context: AssetCheckExecutionContext) -> AssetCheckResult:
                import os, time
                try:
                    import requests
                except ImportError:
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        metadata={"error": "requests not installed"},
                    )

                client_id = os.environ.get(_self.client_id_env_var, "")
                client_secret = os.environ.get(_self.client_secret_env_var, "")
                if not client_id or not client_secret:
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        metadata={
                            "error": (
                                f"DIS creds missing: set {_self.client_id_env_var} "
                                f"and {_self.client_secret_env_var}"
                            ),
                        },
                    )

                # OAuth2 client-credentials.
                token_resp = requests.post(
                    _self.token_url,
                    data={"grant_type": "client_credentials"},
                    auth=(client_id, client_secret),
                    timeout=30,
                )
                token_resp.raise_for_status()
                token = token_resp.json()["access_token"]
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

                # Execute the rule.
                run_resp = requests.post(
                    f"{_self.dq_base_url.rstrip('/')}/datasets/{_self.dataset_id}/rules/run",
                    headers=headers,
                    json={"ruleIds": [rule_id]},
                    timeout=60,
                )
                run_resp.raise_for_status()
                run_id = run_resp.json().get("runId") or run_resp.json().get("id")

                # Poll until COMPLETE or timeout.
                deadline = time.time() + _self.timeout_seconds
                final = None
                while time.time() < deadline:
                    poll = requests.get(
                        f"{_self.dq_base_url.rstrip('/')}/runs/{run_id}",
                        headers=headers,
                        timeout=30,
                    )
                    poll.raise_for_status()
                    payload = poll.json()
                    status = (payload.get("status") or "").upper()
                    if status in ("COMPLETE", "COMPLETED", "FAILED", "ERROR"):
                        final = payload
                        break
                    time.sleep(_self.poll_interval_seconds)

                if final is None:
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        metadata={"error": f"DIS DQ run {run_id} timed out after {_self.timeout_seconds}s"},
                    )

                # Pull per-rule results.
                results_resp = requests.get(
                    f"{_self.dq_base_url.rstrip('/')}/runs/{run_id}/results",
                    headers=headers,
                    timeout=30,
                )
                results_resp.raise_for_status()
                items = results_resp.json()
                # DIS returns one entry per rule; we asked for exactly one.
                rule_result = next(
                    (r for r in (items.get("results") or items) if r.get("ruleId") == rule_id),
                    None,
                )
                if not rule_result:
                    return AssetCheckResult(
                        passed=False,
                        severity=severity,
                        metadata={"error": f"No DIS result for rule {rule_id} in run {run_id}"},
                    )

                passed = bool(rule_result.get("passed"))
                metadata: Dict[str, Any] = {
                    "dis_run_id": run_id,
                    "dis_rule_id": rule_id,
                    "dis_status": rule_result.get("status"),
                }
                score = rule_result.get("score")
                if score is not None:
                    try:
                        metadata["dis/score"] = MetadataValue.float(float(score))
                    except (TypeError, ValueError):
                        metadata["dis/score_raw"] = str(score)
                pass_count = rule_result.get("passCount")
                fail_count = rule_result.get("failCount")
                if pass_count is not None:
                    try:
                        metadata["dis/pass_count"] = MetadataValue.int(int(pass_count))
                    except (TypeError, ValueError):
                        pass
                if fail_count is not None:
                    try:
                        metadata["dis/fail_count"] = MetadataValue.int(int(fail_count))
                    except (TypeError, ValueError):
                        pass
                if rule_result.get("message"):
                    metadata["dis_message"] = str(rule_result["message"])

                return AssetCheckResult(
                    passed=passed,
                    severity=severity,
                    metadata=metadata,
                )

            return _check

        checks = [_make_check(rid) for rid in self.rule_ids]
        return dg.Definitions(asset_checks=checks)
