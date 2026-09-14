"""RPA Health Check Component.

Emits a Dagster ``@asset_check`` for a target RPA asset that verifies:

  1. **Freshness** — the asset's last materialization is within ``max_age_hours``.
  2. **Status** — the ``status`` metadata field of that materialization is one
     of the accepted terminal-success values (RPA and scheduler vendors report
     success differently: UiPath ``Successful`` / AA ``COMPLETED`` /
     BluePrism ``Completed`` / PowerAutomate ``Succeeded`` / Control-M and
     RunMyJobs ``Ended OK`` / etc.).
  3. **Output size (optional)** — the ``output_payload`` JSON metadata field
     is non-empty when ``require_non_empty_output=true``.

The check reads the latest materialization event via
``context.instance.get_latest_materialization_event(asset_key)`` and returns
``dg.AssetCheckResult(passed=..., metadata={...})`` — no external calls, so
it works against any DCC RPA / scheduler integration that emits ``status``
+ ``output_payload`` metadata (see the ``uipath_orchestrator_integration``,
``automation_anywhere_integration``, ``blue_prism_integration``,
``power_automate_integration``, ``controlm_integration``, and
``runmyjobs_integration`` components).
"""
from typing import List

import dagster as dg
from pydantic import ConfigDict, Field


class RPAHealthCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Asset check that verifies an RPA asset ran successfully within SLA.

    Reads the latest materialization event's metadata (specifically the
    ``status`` field and the ``output_payload`` JSON field emitted by
    controlm / runmyjobs / uipath / automation_anywhere / blue_prism /
    power_automate integrations) and fails the check when the asset is
    stale, in a non-terminal-success state, or missing output.

    Example:

        ```yaml
        type: dagster_community_components.RPAHealthCheckComponent
        attributes:
          target_asset: uipath_invoice_extract
          check_name: uipath_invoice_health
          max_age_hours: 6.0
          require_non_empty_output: true
        ```
    """

    model_config = ConfigDict(extra="forbid")

    target_asset: str = Field(
        description="Slash-separated asset key to check (e.g. 'uipath_invoice_extract' or 'rpa/uipath/invoices').",
    )
    check_name: str = Field(
        default="rpa_health",
        description="Asset check name shown in the UI.",
    )
    max_age_hours: float = Field(
        default=24.0,
        description="Fail if the latest materialization is older than this many hours.",
    )
    expected_statuses: List[str] = Field(
        default_factory=lambda: [
            "success",
            "Successful",
            "COMPLETED",
            "Completed",
            "Succeeded",
            "Ended OK",
        ],
        description="Terminal-success values across RPA + scheduler vendors. Check passes if the asset's status metadata is in this list.",
    )
    require_non_empty_output: bool = Field(
        default=False,
        description="Also fail if output_payload.output_arguments (or output_payload.trigger_input) is empty.",
    )
    severity: str = Field(
        default="WARN",
        description="'WARN' (flag only) or 'ERROR' (blocks downstream materialization).",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # Close over field values for the check body.
        target_asset = self.target_asset
        check_name = self.check_name
        max_age_hours = self.max_age_hours
        expected_statuses = list(self.expected_statuses)
        require_non_empty_output = self.require_non_empty_output
        severity_enum = dg.AssetCheckSeverity[self.severity.upper()]

        target_key = dg.AssetKey(target_asset.split("/"))

        @dg.asset_check(
            asset=target_key,
            name=check_name,
            blocking=False,
            description=(
                f"RPA health check for {target_asset!r}: freshness "
                f"(<= {max_age_hours}h), status in {expected_statuses}, "
                f"output non-empty={require_non_empty_output}."
            ),
        )
        def _rpa_health(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
            from datetime import datetime, timezone

            event = context.instance.get_latest_materialization_event(target_key)
            if event is None:
                return dg.AssetCheckResult(
                    passed=False,
                    severity=severity_enum,
                    description="No materialization on record yet.",
                    metadata={
                        "reason": "no_materialization",
                        "target_asset": target_asset,
                    },
                )

            ts = event.timestamp
            age_hours = (datetime.now(timezone.utc).timestamp() - ts) / 3600.0

            # Grab the materialization metadata; guard against unexpected event shape.
            try:
                md = event.dagster_event.event_specific_data.materialization.metadata
            except AttributeError:
                md = {}

            def _md_scalar(val):
                """Best-effort unwrap of a Dagster MetadataValue to a Python scalar."""
                if val is None:
                    return None
                for attr in ("text", "value", "url", "path"):
                    got = getattr(val, attr, None)
                    if got is not None:
                        return got
                return val

            def _md_json(val):
                """Best-effort unwrap of a JSON/dict MetadataValue to a dict."""
                if val is None:
                    return {}
                data = getattr(val, "data", None)
                if data is not None:
                    return data
                value = getattr(val, "value", None)
                if isinstance(value, dict):
                    return value
                if isinstance(val, dict):
                    return val
                return {}

            # --- 1. Freshness ---
            fresh_ok = age_hours <= max_age_hours

            # --- 2. Status ---
            status_raw = md.get("status") if hasattr(md, "get") else None
            status_str = _md_scalar(status_raw)
            status_str = "N/A" if status_str is None else str(status_str)
            status_ok = status_str in expected_statuses

            # --- 3. Output (optional) ---
            output_ok = True
            output_len = 0
            output_source = None
            if require_non_empty_output:
                payload_raw = md.get("output_payload") if hasattr(md, "get") else None
                payload = _md_json(payload_raw)
                output_field = ""
                for candidate in ("output_arguments", "trigger_input", "stdout", "output"):
                    val = (payload or {}).get(candidate)
                    if val:
                        output_field = val
                        output_source = candidate
                        break
                output_len = len(str(output_field)) if output_field else 0
                output_ok = bool(output_field)

            checks_ok = fresh_ok and status_ok and output_ok

            metadata = {
                "target_asset": target_asset,
                "check_freshness_ok": fresh_ok,
                "check_status_ok": status_ok,
                "age_hours": round(age_hours, 2),
                "max_age_hours": max_age_hours,
                "observed_status": status_str,
                "expected_any_of": dg.MetadataValue.json(expected_statuses),
            }
            if require_non_empty_output:
                metadata["check_output_ok"] = output_ok
                metadata["output_field_chars"] = output_len
                metadata["output_field_source"] = output_source or "none"

            failure_reasons = []
            if not fresh_ok:
                failure_reasons.append(
                    f"stale ({round(age_hours, 2)}h > {max_age_hours}h)"
                )
            if not status_ok:
                failure_reasons.append(
                    f"status {status_str!r} not in {expected_statuses}"
                )
            if require_non_empty_output and not output_ok:
                failure_reasons.append("output_payload is empty")

            description = (
                "RPA health OK."
                if checks_ok
                else "RPA health FAIL: " + "; ".join(failure_reasons)
            )

            return dg.AssetCheckResult(
                passed=checks_ok,
                severity=severity_enum,
                description=description,
                metadata=metadata,
            )

        return dg.Definitions(asset_checks=[_rpa_health])
