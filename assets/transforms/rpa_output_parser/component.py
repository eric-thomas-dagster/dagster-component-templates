"""RPAOutputParserComponent.

Normalizes the vendor-specific `output_payload` metadata that the DCC
RPA integration components (uipath_orchestrator_integration,
automation_anywhere_integration, blue_prism_integration,
power_automate_integration) emit on each materialization into a single
flat DataFrame shape:

    vendor  run_id  status  asset_key  materialized_at  output_field  raw_payload

Reads the upstream RPA asset(s) via `context.instance.get_latest_materialization_event`,
extracts the `output_payload` JSON metadata, extracts the vendor-specific
output-carrying field (UiPath OutputArguments, AA bot_input echo, BP
inputs, PA trigger_input echo), and materializes one DataFrame row per
upstream asset.

Solves the RPA cross-vendor mess: downstream `extract` / `classify`
steps in an AgenticPipeline can consume a uniform shape regardless of
which bot vendor produced the raw data.
"""
from typing import Any, List

import dagster as dg
from dagster import AssetExecutionContext
from pydantic import ConfigDict, Field


# The output-payload field name that carries the raw bot output for each vendor.
# The RPA integration components emit output_payload.<field> with the vendor's
# native input/output shape.
_VENDOR_OUTPUT_FIELD = {
    "uipath": "output_arguments",
    "automation_anywhere": "bot_input",
    "blue_prism": "inputs",
    "power_automate": "trigger_input",
}


def _unwrap_metadata_value(md_value: Any) -> Any:
    """Extract the underlying python value from a MetadataValue."""
    if md_value is None:
        return None
    for attr in ("value", "text", "data", "url", "path"):
        if hasattr(md_value, attr):
            v = getattr(md_value, attr)
            if v is not None:
                return v
    return md_value


class RPAOutputParserComponent(dg.Component, dg.Model, dg.Resolvable):
    """Normalize RPA output_payload across vendors into a flat DataFrame.

    Materializes one DataFrame row per upstream RPA asset's latest
    materialization. Downstream extract / classify / AgenticPipeline steps
    consume a single shape regardless of the source vendor.
    """

    model_config = ConfigDict(extra="forbid")

    asset_name: str = Field(description="Name of the resulting normalized DataFrame asset.")
    upstream_asset_keys: List[str] = Field(
        description="Slash-separated asset keys of upstream RPA assets (from uipath_orchestrator_integration, automation_anywhere_integration, blue_prism_integration, power_automate_integration).",
    )
    group_name: str = Field(default="rpa_normalized", description="Dagster asset group.")
    kinds: List[str] = Field(default_factory=lambda: ["python", "rpa", "normalized"], description="Asset kinds to attach.")
    description: str = Field(default="", description="Prose description shown in the Dagster UI.")
    fail_on_missing_upstream: bool = Field(
        default=False,
        description="If true, raise when any upstream has no materialization on record; else emit a row with status='MISSING'.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        upstream_keys = [dg.AssetKey(k.split("/")) for k in self.upstream_asset_keys]
        _asset_name = self.asset_name
        _kinds = set(self.kinds)
        _group = self.group_name
        _desc = self.description or (
            f"Normalized RPA output DataFrame combining {len(upstream_keys)} upstream RPA asset(s). "
            f"One row per vendor with columns: vendor, run_id, status, asset_key, materialized_at, output_field, raw_payload."
        )
        _fail_on_missing = self.fail_on_missing_upstream

        @dg.asset(
            name=_asset_name,
            kinds=_kinds,
            group_name=_group,
            deps=upstream_keys,
            description=_desc,
            metadata={
                "tier": "silver",
                "domain": "rpa_normalized",
                "vendor_agnostic": True,
            },
        )
        def _rpa_parser(context: AssetExecutionContext) -> "list":
            rows = []
            for asset_key in upstream_keys:
                event = context.instance.get_latest_materialization_event(asset_key)
                if event is None:
                    if _fail_on_missing:
                        raise dg.Failure(description=f"No materialization on record for upstream {asset_key.to_user_string()!r}")
                    rows.append({
                        "vendor": "unknown", "run_id": None, "status": "MISSING",
                        "asset_key": asset_key.to_user_string(), "materialized_at": None,
                        "output_field": None, "raw_payload": None,
                    })
                    continue

                md = event.dagster_event.event_specific_data.materialization.metadata
                payload = _unwrap_metadata_value(md.get("output_payload"))
                if not isinstance(payload, dict):
                    context.log.warning(
                        f"Upstream {asset_key.to_user_string()!r} materialization has no `output_payload` "
                        f"metadata (dict); got {type(payload).__name__}. Skipping normalization."
                    )
                    rows.append({
                        "vendor": "unknown", "run_id": None, "status": "MISSING_PAYLOAD",
                        "asset_key": asset_key.to_user_string(), "materialized_at": event.timestamp,
                        "output_field": None, "raw_payload": payload,
                    })
                    continue

                vendor = str(payload.get("vendor", "unknown"))
                output_field_name = _VENDOR_OUTPUT_FIELD.get(vendor)
                output_field = payload.get(output_field_name) if output_field_name else None
                rows.append({
                    "vendor": vendor,
                    "run_id": payload.get("run_id"),
                    "status": payload.get("status"),
                    "asset_key": asset_key.to_user_string(),
                    "materialized_at": event.timestamp,
                    "output_field": output_field,
                    "raw_payload": payload,
                })

            context.log.info(f"[NORMALIZE] {len(rows)} rows: " + ", ".join(f"{r['vendor']}:{r['status']}" for r in rows))
            context.add_output_metadata({
                "row_count": len(rows),
                "vendors": dg.MetadataValue.json(sorted({r["vendor"] for r in rows})),
                "status_counts": dg.MetadataValue.json({s: sum(1 for r in rows if r["status"] == s) for s in sorted({r["status"] for r in rows})}),
                "preview": dg.MetadataValue.json(rows[:5]),
            })
            return rows

        return dg.Definitions(assets=[_rpa_parser])
