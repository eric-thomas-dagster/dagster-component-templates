"""OpenlineageEmitterComponent.

Emit OpenLineage events when an upstream asset materializes — wires Dagster's
asset lineage into OpenLineage-compatible backends (Marquez, DataHub, OpenMetadata,
Astro Observe, Atlan, etc.).

This is an asset_check that runs after the upstream asset materializes; it
constructs a START/COMPLETE event pair conforming to the OpenLineage v1.x spec
and POSTs to the configured `openlineage_url`.

Authentication
--------------
If your OL backend requires auth, set `auth_token_env` to the env var name
holding the bearer token.

Backends tested against
-----------------------
- Marquez (open-source reference impl)
- DataHub (the OL receiver endpoint)
- OpenMetadata
- Astro Observe / Atlan

The exact field shapes and required producer URI vary slightly by backend —
see backend docs.
"""

import json
import os
import datetime as dt
from typing import Optional
from uuid import uuid4

import dagster as dg
import pandas as pd
from pydantic import Field


class OpenlineageEmitterComponent(dg.Component, dg.Model, dg.Resolvable):
    """Asset check that emits OpenLineage START + COMPLETE events for an upstream asset."""

    check_name: str = Field(description="Asset check name")
    upstream_asset_key: str = Field(description="Asset whose materialization triggers an OL event")

    openlineage_url: str = Field(description="OL backend endpoint (e.g. 'https://marquez.acme.com/api/v1/lineage')")
    auth_token_env: Optional[str] = Field(default=None, description="Env var with bearer token (omit for unauthenticated)")

    namespace: str = Field(default="dagster", description="OL namespace for the asset")
    producer_url: str = Field(
        default="https://github.com/eric-thomas-dagster/dagster-component-templates",
        description="`producer` URL field on the event (identifies the emitter)",
    )
    job_namespace: Optional[str] = Field(default=None, description="Override job namespace (defaults to `namespace`)")
    job_name_override: Optional[str] = Field(default=None, description="Override OL job name (default: derived from asset)")

    schema_facet: bool = Field(default=True, description="Include the schema facet inferred from DataFrame columns")
    column_lineage_facet: Optional[dict] = Field(
        default=None,
        description="Optional column-lineage map: {output_col: [input_col, ...]} → emitted as the columnLineage facet",
    )
    upstream_input_assets: Optional[list[str]] = Field(
        default=None,
        description="Asset keys to declare as `inputs` on the OL job (None = no inputs declared)",
    )

    description: Optional[str] = Field(default=None)
    blocking: bool = Field(default=False, description="Block downstream when emit fails")
    timeout_seconds: int = Field(default=30)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset_check(
            name=self.check_name,
            asset=dg.AssetKey.from_user_string(self.upstream_asset_key),
            blocking=self.blocking,
            description=self.description or "Emit OpenLineage events for this asset",
        )
        def _check(df: pd.DataFrame) -> dg.AssetCheckResult:
            import requests

            run_id = str(uuid4())
            now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
            job_namespace = _self.job_namespace or _self.namespace
            job_name = _self.job_name_override or _self.upstream_asset_key.replace("/", ".")

            base_event = {
                "eventTime": now_iso,
                "producer": _self.producer_url,
                "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunEvent",
                "run": {"runId": run_id},
                "job": {"namespace": job_namespace, "name": job_name},
                "inputs": [
                    {"namespace": _self.namespace, "name": k}
                    for k in (_self.upstream_input_assets or [])
                ],
                "outputs": [{
                    "namespace": _self.namespace,
                    "name": _self.upstream_asset_key,
                }],
            }

            # Schema facet on the output
            if _self.schema_facet and len(df) > 0:
                fields = [
                    {"name": str(c), "type": str(df[c].dtype)}
                    for c in df.columns
                ]
                base_event["outputs"][0]["facets"] = {
                    "schema": {
                        "_producer": _self.producer_url,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json",
                        "fields": fields,
                    }
                }

            # Column lineage facet
            if _self.column_lineage_facet:
                facets = base_event["outputs"][0].setdefault("facets", {})
                facets["columnLineage"] = {
                    "_producer": _self.producer_url,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
                    "fields": {
                        out_col: {
                            "inputFields": [
                                {"namespace": _self.namespace, "name": ".".join(in_col.split(".")[:-1]) or _self.upstream_asset_key,
                                 "field": in_col.split(".")[-1]}
                                for in_col in in_cols
                            ]
                        }
                        for out_col, in_cols in _self.column_lineage_facet.items()
                    },
                }

            headers = {"Content-Type": "application/json"}
            if _self.auth_token_env:
                token = os.environ.get(_self.auth_token_env)
                if token:
                    headers["Authorization"] = f"Bearer {token}"

            errors = []
            for event_type in ("START", "COMPLETE"):
                event = {**base_event, "eventType": event_type, "eventTime": dt.datetime.now(dt.timezone.utc).isoformat()}
                try:
                    r = requests.post(_self.openlineage_url, json=event, headers=headers, timeout=_self.timeout_seconds)
                    if r.status_code >= 300:
                        errors.append(f"{event_type}: HTTP {r.status_code} — {r.text[:200]}")
                except Exception as exc:
                    errors.append(f"{event_type}: {type(exc).__name__}: {exc}")

            passed = not errors
            return dg.AssetCheckResult(
                passed=passed,
                severity=dg.AssetCheckSeverity.ERROR if not passed else dg.AssetCheckSeverity.WARN,
                metadata={
                    "openlineage_url": dg.MetadataValue.text(_self.openlineage_url),
                    "namespace": dg.MetadataValue.text(_self.namespace),
                    "ol_run_id": dg.MetadataValue.text(run_id),
                    "ol_job_name": dg.MetadataValue.text(job_name),
                    "errors": dg.MetadataValue.json(errors),
                    "row_count": dg.MetadataValue.int(int(len(df))),
                },
            )

        return dg.Definitions(asset_checks=[_check])
