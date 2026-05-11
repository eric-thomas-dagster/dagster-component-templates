"""BigqueryDryRunCheckComponent — cost guardrail for a BigQuery query.

Runs a BigQuery dry-run (server-side query plan + estimated bytes scanned,
no data returned, no cost charged) and fails the Dagster asset check if
the estimated bytes exceed a configured threshold.

Use as:
  - Pre-flight gate for expensive scheduled jobs ("never scan > 100GB")
  - Cost-anomaly detection ("our nightly query just went from 5GB → 500GB,
    something's wrong upstream")
  - PR gate (CI runs the check; new queries that scan too much fail review)

A dry run is free — `dry_run=True` in the BigQuery client returns the plan
without reading data. This component is the recommended pattern for cost
discipline; running an actual query in your asset check would cost money
and slow CI.
"""

import json
import os
from typing import Any, Dict, Literal, Optional

import dagster as dg
from pydantic import Field


class BigqueryDryRunCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Asset check that dry-runs a BigQuery query and fails on excessive bytes scanned."""

    asset_key: str = Field(description="Asset key the check validates.")
    check_name: str = Field(default="bigquery_dry_run_cost_check")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)
    project_id: Optional[str] = Field(default=None)
    location: Optional[str] = Field(default=None, description="BQ location (e.g. 'US', 'EU').")

    sql: str = Field(description="The query to dry-run. Same SQL you'd run in production.")

    max_bytes: Optional[int] = Field(
        default=None,
        description=(
            "Max estimated bytes scanned. Check fails if exceeded. "
            "E.g. 100_000_000_000 = 100 GB. Pick this OR max_cost_usd (or both — strictest wins)."
        ),
    )
    max_cost_usd: Optional[float] = Field(
        default=None,
        description=(
            "Max estimated USD cost. NOTE: BigQuery's dry-run does NOT return a cost — only bytes. "
            "This is computed locally as bytes_scanned / 1e12 * on_demand_price_per_tb_usd. "
            "The rate is YOUR responsibility to keep current. For flat-rate / capacity reservations, "
            "this number is meaningless — use max_slot_ms instead."
        ),
    )
    on_demand_price_per_tb_usd: float = Field(
        default=6.25,
        description=(
            "On-demand $/TB rate used to compute `max_cost_usd`. "
            "Default $6.25/TB (US, on-demand, as of 2025). "
            "Verify against https://cloud.google.com/bigquery/pricing#analysis_pricing for your region "
            "+ contract. If you're on capacity reservations, ignore this — use max_slot_ms."
        ),
    )
    max_slot_ms: Optional[int] = Field(
        default=None,
        description="Optional cap on estimated slot-ms. Useful for capacity-reservation projects where bytes/cost don't reflect actual spend.",
    )

    severity: Literal["ERROR", "WARN"] = Field(default="ERROR")
    blocking: bool = Field(default=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        if self.max_bytes is None and self.max_cost_usd is None and self.max_slot_ms is None:
            raise ValueError("Configure at least one of: max_bytes, max_cost_usd, max_slot_ms.")

        asset_key = dg.AssetKey.from_user_string(self.asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        sql = self.sql
        max_bytes = self.max_bytes
        max_cost_usd = self.max_cost_usd
        price_per_tb = self.on_demand_price_per_tb_usd
        max_slot_ms = self.max_slot_ms
        severity = dg.AssetCheckSeverity[self.severity]

        # Resolve a human-readable label for the check description
        if max_cost_usd is not None:
            cap_label = f"max ${max_cost_usd:.2f} at ${price_per_tb}/TB"
        elif max_bytes is not None:
            cap_label = f"max {max_bytes:,} bytes"
        else:
            cap_label = f"max {max_slot_ms:,} slot-ms"

        @dg.asset_check(
            asset=asset_key,
            name=self.check_name,
            blocking=self.blocking,
            description=f"BigQuery dry-run cost check ({cap_label}).",
        )
        def _check(context) -> dg.AssetCheckResult:
            try:
                from google.cloud import bigquery
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigquery google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(project=project_id, credentials=sa_creds, location=location)

            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            job = client.query(sql, job_config=job_config)
            bytes_scanned = int(job.total_bytes_processed or 0)
            est_cost_usd = bytes_scanned / 1e12 * price_per_tb
            slot_ms = getattr(job, "slot_millis", None)

            failures = []
            if max_bytes is not None and bytes_scanned > max_bytes:
                failures.append(f"bytes_scanned={bytes_scanned:,} > max_bytes={max_bytes:,}")
            if max_cost_usd is not None and est_cost_usd > max_cost_usd:
                failures.append(f"est_cost=${est_cost_usd:.4f} > max_cost_usd=${max_cost_usd:.4f}")
            if max_slot_ms is not None and slot_ms is not None and slot_ms > max_slot_ms:
                failures.append(f"slot_ms={slot_ms:,} > max_slot_ms={max_slot_ms:,}")

            passed = len(failures) == 0
            md: Dict[str, Any] = {
                "bytes_scanned":       dg.MetadataValue.int(bytes_scanned),
                "bytes_scanned_human": dg.MetadataValue.text(_human_bytes(bytes_scanned)),
                "estimated_cost_usd":  dg.MetadataValue.float(round(est_cost_usd, 4)),
                "on_demand_price_per_tb_usd": dg.MetadataValue.float(price_per_tb),
                "slot_ms":             dg.MetadataValue.int(int(slot_ms or 0)),
                "failures":            dg.MetadataValue.json(failures) if failures else dg.MetadataValue.text("ok"),
            }
            if max_bytes is not None:
                md["max_bytes"]       = dg.MetadataValue.int(max_bytes)
                md["max_bytes_human"] = dg.MetadataValue.text(_human_bytes(max_bytes))
            if max_cost_usd is not None:
                md["max_cost_usd"]    = dg.MetadataValue.float(max_cost_usd)
            if max_slot_ms is not None:
                md["max_slot_ms"]     = dg.MetadataValue.int(max_slot_ms)

            return dg.AssetCheckResult(
                passed=passed,
                severity=severity if not passed else dg.AssetCheckSeverity.WARN,
                metadata=md,
            )

        return dg.Definitions(asset_checks=[_check])


def _human_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024  # type: ignore[assignment]
    return f"{b:.1f} EB"
