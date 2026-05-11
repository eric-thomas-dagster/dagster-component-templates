"""CloudDlpPiiCheckComponent — pass/fail asset check for PII in a DataFrame.

Sister to `cloud_dlp_inspect_asset` (which augments a DataFrame with findings).
This component runs the SAME Cloud DLP scan but emits an `AssetCheckResult` —
the right shape when you want to GATE downstream work on PII presence rather
than carry findings forward.

Common use:
  - Block warehouse load when any row contains SSN / credit-card numbers
  - Warn when emails appear in a column they shouldn't (PII drift detection)
  - Compliance gate at the boundary between raw + analytic schemas
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


class CloudDlpPiiCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Cloud DLP-backed asset check: fail (or warn) if forbidden PII is present."""

    asset_key: str = Field(description="Asset key the check validates (the upstream DataFrame).")
    check_name: str = Field(default="cloud_dlp_pii_check", description="Name of the check.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)
    project_id: Optional[str] = Field(default=None)

    text_columns: List[str] = Field(
        description="Columns whose string values are inspected. Each row's columns are joined with newlines.",
    )

    forbidden_info_types: List[str] = Field(
        default_factory=lambda: [
            "US_SOCIAL_SECURITY_NUMBER",
            "CREDIT_CARD_NUMBER",
            "US_DRIVERS_LICENSE_NUMBER",
            "IBAN_CODE",
            "AUTH_TOKEN",
            "GCP_CREDENTIALS",
            "AWS_CREDENTIALS",
        ],
        description="InfoTypes that, if detected, count as failures.",
    )

    min_likelihood: Literal[
        "LIKELIHOOD_UNSPECIFIED", "VERY_UNLIKELY", "UNLIKELY", "POSSIBLE",
        "LIKELY", "VERY_LIKELY",
    ] = Field(
        default="LIKELY",
        description="Minimum likelihood to count. Default LIKELY (cuts noise vs. inspect_asset's POSSIBLE).",
    )

    max_allowed_findings: int = Field(
        default=0,
        description="Maximum forbidden findings before the check fails. 0 = any forbidden finding fails.",
    )

    sample_size: Optional[int] = Field(
        default=200,
        description="Only scan the first N rows. Default 200 — full scans on large frames get expensive.",
    )

    severity: Literal["ERROR", "WARN"] = Field(default="ERROR")
    blocking: bool = Field(default=True, description="If True, failure blocks downstream materializations.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_key = dg.AssetKey.from_user_string(self.asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        text_columns = self.text_columns
        forbidden_types = self.forbidden_info_types
        min_likelihood = self.min_likelihood
        max_allowed = self.max_allowed_findings
        sample_size = self.sample_size
        severity = dg.AssetCheckSeverity[self.severity]

        @dg.asset_check(
            asset=asset_key,
            name=self.check_name,
            blocking=self.blocking,
            description="Cloud DLP-backed PII presence check on upstream DataFrame.",
        )
        def _pii_check(context, upstream) -> dg.AssetCheckResult:
            try:
                import pandas as pd
                from google.cloud import dlp_v2
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-dlp google-auth pandas")

            missing = [c for c in text_columns if c not in upstream.columns]
            if missing:
                return dg.AssetCheckResult(
                    passed=False,
                    severity=severity,
                    metadata={"error": dg.MetadataValue.text(
                        f"text_columns not in upstream: {missing}. Available: {list(upstream.columns)}"
                    )},
                )

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = dlp_v2.DlpServiceClient(credentials=sa_creds)
            parent = f"projects/{project_id}"

            inspect_config = {
                "info_types":     [{"name": it} for it in forbidden_types],
                "min_likelihood": min_likelihood,
                "limits":         {"max_findings_per_request": 100},
                "include_quote":  False,
            }

            df = upstream.head(sample_size) if sample_size else upstream
            forbidden_hits: Dict[str, int] = {}
            rows_scanned = 0
            rows_with_findings = 0
            for _, row in df.iterrows():
                rows_scanned += 1
                text = "\n".join(str(row[c]) for c in text_columns if bool(pd.notna(row[c])))
                if not text.strip():
                    continue
                resp = client.inspect_content(
                    request={"parent": parent, "inspect_config": inspect_config, "item": {"value": text}}
                )
                findings = list(resp.result.findings or [])
                if findings:
                    rows_with_findings += 1
                for f in findings:
                    forbidden_hits[f.info_type.name] = forbidden_hits.get(f.info_type.name, 0) + 1

            total = sum(forbidden_hits.values())
            passed = total <= max_allowed
            return dg.AssetCheckResult(
                passed=passed,
                severity=severity if not passed else dg.AssetCheckSeverity.WARN,
                metadata={
                    "rows_scanned":           dg.MetadataValue.int(rows_scanned),
                    "rows_with_findings":     dg.MetadataValue.int(rows_with_findings),
                    "total_forbidden_findings": dg.MetadataValue.int(total),
                    "max_allowed":            dg.MetadataValue.int(max_allowed),
                    "hits_by_info_type":      dg.MetadataValue.json(forbidden_hits),
                    "scanned_columns":        dg.MetadataValue.json(text_columns),
                },
            )

        return dg.Definitions(asset_checks=[_pii_check])
