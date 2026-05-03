"""OcsfValidatorComponent.

Asset check that validates rows conform to OCSF v1.x. Verifies required fields,
type sanity, severity range, and (optionally) that class_uid is in a known set.
"""

import json
from typing import Optional

import dagster as dg
import pandas as pd
from pydantic import Field


KNOWN_CLASS_UIDS = {
    # System Activity
    1001, 1002, 1003, 1004, 1005, 1006, 1007,
    # Findings
    2001, 2002, 2003, 2004,
    # IAM
    3001, 3002, 3003, 3004, 3005, 3006, 3007,
    # Network Activity
    4001, 4002, 4003, 4004, 4005, 4006, 4007, 4008, 4009, 4010, 4011,
    # Discovery
    5001, 5002, 5003,
    # Application Activity
    6001, 6002, 6003, 6004, 6005, 6006, 6007,
}

REQUIRED_FIELDS = ["time", "class_uid", "category_uid", "severity_id", "metadata.version"]


class OcsfValidatorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Asset check: validates OCSF conformance on an upstream DataFrame asset."""

    check_name: str = Field(description="Asset check name")
    upstream_asset_key: str = Field(description="OCSF DataFrame asset to validate")
    blocking: bool = Field(default=True, description="Block downstream materializations on failure")
    require_known_class_uid: bool = Field(default=True, description="Fail when class_uid is not in the registered OCSF v1.x set")
    max_invalid_rows: int = Field(default=0, description="Allowed invalid rows before failure (0 = strict)")
    description: Optional[str] = Field(default=None)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        @dg.asset_check(
            name=self.check_name,
            asset=dg.AssetKey.from_user_string(self.upstream_asset_key),
            blocking=self.blocking,
            description=self.description or "OCSF v1.x conformance check",
        )
        def _check(df: pd.DataFrame) -> dg.AssetCheckResult:
            issues: list[dict] = []
            for field in REQUIRED_FIELDS:
                if field not in df.columns:
                    issues.append({"reason": "missing_required_field", "field": field})
            if "severity_id" in df.columns:
                bad = df[~df["severity_id"].between(0, 6)]
                if len(bad):
                    issues.append({"reason": "severity_id_out_of_range", "count": int(len(bad))})
            if "class_uid" in df.columns:
                bad = df[~df["class_uid"].isin([0, *KNOWN_CLASS_UIDS])]
                if len(bad) and _self.require_known_class_uid:
                    issues.append({
                        "reason": "unknown_class_uid",
                        "count": int(len(bad)),
                        "examples": list(bad["class_uid"].unique()[:5].tolist()),
                    })
            invalid_rows = sum(i.get("count", 0) for i in issues if "count" in i)
            passed = (
                not [i for i in issues if "field" in i]
                and invalid_rows <= _self.max_invalid_rows
            )
            return dg.AssetCheckResult(
                passed=passed,
                severity=dg.AssetCheckSeverity.ERROR if not passed else dg.AssetCheckSeverity.WARN,
                metadata={
                    "issues": dg.MetadataValue.json(issues),
                    "row_count": dg.MetadataValue.int(int(len(df))),
                    "invalid_rows": dg.MetadataValue.int(int(invalid_rows)),
                },
            )

        return dg.Definitions(asset_checks=[_check])
