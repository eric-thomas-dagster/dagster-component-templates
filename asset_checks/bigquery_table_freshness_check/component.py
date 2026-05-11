"""BigqueryTableFreshnessCheckComponent — SLO check on max-age of a BQ table.

Reads the BigQuery table's `last_modified_time` (metadata only, free + fast)
and fails the asset check if the table is older than `max_age_minutes`.

Use for SLO enforcement:
  - "Our customer-facing dashboard reads from `analytics.daily_summary`;
    that table MUST refresh every 24h. Alert if it doesn't."
  - "Source `raw.events` should be re-loaded every hour. Page if stale."

This is metadata-only — no rows read, no query cost, runs in < 1s.
"""

import json
import os
from typing import Any, Dict, Literal, Optional

import dagster as dg
from pydantic import Field


class BigqueryTableFreshnessCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Asset check that fails if a BigQuery table's last_modified_time is too old."""

    asset_key: str = Field(description="Asset key the check validates.")
    check_name: str = Field(default="bigquery_table_freshness_check")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    table_id: str = Field(description="Fully-qualified BQ table id: `project.dataset.table`.")

    max_age_minutes: int = Field(
        description="Max age of the table's last_modified_time. Check fails if exceeded.",
    )

    use_partition_freshness: bool = Field(
        default=False,
        description=(
            "If True and the table is partitioned, check the freshest partition's "
            "modification time instead of the table-level last_modified_time. "
            "Useful for ingest-time partitioned tables that grow daily."
        ),
    )

    severity: Literal["ERROR", "WARN"] = Field(default="ERROR")
    blocking: bool = Field(default=False, description="Default False — SLO checks usually alert, not block.")

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
        table_id = self.table_id
        max_age_minutes = self.max_age_minutes
        use_partition_freshness = self.use_partition_freshness
        severity = dg.AssetCheckSeverity[self.severity]

        @dg.asset_check(
            asset=asset_key,
            name=self.check_name,
            blocking=self.blocking,
            description=f"BigQuery freshness SLO check on `{table_id}` (max age {max_age_minutes}m).",
        )
        def _check(context) -> dg.AssetCheckResult:
            try:
                import datetime as dt
                from google.cloud import bigquery
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-bigquery google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = bigquery.Client(credentials=sa_creds)

            try:
                table = client.get_table(table_id)
            except Exception as e:
                return dg.AssetCheckResult(
                    passed=False,
                    severity=severity,
                    metadata={"error": dg.MetadataValue.text(f"get_table failed: {e}")},
                )

            last_modified = table.modified
            partition_label = "table"
            if use_partition_freshness and table.time_partitioning:
                # Query INFORMATION_SCHEMA for freshest partition modification time
                proj, dataset, tbl = table_id.split(".")
                sql = (
                    f"SELECT MAX(last_modified_time) AS last "
                    f"FROM `{proj}.{dataset}.INFORMATION_SCHEMA.PARTITIONS` "
                    f"WHERE table_name = '{tbl}'"
                )
                row = next(iter(client.query(sql).result()), None)
                if row and row["last"] is not None:
                    last_modified = row["last"]
                    partition_label = "freshest_partition"

            if last_modified is None:
                return dg.AssetCheckResult(
                    passed=False,
                    severity=severity,
                    metadata={"error": dg.MetadataValue.text("no modification time available")},
                )

            now = dt.datetime.now(dt.timezone.utc)
            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=dt.timezone.utc)
            age_minutes = (now - last_modified).total_seconds() / 60.0
            passed = age_minutes <= max_age_minutes

            return dg.AssetCheckResult(
                passed=passed,
                severity=severity if not passed else dg.AssetCheckSeverity.WARN,
                metadata={
                    "table_id":               dg.MetadataValue.text(table_id),
                    "last_modified":          dg.MetadataValue.text(last_modified.isoformat()),
                    "age_minutes":            dg.MetadataValue.float(round(age_minutes, 2)),
                    "max_age_minutes":        dg.MetadataValue.int(max_age_minutes),
                    "freshness_source":       dg.MetadataValue.text(partition_label),
                    "num_rows":               dg.MetadataValue.int(int(table.num_rows or 0)),
                },
            )

        return dg.Definitions(asset_checks=[_check])
