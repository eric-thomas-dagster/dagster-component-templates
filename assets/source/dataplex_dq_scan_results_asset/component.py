"""DataplexDqScanResultsAssetComponent — pull the latest Dataplex Data-Quality scan results.

Dataplex Data-Quality scans are managed, BQ-native DQ rule evaluations
(uniqueness, null-rate, regex matches, custom SQL, statistics range).
This component pulls the latest scan results into a Dagster asset so:

  - DQ failures land in the Dagster catalog alongside data
  - Downstream pipelines can gate on a check that wraps this asset
  - Historical DQ trends are queryable from a single source

Two modes:
  - **Latest only** (default): one scan_job per scan_id, returns each rule's
    pass/fail + threshold + assertion.
  - **History**: set `since_iso=`, gets all jobs since that timestamp.

Note: This is **observation only** — the component does not trigger new
scans. Scans run on Dataplex's own schedule. Trigger a scan via
`gcloud dataplex datascans run <scan_id>` or the REST API.
"""

import datetime as dt
import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class DataplexDqScanResultsAssetComponent(Component, Model, Resolvable):
    """Pull Dataplex Data-Quality scan results into a DataFrame."""

    asset_name: str = Field(description="Output asset name.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)
    location: str = Field(description="Dataplex region (e.g. 'us-central1').")
    scan_id: str = Field(description="Dataplex Data-Quality scan id.")

    mode: Literal["latest", "history"] = Field(
        default="latest",
        description="`latest`: most recent job only. `history`: all jobs since `since_iso`.",
    )
    since_iso: Optional[str] = Field(
        default=None,
        description="History-mode only: ISO-8601 timestamp lower bound (e.g. '2026-01-01T00:00:00Z').",
    )
    max_jobs: int = Field(default=50, description="Cap on jobs returned in history mode.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Useful for transient errors like network glitches or rate limits.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )
    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned.",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static partitioning, e.g. 'us,eu,asia'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition when partition_type='dynamic'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, DynamicPartitionsDefinition,
            )
            _pt = self.partition_type
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if _pt in ("daily", "weekly", "monthly", "hourly") and not self.partition_start:
                raise ValueError(f"partition_type={_pt!r} requires partition_start (ISO date).")
            if _pt == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=self.partition_start)
            elif _pt == "static":
                if not _values:
                    raise ValueError("partition_type='static' requires partition_values.")
                partitions_def = StaticPartitionsDefinition(_values)
            elif _pt == "dynamic":
                if not self.dynamic_partition_name:
                    raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
                partitions_def = DynamicPartitionsDefinition(name=self.dynamic_partition_name)

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy

            freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        if self.mode == "history" and not self.since_iso:
            raise ValueError("`since_iso` is required when mode='history'.")

        asset_name = self.asset_name
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        scan_id = self.scan_id
        mode = self.mode
        since_iso = self.since_iso
        max_jobs = self.max_jobs

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Dataplex DQ scan results: {project_id}/{location}/{scan_id}.",
            group_name=self.group_name,
            kinds={"google", "dataplex", "data-quality"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext):
            try:
                from google.cloud import dataplex_v1
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-dataplex google-auth")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = dataplex_v1.DataScanServiceClient(credentials=sa_creds)
            scan_resource = f"projects/{project_id}/locations/{location}/dataScans/{scan_id}"

            jobs_to_load: List[Any] = []
            if mode == "latest":
                jobs = list(client.list_data_scan_jobs(request={"parent": scan_resource, "page_size": 1}))
                jobs_to_load = jobs[:1]
            else:
                since_dt = dt.datetime.fromisoformat(since_iso.replace("Z", "+00:00"))  # type: ignore[arg-type]
                for j in client.list_data_scan_jobs(request={"parent": scan_resource, "page_size": min(100, max_jobs)}):
                    if j.start_time and j.start_time.replace(tzinfo=dt.timezone.utc) < since_dt:
                        break
                    jobs_to_load.append(j)
                    if len(jobs_to_load) >= max_jobs:
                        break

            if not jobs_to_load:
                context.log.warning(f"No scan jobs found for {scan_resource} matching the filter.")
                return Output(
                    value=pd.DataFrame(),
                    metadata={
                        "scan":     MetadataValue.text(scan_resource),
                        "jobs":     MetadataValue.int(0),
                        "preview":  MetadataValue.text("(no jobs)"),
                    },
                )

            rows: List[Dict[str, Any]] = []
            for job_summary in jobs_to_load:
                # Fetch FULL job to get rule-level results
                full = client.get_data_scan_job(
                    request={"name": job_summary.name, "view": dataplex_v1.GetDataScanJobRequest.DataScanJobView.FULL},
                )
                dq_result = full.data_quality_result
                base_row = {
                    "job_name":         full.name,
                    "job_state":        dataplex_v1.DataScanJob.State(full.state).name,
                    "start_time":       full.start_time,
                    "end_time":         full.end_time,
                    "passed":           dq_result.passed if dq_result else None,
                    "row_count":        dq_result.row_count if dq_result else None,
                    "scanned_data":     str(dq_result.scanned_data) if dq_result and dq_result.scanned_data else None,
                }
                if not dq_result or not (dq_result.rules or []):
                    rows.append({**base_row, "rule_index": None, "rule_passed": None, "rule_name": None})
                    continue
                for i, rule_result in enumerate(dq_result.rules or []):
                    rows.append({
                        **base_row,
                        "rule_index":         i,
                        "rule_passed":        rule_result.passed,
                        "rule_name":          getattr(rule_result.rule, "name", None) or f"rule_{i}",
                        "rule_dimension":     getattr(rule_result.rule, "dimension", None),
                        "rule_column":        getattr(rule_result.rule, "column", None),
                        "evaluated_count":    rule_result.evaluated_count,
                        "passing_count":      rule_result.passing_count,
                        "failing_rows_query": rule_result.failing_rows_query,
                    })

            df = pd.DataFrame(rows)
            failed_rules = int((~df["rule_passed"].fillna(True)).sum()) if "rule_passed" in df.columns and not df.empty else 0
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no results)"
            return Output(
                value=df,
                metadata={
                    "scan":          MetadataValue.text(scan_resource),
                    "jobs_loaded":   MetadataValue.int(len(jobs_to_load)),
                    "rule_rows":     MetadataValue.int(len(df)),
                    "failed_rules":  MetadataValue.int(failed_rules),
                    "preview":       MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
