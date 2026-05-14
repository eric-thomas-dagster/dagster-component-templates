"""CloudDlpInspectAssetComponent — scan DataFrame text columns for PII via Cloud DLP.

For each row of an upstream DataFrame, inspect one or more text columns
with Cloud DLP (Data Loss Prevention) and emit findings per row:
  - `dlp_finding_count`: total findings across all configured infoTypes
  - `dlp_infotypes`: list of distinct infoType names detected
  - `dlp_findings`: list of {infoType, quote, likelihood} dicts (capped)

Useful for:
  - Pre-warehouse PII screening (block load if PII found in unexpected columns)
  - Compliance reporting (which rows touch SSNs / payment cards / etc.)
  - Driving downstream redaction / encryption decisions

InfoTypes browseable at:
  https://cloud.google.com/dlp/docs/infotypes-reference
"""

import json
import os
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
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


class CloudDlpInspectAssetComponent(Component, Model, Resolvable):
    """Inspect DataFrame text columns for PII / sensitive data via Cloud DLP."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None)

    project_id: Optional[str] = Field(default=None)

    text_columns: List[str] = Field(
        description="Columns whose string values should be inspected. Each row's columns are joined with newlines before inspection.",
    )

    info_types: List[str] = Field(
        default_factory=lambda: [
            "EMAIL_ADDRESS",
            "PHONE_NUMBER",
            "US_SOCIAL_SECURITY_NUMBER",
            "CREDIT_CARD_NUMBER",
            "PERSON_NAME",
            "STREET_ADDRESS",
        ],
        description=(
            "List of Cloud DLP infoType names to detect. See "
            "https://cloud.google.com/dlp/docs/infotypes-reference"
        ),
    )

    min_likelihood: Literal[
        "LIKELIHOOD_UNSPECIFIED", "VERY_UNLIKELY", "UNLIKELY", "POSSIBLE",
        "LIKELY", "VERY_LIKELY",
    ] = Field(
        default="POSSIBLE",
        description="Minimum likelihood to surface as a finding.",
    )

    max_findings_per_row: int = Field(
        default=20,
        description="Cap on how many individual findings are recorded per row.",
    )

    include_quote: bool = Field(
        default=False,
        description=(
            "If True, include the matching substring in `dlp_findings`. "
            "Disabled by default — quotes can themselves be PII."
        ),
    )

    output_prefix: str = Field(default="dlp_", description="Column-name prefix for added analysis columns.")

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
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

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        text_columns = self.text_columns
        info_types_in = self.info_types
        min_likelihood = self.min_likelihood
        max_findings = self.max_findings_per_row
        include_quote = self.include_quote
        output_prefix = self.output_prefix

        @asset(
            name=asset_name,
            description=self.description or f"Cloud DLP inspect of {text_columns} in {project_id}.",
            group_name=self.group_name,
            kinds={"google", "cloud-dlp", "ai", "security"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                from google.cloud import dlp_v2
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-dlp google-auth")

            missing = [c for c in text_columns if c not in upstream.columns]
            if missing:
                raise ValueError(f"text_columns not in upstream: {missing}. Available: {list(upstream.columns)}")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = dlp_v2.DlpServiceClient(credentials=sa_creds)
            parent = f"projects/{project_id}"

            inspect_config = {
                "info_types":     [{"name": it} for it in info_types_in],
                "min_likelihood": min_likelihood,
                "limits":         {"max_findings_per_request": max_findings},
                "include_quote":  include_quote,
            }

            df = upstream.copy().reset_index(drop=True)
            per_row_count: List[int] = []
            per_row_types: List[List[str]] = []
            per_row_findings: List[List[Dict[str, Any]]] = []

            for _, row in df.iterrows():
                text = "\n".join(str(row[c]) for c in text_columns if bool(pd.notna(row[c])))
                if not text.strip():
                    per_row_count.append(0)
                    per_row_types.append([])
                    per_row_findings.append([])
                    continue

                resp = client.inspect_content(
                    request={
                        "parent": parent,
                        "inspect_config": inspect_config,
                        "item": {"value": text},
                    }
                )
                findings = list(resp.result.findings or [])
                row_findings: List[Dict[str, Any]] = []
                row_types: set = set()
                for f in findings[:max_findings]:
                    info_name = f.info_type.name
                    row_types.add(info_name)
                    entry = {
                        "info_type":  info_name,
                        "likelihood": dlp_v2.Likelihood(f.likelihood).name,
                    }
                    if include_quote:
                        entry["quote"] = f.quote
                    row_findings.append(entry)

                per_row_count.append(len(findings))
                per_row_types.append(sorted(row_types))
                per_row_findings.append(row_findings)

            df[f"{output_prefix}finding_count"] = per_row_count
            df[f"{output_prefix}infotypes"]    = per_row_types
            df[f"{output_prefix}findings"]     = per_row_findings

            total_findings = sum(per_row_count)
            rows_with_pii = sum(1 for c in per_row_count if c > 0)
            all_types: Dict[str, int] = {}
            for types in per_row_types:
                for t in types:
                    all_types[t] = all_types.get(t, 0) + 1

            preview_df = df[[*text_columns, f"{output_prefix}finding_count", f"{output_prefix}infotypes"]].head(10)
            return Output(
                value=df,
                metadata={
                    "row_count":         MetadataValue.int(len(df)),
                    "rows_with_pii":     MetadataValue.int(rows_with_pii),
                    "total_findings":    MetadataValue.int(total_findings),
                    "infotypes_seen":    MetadataValue.json(all_types),
                    "scanned_columns":   MetadataValue.json(text_columns),
                    "preview":           MetadataValue.md(preview_df.to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
