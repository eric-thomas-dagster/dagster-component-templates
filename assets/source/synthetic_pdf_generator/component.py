"""SyntheticPdfGeneratorComponent — generate sample PDFs for OCR / Document AI demos.

Emits a DataFrame with one row per generated file:
  - `doc_id`         — caller-provided id
  - `kind`           — caller-provided category (`invoice`, `letter`, etc.)
  - `file_path`      — absolute path to the written PDF
  - `pages`          — number of pages

Useful as the upstream of `document_ai_extractor`, `vision_api_asset` (for
image rendering), or signature-extraction pipelines. Lets demos stay 100%
component-driven without hand-rolled PDF generators in `defs/`.

The component supports two modes:
  - **Built-in samples** (default, `samples: default`): a curated set of small
    PDFs designed to exercise OCR (invoice with totals, shipping letter with
    a tracking number, etc.). Stable across runs.
  - **Explicit `documents` list**: each entry is `{doc_id, kind, title, body}`.
    Each `body` is rendered as one PDF page.
"""

import os
from typing import Any, Dict, List, Optional

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


_DEFAULT_DOCS: List[Dict[str, Any]] = [
    {
        "doc_id": "INV-2026-0042",
        "kind":   "invoice",
        "title":  "INVOICE #INV-2026-0042",
        "body": [
            "From: Acme Corp",
            "To:   Bob Smith",
            "Date: 2026-05-11",
            "",
            "Subtotal:   $1,200.00",
            "Tax:        $96.00",
            "Total:      $1,296.00",
        ],
    },
    {
        "doc_id": "shipping-notice",
        "kind":   "letter",
        "title":  "Dear Customer,",
        "body": [
            "Thank you for your recent purchase. Your order has shipped",
            "via UPS tracking number 1Z999AA10123456784.",
            "Estimated delivery is 2026-05-15.",
            "",
            "Best regards,",
            "Acme Customer Care",
        ],
    },
]


class SyntheticPdfGeneratorComponent(Component, Model, Resolvable):
    """Generate sample PDFs and emit a DataFrame describing them."""

    asset_name: str = Field(description="Output asset name.")

    output_dir: str = Field(
        default="/tmp/synthetic_pdfs",
        description="Filesystem directory to write PDFs into (created if missing).",
    )

    samples: str = Field(
        default="default",
        description="`default` to use the built-in 2-doc sample set, or `custom` to use `documents` instead.",
    )

    documents: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "When samples='custom', list of {doc_id, kind, title, body} dicts. "
            "`body` is a list of strings rendered as separate lines."
        ),
    )

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

        asset_name = self.asset_name
        output_dir = self.output_dir
        if self.samples == "custom":
            if not self.documents:
                raise ValueError("samples='custom' requires `documents` to be set.")
            docs = self.documents
        else:
            docs = _DEFAULT_DOCS

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Synthetic PDFs ({len(docs)}) for OCR / Document AI demos.",
            group_name=self.group_name,
            kinds={"reportlab"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext) -> Output:
            try:
                from reportlab.lib.pagesizes import letter
                from reportlab.pdfgen import canvas
            except ImportError:
                raise ImportError("pip install reportlab")

            os.makedirs(output_dir, exist_ok=True)
            rows: List[Dict[str, Any]] = []
            for d in docs:
                doc_id = str(d["doc_id"])
                kind = str(d.get("kind", "doc"))
                title = str(d.get("title", doc_id))
                body_lines = d.get("body") or []
                path = os.path.join(output_dir, f"{doc_id}.pdf")
                c = canvas.Canvas(path, pagesize=letter)
                c.setFont("Helvetica-Bold", 14)
                c.drawString(72, 720, title)
                c.setFont("Helvetica", 12)
                y = 690
                for line in body_lines:
                    c.drawString(72, y, str(line))
                    y -= 20
                c.save()
                rows.append({"doc_id": doc_id, "kind": kind, "file_path": path, "pages": 1})

            df = pd.DataFrame(rows)
            return Output(
                value=df,
                metadata={
                    "output_dir":  MetadataValue.path(output_dir),
                    "doc_count":   MetadataValue.int(len(df)),
                    "preview":     MetadataValue.md(df.to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])
