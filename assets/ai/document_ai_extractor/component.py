"""DocumentAiExtractorComponent — parse documents via Cloud Document AI.

Calls Google Cloud Document AI on each row of an upstream DataFrame
(document referenced by file path or `gs://` URI). Document AI's
processors handle structured parsing that flat OCR can't:

  - **Form parsers**: extract key/value pairs from any form
  - **Invoice / receipt / W2 / 1040 / paystub** processors: domain-tuned
  - **OCR processor**: rich OCR with layout + paragraphs + handwriting
  - **Layout parser**: full document structure (sections, tables, headers)
  - **Custom Document Extractor (CDE)**: your trained processor

The processor is identified by `processor_id` (from the Document AI
console). Use the `OCR_PROCESSOR` for general-purpose OCR; for forms
use the FORM_PARSER processor.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

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


_MIME_BY_EXT = {
    ".pdf":  "application/pdf",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif":  "image/gif",
    ".webp": "image/webp",
    ".tif":  "image/tiff",
    ".tiff": "image/tiff",
    ".bmp":  "image/bmp",
}


class DocumentAiExtractorComponent(Component, Model, Resolvable):
    """Run a Document AI processor over a column of document references."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    location: str = Field(default="us", description="Document AI region — `us` or `eu`.")
    processor_id: str = Field(
        description=(
            "Document AI processor id (UUID-style) from the console. "
            "Distinct from the processor TYPE — each processor is a deployed "
            "instance of a type."
        ),
    )
    processor_version: Optional[str] = Field(
        default=None,
        description="Optional pinned processor version (e.g. 'pretrained-form-parser-v2.0-2022-11-10'). Default: rc / stable.",
    )

    document_column: str = Field(description="Column with document file paths or `gs://` URIs.")
    mime_type_column: Optional[str] = Field(
        default=None,
        description="Optional column with explicit MIME type per row. Default: auto-detect from file extension.",
    )

    # Output toggles
    extract_text: bool = Field(default=True, description="Adds `doc_text` (full plain text).")
    extract_form_fields: bool = Field(
        default=True,
        description="Adds `doc_form_fields` (list of {name, value, confidence}). Only meaningful for FORM_PARSER and similar.",
    )
    extract_entities: bool = Field(
        default=True,
        description="Adds `doc_entities` (list of {type, mention_text, confidence, normalized_value}). Used by Invoice / Receipt / 1040 / etc.",
    )
    extract_tables: bool = Field(
        default=False,
        description="Adds `doc_tables` (list of tables as 2D string arrays).",
    )

    output_prefix: str = Field(default="doc_", description="Column-name prefix for added analysis columns.")

    rate_limit_delay: float = Field(default=0.0)
    max_retries: int = Field(default=3)

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

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        processor_id = self.processor_id
        processor_version = self.processor_version
        document_column = self.document_column
        mime_type_column = self.mime_type_column
        extract_text = self.extract_text
        extract_form_fields = self.extract_form_fields
        extract_entities = self.extract_entities
        extract_tables = self.extract_tables
        output_prefix = self.output_prefix
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries

        @asset(
            name=asset_name,
            description=self.description or f"Document AI extraction via processor {processor_id}.",
            group_name=self.group_name,
            kinds={"google", "document-ai", "ai"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                from google.cloud import documentai_v1 as documentai
                from google.oauth2 import service_account
                from google.api_core.client_options import ClientOptions
            except ImportError:
                raise ImportError("pip install google-cloud-documentai google-auth")

            if document_column not in upstream.columns:
                raise ValueError(f"document_column={document_column!r} not in upstream: {list(upstream.columns)}")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client_options = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
            client = documentai.DocumentProcessorServiceClient(credentials=sa_creds, client_options=client_options)

            if processor_version:
                resource = f"projects/{project_id}/locations/{location}/processors/{processor_id}/processorVersions/{processor_version}"
            else:
                resource = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

            df = upstream.copy().reset_index(drop=True)
            results: List[Dict[str, Any]] = []

            def _detect_mime(ref: str, override: Optional[str]) -> str:
                if override:
                    return str(override)
                ext = os.path.splitext(ref)[1].lower()
                return _MIME_BY_EXT.get(ext, "application/pdf")

            for i, row in df.iterrows():
                ref = str(row[document_column])
                mime = _detect_mime(ref, row[mime_type_column] if mime_type_column and mime_type_column in df.columns else None)
                row_out: Dict[str, Any] = {}

                # Build the request — local file (read + bytes) or gs:// URI.
                if ref.startswith("gs://"):
                    raw = documentai.RawDocument()  # not used; use gcs_document
                    request = documentai.ProcessRequest(
                        name=resource,
                        gcs_document=documentai.GcsDocument(gcs_uri=ref, mime_type=mime),
                    )
                else:
                    try:
                        with open(ref, "rb") as fh:
                            content = fh.read()
                    except Exception as e:
                        results.append({f"{output_prefix}error": f"could not read {ref}: {e}"})
                        continue
                    request = documentai.ProcessRequest(
                        name=resource,
                        raw_document=documentai.RawDocument(content=content, mime_type=mime),
                    )

                attempt = 0
                last_err = None
                resp = None
                while attempt <= max_retries:
                    try:
                        resp = client.process_document(request=request)
                        last_err = None
                        break
                    except Exception as e:
                        last_err = e
                        attempt += 1
                        if attempt > max_retries:
                            break
                        time.sleep((2 ** attempt) * 0.5)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    if "PERMISSION_DENIED" in err_str:
                        context.log.error("Document AI 403: SA needs roles/documentai.apiUser + API enabled.")
                    elif "404" in err_str or "NOT_FOUND" in err_str:
                        context.log.error(
                            f"Processor {processor_id!r} not found in {project_id}/{location}. "
                            f"Create one at https://console.cloud.google.com/ai/document-ai/processors?project={project_id}"
                        )
                    row_out[f"{output_prefix}error"] = err_str
                    results.append(row_out)
                    continue

                doc = resp.document
                if extract_text:
                    row_out[f"{output_prefix}text"] = doc.text or ""

                if extract_form_fields:
                    fields_out = []
                    for page in doc.pages or []:
                        for field in page.form_fields or []:
                            try:
                                name_text = _text_segment(doc.text, field.field_name) if field.field_name else None
                                value_text = _text_segment(doc.text, field.field_value) if field.field_value else None
                                fields_out.append({
                                    "name":       (name_text or "").strip(),
                                    "value":      (value_text or "").strip(),
                                    "confidence": float(field.field_value.confidence) if field.field_value else None,
                                })
                            except Exception:
                                continue
                    row_out[f"{output_prefix}form_fields"] = fields_out

                if extract_entities:
                    ents_out = []
                    for ent in doc.entities or []:
                        ents_out.append({
                            "type":             ent.type_,
                            "mention_text":     ent.mention_text,
                            "confidence":       float(ent.confidence or 0.0),
                            "normalized_value": ent.normalized_value.text if ent.normalized_value else None,
                        })
                    row_out[f"{output_prefix}entities"] = ents_out

                if extract_tables:
                    tables_out: List[List[List[str]]] = []
                    for page in doc.pages or []:
                        for tbl in page.tables or []:
                            grid: List[List[str]] = []
                            for tr in (tbl.body_rows or []) + (tbl.header_rows or []):
                                row_cells = []
                                for cell in tr.cells or []:
                                    row_cells.append((_text_segment(doc.text, cell.layout) or "").strip())
                                grid.append(row_cells)
                            tables_out.append(grid)
                    row_out[f"{output_prefix}tables"] = tables_out

                row_out[f"{output_prefix}page_count"] = len(doc.pages or [])
                results.append(row_out)
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            results_df = pd.DataFrame(results)
            out = pd.concat([df.reset_index(drop=True), results_df.reset_index(drop=True)], axis=1)
            ok = sum(1 for r in results if f"{output_prefix}error" not in r)
            return Output(
                value=out,
                metadata={
                    "rows":         MetadataValue.int(len(out)),
                    "ok":           MetadataValue.int(ok),
                    "processor":    MetadataValue.text(resource),
                    "preview":      MetadataValue.md(out.head(5).to_markdown(index=False) or ""),
                },
            )

        return Definitions(assets=[_asset])


def _text_segment(full_text: str, layout_or_anchor) -> Optional[str]:
    """Extract the substring of full_text covered by a Document AI text-anchor.

    Handles both Layout (with .text_anchor) and TextAnchor directly.
    """
    if layout_or_anchor is None:
        return None
    anchor = getattr(layout_or_anchor, "text_anchor", None) or layout_or_anchor
    segs = getattr(anchor, "text_segments", None) or []
    pieces = []
    for s in segs:
        start = int(getattr(s, "start_index", 0) or 0)
        end = int(getattr(s, "end_index", 0) or 0)
        if end > start:
            pieces.append(full_text[start:end])
    return "".join(pieces) or None
