"""Structured Document Extractor Component.

Extract structured fields from ANY document type using an LLM via litellm --
one component, a `document_type` parameter picks a sensible default field
list (invoice, receipt, resume, ...), or `custom` for your own.

Consolidates what used to be 13 near-identical components
(invoice_extractor, receipt_extractor, bank_statement_extractor,
expense_report_extractor, purchase_order_extractor, shipping_label_extractor,
contract_extractor, legal_document_extractor, insurance_claim_extractor,
medical_record_extractor, resume_extractor, job_posting_extractor,
scientific_paper_extractor) -- confirmed by diffing them directly: identical
upstream_asset_key/input_column/model/api_key_env_var/output_fields/
batch_size fields and identical LLM-call logic, differing only in the
DEFAULT value of output_fields and the prompt's document-type wording.
Those components are kept for backward compatibility (existing YAML
referencing them keeps working) but new usage should prefer this one --
same mechanism, `document_type` picks the preset instead of picking a
whole separate component.
"""

from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import tempfile
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
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.
    Canonical implementation — copied as-is per FIELD_CONVENTIONS.md."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# Default output_fields per document_type -- copied verbatim from each of
# the 13 single-purpose extractors' own defaults (diffed directly against
# their real source, not guessed), so picking document_type="invoice" here
# behaves identically to installing invoice_extractor with its stock config.
_PRESET_FIELDS: Dict[str, List[str]] = {
    "invoice": ["invoice_number", "date", "vendor", "total_amount", "line_items", "tax", "currency"],
    "receipt": ["merchant_name", "merchant_address", "date", "time", "items", "subtotal", "tax", "total", "payment_method", "card_last_four", "receipt_number"],
    "bank_statement": ["account_number", "account_holder", "bank_name", "statement_period", "opening_balance", "closing_balance", "transactions", "total_credits", "total_debits"],
    "expense_report": ["employee_name", "employee_id", "department", "report_date", "period_start", "period_end", "line_items", "total_amount", "currency", "approver", "status"],
    "purchase_order": ["po_number", "vendor", "buyer", "issue_date", "delivery_date", "line_items", "subtotal", "tax", "total", "payment_terms", "shipping_address", "billing_address"],
    "shipping_label": ["tracking_number", "carrier", "service_type", "sender_name", "sender_address", "recipient_name", "recipient_address", "weight", "dimensions", "ship_date", "estimated_delivery"],
    "contract": ["contract_type", "parties", "effective_date", "expiration_date", "governing_law", "payment_terms", "termination_clause", "liability_cap", "signatures"],
    "legal_document": ["document_type", "jurisdiction", "court", "case_number", "parties", "filing_date", "key_dates", "relief_sought", "defined_terms", "obligations", "penalties"],
    "insurance_claim": ["claim_id", "policy_number", "insurer", "claimant_name", "claimant_contact", "incident_date", "incident_description", "damage_type", "claimed_amount", "adjuster", "status"],
    "medical_record": ["patient_name", "dob", "provider", "visit_date", "chief_complaint", "diagnoses", "icd_codes", "medications", "procedures", "cpt_codes", "follow_up"],
    "resume": ["name", "email", "phone", "location", "summary", "skills", "experience", "education", "certifications", "languages"],
    "job_posting": ["job_title", "company", "location", "remote_policy", "employment_type", "salary_range", "required_skills", "preferred_skills", "experience_required", "education_required", "responsibilities", "benefits", "application_deadline"],
    "scientific_paper": ["title", "authors", "journal", "publication_date", "doi", "abstract", "keywords", "methodology", "key_findings", "limitations", "citations_count", "data_availability"],
}
_DOCUMENT_TYPES = sorted(_PRESET_FIELDS.keys()) + ["custom"]


def _list_files_direct(path: str, max_files: Optional[int], download: bool, download_dir: Optional[str]) -> "pd.DataFrame":
    """List (and by default download) files matching `path` directly --
    same logic as file_lister's own _list_files, duplicated rather than
    imported since community components are standalone files. Used by
    the `path` input mode below so this component can be the upstream
    itself instead of requiring a separate file_lister asset -- only
    needed when the user actually wants a shared, reusable listing asset
    (e.g. multiple extractors reading the same source); a one-off
    extraction over a bucket/folder shouldn't need a second component.
    """
    import fsspec

    fs, _, paths = fsspec.get_fs_token_paths(path)
    if len(paths) == 1 and fs.isdir(paths[0]):
        paths = [p for p in fs.ls(paths[0], detail=False) if not fs.isdir(p)]
    if max_files is not None:
        paths = paths[:max_files]

    rows = []
    for p in paths:
        filename = p.rsplit("/", 1)[-1]
        local_path = p
        if download:
            cache_dir_p = Path(download_dir) if download_dir else Path(tempfile.gettempdir()) / "structured_document_extractor"
            cache_dir_p.mkdir(parents=True, exist_ok=True)
            target = cache_dir_p / filename
            fs.get(p, str(target))
            local_path = str(target)
        rows.append({"path": p, "local_path": local_path, "filename": filename})
    return pd.DataFrame(rows, columns=["path", "local_path", "filename"])


class StructuredDocumentExtractorComponent(Component, Model, Resolvable):
    """Extract structured fields from any document type using an LLM.

    Each row in the upstream DataFrame is treated as one document. The LLM
    extracts the requested fields and returns them as additional columns.

    Set `document_type` to one of the built-in presets (invoice, receipt,
    bank_statement, expense_report, purchase_order, shipping_label,
    contract, legal_document, insurance_claim, medical_record, resume,
    job_posting, scientific_paper) to get a sensible default `output_fields`
    list for that kind of document -- or `custom` and set `output_fields`
    yourself for anything else. Setting `output_fields` explicitly always
    overrides the preset, even when `document_type` is one of the presets,
    so you can start from a preset and tweak it.

    Features:
    - One component covers every document type -- swap `document_type`
      instead of installing a different component per kind of document.
    - Works with any litellm-compatible model (OpenAI, Anthropic, etc.)
    - Batch processing with configurable batch size
    - Null-safe: missing fields become None rather than raising errors
    - Supports raw text or file path inputs (pairs with file_lister's
      `local_path` column for a file-based source)
    """

    model_config = ConfigDict(populate_by_name=True)
    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream asset key providing a DataFrame with document content. "
            "Mutually exclusive with `path` -- set exactly one. Use this when "
            "another asset (e.g. file_lister) already lists/produces the "
            "documents; use `path` when this component should list them itself."
        ),
    )
    path: Optional[str] = Field(
        default=None,
        description=(
            "Glob pattern or fsspec URI to list documents from directly (s3://, "
            "gs://, abfss:// / abfs:// / az://, or a local path), e.g. "
            "'s3://my-bucket/invoices/**/*.pdf'. Mutually exclusive with "
            "`upstream_asset_key` -- set exactly one. No separate file_lister "
            "asset needed; this becomes a root asset that lists (and by "
            "default downloads) the files itself, then extracts from them."
        ),
    )
    download: bool = Field(
        default=True,
        description="When using `path`: download each matched file to a local cache directory first. Ignored when using `upstream_asset_key`.",
    )
    download_dir: Optional[str] = Field(
        default=None,
        description="When using `path` with download=true: local cache directory. Auto-generated under the system temp dir if unset.",
    )
    max_files: Optional[int] = Field(
        default=None,
        description="When using `path`: safety cap on how many matched files to process in one materialize.",
    )
    document_type: str = Field(
        default="custom",
        description=(
            "Picks a default output_fields preset: "
            + ", ".join(sorted(_PRESET_FIELDS.keys()))
            + ", or 'custom' (requires output_fields to be set explicitly)."
        ),
    )
    input_column: Union[str, int] = Field(
        default="local_path",
        description="Column with document content (text or file path) -- 'local_path' pairs directly with file_lister's output.",
    )
    input_type: str = Field(
        default="file",
        description="Input type: 'text' (raw document text already in the column) or 'file' (read the file at that path).",
    )
    model_id: str = Field(
        alias="model",
        default="gpt-4o", description="LLM model name (litellm format)")
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable name holding the API key",
    )
    output_fields: Optional[List[str]] = Field(
        default=None,
        description=(
            "Fields to extract from each document. Overrides the "
            "document_type preset when set. Required when document_type='custom'."
        ),
    )
    batch_size: int = Field(default=5, description="Number of documents per LLM batch")
    post_process: str = Field(
        default="none",
        description=(
            "What to do with each SOURCE file (not the LLM output) once it's "
            "been successfully extracted: 'none' (leave in place -- the same "
            "files get reprocessed on every materialize, so `path` mode with "
            "'none' is only really safe for a fixed, one-off batch), 'move' "
            "(move to post_process_dir, so future runs only see new files), "
            "or 'delete' (remove permanently). Only ever applied to rows that "
            "extracted successfully -- a row that failed keeps its source "
            "file in place so the next run retries it. Requires "
            "input_type='file' (there's no file to act on for raw-text rows)."
        ),
    )
    post_process_dir: Optional[str] = Field(
        default=None,
        description="Destination directory when post_process='move'. Same fsspec scheme as the source file (local, s3://, gs://, ...). Required when post_process='move'.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )
    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output data in metadata (first 5 rows as a markdown table). Used by builder UIs to render asset shape without warehouse access.",
    )
    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description="Rows to include in the preview metadata when include_preview_metadata is True.",
    )
    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        direct_path = self.path
        direct_download = self.download
        direct_download_dir = self.download_dir
        direct_max_files = self.max_files
        input_column = self.input_column
        input_type = self.input_type
        model = self.model_id
        api_key_env_var = self.api_key_env_var
        document_type = self.document_type
        batch_size = self.batch_size
        post_process = self.post_process
        post_process_dir = self.post_process_dir

        _modes_set = sum(bool(x) for x in (upstream_asset_key, direct_path))
        if _modes_set != 1:
            raise ValueError(
                f"{asset_name}: must set exactly one of `upstream_asset_key` "
                f"(read from an existing asset) or `path` (list files directly, "
                f"no separate asset needed) -- got {_modes_set} set."
            )

        if post_process not in ("none", "move", "delete"):
            raise ValueError(f"{asset_name}: post_process must be 'none', 'move', or 'delete', got {post_process!r}.")
        if post_process == "move" and not post_process_dir:
            raise ValueError(f"{asset_name}: post_process='move' requires post_process_dir.")
        if post_process != "none" and input_type != "file":
            raise ValueError(
                f"{asset_name}: post_process={post_process!r} requires input_type='file' "
                f"-- there's no source file to act on when input_type='text'."
            )

        if document_type != "custom" and document_type not in _PRESET_FIELDS:
            raise ValueError(
                f"{asset_name}: unknown document_type {document_type!r}. "
                f"Must be one of: {_DOCUMENT_TYPES}"
            )
        output_fields = self.output_fields or _PRESET_FIELDS.get(document_type)
        if not output_fields:
            raise ValueError(
                f"{asset_name}: document_type='custom' requires output_fields to be set explicitly "
                f"(no preset exists for 'custom')."
            )

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _inferred_kinds = self.kinds or ["python"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        _doc_label = document_type.replace("_", " ") if document_type != "custom" else "document"

        def _do_extraction(context: AssetExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            """Shared extraction logic for both input modes -- given a
            DataFrame with `input_column` already populated (file paths or
            raw text), extract `output_fields` from each row via LLM and
            return the enriched DataFrame + metadata."""
            import os
            import json

            try:
                from litellm import completion
            except ImportError:
                raise ImportError("litellm required: pip install litellm")

            _original_cols = set(df.columns)
            results = []
            # One entry per row, aligned with `results` -- the fsspec path to
            # act on for post_process (prefers the original source `path`
            # column when present, e.g. file_lister/`_list_files_direct`'s
            # own output, over `input_column`, which in `path` mode with
            # download=true is a local cache copy, not the real source
            # location that needs to stop showing up in future listings)
            # and whether extraction actually succeeded for that row.
            _post_process_rows: list[tuple[str, bool]] = []
            has_path_col = "path" in df.columns
            total = len(df)
            context.log.info(f"Extracting {_doc_label} fields from {total} rows using {model}")

            for i in range(0, total, batch_size):
                batch = df.iloc[i: i + batch_size]
                context.log.info(f"Processing batch {i // batch_size + 1}/{(total - 1) // batch_size + 1}")
                for _, row in batch.iterrows():
                    file_ref = str(row[input_column])
                    source_path = str(row["path"]) if has_path_col and pd.notna(row["path"]) else file_ref
                    content = file_ref
                    if input_type == "file":
                        try:
                            with open(content, "r", encoding="utf-8", errors="replace") as fh:
                                content = fh.read()
                        except Exception as e:
                            context.log.warning(f"Could not read file {content}: {e}")

                    prompt = (
                        f"Extract the following fields from this {_doc_label} as JSON: {output_fields}\n\n"
                        f"Document content:\n{content}\n\n"
                        "Return only a JSON object with the requested fields. Use null for missing fields."
                    )
                    success = False
                    try:
                        resp = completion(
                            model=model,
                            messages=[{"role": "user", "content": prompt}],
                            api_key=os.environ.get(api_key_env_var),
                        )
                        raw = resp.choices[0].message.content
                        raw = raw.strip()
                        if raw.startswith("```"):
                            raw = raw.split("```")[1]
                            if raw.startswith("json"):
                                raw = raw[4:]
                        extracted = json.loads(raw)
                        success = True
                    except Exception as e:
                        context.log.warning(f"Extraction failed for row: {e}")
                        extracted = {f: None for f in output_fields}
                    results.append(extracted)
                    _post_process_rows.append((source_path, success))

            extracted_df = pd.DataFrame(results)
            for col in extracted_df.columns:
                df[col] = extracted_df[col].values

            if post_process != "none":
                import fsspec

                moved, deleted, failed = 0, 0, 0
                for source_path, success in _post_process_rows:
                    if not success:
                        continue
                    try:
                        fs, _, [resolved] = fsspec.get_fs_token_paths(source_path)
                        if post_process == "delete":
                            fs.rm(resolved)
                            deleted += 1
                        else:
                            filename = resolved.rsplit("/", 1)[-1]
                            dest_fs, _, [dest_dir] = fsspec.get_fs_token_paths(post_process_dir)
                            dest_fs.makedirs(dest_dir, exist_ok=True)
                            fs.mv(resolved, f"{dest_dir.rstrip('/')}/{filename}")
                            moved += 1
                    except Exception as e:
                        failed += 1
                        context.log.warning(f"post_process={post_process!r} failed for {source_path!r}: {e}")
                context.log.info(f"post_process={post_process!r}: moved={moved} deleted={deleted} failed={failed}")
                context.add_output_metadata({
                    "post_process_moved": moved,
                    "post_process_deleted": deleted,
                    "post_process_failed": failed,
                })

            context.add_output_metadata({
                "num_documents": total,
                "document_type": document_type,
                "extracted_fields": output_fields,
                "model": model,
            })
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    context.add_output_metadata({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")

            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[TableColumn(name=str(col), type=str(df.dtypes[col])) for col in df.columns])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            _effective_lineage = column_lineage
            if not _effective_lineage:
                _effective_lineage = {col.name: [col.name] for col in _col_schema.columns if col.name in _original_cols}
            if _effective_lineage and upstream_asset_key:
                # Only meaningful in upstream mode -- direct-path mode has
                # no real Dagster asset to link column lineage to (the
                # files came straight from object storage, not another
                # asset), so upstream_asset_key is None there and this is
                # skipped entirely.
                _upstream_key = AssetKey.from_user_string(upstream_asset_key)
                _lineage_deps = {
                    str(out_col): [TableColumnDep(asset_key=_upstream_key, column_name=str(ic)) for ic in in_cols]
                    for out_col, in_cols in _effective_lineage.items()
                }
                _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(TableColumnLineage(_lineage_deps))
            context.add_output_metadata(_metadata)
            return df

        asset_kwargs = dict(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            partitions_def=partitions_def,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            group_name=self.group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )

        if direct_path:
            @asset(**asset_kwargs)
            def _asset(context: AssetExecutionContext) -> pd.DataFrame:
                context.log.info(f"Listing files matching {direct_path!r}")
                df = _list_files_direct(direct_path, direct_max_files, direct_download, direct_download_dir)
                context.log.info(f"Found {len(df)} file(s)")
                return _do_extraction(context, df)
        else:
            @asset(ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}, **asset_kwargs)
            def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
                if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                    upstream = upstream.value
                if isinstance(upstream, dict):
                    _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
                if context.has_partition_key:
                    _pk = context.partition_key
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                    if partition_date_column and partition_date_column in upstream.columns and _date_key:
                        upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                    if partition_static_column and partition_static_column in upstream.columns and _static_key:
                        upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                        upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
                return _do_extraction(context, upstream.copy())

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[_asset])
        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))
