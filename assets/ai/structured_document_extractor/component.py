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
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
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


_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".tif"}
_MIME_BY_EXT = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif",
    ".webp": "image/webp", ".bmp": "image/bmp", ".tiff": "image/tiff", ".tif": "image/tiff",
}


def _extract_file_content(path: str) -> "tuple[Optional[str], Optional[dict]]":
    """Read a document/image file for the LLM prompt -- returns
    (text, image_content_block), exactly one non-None.

    Previously this just opened every file in TEXT mode with
    errors='replace' regardless of extension, so a real PDF or image got
    read as raw-bytes-decoded-as-UTF-8 garbage and fed straight into the
    prompt -- silently not extracting anything real for the component's
    stated primary use case. Images are base64-encoded into a litellm
    vision content block (same pattern as image_llm_extractor); PDFs use
    pdfplumber to pull embedded text (same library as pdf_text_extractor).
    A PDF with no extractable text (a scanned/photographed page rather
    than a digitally-generated one) raises rather than silently sending
    empty content to the LLM -- true OCR-of-scanned-PDFs isn't supported
    yet (see README); route those through ocr_extractor/document_ai_
    extractor first, or convert pages to images and use `path` mode here.
    """
    import base64
    ext = Path(path).suffix.lower()
    if ext in _IMAGE_EXTENSIONS:
        with open(path, "rb") as f:
            img_data = base64.b64encode(f.read()).decode("utf-8")
        mime = _MIME_BY_EXT.get(ext, "image/jpeg")
        return None, {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{img_data}"}}
    if ext == ".pdf":
        try:
            import pdfplumber
        except ImportError:
            raise ImportError("PDF extraction requires pdfplumber: pip install pdfplumber")
        with pdfplumber.open(path) as pdf:
            text = "\n".join((page.extract_text() or "") for page in pdf.pages)
        if not text.strip():
            raise ValueError(
                f"No extractable text in {path!r} -- likely a scanned/image-based PDF, "
                "which isn't OCR'd here (only digitally-generated PDF text and image "
                "files are supported directly)."
            )
        return text, None
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return fh.read(), None


def _crop_region_blocks(path: str, field_regions: Dict[str, Dict[str, float]]) -> List[dict]:
    """Crop each named field's region out of an image file and return
    interleaved [text-label, image_url] content blocks for each -- sent
    alongside the full image as a close-up HINT, not a hard constraint,
    since a region was drawn against ONE sample document and a real
    batch's layout can shift. Field-location UI lives in Dagster Designer
    (its own draw-a-box tool); this is just what consumes the result.
    """
    try:
        from PIL import Image
    except ImportError:
        raise ImportError("field_regions requires Pillow: pip install Pillow")
    import base64
    import io

    img = Image.open(path)
    w, h = img.size
    blocks: List[dict] = []
    for field, region in field_regions.items():
        left = int(region["x"] * w)
        top = int(region["y"] * h)
        right = int((region["x"] + region["width"]) * w)
        bottom = int((region["y"] + region["height"]) * h)
        if right <= left or bottom <= top:
            continue
        crop = img.crop((left, top, right, bottom)).convert("RGB")
        buf = io.BytesIO()
        crop.save(buf, format="PNG")
        crop_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        blocks.append({"type": "text", "text": f"Close-up crop of approximately where '{field}' appears (a hint, not a hard constraint):"})
        blocks.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{crop_b64}"}})
    return blocks


def build_extraction_messages(
    file_ref: str,
    *,
    input_type: str,
    output_fields: List[str],
    doc_label: str = "document",
    max_content_chars: Optional[int] = None,
    field_regions: Optional[Dict[str, Dict[str, float]]] = None,
    warn: Any = None,
) -> List[dict]:
    """The exact chat messages sent to the model for one document.

    Module-level (not buried in the asset) so anything that wants to preview an extraction, such as a builder UI's "try it on
    these samples", sends the SAME prompt production will -- a preview that differs from the real run is worse than none.
    """
    image_block = None
    if input_type == "file":
        text_content, image_block = _extract_file_content(file_ref)
        content = text_content if text_content is not None else file_ref
    else:
        content = file_ref

    if image_block is None and max_content_chars is not None and len(content) > max_content_chars:
        content = content[:max_content_chars]

    if image_block is not None:
        prompt_text = (
            f"Extract the following fields from this {doc_label} image as JSON: {output_fields}\n\n"
            "Return only a JSON object with the requested fields. Use null for missing fields."
        )
        message_content: Any = [{"type": "text", "text": prompt_text}, image_block]
        if field_regions:
            try:
                message_content += _crop_region_blocks(file_ref, field_regions)
            except Exception as e:
                if warn:
                    warn(f"Could not build field-location crops for {file_ref!r}: {e}")
    else:
        message_content = (
            f"Extract the following fields from this {doc_label} as JSON: {output_fields}\n\n"
            f"Document content:\n{content}\n\n"
            "Return only a JSON object with the requested fields. Use null for missing fields."
        )
    return [{"role": "user", "content": message_content}]


def parse_extraction_response(raw: str, output_fields: List[str]) -> Dict[str, Any]:
    """Parse the model's reply into exactly `output_fields` (anything it omitted becomes None, anything extra is dropped)."""
    import json

    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    extracted = json.loads(raw)
    if not isinstance(extracted, dict):
        raise ValueError(f"LLM returned {type(extracted).__name__}, expected a JSON object")
    # Keeps every row's dict shape identical regardless of what the model chose to include.
    return {f: extracted.get(f) for f in output_fields}


def extract_one(
    file_ref: str,
    *,
    input_type: str = "file",
    output_fields: List[str],
    doc_label: str = "document",
    model: str,
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    llm_max_retries: int = 2,
    max_content_chars: Optional[int] = 20000,
    field_regions: Optional[Dict[str, Dict[str, float]]] = None,
    warn: Any = None,
) -> "tuple[Dict[str, Any], str]":
    """Extract `output_fields` from ONE document. Returns (fields, raw model reply); raises on any failure.

    The asset calls this per row and turns an exception into an all-None row; a builder UI calls it directly to show a
    sample's result (and the raw reply when it goes wrong).
    """
    try:
        from litellm import completion
    except ImportError:
        raise ImportError("litellm required: pip install litellm")

    messages = build_extraction_messages(
        file_ref,
        input_type=input_type,
        output_fields=output_fields,
        doc_label=doc_label,
        max_content_chars=max_content_chars,
        field_regions=field_regions,
        warn=warn,
    )
    extra: Dict[str, Any] = {}
    if api_base:
        extra["api_base"] = api_base
    resp = completion(
        model=model,
        messages=messages,
        api_key=api_key,
        num_retries=llm_max_retries,
        **extra,
    )
    raw = resp.choices[0].message.content
    return parse_extraction_response(raw, output_fields), raw


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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


_VALIDATION_KEYS = {"required_fields", "reconcile", "min_pass_rate"}
_RECONCILE_KEYS = {"total_field", "items_field", "item_amount_key", "add_fields", "tolerance"}
QUALITY_CHECK_NAME = "extraction_quality"
STATUS_COLUMN = "_validation_status"
ISSUES_COLUMN = "_validation_issues"


def _to_number(value: Any) -> Optional[float]:
    """A float from a number or a money-ish string ("$1,250.50"); None when it isn't one."""
    import re

    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value == value else None
    if isinstance(value, str):
        cleaned = re.sub(r"[$€£¥,\s]", "", value.strip())
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", cleaned):
            return float(cleaned)
    return None


def _is_empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip()) or (isinstance(value, (list, dict)) and not value)


def _line_items(value: Any) -> Optional[list]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip().startswith("["):
        import json

        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else None
        except ValueError:
            return None
    return None


def normalize_validation(asset_name: str, raw: Optional[Dict[str, Any]], output_fields: List[str]) -> Optional[Dict[str, Any]]:
    """Validate and complete the `validation` setting. None means no validation was asked for."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(f"{asset_name}: validation must be a mapping, got {type(raw).__name__}.")
    unknown = set(raw) - _VALIDATION_KEYS
    if unknown:
        raise ValueError(f"{asset_name}: unknown validation key(s) {sorted(unknown)}; allowed: {sorted(_VALIDATION_KEYS)}.")
    required = list(raw.get("required_fields") or [])
    missing = [f for f in required if f not in output_fields]
    if missing:
        raise ValueError(f"{asset_name}: validation.required_fields {missing} are not in output_fields {output_fields}.")
    min_pass_rate = raw.get("min_pass_rate", 0.9)
    if not isinstance(min_pass_rate, (int, float)) or isinstance(min_pass_rate, bool) or not 0 <= min_pass_rate <= 1:
        raise ValueError(f"{asset_name}: validation.min_pass_rate must be between 0 and 1, got {min_pass_rate!r}.")
    reconcile = raw.get("reconcile")
    if reconcile is not None:
        if not isinstance(reconcile, dict):
            raise ValueError(f"{asset_name}: validation.reconcile must be a mapping.")
        bad = set(reconcile) - _RECONCILE_KEYS
        if bad:
            raise ValueError(f"{asset_name}: unknown validation.reconcile key(s) {sorted(bad)}; allowed: {sorted(_RECONCILE_KEYS)}.")
        for key in ("total_field", "items_field"):
            if reconcile.get(key) not in output_fields:
                raise ValueError(f"{asset_name}: validation.reconcile.{key} must name one of output_fields {output_fields}.")
        extra = [f for f in (reconcile.get("add_fields") or []) if f not in output_fields]
        if extra:
            raise ValueError(f"{asset_name}: validation.reconcile.add_fields {extra} are not in output_fields.")
        tolerance = reconcile.get("tolerance", 0.01)
        if not isinstance(tolerance, (int, float)) or isinstance(tolerance, bool) or tolerance < 0:
            raise ValueError(f"{asset_name}: validation.reconcile.tolerance must be >= 0, got {tolerance!r}.")
        reconcile = {
            "total_field": reconcile["total_field"],
            "items_field": reconcile["items_field"],
            "item_amount_key": reconcile.get("item_amount_key", "amount"),
            "add_fields": list(reconcile.get("add_fields") or []),
            "tolerance": float(tolerance),
        }
    return {"required_fields": required, "reconcile": reconcile, "min_pass_rate": float(min_pass_rate)}


def validate_extraction(fields: Dict[str, Any], extracted_ok: bool, validation: Dict[str, Any]) -> List[str]:
    """Reasons one document needs a person to look at it (empty = fine). `validation` comes from normalize_validation."""
    if not extracted_ok:
        return ["extraction_failed"]
    issues = [f"missing:{f}" for f in validation["required_fields"] if _is_empty(fields.get(f))]
    reconcile = validation["reconcile"]
    if reconcile:
        total = _to_number(fields.get(reconcile["total_field"]))
        items = _line_items(fields.get(reconcile["items_field"]))
        amounts = [_to_number(i.get(reconcile["item_amount_key"])) for i in (items or []) if isinstance(i, dict)]
        if total is not None and items and amounts and all(a is not None for a in amounts):
            adjustments = [_to_number(fields.get(f)) or 0.0 for f in reconcile["add_fields"]]
            expected = sum(amounts) + sum(adjustments)
            if abs(expected - total) > reconcile["tolerance"]:
                issues.append(f"totals_mismatch:items+adjustments={expected:g},total={total:g}")
    return issues


def quality_summary(df: "pd.DataFrame", source_column: str, min_pass_rate: float, max_listed: int = 50) -> Dict[str, Any]:
    """Pass rate and the documents to review, from the validation columns of an output frame."""
    total = len(df)
    flagged = df[df[STATUS_COLUMN] != "ok"] if total else df
    passed_count = total - len(flagged)
    pass_rate = (passed_count / total) if total else 1.0
    review = [
        {"source": str(row[source_column]), "issues": str(row[ISSUES_COLUMN])}
        for _, row in flagged.head(max_listed).iterrows()
    ]
    return {
        "documents": total,
        "ok": passed_count,
        "needs_review": len(flagged),
        "pass_rate": pass_rate,
        "passed": pass_rate >= min_pass_rate,
        "review": review,
    }


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
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
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
    api_base_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Optional environment variable holding an API base URL, for a self-hosted or local OpenAI-compatible model "
            "(Ollama, LM Studio, vLLM...), e.g. OLLAMA_API_BASE=http://localhost:11434 with model 'ollama/llava'. "
            "Unset = the provider's default endpoint."
        ),
    )
    output_fields: Optional[List[str]] = Field(
        default=None,
        description=(
            "Fields to extract from each document. Overrides the "
            "document_type preset when set. Required when document_type='custom'."
        ),
    )
    batch_size: int = Field(default=5, description="Number of documents per LLM batch")
    llm_max_retries: int = Field(
        default=2,
        description="Retry a document's LLM call up to this many times on transient errors (rate limits, timeouts) before giving up on that row -- forwarded to litellm's own num_retries.",
    )
    max_content_chars: Optional[int] = Field(
        default=20000,
        description="Truncate extracted document text to this many characters before prompting the LLM -- guards against blowing the model's context window or racking up cost on unusually large text-PDF/text-input rows. Doesn't apply to image rows (sent as a vision content block, not text). Set to null to disable.",
    )
    field_regions: Optional[Dict[str, Dict[str, float]]] = Field(
        default=None,
        description=(
            "Optional per-field bounding boxes (fractional 0-1 coordinates: "
            "x, y, width, height) marking approximately where each field "
            "appears on the document, e.g. {'invoice_number': {'x': 0.6, "
            "'y': 0.05, 'width': 0.3, 'height': 0.08}}. Only meaningful for "
            "image documents (input_type='file' with an image extension) -- "
            "silently ignored for PDF/text. Each region is cropped and sent "
            "to the LLM as a labeled close-up alongside the full image, as "
            "a HINT, not a hard constraint (a real batch's layout can shift "
            "slightly from whatever single sample document this was drawn "
            "against)."
        ),
    )
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
    validation: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Flag documents that need a person to look at them, and emit one asset check (`extraction_quality`) on the "
            "share that passed. `{required_fields: [...], reconcile: {total_field, items_field, item_amount_key: amount, "
            "add_fields: [tax], tolerance: 0.01}, min_pass_rate: 0.9}`. `required_fields` must come back non-empty; "
            "`reconcile` requires the line items' amounts (plus `add_fields`, e.g. tax) to add up to the total. Adds "
            "`_validation_status` ('ok' / 'needs_review') and `_validation_issues` columns to the output so downstream "
            "assets can filter on them. The check is a warning (not blocking) and fails when fewer than `min_pass_rate` "
            "of the documents pass. Unset = no validation, no extra columns, no check."
        ),
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
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
    partition_values: Optional[Union[str, List[str]]] = Field(
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
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

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
        api_base_env_var = self.api_base_env_var
        document_type = self.document_type
        batch_size = self.batch_size
        llm_max_retries = self.llm_max_retries
        max_content_chars = self.max_content_chars
        field_regions = self.field_regions
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
        validation_cfg = normalize_validation(asset_name, self.validation, list(output_fields))

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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

        def _do_extraction(context: AssetExecutionContext, df: pd.DataFrame, meta: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
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

            def _add_meta(values: Dict[str, Any]) -> None:
                # An asset with a check has several outputs, where add_output_metadata can't tell which one is meant,
                # so the caller passes a dict and attaches it to the Output itself.
                if meta is not None:
                    meta.update(values)
                else:
                    context.add_output_metadata(values)

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
                    success = False
                    try:
                        extracted, _raw = extract_one(
                            file_ref,
                            input_type=input_type,
                            output_fields=output_fields,
                            doc_label=_doc_label,
                            model=model,
                            api_key=os.environ.get(api_key_env_var),
                            api_base=os.environ.get(api_base_env_var) if api_base_env_var else None,
                            llm_max_retries=llm_max_retries,
                            max_content_chars=max_content_chars,
                            field_regions=field_regions,
                            warn=context.log.warning,
                        )
                        success = True
                    except Exception as e:
                        context.log.warning(f"Extraction failed for {source_path!r}: {e}")
                        extracted = {f: None for f in output_fields}
                    results.append(extracted)
                    _post_process_rows.append((source_path, success))

            extracted_df = pd.DataFrame(results)
            for col in extracted_df.columns:
                df[col] = extracted_df[col].values

            if validation_cfg is not None:
                _issues = [validate_extraction(r, ok, validation_cfg) for r, (_, ok) in zip(results, _post_process_rows)]
                df[STATUS_COLUMN] = ["ok" if not i else "needs_review" for i in _issues]
                df[ISSUES_COLUMN] = [", ".join(i) for i in _issues]

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
                _add_meta({
                    "post_process_moved": moved,
                    "post_process_deleted": deleted,
                    "post_process_failed": failed,
                })

            extraction_failures = sum(1 for _, ok in _post_process_rows if not ok)
            _add_meta({
                "num_documents": total,
                "document_type": document_type,
                "extracted_fields": output_fields,
                "model": model,
                "extraction_failures": extraction_failures,
            })
            if extraction_failures > 0:
                context.log.warning(
                    f"{extraction_failures}/{total} row(s) failed extraction (all-None fields) -- see prior warnings for why."
                )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _add_meta({"preview": MetadataValue.md(_prev.to_markdown(index=False))})
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
            _add_meta(_metadata)
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

        def _run_direct(context: AssetExecutionContext, meta: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
            context.log.info(f"Listing files matching {direct_path!r}")
            df = _list_files_direct(direct_path, direct_max_files, direct_download, direct_download_dir)
            context.log.info(f"Found {len(df)} file(s)")
            return _do_extraction(context, df, meta)

        def _run_upstream(context: AssetExecutionContext, upstream: Any, meta: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            return _do_extraction(context, upstream.copy(), meta)

        _source_column = "path" if direct_path else input_column

        def _emit_with_quality_check(df: pd.DataFrame, meta: Dict[str, Any]):
            from dagster import AssetCheckResult, AssetCheckSeverity, Output

            summary = quality_summary(df, _source_column if _source_column in df.columns else df.columns[0], validation_cfg["min_pass_rate"])
            yield Output(df, metadata=meta)
            yield AssetCheckResult(
                check_name=QUALITY_CHECK_NAME,
                passed=summary["passed"],
                severity=AssetCheckSeverity.WARN,
                description=f"{summary['ok']}/{summary['documents']} documents passed validation ({summary['pass_rate']:.0%}; need {validation_cfg['min_pass_rate']:.0%})",
                metadata={
                    "documents": MetadataValue.int(summary["documents"]),
                    "ok": MetadataValue.int(summary["ok"]),
                    "needs_review": MetadataValue.int(summary["needs_review"]),
                    "pass_rate": MetadataValue.float(summary["pass_rate"]),
                    "review": MetadataValue.json(summary["review"]),
                },
            )

        if validation_cfg is None:
            if direct_path:
                @asset(description=self.description, **asset_kwargs)
                def _asset(context: AssetExecutionContext) -> pd.DataFrame:
                    return _run_direct(context)
            else:
                @asset(description=self.description, ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}, **asset_kwargs)
                def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
                    return _run_upstream(context, upstream)
        else:
            from dagster import AssetCheckSpec

            _check_specs = [AssetCheckSpec(name=QUALITY_CHECK_NAME, asset=AssetKey.from_user_string(asset_name))]
            if direct_path:
                @asset(description=self.description, check_specs=_check_specs, **asset_kwargs)
                def _asset(context: AssetExecutionContext):
                    meta: Dict[str, Any] = {}
                    yield from _emit_with_quality_check(_run_direct(context, meta), meta)
            else:
                @asset(description=self.description, check_specs=_check_specs, ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}, **asset_kwargs)
                def _asset(context: AssetExecutionContext, upstream: Any):
                    meta: Dict[str, Any] = {}
                    yield from _emit_with_quality_check(_run_upstream(context, upstream, meta), meta)

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=[_asset])
        return Definitions(assets=[_asset], asset_checks=list(_schema_checks))


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
