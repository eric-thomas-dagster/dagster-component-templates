"""DocumentIntelligenceToolsComponent -- the missing building block for an "Agent Diligence Review" recipe:
an MCP tool server an agent can call to extract fields from ONE document, or ask for a schema suggestion,
rather than only having full batch-pipeline assets available.

An earlier gap audit of the 13 Pulse-style document-intelligence recipes found that every other recipe
(bank statement -> JSON, annual report split->schema, financial tables, spreadsheet financial review,
footnote citation review, word-level review overlays, chunking for RAG, LangChain RAG ingestion, vector
metadata contract, S3 storage pipeline, batch document intake, production webhooks) has a concrete asset or
component already in this repo -- but "agent diligence review" (an agent that inspects documents
interactively, one at a time, deciding what to look at next) had nothing: `structured_document_extractor`
only runs as a batch asset over a whole upstream DataFrame, with no single-document, callable entry point.

Three tools:
  extract(file_ref, output_fields, doc_label=None, model=None)   -- extract named fields from ONE document.
                                                                    Read-only by default; gains ONE explicit,
                                                                    opt-in, clearly-logged side effect when
                                                                    `review_queue_approval_dir` is configured
                                                                    (see below): it may write a pending-review
                                                                    record.
  suggest_schema(sample_file_ref, doc_label=None, model=None, max_fields=12) -- ask the LLM what fields are
                                                                    worth extracting from documents like this one.
                                                                    Fully read-only.
  check_review_decision(approval_dir, key)                        -- read a human review decision already
                                                                    written for `key`, if any. Fully read-only --
                                                                    it never writes a decision, only reports one
                                                                    a human already made.

Review-queue wiring (`extract` only, opt-in): when `review_queue_approval_dir` is set, `extract` feeds the SAME
human review queue `agentic_pipeline`'s `approve` step feeds (`assets/ai/agent_review_queue/component.py`,
which scans `<approval_dir>/pending/*.json`) whenever a single extraction call looks untrustworthy. There is no
real confidence score anywhere in this extraction pipeline (`extract_one`/`parse_extraction_response` never
produce one) -- this is a deliberate, honest proxy, not a fabricated confidence model, built from the only two
honest signals actually available: (a) the LLM call raising an exception at all (`extraction_error` -- always
queued, regardless of threshold), and (b) the fraction of requested `output_fields` that came back
`None`/empty (`null_rate` -- queued once it exceeds `low_confidence_null_rate_threshold`, default 0.3). Queuing
is a one-off, non-blocking write-and-return: unlike `agentic_pipeline`'s `approve` step, `extract` never blocks
waiting for a human decision (that would make an MCP tool call hang indefinitely) -- it writes the pending
record and returns immediately with `queued_for_review`/`review_key`/`review_reason` on the result.
`check_review_decision` is the read-only complement: given an `approval_dir` + `key`, it reports whatever
decision a human has (or hasn't) already written -- it does NOT consume or act on that decision automatically.
Building an auto-resume loop on top of it (auto-retry, resuming a blocked call) is a separate, bigger piece of
scope this file does NOT do.

`extract` is NOT new extraction logic -- it is the SAME extraction core as `structured_document_extractor`'s
own `extract_one` (which itself calls `build_extraction_messages`/`parse_extraction_response`), copied here
byte-faithfully per this repo's self-contained-component convention (no cross-component imports). The only
things dropped going from `structured_document_extractor` to this file are config surface unrelated to a
single-document tool call: `field_regions` (per-field bounding-box crops) and `_crop_region_blocks` (the
helper that builds them) go entirely -- a field-location hint drawn against one sample document is a
batch-asset feature, not something a one-off tool call needs -- and there is no `batch_size`/partitioning/
`upstream_asset_key`/`path`-listing surface at all, since this component processes exactly one document per
call. The actual LLM-call/parse logic (`build_extraction_messages`'s prompt construction,
`parse_extraction_response`'s JSON-fence-stripping and field-shaping, `extract_one`'s litellm call) is
unmodified.

`suggest_schema` IS new logic (no existing function to port): it asks the LLM, via the same `litellm`
library `extract_one` already uses, to propose a JSON array of `{"field": str, "description": str}` worth
extracting from documents like the one sample given, reusing the same file-reading (`_extract_file_content`)
and JSON-fence-stripping convention `parse_extraction_response` already established. An invalid JSON reply
raises a clear error rather than returning anything.

Deliberately NOT built here (tracked separately, each its own recipe gap):
  - a "split" tool (page/section splitting for "Annual Report Split->Schema")
  - a "tables" tool claiming real financial-table structure extraction
  - a "get_citations" tool (blocked on unbuilt citation/word-coordinate infrastructure -- not faked)
  - auto-consuming a review decision (no auto-retry, no auto-resuming a blocked call); `check_review_decision`
    only reports what a human already decided

Same dual-mode shape as this repo's other MCP tool-server components (`dagster_evidence_tools`,
`dagster_discovery_tools`, `dagster_workflow_tools`): a stateless, read-only toolset with NO practice_mode
and NO outcome ledger (pure analysis, like `dagster_evidence_tools` -- not an agent that files things):
  1. An importable toolset (`DocumentIntelligenceTools`, plus the component that registers it as a Dagster
     resource).
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field

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


# ── extraction core (same extraction core as structured_document_extractor's extract_one, copied per this
# repo's self-contained-component convention -- build_extraction_messages/parse_extraction_response/
# extract_one/_extract_file_content ported byte-faithfully; field_regions/_crop_region_blocks dropped since
# per-field bounding-box crops are a batch-asset feature unrelated to a single-document tool call) ──────────

_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".tif"}
_MIME_BY_EXT = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png", ".gif": "image/gif",
    ".webp": "image/webp", ".bmp": "image/bmp", ".tiff": "image/tiff", ".tif": "image/tiff",
}


def _extract_file_content(path: str) -> "tuple[Optional[str], Optional[dict]]":
    """Read a document/image file for the LLM prompt -- returns (text, image_content_block), exactly one
    non-None. Images are base64-encoded into a litellm vision content block; PDFs use pdfplumber to pull
    embedded text. A PDF with no extractable text (a scanned/photographed page rather than a
    digitally-generated one) raises rather than silently sending empty content to the LLM."""
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


def build_extraction_messages(
    file_ref: str,
    *,
    input_type: str,
    output_fields: List[str],
    doc_label: str = "document",
    max_content_chars: Optional[int] = None,
    warn: Any = None,
) -> List[dict]:
    """The exact chat messages sent to the model for one document -- identical prompt construction to
    structured_document_extractor's own `build_extraction_messages` (minus `field_regions`, which that
    component's batch-asset config surface supports and this single-document tool does not)."""
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
    warn: Any = None,
) -> "tuple[Dict[str, Any], str]":
    """Extract `output_fields` from ONE document. Returns (fields, raw model reply); raises on any failure.
    Identical call shape to structured_document_extractor's own `extract_one`."""
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


# ── schema suggestion (new logic -- no existing function to port; mirrors extract_one's shape and reuses
# the same file-reading + JSON-fence-stripping conventions) ──────────────────────────────────────────────────

def build_schema_suggestion_messages(
    file_ref: str,
    *,
    input_type: str = "file",
    doc_label: str = "document",
    max_content_chars: Optional[int] = None,
    max_fields: int = 12,
) -> List[dict]:
    """The exact chat messages sent to the model to suggest a field schema for ONE sample document."""
    image_block = None
    if input_type == "file":
        text_content, image_block = _extract_file_content(file_ref)
        content = text_content if text_content is not None else file_ref
    else:
        content = file_ref

    if image_block is None and max_content_chars is not None and len(content) > max_content_chars:
        content = content[:max_content_chars]

    instructions = (
        f"You are looking at a sample {doc_label}. Propose up to {max_fields} fields worth extracting from "
        f"documents like this one -- the kind of structured data someone reviewing this {doc_label} would "
        "want pulled out automatically. "
        'Return ONLY a JSON array of objects, each shaped exactly like {"field": "snake_case_name", '
        '"description": "one short sentence describing what this field holds"}. No prose, no markdown '
        "outside the JSON array itself."
    )
    if image_block is not None:
        message_content: Any = [{"type": "text", "text": instructions}, image_block]
    else:
        message_content = f"{instructions}\n\nDocument content:\n{content}"
    return [{"role": "user", "content": message_content}]


def parse_schema_suggestion_response(raw: str, max_fields: int) -> List[Dict[str, str]]:
    """Parse the model's reply into a list of {"field", "description"} dicts, capped at `max_fields`.
    Reuses the same ```json fence-stripping convention as `parse_extraction_response`. Raises a clear
    error (never silently returns garbage) when the reply isn't valid JSON or isn't a JSON array of
    field/description objects."""
    import json

    text = raw.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"suggest_schema: model did not return valid JSON ({e}); raw reply: {raw[:300]!r}") from e
    if not isinstance(parsed, list):
        raise ValueError(f"suggest_schema: expected a JSON array of fields, got {type(parsed).__name__}: {raw[:300]!r}")
    fields: List[Dict[str, str]] = []
    for item in parsed[: max(1, int(max_fields))]:
        if not isinstance(item, dict) or "field" not in item:
            raise ValueError(f"suggest_schema: expected items shaped {{'field': ..., 'description': ...}}, got {item!r}")
        fields.append({"field": str(item["field"]), "description": str(item.get("description") or "")})
    return fields


def suggest_fields_one(
    file_ref: str,
    *,
    input_type: str = "file",
    doc_label: str = "document",
    model: str,
    api_key: Optional[str] = None,
    api_base: Optional[str] = None,
    llm_max_retries: int = 2,
    max_content_chars: Optional[int] = 20000,
    max_fields: int = 12,
) -> "tuple[List[Dict[str, str]], str]":
    """Ask the LLM for a field schema from ONE sample document. Returns (fields, raw model reply); raises
    on any failure (including an unparseable reply -- never returns garbage silently)."""
    try:
        from litellm import completion
    except ImportError:
        raise ImportError("litellm required: pip install litellm")

    messages = build_schema_suggestion_messages(
        file_ref, input_type=input_type, doc_label=doc_label,
        max_content_chars=max_content_chars, max_fields=max_fields,
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
    return parse_schema_suggestion_response(raw, max_fields), raw


# ── review-queue wiring (feeds agent_review_queue's `<approval_dir>/pending/*.json`; opt-in, off by default) ──
# Re-implemented here rather than imported from agent_review_queue/component.py's own `_Store`, per this repo's
# self-contained-component convention (no cross-component imports) -- same local-or-fsspec resolution.

class _ReviewStore:
    """Local directory or any fsspec URI (s3://, gs://, abfs://, memory://) -- same resolution agent_review_queue's
    own `_Store` uses, with write support added since this side writes pending records instead of only reading
    them. Install the driver you need (s3fs/gcsfs/adlfs) for a real cloud URI."""

    def __init__(self, root: str):
        self.is_uri = "://" in root
        if self.is_uri:
            import fsspec
            self.fs, self.root = fsspec.core.url_to_fs(root)
            self.root = self.root.rstrip("/")
        else:
            self.fs, self.root = None, str(Path(root).expanduser().resolve())
        self.display = root.rstrip("/")

    def write_text(self, relative_path: str, text: str) -> None:
        full = f"{self.root}/{relative_path}"
        if self.is_uri:
            with self.fs.open(full, "w") as f:
                f.write(text)
        else:
            p = Path(full)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(text)

    def read_text(self, relative_path: str) -> Optional[str]:
        full = f"{self.root}/{relative_path}"
        if self.is_uri:
            if not self.fs.exists(full):
                return None
            with self.fs.open(full, "r") as f:
                return f.read()
        p = Path(full)
        return p.read_text() if p.exists() else None

    def shown(self, relative_path: str) -> str:
        return f"{self.display}/{relative_path}"


def _null_rate(fields: Dict[str, Any], output_fields: List[str]) -> float:
    """Fraction of requested `output_fields` whose extracted value came back `None` or an empty string -- the
    ONLY honest "confidence" proxy available here. There is no real confidence score anywhere in this
    extraction pipeline (`extract_one`/`parse_extraction_response` never produce one); this is a deliberate,
    honest stand-in, not a fabricated model-confidence estimate."""
    if not output_fields:
        return 0.0
    empty = sum(1 for f in output_fields if fields.get(f) is None or fields.get(f) == "")
    return empty / len(output_fields)


def _review_fingerprint(file_ref: str, doc_label: str, output_fields: List[str]) -> str:
    """Binds a pending review record to exactly what was shown -- same spirit as agentic_pipeline's approval
    fingerprint, just a simpler hash since a one-off tool call has no "reuse until input changes" semantics."""
    import hashlib
    blob = f"{file_ref}|{doc_label}|{sorted(output_fields)}"
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _queue_extract_for_review(
    approval_dir: str, *, file_ref: str, doc_label: str, output_fields: List[str],
    fields: Dict[str, Any], error: Optional[str], null_rate: float, review_reason: str,
) -> str:
    """Write a pending review record -- in agent_review_queue's exact `_collect_pending()` schema -- to
    `<approval_dir>/pending/<key>.json`, and return `key`. Non-blocking: this writes and returns immediately,
    it never waits for a human decision (unlike agentic_pipeline's `approve` step)."""
    import json

    fingerprint = _review_fingerprint(file_ref, doc_label, output_fields)
    key = fingerprint[:16]
    store = _ReviewStore(approval_dir)
    if review_reason == "extraction_error":
        reason = f"extraction_error: {error}"
        preview = str(error or "")[:2000]
    else:
        reason = f"low_confidence (null_rate={null_rate:.2f})"
        preview = json.dumps(fields, default=str)[:2000]
    summary = f"document_intelligence_tools.extract on {doc_label} ({file_ref}): {reason}"
    record = {
        "step": "document_intelligence_tools.extract",
        "key": key,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "fingerprint": fingerprint,
        "state": "new",
        "reason": reason,
        "reuse": "once",
        "max_age_hours": None,
        "expires_at": None,
        "on_timeout": None,
        "summary": summary,
        "preview": preview,
        "approve_by_writing": {
            "path": store.shown(f"{key}.json"),
            "json": {
                "approved": True, "approver": "<you>", "reason": "<why>",
                "fingerprint": fingerprint, "approved_at": "<iso timestamp>",
            },
        },
    }
    store.write_text(f"pending/{key}.json", json.dumps(record, indent=2))
    return key


class DocumentIntelligenceToolError(Exception):
    """A user-facing, safe error message (returned to the agent as {"error": ...})."""


ALL_TOOLS = ("extract", "suggest_schema", "check_review_decision")


class DocumentIntelligenceTools:
    """`call(tool, args)` is the one safe entry point (allow-list, audit, size cap, error shaping); the
    `t_*` methods are the raw implementations. `suggest_schema` and `check_review_decision` are read-only /
    side-effect-free; `extract` is read-only by default and gains one explicit, opt-in side effect (writing a
    pending review record) only when `review_queue_approval_dir` is set."""

    def __init__(
        self, *,
        default_model: str = "gpt-4o",
        api_key_env_var: str = "OPENAI_API_KEY",
        api_base_env_var: Optional[str] = None,
        llm_max_retries: int = 2,
        max_content_chars: Optional[int] = 20000,
        allow: Optional[List[str]] = None,
        max_result_chars: int = 12000,
        audit: Optional[Callable[[str], None]] = None,
        review_queue_approval_dir: Optional[str] = None,
        low_confidence_null_rate_threshold: float = 0.3,
    ):
        import logging
        self._default_model = default_model
        self._api_key_env_var = api_key_env_var
        self._api_base_env_var = api_base_env_var
        self._llm_max_retries = llm_max_retries
        self._max_content_chars = max_content_chars
        self._review_queue_approval_dir = review_queue_approval_dir
        self._low_confidence_null_rate_threshold = float(low_confidence_null_rate_threshold)
        if allow is None:
            allow = list(ALL_TOOLS)
        unknown = sorted(set(allow) - set(ALL_TOOLS))
        if unknown:
            raise ValueError(f"unknown tool(s) in allow: {unknown}; valid: {list(ALL_TOOLS)}")
        self.allow = frozenset(allow)
        self.max_result_chars = max(1000, int(max_result_chars))
        self._audit = audit or (lambda line: logging.getLogger("document_intelligence_tools.audit").info(line))

    def tool_names(self) -> List[str]:
        return [t for t in ALL_TOOLS if t in self.allow]

    def call(self, tool: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import json
        args = dict(args or {})
        try:
            self._audit(f"document_intelligence_tools call tool={tool} args={json.dumps(args, default=str)[:300]}")
        except Exception:
            pass
        if tool not in ALL_TOOLS:
            return {"error": f"unknown tool {tool!r}; available: {self.tool_names()}"}
        if tool not in self.allow:
            return {"error": f"tool {tool!r} is not enabled (allowed: {self.tool_names()})"}
        try:
            result = getattr(self, f"t_{tool}")(**args)
        except DocumentIntelligenceToolError as e:
            return {"error": str(e)}
        except TypeError as e:
            return {"error": f"bad arguments for {tool}: {e}"}
        except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
            return {"error": f"{tool} failed: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"}
        return self._fit(result)

    def _fit(self, result: Dict[str, Any]) -> Dict[str, Any]:
        import json
        cap = self.max_result_chars
        text = json.dumps(result, default=str)
        if len(text) <= cap:
            return result
        return {"truncated": True, "preview": text[: max(0, cap - 40)]}

    # -- extract ---------------------------------------------------------------------------------------------
    def t_extract(
        self, file_ref: str, output_fields: List[str], doc_label: Optional[str] = None, model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Extract `output_fields` from ONE document at `file_ref`, via the same extraction core
        structured_document_extractor's own `extract_one` uses.

        `queued_for_review` is always present on the result. It is only ever true when
        `review_queue_approval_dir` is configured on this component/toolset AND either the LLM call raised
        (`review_reason='extraction_error'`, queued regardless of threshold) or too many requested fields came
        back null/empty (`review_reason='null_rate'`, queued once `_null_rate()` exceeds
        `low_confidence_null_rate_threshold`). When `review_queue_approval_dir` is unset this method behaves
        exactly as before: any extraction failure raises, no file is ever written."""
        if not file_ref or not str(file_ref).strip():
            raise DocumentIntelligenceToolError("extract requires `file_ref` (path to a document).")
        if not output_fields:
            raise DocumentIntelligenceToolError("extract requires a non-empty `output_fields` list.")
        _model = model or self._default_model
        _doc_label = doc_label or "document"
        _output_fields = list(output_fields)

        if not self._review_queue_approval_dir:
            # Feature off: unchanged behavior -- any failure raises, nothing is ever written.
            fields, _raw = extract_one(
                str(file_ref), input_type="file", output_fields=_output_fields, doc_label=_doc_label, model=_model,
                api_key=os.environ.get(self._api_key_env_var),
                api_base=os.environ.get(self._api_base_env_var) if self._api_base_env_var else None,
                llm_max_retries=self._llm_max_retries, max_content_chars=self._max_content_chars,
            )
            return {"fields": fields, "model": _model, "queued_for_review": False}

        # Feature on: catch the failure ourselves so a low-confidence/erroring result can still be queued and
        # reported back, rather than only raised and lost.
        error: Optional[str] = None
        fields: Dict[str, Any] = {}
        try:
            fields, _raw = extract_one(
                str(file_ref), input_type="file", output_fields=_output_fields, doc_label=_doc_label, model=_model,
                api_key=os.environ.get(self._api_key_env_var),
                api_base=os.environ.get(self._api_base_env_var) if self._api_base_env_var else None,
                llm_max_retries=self._llm_max_retries, max_content_chars=self._max_content_chars,
            )
        except Exception as e:  # noqa: BLE001 -- captured as data so it can still be queued for review below
            error = f"{type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"

        result: Dict[str, Any] = {"model": _model, "queued_for_review": False}
        if error is None:
            result["fields"] = fields
        else:
            result["error"] = f"extract failed: {error}"

        rate = 1.0 if error is not None else _null_rate(fields, _output_fields)
        if error is not None or rate > self._low_confidence_null_rate_threshold:
            review_reason = "extraction_error" if error is not None else "null_rate"
            key = _queue_extract_for_review(
                self._review_queue_approval_dir, file_ref=str(file_ref), doc_label=_doc_label,
                output_fields=_output_fields, fields=fields, error=error, null_rate=rate, review_reason=review_reason,
            )
            result["queued_for_review"] = True
            result["review_key"] = key
            result["review_reason"] = review_reason
        return result

    # -- check_review_decision ---------------------------------------------------------------------------------
    def t_check_review_decision(self, approval_dir: str, key: str) -> Dict[str, Any]:
        """Read-only: reports a human review decision already written to `<approval_dir>/<key>.json` (the token
        file named by a pending record's own `approve_by_writing.path`), if one exists. Never writes a decision
        itself -- only `agent_review_queue`'s documented approve-by-writing convention does that."""
        if not approval_dir or not str(approval_dir).strip():
            raise DocumentIntelligenceToolError("check_review_decision requires `approval_dir`.")
        if not key or not str(key).strip():
            raise DocumentIntelligenceToolError("check_review_decision requires `key`.")
        import json

        store = _ReviewStore(str(approval_dir))
        raw = store.read_text(f"{key}.json")
        if raw is None:
            return {"decided": False, "approved": None, "approver": None, "reason": None, "approved_at": None}
        try:
            tok = json.loads(raw)
        except json.JSONDecodeError:
            return {"decided": False, "approved": None, "approver": None, "reason": None, "approved_at": None}
        if not isinstance(tok, dict):
            return {"decided": False, "approved": None, "approver": None, "reason": None, "approved_at": None}
        return {
            "decided": True,
            "approved": tok.get("approved"),
            "approver": tok.get("approver"),
            "reason": tok.get("reason"),
            "approved_at": tok.get("approved_at"),
        }

    # -- suggest_schema ----------------------------------------------------------------------------------------
    def t_suggest_schema(
        self, sample_file_ref: str, doc_label: Optional[str] = None, model: Optional[str] = None, max_fields: int = 12,
    ) -> Dict[str, Any]:
        """Propose up to `max_fields` {field, description} entries worth extracting from documents like
        ONE sample document at `sample_file_ref`."""
        if not sample_file_ref or not str(sample_file_ref).strip():
            raise DocumentIntelligenceToolError("suggest_schema requires `sample_file_ref` (path to a sample document).")
        _model = model or self._default_model
        _max_fields = max(1, int(max_fields))
        fields, _raw = suggest_fields_one(
            str(sample_file_ref),
            input_type="file",
            doc_label=doc_label or "document",
            model=_model,
            api_key=os.environ.get(self._api_key_env_var),
            api_base=os.environ.get(self._api_base_env_var) if self._api_base_env_var else None,
            llm_max_retries=self._llm_max_retries,
            max_content_chars=self._max_content_chars,
            max_fields=_max_fields,
        )
        return {"fields": fields, "model": _model}


def _this_module_name() -> str:
    """The real importable dotted path for `python -m <this>` -- same mechanism as this repo's other MCP
    tool-server components' own copies (duplicated here, not imported, per this repo's
    self-contained-component convention)."""
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "document_intelligence_tools's mcp_server_resource_key path must be run as `python -m "
        "<dotted.path.to.this.module>`, not `python component.py` directly. See README.md."
    )


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined here rather than imported from
    resources/mcp_server_resource (confirmed not importable across components). Same copy
    dagster_discovery_tools/dagster_api_check_tools/dagster_instance_tools/dagster_evidence_tools each carry."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    live_env_passthrough: Optional[List[str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        passthrough = dumped.pop("live_env_passthrough", None)
        if dumped.get("type") == "stdio" and passthrough:
            env = dict(dumped.get("env") or {})
            for var_name in passthrough:
                value = os.environ.get(var_name)
                if value:
                    env[var_name] = value
            dumped["env"] = env
        return dumped


class DocumentIntelligenceToolsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers the document-intelligence toolset (`extract`, `suggest_schema`, `check_review_decision`) as a
    resource (default key `document_intelligence_tools`). For an agent (MCP only), run THIS FILE as the stdio
    MCP server -- see README.md.

    Set `review_queue_approval_dir` to feed a low-confidence/erroring `extract` call into the SAME human review
    queue `agentic_pipeline`'s `approve` step feeds (`AgentReviewQueueComponent`, which scans
    `<approval_dir>/pending/*.json`) -- point both components at the same directory. Unset (default), `extract`
    behaves exactly as before: fully read-only, nothing is ever written.

    Example:

    ```yaml
    type: dagster_community_components.DocumentIntelligenceToolsComponent
    attributes:
      default_model: gpt-4o
      api_key_env_var: OPENAI_API_KEY
      mcp_server_resource_key: document_intelligence_tools_mcp
      review_queue_approval_dir: /data/agent_reviews/document_intelligence
      low_confidence_null_rate_threshold: 0.3
    ```
    """

    resource_key: str = Field(default="document_intelligence_tools", description="Resource key the toolset is registered under.")
    default_model: Optional[str] = Field(
        default="gpt-4o",
        description="Fallback LLM model (litellm format) used when a tool call doesn't specify its own `model`.",
    )
    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Environment variable name holding the API key.",
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Optional environment variable holding an API base URL, for a self-hosted or local OpenAI-compatible "
            "model (Ollama, LM Studio, vLLM...), e.g. OLLAMA_API_BASE=http://localhost:11434 with model "
            "'ollama/llava'. Unset = the provider's default endpoint."
        ),
    )
    llm_max_retries: int = Field(
        default=2,
        description="Retry a tool call's LLM call up to this many times on transient errors (rate limits, timeouts) -- forwarded to litellm's own num_retries.",
    )
    max_content_chars: Optional[int] = Field(
        default=20000,
        description="Truncate extracted document text to this many characters before prompting the LLM -- guards against blowing the model's context window or racking up cost on unusually large text-PDF/text-input documents. Doesn't apply to image documents (sent as a vision content block, not text). Set to null to disable.",
    )
    allow: Optional[List[str]] = Field(default=None, description=f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}.")
    max_result_chars: int = Field(default=12000, description="Upper bound on the serialized size of any tool result.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description="If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this "
                     "module, with DOCUMENT_INTELLIGENCE_TOOLS_* env vars set from this component's own config). "
                     "Any mcp_servers-aware component can then reference it via server_resource_key.",
    )
    mcp_extra_env_vars: Optional[List[str]] = Field(default=None, description="mcp_server_resource_key only. Extra env var NAMES to copy into the spawned MCP server's env.")
    review_queue_approval_dir: Optional[str] = Field(
        default=None,
        description=(
            "Approval directory used by AgentReviewQueueComponent (the SAME `approval_dir` value your review-queue "
            "asset scans). When set, `extract` writes a pending review record to `<review_queue_approval_dir>/"
            "pending/<key>.json` whenever the LLM call errors, or too many requested fields come back null/empty -- "
            "feeding the exact same human review queue agentic_pipeline's `approve` step feeds. Unset (default) = "
            "feature off; `extract` stays fully read-only and never writes anything."
        ),
    )
    low_confidence_null_rate_threshold: float = Field(
        default=0.3,
        description=(
            "Only used when `review_queue_approval_dir` is set. Fraction (0-1) of requested `output_fields` that "
            "came back null/empty above which `extract` queues the result for review. There is no real confidence "
            "score anywhere in this extraction pipeline -- this null-rate is a deliberate, honest proxy, not a "
            "fabricated model-confidence estimate. An LLM-call exception always queues regardless of this threshold."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("allow", "mcp_extra_env_vars"), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Document Intelligence Tools", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        allow = list(self.allow) if self.allow else None
        if allow is not None:
            bad = sorted(set(allow) - set(ALL_TOOLS))
            if bad:
                raise ValueError(f"unknown tool(s) in allow: {bad}; valid: {list(ALL_TOOLS)}")
        default_model = self.default_model or "gpt-4o"
        api_key_env_var = self.api_key_env_var
        api_base_env_var = self.api_base_env_var
        llm_max_retries = self.llm_max_retries
        max_content_chars = self.max_content_chars
        max_chars = int(self.max_result_chars)
        review_queue_approval_dir = self.review_queue_approval_dir
        low_confidence_null_rate_threshold = float(self.low_confidence_null_rate_threshold)

        def _init(init_context):
            return DocumentIntelligenceTools(
                default_model=default_model, api_key_env_var=api_key_env_var, api_base_env_var=api_base_env_var,
                llm_max_retries=llm_max_retries, max_content_chars=max_content_chars, allow=allow,
                max_result_chars=max_chars, review_queue_approval_dir=review_queue_approval_dir,
                low_confidence_null_rate_threshold=low_confidence_null_rate_threshold,
            )

        res = dg.ResourceDefinition(
            resource_fn=_init,
            description="Document-intelligence tools for an agent (extract named fields from one document; "
                         "suggest a field schema from one sample document; check a human review decision). "
                         "`suggest_schema`/`check_review_decision` are read-only; `extract` is read-only unless "
                         "`review_queue_approval_dir` is configured, in which case it may also queue a low-"
                         "confidence/erroring result for human review.",
        )
        resources: Dict[str, Any] = {self.resource_key: res}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {
                "DOCUMENT_INTELLIGENCE_TOOLS_DEFAULT_MODEL": default_model,
                "DOCUMENT_INTELLIGENCE_TOOLS_API_KEY_ENV_VAR": api_key_env_var,
                "DOCUMENT_INTELLIGENCE_TOOLS_LLM_MAX_RETRIES": str(llm_max_retries),
                "DOCUMENT_INTELLIGENCE_TOOLS_MAX_CHARS": str(max_chars),
                "DOCUMENT_INTELLIGENCE_TOOLS_LOW_CONFIDENCE_NULL_RATE_THRESHOLD": str(low_confidence_null_rate_threshold),
            }
            if api_base_env_var:
                mcp_env["DOCUMENT_INTELLIGENCE_TOOLS_API_BASE_ENV_VAR"] = api_base_env_var
            if max_content_chars is not None:
                mcp_env["DOCUMENT_INTELLIGENCE_TOOLS_MAX_CONTENT_CHARS"] = str(max_content_chars)
            if allow:
                mcp_env["DOCUMENT_INTELLIGENCE_TOOLS_ALLOW"] = ",".join(allow)
            if review_queue_approval_dir:
                mcp_env["DOCUMENT_INTELLIGENCE_TOOLS_REVIEW_QUEUE_APPROVAL_DIR"] = review_queue_approval_dir
            passthrough = list(self.mcp_extra_env_vars or []) + [api_key_env_var]
            if api_base_env_var:
                passthrough.append(api_base_env_var)
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio", command=[sys.executable, "-m", _this_module_name()],
                env=mcp_env, live_env_passthrough=passthrough,
            )

        return dg.Definitions(resources=resources)


# ── stdio MCP server entrypoint:  python -m <this module>  ──────────────────────────────────────────────────────────
def build_mcp_server(tools: DocumentIntelligenceTools):
    """MCP server (official `mcp` package) exposing exactly the allowed tools. Works on mcp 1.x and 2.x."""
    try:  # mcp 2.x renamed FastMCP -> MCPServer (mcp.server.fastmcp now raises ModuleNotFoundError)
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:  # mcp 1.x
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server("document-intelligence-tools")
    allowed = set(tools.tool_names())

    def _tool(fn):
        if fn.__name__ in allowed:
            mcp.tool()(fn)
        return fn

    @_tool
    def extract(file_ref: str, output_fields: list, doc_label: str = None, model: str = None) -> dict:
        """Extract the named fields from ONE document at `file_ref` and return them as JSON."""
        return tools.call("extract", {"file_ref": file_ref, "output_fields": output_fields, "doc_label": doc_label, "model": model})

    @_tool
    def suggest_schema(sample_file_ref: str, doc_label: str = None, model: str = None, max_fields: int = 12) -> dict:
        """Given ONE sample document, propose a list of field names (with a one-line description each)
        worth extracting from documents like this one."""
        return tools.call("suggest_schema", {"sample_file_ref": sample_file_ref, "doc_label": doc_label, "model": model, "max_fields": max_fields})

    @_tool
    def check_review_decision(approval_dir: str, key: str) -> dict:
        """Read-only: report a human review decision already written for `key` in `approval_dir`, if any
        (never writes a decision itself)."""
        return tools.call("check_review_decision", {"approval_dir": approval_dir, "key": key})

    return mcp


def main(argv: Optional[List[str]] = None) -> None:
    """Env: DOCUMENT_INTELLIGENCE_TOOLS_DEFAULT_MODEL (default 'gpt-4o'), _API_KEY_ENV_VAR (default
    OPENAI_API_KEY, the NAME of the env var holding the key -- that env var itself must also be set),
    _API_BASE_ENV_VAR, _LLM_MAX_RETRIES, _MAX_CONTENT_CHARS, _ALLOW (csv), _MAX_CHARS,
    _REVIEW_QUEUE_APPROVAL_DIR (unset = feature off), _LOW_CONFIDENCE_NULL_RATE_THRESHOLD (default 0.3)."""
    import logging
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")  # stdout is the MCP channel

    env = os.environ
    allow_env = env.get("DOCUMENT_INTELLIGENCE_TOOLS_ALLOW")
    allow = [t.strip() for t in allow_env.split(",") if t.strip()] if allow_env else None
    max_content_chars_env = env.get("DOCUMENT_INTELLIGENCE_TOOLS_MAX_CONTENT_CHARS")

    tools = DocumentIntelligenceTools(
        default_model=env.get("DOCUMENT_INTELLIGENCE_TOOLS_DEFAULT_MODEL", "gpt-4o"),
        api_key_env_var=env.get("DOCUMENT_INTELLIGENCE_TOOLS_API_KEY_ENV_VAR", "OPENAI_API_KEY"),
        api_base_env_var=env.get("DOCUMENT_INTELLIGENCE_TOOLS_API_BASE_ENV_VAR"),
        llm_max_retries=int(env.get("DOCUMENT_INTELLIGENCE_TOOLS_LLM_MAX_RETRIES", "2")),
        max_content_chars=int(max_content_chars_env) if max_content_chars_env else 20000,
        allow=allow, max_result_chars=int(env.get("DOCUMENT_INTELLIGENCE_TOOLS_MAX_CHARS", "12000")),
        review_queue_approval_dir=env.get("DOCUMENT_INTELLIGENCE_TOOLS_REVIEW_QUEUE_APPROVAL_DIR"),
        low_confidence_null_rate_threshold=float(env.get("DOCUMENT_INTELLIGENCE_TOOLS_LOW_CONFIDENCE_NULL_RATE_THRESHOLD", "0.3")),
    )
    build_mcp_server(tools).run(transport="stdio")


if __name__ == "__main__":
    main()
