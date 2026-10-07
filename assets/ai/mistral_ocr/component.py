"""MistralOcrComponent — native Mistral AI Document OCR.

Per-row document/image-to-markdown OCR via Mistral's dedicated OCR API
(`client.ocr.process`, POST https://api.mistral.ai/v1/ocr) -- a real,
distinct Mistral product, not a chat-completions call. Unlike
`ocr_extractor` (local Tesseract, image files only), Mistral's OCR model
is trained specifically on documents: multi-column layouts, tables,
embedded images, and handwriting all come back as structured markdown,
and it natively handles multi-page PDFs (Tesseract does not).

Three real input modes, auto-selected per row from `input_column`'s
value (override via `input_kind`):

- A public http(s) URL to a PDF/office doc -> `{"type": "document_url", ...}`.
- A public http(s) URL to an image -> `{"type": "image_url", ...}`.
- A local file path -> read bytes locally, then:
  - image extensions: base64-encoded as a `data:` URI, sent as `image_url`
    (no upload needed -- this is how Mistral's own docs show local images).
  - document extensions (pdf/doc/docx/ppt/pptx, or unrecognized): uploaded
    via `client.files.upload(..., purpose="ocr")`, then exchanged for a
    `client.files.get_signed_url(...)` consumed as `document_url` -- the
    real documented flow for local PDFs (OCR has no base64 "document"
    input; only images support inline base64).

Sibling of `openai_llm`/`anthropic_llm`/`gemini_llm`/`mistral_llm` in
spirit (single-vendor, no LiteLLM) but OCR, not chat -- paired instead
with `ocr_extractor`'s per-row DataFrame shape.
"""

import base64
import mimetypes
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

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
from pydantic import Field

_DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx", ".ppt", ".pptx"}
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".tiff", ".tif"}


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    """Canonical partition factory shared across the registry."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dim type={t!r} requires 'start'")
        if t == "daily":   return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":  return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":  return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dim type='static' requires 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dim type='dynamic' requires a name")
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
        raise ValueError(f"partition_type={partition_type!r} requires partition_start")
    if partition_type == "daily":   return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":  return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":  return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values or not partition_start:
            raise ValueError("partition_type='multi' requires partition_start + partition_values")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _call_ocr_process(client: Any, **kwargs: Any) -> Any:
    """The one external OCR network call, isolated so tests can monkeypatch
    `mistral_ocr_component._call_ocr_process` instead of hitting the real
    (paid) Mistral API."""
    return client.ocr.process(**kwargs)


def _upload_and_get_signed_url(client: Any, file_name: str, content: bytes) -> str:
    """Uploads a local document for OCR and exchanges it for a signed URL --
    the real documented flow for local PDFs (OCR's `document_url` type has
    no inline-base64 sibling; only `image_url` does). Isolated into its own
    function so tests can monkeypatch both calls without touching disk I/O."""
    uploaded = client.files.upload(
        file={"file_name": file_name, "content": content},
        purpose="ocr",
    )
    signed = client.files.get_signed_url(file_id=uploaded.id)
    return signed.url


def _classify_input(value: str, input_kind: str) -> str:
    """Returns 'remote_document' | 'remote_image' | 'local_document' | 'local_image'."""
    is_remote = value.startswith("http://") or value.startswith("https://")
    ext = Path(value.split("?")[0]).suffix.lower()
    if input_kind == "document":
        is_image = False
    elif input_kind == "image":
        is_image = True
    else:
        is_image = ext in _IMAGE_EXTENSIONS and ext not in _DOCUMENT_EXTENSIONS
    if is_remote:
        return "remote_image" if is_image else "remote_document"
    return "local_image" if is_image else "local_document"


def _build_document_arg(client: Any, value: str, input_kind: str) -> Dict[str, Any]:
    kind = _classify_input(value, input_kind)
    if kind == "remote_document":
        return {"type": "document_url", "document_url": value}
    if kind == "remote_image":
        return {"type": "image_url", "image_url": value}
    if kind == "local_image":
        content = Path(value).read_bytes()
        mime_type = mimetypes.guess_type(value)[0] or "image/png"
        b64 = base64.b64encode(content).decode("utf-8")
        return {"type": "image_url", "image_url": f"data:{mime_type};base64,{b64}"}
    # local_document
    content = Path(value).read_bytes()
    signed_url = _upload_and_get_signed_url(client, Path(value).name, content)
    return {"type": "document_url", "document_url": signed_url}


class MistralOcrComponent(Component, Model, Resolvable):
    """Per-row document/image OCR via Mistral AI's dedicated OCR API
    (`client.ocr.process`). Sibling of `ocr_extractor` (local Tesseract,
    images only) but trained specifically on documents -- multi-column
    layouts, tables, and multi-page PDFs come back as structured markdown.
    """

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    input_column: Union[str, int] = Field(
        description=(
            "Column holding either a public http(s) URL or a local file path "
            "to the document/image to OCR."
        )
    )
    input_kind: str = Field(
        default="auto",
        description=(
            "'auto' infers document-vs-image from the file extension (.pdf/.doc/.docx/"
            ".ppt/.pptx -> document; else image). Set 'document' or 'image' to force it."
        ),
    )

    api_key_env_var: str = Field(default="MISTRAL_API_KEY", description="Env var holding the Mistral API key.")
    model: str = Field(
        default="mistral-ocr-latest",
        description="Mistral OCR model id. '-latest' resolves to Mistral's current OCR release.",
    )

    output_column: str = Field(default="mistral_ocr_markdown", description="Column for the concatenated per-document markdown.")
    output_pages_column: Optional[str] = Field(
        default=None,
        description="If set, write a JSON array of {index, markdown} per page to this column (structure beyond flat markdown).",
    )
    include_image_base64: bool = Field(default=False, description="Ask Mistral to also return base64 image data for embedded figures.")
    pages: Optional[List[int]] = Field(default=None, description="Zero-indexed page subset to process. Omit to process the whole document.")

    rate_limit_delay: float = Field(default=0.0, description="Seconds to sleep between rows.")
    max_retries: int = Field(default=3)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
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

        if self.input_kind not in ("auto", "document", "image"):
            raise ValueError(f"MistralOcrComponent: input_kind must be 'auto', 'document', or 'image', got {self.input_kind!r}")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        api_key_env = self.api_key_env_var
        model = self.model
        input_column = self.input_column
        input_kind = self.input_kind
        output_column = self.output_column
        output_pages_column = self.output_pages_column
        include_image_base64 = self.include_image_base64
        pages = self.pages
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries
        deps_keys = [AssetKey.from_user_string(k) for k in (self.deps or [])]

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Mistral OCR per row from {self.upstream_asset_key} ({model}).",
            group_name=self.group_name,
            kinds={"mistral", "ocr"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=deps_keys or None,
            partitions_def=partitions_def,
            ins={"upstream": AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            try:
                from mistralai import Mistral
            except ImportError:
                try:
                    from mistralai.client import Mistral
                except ImportError:
                    raise ImportError("mistral_ocr requires the Mistral SDK. Install: pip install mistralai")

            api_key = os.environ.get(api_key_env)
            if not api_key:
                raise ValueError(f"{api_key_env} not set. Get a key at https://console.mistral.ai/api-keys")

            client = Mistral(api_key=api_key)

            df = upstream.copy().reset_index(drop=True)
            if df.empty:
                df[output_column] = []
                return df

            if input_column not in df.columns:
                raise ValueError(f"input_column={input_column!r} not in upstream columns: {list(df.columns)}")

            markdowns: List[Optional[str]] = []
            page_payloads: List[Optional[str]] = []
            errors: List[Optional[str]] = []
            success = 0
            total_pages_processed = 0

            for idx, row in df.iterrows():
                value = str(row[input_column])

                kwargs: Dict[str, Any] = {"model": model, "include_image_base64": include_image_base64}
                if pages is not None:
                    kwargs["pages"] = pages

                attempt = 0
                last_err: Optional[Exception] = None
                resp = None
                while attempt <= max_retries:
                    try:
                        kwargs["document"] = _build_document_arg(client, value, input_kind)
                        resp = _call_ocr_process(client, **kwargs)
                        last_err = None
                        break
                    except Exception as e:
                        err_str = str(e)
                        is_not_found = "404" in err_str or "not found" in err_str.lower()
                        last_err = e
                        attempt += 1
                        if is_not_found or attempt > max_retries:
                            break
                        wait = (2 ** attempt) * 0.5
                        context.log.warning(
                            f"row {idx}: mistral OCR call failed ({e!r}), retrying in {wait}s "
                            f"(attempt {attempt}/{max_retries})"
                        )
                        time.sleep(wait)

                if last_err is not None or resp is None:
                    err_str = str(last_err) if last_err else "no response"
                    markdowns.append(None)
                    page_payloads.append(None)
                    errors.append(err_str)
                    if "401" in err_str or "invalid_api_key" in err_str.lower() or "unauthorized" in err_str.lower():
                        context.log.error(f"row {idx}: invalid Mistral API key. Get a key at https://console.mistral.ai/api-keys")
                    elif "429" in err_str.lower() or "rate" in err_str.lower():
                        context.log.error(f"row {idx}: Mistral rate limit hit. Set rate_limit_delay > 0 to throttle.")
                    else:
                        context.log.error(f"row {idx}: mistral OCR call ultimately failed: {last_err}")
                    if rate_limit_delay > 0:
                        time.sleep(rate_limit_delay)
                    continue

                page_list = list(getattr(resp, "pages", None) or [])
                combined_md = "\n\n---\n\n".join((p.markdown or "") for p in page_list)
                markdowns.append(combined_md or None)
                if output_pages_column:
                    import json
                    page_payloads.append(json.dumps([
                        {"index": p.index, "markdown": p.markdown} for p in page_list
                    ]))
                else:
                    page_payloads.append(None)
                errors.append(None)
                success += 1
                total_pages_processed += len(page_list)
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)

            df[output_column] = markdowns
            if output_pages_column:
                df[output_pages_column] = page_payloads
            if any(errors):
                df[f"{output_column}_error"] = errors

            preview_md = df.head(5).to_markdown(index=False) or ""
            context.add_output_metadata({
                "rows":           MetadataValue.int(len(df)),
                "documents_ocrd": MetadataValue.int(success),
                "pages_processed": MetadataValue.int(total_pages_processed),
                "model":          MetadataValue.text(model),
                "provider":       MetadataValue.text("Mistral AI"),
                "preview":        MetadataValue.md(preview_md),
            })
            return df

        return Definitions(assets=[_asset])
