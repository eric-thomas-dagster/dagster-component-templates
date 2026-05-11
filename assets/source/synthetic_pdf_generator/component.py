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

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        output_dir = self.output_dir
        if self.samples == "custom":
            if not self.documents:
                raise ValueError("samples='custom' requires `documents` to be set.")
            docs = self.documents
        else:
            docs = _DEFAULT_DOCS

        @asset(
            name=asset_name,
            description=self.description or f"Synthetic PDFs ({len(docs)}) for OCR / Document AI demos.",
            group_name=self.group_name,
            kinds={"reportlab"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
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
