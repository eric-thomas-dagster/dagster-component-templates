# Cloud Document AI

Structured document parsing via Google Cloud Document AI. Pass a column of document references (PDFs, images) — get back text, form fields, entities, and tables in a structured DataFrame.

```yaml
type: dagster_component_templates.DocumentAiExtractorComponent
attributes:
  asset_name: parsed_invoices
  upstream_asset_key: invoice_files
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  processor_id: 1234567890abcdef
  location: us
  document_column: file_path
  extract_entities: true       # for INVOICE / RECEIPT / W2 / 1040 / etc.
```

## What kind of processor?

Document AI uses **deployed processors** identified by an UUID-style id. Each processor is a deployed instance of a *processor type*. Common types:

| Type | When |
|---|---|
| **OCR_PROCESSOR** | Rich OCR with layout, paragraphs, handwriting. Best general-purpose. |
| **FORM_PARSER_PROCESSOR** | Extract `key/value` pairs from any form. |
| **LAYOUT_PARSER_PROCESSOR** | Full document structure (sections, headers, tables). |
| **INVOICE_PROCESSOR** | Invoice-tuned: vendor, total, line items, dates. |
| **RECEIPT_PROCESSOR** | Receipt-tuned. |
| **W2_PROCESSOR** / **1040_PROCESSOR** | US tax forms. |
| **PAYSTUB_PROCESSOR** | US paystubs. |
| **CUSTOM_DOCUMENT_EXTRACTOR (CDE)** | Your trained processor for a custom doc type. |

Create / find processors at <https://console.cloud.google.com/ai/document-ai/processors>.

## Output columns

Per the toggles, the component adds:

| Column | Type | Notes |
|---|---|---|
| `doc_text` | str | Full plain text of the document |
| `doc_form_fields` | list[dict] | `[{name, value, confidence}]` — only meaningful for FORM_PARSER |
| `doc_entities` | list[dict] | `[{type, mention_text, confidence, normalized_value}]` — used by Invoice / Receipt / W2 / etc. |
| `doc_tables` | list[2D list] | Each table as a 2D string array |
| `doc_page_count` | int | Page count of the document |
| `doc_error` | str (optional) | Per-row error |

## Document AI vs Vision OCR

| Need | Component |
|---|---|
| Just OCR a photo with text | `vision_api_asset` (DOCUMENT_TEXT_DETECTION feature) |
| Parse a structured form / invoice / receipt | **`document_ai_extractor`** (this) |
| OCR with rich layout (paragraphs, tables) | `document_ai_extractor` with the OCR or LAYOUT_PARSER processor |

## Required SA roles

`roles/documentai.apiUser` on the project + Document AI API enabled.

## Sister components

- `vision_api_asset` — flat OCR for image content.
- `gemini_image_generation` — generate images.
- `vertex_ai_text_embeddings_asset` — embed the extracted `doc_text` for RAG.
