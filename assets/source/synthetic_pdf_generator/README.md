# Synthetic PDF Generator

Generate sample PDFs and emit a DataFrame describing them — built for OCR / Document AI / Vision demos that need a deterministic source of PDFs without committing binary fixtures.

```yaml
type: dagster_component_templates.SyntheticPdfGeneratorComponent
attributes:
  asset_name: sample_documents
  samples: default
```

## Output shape

| Column | Type |
|---|---|
| `doc_id` | str — id provided in the document definition |
| `kind` | str — category (`invoice`, `letter`, etc.) |
| `file_path` | str — absolute path to the written PDF |
| `pages` | int — page count |

## Built-in sample set

The default set has 2 PDFs designed to exercise OCR:
- `INV-2026-0042` (invoice with totals)
- `shipping-notice` (letter with a UPS tracking number)

Both fit in plain Helvetica 12pt — Document AI's OCR_PROCESSOR extracts both cleanly.

## Custom documents

```yaml
attributes:
  asset_name: my_docs
  output_dir: /tmp/my_pdfs
  samples: custom
  documents:
    - doc_id: contract-001
      kind: contract
      title: "AGREEMENT"
      body:
        - "This Master Service Agreement ('Agreement') is entered into on..."
        - "between Party A and Party B."
        - ""
        - "Signed: ___________________"
```

Each `body` entry is one line on the PDF.

## Typical chain

```yaml
sample_documents     ← synthetic_pdf_generator
       │
       └── documents_extracted   ← document_ai_extractor
```

## Why this exists

Demos must be 100% components — no custom `definitions.py` writing PDFs with `reportlab` inline. This component is the shared upstream for any document-processing demo.

## Sister components

- `synthetic_data_generator` — synthetic tabular DataFrames (orders, sensors, events, etc.)
- `document_ai_extractor` — typical downstream (Cloud Document AI)
- `vision_api_asset` — alternative downstream for image-based OCR (rasterize PDFs first)
