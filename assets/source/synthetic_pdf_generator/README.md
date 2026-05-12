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
## ⚠️ Deployment note (Dagster+ / Kubernetes)

This component reads or writes local filesystem paths. Behavior across deployments:

| Environment | Works? |
|---|---|
| Local dev | ✅ Yes |
| Dagster+ Serverless (multiprocess executor, default) | ✅ Within a single run — `/tmp/...` is shared across ops in the same run. Files do **not** persist after the run ends. |
| Dagster Hybrid on k8s with `k8s_job` executor (op-per-pod) | ❌ Each op runs in its own pod with its own `/tmp` — files don't travel between ops, even within one run. Set the run to use the `in_process` executor as a workaround. |
| Cross-run reads (run N writes, run N+1 reads) | ❌ Anywhere — the local filesystem is ephemeral by definition. |

**Recommended alternatives for production:**

1. **Return bytes as the asset value** instead of writing a file. The default `PickledObjectFilesystemIOManager` (and the Dagster+ Serverless S3-backed IO manager) serialize binary data fine. Downstream ops read the bytes from the IO manager regardless of pod / run.
2. **Use a cloud-storage sink** for cross-run persistence: [`dataframe_to_s3`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_s3), [`dataframe_to_gcs`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_gcs), [`dataframe_to_adls`](https://github.com/eric-thomas-dagster/dagster-component-templates/tree/main/assets/sinks/dataframe_to_adls).
3. **Mount a shared volume** (k8s PVC / Cloud Run volumes) if you genuinely need a shared filesystem path across pods.
