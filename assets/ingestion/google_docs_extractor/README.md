# Google Docs Extractor

Extract plain text + headings from Google Docs by ID via a service account.

Two input modes:

- **Explicit `doc_ids: [...]`** — fetch each Doc by ID directly.
- **Upstream DataFrame** via `upstream_asset_key` + `id_column` — typical pairing with [`google_drive_ingestion`](../google_drive_ingestion/README.md) (filter Drive to `mimeType='application/vnd.google-apps.document'`, then feed the IDs here).

For everything else (PDFs, CSVs, raw uploads), use `google_drive_ingestion` directly.
For Google Sheets specifically, use `google_sheets_ingestion`.

## Required packages

```
dagster>=1.8.0
pandas>=1.5.0
google-auth>=2.0.0
google-api-python-client>=2.0.0
```

## Required env var (or alternative)

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
```

## Setup gotchas

1. **Each Doc must be shared with the SA email.** Open the Doc → Share → paste the SA email → Viewer.
2. **Docs API has to be enabled** on the SA's GCP project. If not, the first call returns `403 SERVICE_DISABLED` with the activation URL — click and Enable.

## Output schema

| Column | Type | Notes |
|---|---|---|
| `id` | str | Doc ID |
| `title` | str | Doc title (None on failure) |
| `word_count` | int | Word count of the extracted text |
| `text` | str (optional) | Full plain text — set `include_text: false` to suppress |
| `headings` | list (optional) | List of heading strings — set `include_headings: false` to suppress |
| `_error` | str (optional) | Per-row error if a Doc failed; column omitted if all rows succeeded |

## Example — explicit IDs

```yaml
type: dagster_component_templates.GoogleDocsExtractorComponent
attributes:
  asset_name: doc_texts
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  doc_ids:
    - "1abcdefGHIjklmnoPQRstuvwXYZ0123456789"
    - "1OTHERdocID9876543210FGHIJKLmnopQRStuvw"
  include_text: true
  include_headings: true
  group_name: docs
```

## Example — pipeline with `google_drive_ingestion`

Two components, with the Docs extractor consuming the Drive listing:

```yaml
# defs/drive_docs_listing/defs.yaml
type: dagster_component_templates.GoogleDriveIngestionComponent
attributes:
  asset_name: drive_docs_listing
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  query: "mimeType='application/vnd.google-apps.document'"
  download: false
  group_name: drive
```

```yaml
# defs/doc_texts/defs.yaml
type: dagster_component_templates.GoogleDocsExtractorComponent
attributes:
  asset_name: doc_texts
  upstream_asset_key: drive_docs_listing
  id_column: id
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  group_name: docs
```

The Drive component lists every Google Doc the SA can see; the Docs component extracts text from each. Drop in any `transform_*` or `llm_*` component downstream for embeddings / RAG / summarization.
