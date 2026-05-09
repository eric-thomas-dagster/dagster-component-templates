# Google Drive Ingestion

List (and optionally download) Google Drive files via a service account. Returns a pandas DataFrame, one row per matching file.

For specific Google formats, use the dedicated component instead:
- **Google Sheets** → [`google_sheets_ingestion`](../google_sheets_ingestion/README.md)
- **Google Docs** (text) → [`google_docs_extractor`](../google_docs_extractor/README.md)

This component is the right pick for everything else: PDFs, CSVs, images, audio, arbitrary uploads.

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

Or pass `credentials_path` / `credentials` (inline dict) directly in the YAML.

## Setup gotcha

Service accounts can't see anything in Drive unless explicitly shared with them. The SA email is in your JSON's `client_email` field.

- **Folder share**: open the Drive folder → Share → paste the SA email → Viewer.
- **Per-file share**: same flow, on a single file.
- **Domain-wide delegation**: only needed for Workspace-managed user impersonation. Skip unless you need to read user mailboxes.

Also: the **Drive API has to be enabled** on the SA's GCP project. If it's not, the first call returns a `403 SERVICE_DISABLED` with the exact activation URL — click and Enable.

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `asset_name` | yes | — | Output asset name |
| `credentials` | no* | — | SA JSON as a dict |
| `credentials_path` | no* | — | Path to SA JSON file |
| `folder_id` | no | — | Drive folder ID to list. Mutually exclusive with `query`. |
| `query` | no | — | Drive search query (`q` syntax). Mutually exclusive with `folder_id`. |
| `mime_type_filter` | no | — | List of MIME types to keep |
| `include_trashed` | no | `false` | Include trashed files |
| `max_files` | no | `100` | Stop after this many files |
| `download` | no | `false` | Also download file contents |
| `download_dir` | no | `/tmp/google_drive_ingestion` | Where to write files |
| `download_max_bytes` | no | `10 MB` | Skip files larger than this |
| `inline_text_content` | no | `false` | Also embed decoded text in a `content` column |
| `description` / `group_name` / `deps` / `tags` / `owners` | no | — | Standard Dagster attrs |
| `partition_*` | no | — | Canonical registry partition shape |

*one of `credentials`, `credentials_path`, or `GOOGLE_APPLICATION_CREDENTIALS` must be set.

## Output schema

| Column | Type | Notes |
|---|---|---|
| `id` | str | Drive file ID |
| `name` | str | Filename |
| `mimeType` | str | e.g. `application/pdf` |
| `size` | str | Bytes (str — Drive API returns it as a string) |
| `modifiedTime` | str | ISO timestamp |
| `webViewLink` | str | Browser URL |
| `parents` | list | Parent folder IDs |
| `owner_email` | str | First owner's email |
| `path` | str (optional) | Local file path when `download=true` |
| `download_skipped` | str (optional) | Reason for skip when `download=true` |
| `content` | str (optional) | Decoded text when `inline_text_content=true` |

## Common Drive query patterns

```yaml
# All PDFs
query: "mimeType='application/pdf'"

# Files in a specific folder
folder_id: 1ABCdefGHIjklMNOpqrSTUv

# Files modified this week
query: "modifiedTime > '2026-05-01T00:00:00'"

# Names containing 'invoice', any type
query: "name contains 'invoice'"

# Compound: PDFs in a folder modified recently
folder_id: 1ABCdefGHIjklMNOpqrSTUv
query: "mimeType='application/pdf' and modifiedTime > '2026-05-01T00:00:00'"
```

(`folder_id` and `query` can be combined when the component is invoked — both get AND-joined into the Drive `q` parameter.)

## Native Google formats

When `download=true` and the file's MIME type is `application/vnd.google-apps.*` (Docs / Sheets / Slides), this component **skips** the download and writes a reason into `download_skipped`. Use the dedicated component:

- Docs → `google_docs_extractor`
- Sheets → `google_sheets_ingestion`

## Example

```yaml
type: dagster_component_templates.GoogleDriveIngestionComponent
attributes:
  asset_name: drive_invoices
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  query: "mimeType='application/pdf' and name contains 'invoice'"
  max_files: 50
  download: true
  download_dir: /tmp/drive_invoices
  description: PDF invoices from Drive
  group_name: drive
```
