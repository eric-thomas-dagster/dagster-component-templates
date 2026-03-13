# PdfTextExtractorComponent

Extract text from a column containing PDF file paths or PDF bytes. Uses `pdfplumber`.

## Fields

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `asset_name` | string | yes | — | Output Dagster asset name |
| `upstream_asset_key` | string | yes | — | Upstream asset key providing a DataFrame |
| `group_name` | string | no | null | Dagster asset group name |
| `column` | string | yes | — | Column containing PDF file paths or bytes |
| `output_column` | string | no | `"pdf_text"` | Column to write extracted text to |
| `pages` | string | no | null | Comma-separated page numbers or ranges, e.g. `"1,3,5-10"` |
| `include_page_numbers` | boolean | no | false | Prefix each page's text with `[Page N]` |

## Example

```yaml
component_type: dagster_component_templates.PdfTextExtractorComponent
asset_name: extracted_pdf_text
upstream_asset_key: raw_pdf_files
column: file_path
output_column: pdf_text
pages: "1-5,10"
include_page_numbers: true
```
