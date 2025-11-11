# Document Text Extractor Asset

Extract text content from PDF, DOCX, HTML, Markdown, and other document formats with OCR support and metadata extraction.

## Overview

This asset component extracts text from various document formats for downstream processing. Perfect for:
- Document ingestion pipelines
- Content extraction for search/indexing
- Text preprocessing for NLP/LLM tasks
- Automated document processing
- Data extraction from contracts/reports

## Using with Sensors and RunConfig

**This is the IDEAL pattern for source assets that need dynamic file inputs.**

### The Pattern: File Paths from RunConfig

File paths should come from **RunConfig at runtime**, not hardcoded in component configuration. This makes the asset perfect for use with sensors that detect new files.

### Why This Pattern?

- **Dynamic Processing**: Process files that don't exist when the asset is defined
- **Sensor-Driven**: Sensors can trigger runs for newly detected files
- **Flexibility**: Different files can be processed on each run
- **Separation of Concerns**: Configuration defines behavior, RunConfig provides inputs

### Complete Sensor Example

Here's a working sensor that monitors a directory and creates runs for new PDF files:

```python
from dagster import sensor, RunRequest, SensorEvaluationContext
import os
from pathlib import Path

@sensor(
    name="new_documents_sensor",
    minimum_interval_seconds=60,
)
def new_documents_sensor(context: SensorEvaluationContext):
    """Monitor directory for new documents and trigger extraction."""

    watch_directory = "/data/incoming/documents"
    processed_key = "processed_files"

    # Get previously processed files
    cursor = context.cursor or ""
    processed_files = set(cursor.split(",")) if cursor else set()

    # Find all documents in directory
    current_files = set()
    for ext in ["*.pdf", "*.docx", "*.html"]:
        current_files.update(
            str(f) for f in Path(watch_directory).glob(ext)
        )

    # Identify new files
    new_files = current_files - processed_files

    # Create RunRequest for each new file
    for file_path in sorted(new_files):
        yield RunRequest(
            run_key=f"document_extract_{os.path.basename(file_path)}",
            run_config={
                "ops": {
                    "report_text": {  # Must match your asset_name
                        "config": {
                            "file_path": file_path
                        }
                    }
                }
            },
            tags={
                "source": "new_documents_sensor",
                "file": os.path.basename(file_path)
            }
        )

    # Update cursor with all current files
    new_cursor = ",".join(current_files) if current_files else ""
    context.update_cursor(new_cursor)
```

### RunConfig Format

When triggering runs (via sensors, schedules, or manually), provide the file path in this exact format:

```python
{
    "ops": {
        "your_asset_name": {  # Must match the asset_name in component config
            "config": {
                "file_path": "/path/to/file.pdf"
            }
        }
    }
}
```

### Input from Upstream Assets (Alternative)

The asset can also receive file paths from upstream assets:

```python
# Upstream asset produces a file path
@asset
def downloaded_document():
    return "/data/downloads/report.pdf"

# This component asset receives it
# Component config just defines the asset_name and behavior
```

The component automatically handles both:
- **String input**: Direct file path
- **Dict input**: `{"file_path": "/path/to/file.pdf", ...}`

## Features

- **Multiple Formats**: PDF, DOCX, HTML, Markdown, TXT
- **OCR Support**: Extract text from scanned PDFs and images (Tesseract)
- **Metadata Extraction**: Author, title, dates, and more
- **Page Range Selection**: Extract specific pages from PDFs
- **Format Preservation**: Optional formatting and structure preservation
- **Multiple Extraction Methods**: pypdf, pdfplumber, pytesseract, docx, beautifulsoup
- **Output Formats**: Text, JSON (with metadata), Markdown

## Input & Output

### Input

The asset receives the file path in one of these ways:

1. **From RunConfig (Recommended for Dynamic Processing)**
   - Provided at runtime via `run_config` in sensors, schedules, or manual runs
   - Format: `{"ops": {"asset_name": {"config": {"file_path": "/path/to/file.pdf"}}}}`
   - Ideal for processing files that are detected dynamically

2. **From Upstream Assets**
   - Receives file path from a dependency
   - Can be a string: `"/path/to/file.pdf"`
   - Or a dict with 'file_path' key: `{"file_path": "/path/to/file.pdf", ...}`

3. **From Component Configuration (Static Only)**
   - Hardcoded in the component YAML definition
   - Only suitable for fixed, known file paths
   - Not recommended for dynamic file processing

**Important**: The file path comes from RunConfig or upstream assets, NOT from component configuration (except for static use cases).

### Output

Returns extracted text in the specified output format:
- **text** (default): Plain text string
- **json**: Dictionary with text, metadata, file_path, and character_count
- **markdown**: Markdown-formatted text string

## Configuration

### Required Parameters

- **asset_name** (string) - Name of the asset

### Optional Parameters

- **file_path** (string) - Path to document file (only for static/hardcoded use cases; prefer RunConfig or upstream assets for dynamic processing)


- **extraction_method** (string) - Method: `"auto"`, `"pypdf"`, `"pdfplumber"`, `"pytesseract"`, `"docx"`, `"html"`, `"markdown"`, `"text"` (default: `"auto"`)
- **ocr_enabled** (boolean) - Enable OCR for scanned documents (default: `false`)
- **preserve_formatting** (boolean) - Preserve document formatting (default: `false`)
- **extract_metadata** (boolean) - Extract document metadata (default: `true`)
- **page_range** (string) - Page range for PDFs (e.g., `"1-5"`, `"1,3,5"`)
- **output_format** (string) - Format: `"text"`, `"json"`, `"markdown"` (default: `"text"`)
- **save_to_file** (boolean) - Save extracted text to file (default: `false`)
- **output_path** (string) - Path to save extracted text
- **description** (string) - Asset description
- **group_name** (string) - Asset group

## Usage Examples

### Extract PDF

```yaml
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: report_text
  file_path: /data/reports/q4_report.pdf
  extraction_method: pypdf
  extract_metadata: true
```

### Extract with OCR

```yaml
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: scanned_contract
  file_path: /data/scanned/contract.pdf
  ocr_enabled: true
  extraction_method: pytesseract
```

### Extract Specific Pages

```yaml
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: executive_summary
  file_path: /data/report.pdf
  page_range: "1-3"
  output_format: markdown
```

### Extract DOCX with Metadata

```yaml
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: proposal_text
  file_path: /data/proposal.docx
  extraction_method: docx
  preserve_formatting: true
  output_format: json
```

## Extraction Methods

### pypdf (PDF)

Fast, good for most PDFs:
```yaml
extraction_method: pypdf
```

### pdfplumber (PDF)

Better table extraction:
```yaml
extraction_method: pdfplumber
```

### pytesseract (OCR)

For scanned documents:
```yaml
extraction_method: pytesseract
ocr_enabled: true
```

Requires Tesseract installation: https://github.com/tesseract-ocr/tesseract

### docx (DOCX/DOC)

Microsoft Word documents:
```yaml
extraction_method: docx
```

### html (HTML)

Web pages and HTML files:
```yaml
extraction_method: html
```

## Output Formats

### Text

Plain text output:
```yaml
output_format: text
```

### JSON

Text with metadata:
```yaml
output_format: json
```

Returns:
```json
{
  "text": "Document content...",
  "metadata": {
    "title": "Report",
    "author": "John Doe",
    "created": "2024-01-15"
  },
  "file_path": "/data/report.pdf",
  "character_count": 5420
}
```

### Markdown

Markdown-formatted text:
```yaml
output_format: markdown
```

## Requirements

- pypdf >= 3.0.0
- pdfplumber >= 0.9.0
- python-docx >= 0.8.11
- beautifulsoup4 >= 4.11.0
- pytesseract >= 0.3.10 (for OCR)
- pdf2image >= 1.16.0 (for OCR)
- Pillow >= 9.0.0 (for OCR)

## Contributing

GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
