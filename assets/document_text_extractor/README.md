# Document Text Extractor Asset

Extract text content from PDF, DOCX, HTML, Markdown, and other document formats with OCR support and metadata extraction.

## Overview

This asset component extracts text from various document formats for downstream processing. Perfect for:
- Document ingestion pipelines
- Content extraction for search/indexing
- Text preprocessing for NLP/LLM tasks
- Automated document processing
- Data extraction from contracts/reports

## Features

- **Multiple Formats**: PDF, DOCX, HTML, Markdown, TXT
- **OCR Support**: Extract text from scanned PDFs and images (Tesseract)
- **Metadata Extraction**: Author, title, dates, and more
- **Page Range Selection**: Extract specific pages from PDFs
- **Format Preservation**: Optional formatting and structure preservation
- **Multiple Extraction Methods**: pypdf, pdfplumber, pytesseract, docx, beautifulsoup
- **Output Formats**: Text, JSON (with metadata), Markdown

## Configuration

### Required Parameters

- **asset_name** (string) - Name of the asset
- **file_path** (string) - Path to document file

### Optional Parameters

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
