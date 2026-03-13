# OCR Extractor Component

Extract text from images using Tesseract OCR. Processes a column of image file paths and writes extracted text to a new column. Supports multi-language OCR, configurable page segmentation modes, and optional PIL-based preprocessing.

## Overview

The OCR Extractor Component uses `pytesseract` (a Python wrapper for Tesseract) to extract text from images. It optionally preprocesses images using PIL (grayscale conversion and thresholding) to improve OCR accuracy on low-contrast or noisy images. It can also write mean OCR confidence scores to a separate column.

## Features

- **Multi-language support**: Configure any Tesseract language pack (e.g. `eng+fra`)
- **Page Segmentation Mode**: Control Tesseract's PSM for different document types
- **Auto-preprocessing**: Grayscale + threshold via PIL when `preprocessed=false`
- **Confidence column**: Optionally extract mean word-level confidence score

## Use Cases

1. **Document digitization**: Convert scanned pages to searchable text
2. **Receipt processing**: Extract text from receipt images for expense analysis
3. **Form data extraction**: OCR structured forms for downstream processing
4. **License plate recognition**: Extract text from vehicle plates (use PSM 8)

## Prerequisites

- `pytesseract>=0.3.10`, `Pillow>=9.0.0`
- Tesseract OCR must be installed on the system: `brew install tesseract` (macOS) or `apt-get install tesseract-ocr`

## Configuration

### Basic OCR

```yaml
type: dagster_component_templates.OcrExtractorComponent
attributes:
  asset_name: ocr_text
  upstream_asset_key: document_images
  image_column: file_path
  output_column: extracted_text
```

### Multi-language with Confidence

```yaml
type: dagster_component_templates.OcrExtractorComponent
attributes:
  asset_name: multilingual_ocr
  upstream_asset_key: document_images
  image_column: file_path
  language: "eng+fra"
  psm: 6
  confidence_column: ocr_confidence
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column` | string | required | Column with image file paths |
| `output_column` | string | `ocr_text` | Column to write extracted text |
| `language` | string | `eng` | Tesseract language code(s) |
| `psm` | integer | `3` | Page segmentation mode (0-13) |
| `preprocessed` | boolean | `false` | Skip PIL preprocessing if true |
| `confidence_column` | string | `None` | Column for mean OCR confidence |
| `group_name` | string | `None` | Asset group |

## Output

Original DataFrame plus `output_column` with extracted text, and optionally `confidence_column` with mean confidence (0-100).

## Troubleshooting

- **TesseractNotFoundError**: Install Tesseract on your system
- **Language not found**: Install additional language packs (e.g. `apt-get install tesseract-ocr-fra`)
- **Low accuracy**: Try `preprocessed=false` to enable auto-preprocessing, or adjust `psm`
