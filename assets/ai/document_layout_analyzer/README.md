# Document Layout Analyzer Component

Analyze the layout of document images (e.g. PDF pages rendered as images) to detect regions: text blocks, tables, figures, and headers. Returns structured region data with bounding boxes and confidence scores.

## Overview

The Document Layout Analyzer Component uses HuggingFace document layout analysis models to identify and locate structural regions within document images. It is particularly useful for extracting table boundaries, figure captions, and text blocks from scanned or rendered PDF pages.

## Features

- **Region detection**: Identify text, table, figure, and header regions
- **Bounding boxes**: Get precise pixel coordinates for each region
- **Region type filtering**: Focus on specific region types (e.g. only tables)
- **HuggingFace integration**: Use LayoutLM and compatible models
- **Device support**: CPU, CUDA, and Apple MPS

## Use Cases

1. **Table extraction**: Detect table boundaries for downstream data extraction
2. **PDF structure analysis**: Understand document organization
3. **Figure captioning**: Locate figures for caption association
4. **Document classification**: Classify pages by layout type

## Prerequisites

- `transformers>=4.30.0`, `Pillow>=9.0.0`, `torch>=2.0.0`

## Configuration

### Detect All Regions

```yaml
type: dagster_component_templates.DocumentLayoutAnalyzerComponent
attributes:
  asset_name: document_layout
  upstream_asset_key: page_images
  image_column: page_path
  model_name: microsoft/layoutlmv3-base
```

### Detect Only Tables and Figures

```yaml
type: dagster_component_templates.DocumentLayoutAnalyzerComponent
attributes:
  asset_name: tables_and_figures
  upstream_asset_key: pdf_pages
  image_column: image_path
  region_types:
    - table
    - figure
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `image_column` | string | required | Column with document image paths |
| `output_column` | string | `layout_regions` | Column for region results |
| `model_name` | string | `microsoft/layoutlmv3-base` | HuggingFace model ID |
| `region_types` | list | `None` | Filter to these region types |
| `device` | string | `cpu` | cpu, cuda, or mps |
| `group_name` | string | `None` | Asset group |

## Output

Original DataFrame plus `output_column` (list of `{type, bbox, confidence}` dicts) and `region_count`.
