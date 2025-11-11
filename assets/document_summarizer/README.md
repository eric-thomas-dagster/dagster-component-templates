# Document Summarizer Asset

Summarize documents using LLMs with map-reduce support for long documents and multiple summary styles.

## Overview

This component summarizes text automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor - no configuration needed for dependencies!

**Compatible with:**
- Document Text Extractor
- Text Chunker
- Any text-producing asset (string output or dict with 'text' key)

Perfect for report generation, content digestion, and document analysis.

## Quick Start

### 1. Extract Text from Document

```yaml
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: report_text
  file_path: /path/to/report.pdf
```

### 2. Summarize the Text

```yaml
# No upstream_asset_key needed - set by drawing connections!
type: dagster_component_templates.DocumentSummarizerComponent
attributes:
  asset_name: report_summary
  provider: openai
  model: gpt-4
  summary_type: executive
  api_key: ${OPENAI_API_KEY}
```

### 3. Draw Connection

In the visual editor: `report_text` → `report_summary`

That's it! The IO manager automatically passes the text.

## Features

- **Visual Dependencies**: Draw connections, no manual configuration
- **Multiple Summary Types**: Concise, detailed, bullet points, executive
- **Map-Reduce**: Handle documents longer than LLM context
- **Multiple Providers**: OpenAI, Anthropic
- **Length Control**: Specify max summary length
- **Auto-Chunking**: Automatic chunking for long documents

## Input Requirements

**Accepts text from upstream assets via IO manager:**
- String content (plain text)
- Dict with 'text' key (e.g., `{"text": "content here"}`)
- Any text-producing asset

## Output Format

**Returns summarized text string** that can be consumed by downstream assets.

## Configuration

### Required
- **asset_name** (string), **provider** (string), **model** (string)

### Optional
- **summary_type** (string) - `"concise"`, `"detailed"`, `"bullet_points"`, `"executive"` (default: `"concise"`)
- **max_length** (integer) - Max summary length in words
- **chunk_size** (integer) - Chunk size for long docs (default: `3000`)
- **use_map_reduce** (boolean) - Use map-reduce (default: `true`)
- **api_key** (string) - `${API_KEY}`
- **temperature** (number) - default: `0.3`

## Example Pipeline

```yaml
# Step 1: Extract text from document
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: report_text
  file_path: /path/to/report.pdf

# Step 2: Summarize the document
type: dagster_component_templates.DocumentSummarizerComponent
attributes:
  asset_name: report_summary
  provider: openai
  model: gpt-4
  summary_type: executive
  max_length: 500
  api_key: ${OPENAI_API_KEY}

# Step 3: Classify the summary sentiment (optional)
type: dagster_component_templates.TextClassifierComponent
attributes:
  asset_name: summary_sentiment
  provider: openai
  model: gpt-4
  categories: '["positive", "negative", "neutral"]'
  api_key: ${OPENAI_API_KEY}
```

**Visual Connections:**
```
report_text → report_summary → summary_sentiment
```

## Standalone Examples

### Concise Summary
```yaml
type: dagster_component_templates.DocumentSummarizerComponent
attributes:
  asset_name: brief_summary
  provider: openai
  model: gpt-4
  summary_type: concise
  max_length: 200
  api_key: ${OPENAI_API_KEY}
```

### Executive Summary with Anthropic
```yaml
type: dagster_component_templates.DocumentSummarizerComponent
attributes:
  asset_name: executive_summary
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  summary_type: executive
  temperature: 0.2
  api_key: ${ANTHROPIC_API_KEY}
```

### Bullet Points
```yaml
type: dagster_component_templates.DocumentSummarizerComponent
attributes:
  asset_name: key_points
  provider: openai
  model: gpt-4
  summary_type: bullet_points
  api_key: ${OPENAI_API_KEY}
```

## Summary Types

- **concise**: Brief 1-2 paragraph summary
- **detailed**: Comprehensive multi-paragraph summary
- **bullet_points**: Key points as bulleted list
- **executive**: Executive summary for leadership

## Requirements
- openai >= 1.0.0, anthropic >= 0.18.0

## License
MIT License
