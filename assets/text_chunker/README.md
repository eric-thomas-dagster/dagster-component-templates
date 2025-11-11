# Text Chunker Asset

Split text into chunks using multiple strategies (fixed tokens, sentences, paragraphs, semantic) for embedding generation, LLM processing, or batch operations.

## Overview

This component chunks text automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor - no configuration needed for dependencies!

**Compatible with:**
- Document Text Extractor
- Document Summarizer
- Any text-producing asset (string output or dict with 'text' key)

Perfect for:
- Preparing text for embedding generation
- Creating chunks for RAG systems
- Splitting documents for LLM context windows
- Batch processing of large documents

## Quick Start

### 1. Extract Text from Document

```yaml
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: document_text
  file_path: /path/to/document.pdf
```

### 2. Chunk the Text

```yaml
# No upstream_asset_key needed - set by drawing connections!
type: dagster_component_templates.TextChunkerComponent
attributes:
  asset_name: document_chunks
  chunking_strategy: fixed_tokens
  chunk_size: 512
  chunk_overlap: 50
  output_format: list
```

### 3. Draw Connection

In the visual editor: `document_text` → `document_chunks`

That's it! The IO manager automatically passes the text.

## Features

- **Visual Dependencies**: Draw connections, no manual configuration
- **Multiple Strategies**: Fixed characters, tokens, sentences, paragraphs, recursive, semantic
- **Overlap Support**: Configure overlap to maintain context
- **Sentence Preservation**: Avoid breaking sentences mid-chunk
- **Metadata Enrichment**: Add chunk positions, indices, and statistics
- **Flexible Output**: List, DataFrame, or JSON

## Input Requirements

**Accepts text from upstream assets via IO manager:**
- String content (plain text)
- Dict with 'text' key (e.g., `{"text": "content here"}`)
- Any text-producing asset

## Output Format

**Based on `output_format` parameter:**
- `list` - Returns list of text chunks
- `dataframe` - Returns DataFrame with columns: `chunk`, `index`, `start_pos`, `end_pos`, `length`
- `json` - Returns JSON-serialized list of chunk objects with metadata

## Configuration

### Required
- **asset_name** (string) - Asset name

### Optional
- **chunking_strategy** (string) - `"fixed_chars"`, `"fixed_tokens"`, `"sentence"`, `"paragraph"`, `"semantic"`, `"recursive"` (default: `"fixed_chars"`)
- **chunk_size** (integer) - Target chunk size (default: `1000`)
- **chunk_overlap** (integer) - Overlap between chunks (default: `200`)
- **separator** (string) - Custom separator for recursive strategy
- **preserve_sentences** (boolean) - Don't break sentences (default: `true`)
- **add_metadata** (boolean) - Add metadata to chunks (default: `true`)
- **output_format** (string) - `"list"`, `"dataframe"`, `"json"` (default: `"list"`)
- **save_to_file** (boolean) - Save chunks (default: `false`)
- **output_path** (string) - Path to save
- **description**, **group_name** (string)

## Example Pipeline

```yaml
# Step 1: Extract text from document
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: document_text
  file_path: /path/to/document.pdf

# Step 2: Chunk the text
type: dagster_component_templates.TextChunkerComponent
attributes:
  asset_name: document_chunks
  chunking_strategy: fixed_tokens
  chunk_size: 512
  chunk_overlap: 50
  output_format: list

# Step 3: Process chunks further (e.g., embeddings)
type: dagster_component_templates.EmbeddingGeneratorComponent
attributes:
  asset_name: document_embeddings
  provider: openai
  model: text-embedding-3-small
```

**Visual Connections:**
```
document_text → document_chunks → document_embeddings
```

## Standalone Examples

### Token-Based for LLM
```yaml
type: dagster_component_templates.TextChunkerComponent
attributes:
  asset_name: llm_chunks
  chunking_strategy: fixed_tokens
  chunk_size: 512
  chunk_overlap: 50
```

### Sentence-Based with DataFrame Output
```yaml
type: dagster_component_templates.TextChunkerComponent
attributes:
  asset_name: sentence_chunks
  chunking_strategy: sentence
  chunk_size: 2000
  output_format: dataframe
```

## Strategies

| Strategy | Best For | Description |
|----------|----------|-------------|
| `fixed_chars` | General use | Fixed character length |
| `fixed_tokens` | LLM context | Approximate token count |
| `sentence` | Semantic units | Full sentences |
| `paragraph` | Document structure | Paragraph boundaries |
| `recursive` | Hierarchical | Multiple separators |
| `semantic` | Context preservation | Structure-aware |

## Requirements
- pandas >= 2.0.0

## License
MIT License
