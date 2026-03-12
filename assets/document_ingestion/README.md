# Document Ingestion

Ingest documents from filesystem for RAG (Retrieval Augmented Generation) and Q&A systems.

## Overview

The Document Ingestion component scans a directory for documents, extracts metadata, and prepares them for embedding and vector storage. It supports multiple document formats and provides a knowledge base foundation for RAG pipelines.

## Features

- **Multi-Format Support**: Handles .txt, .md, .pdf, .doc, .docx, .html files
- **Recursive Scanning**: Searches subdirectories for documents
- **Metadata Extraction**: Captures file paths, sizes, modification times
- **Sample Fallback**: Provides sample knowledge base when no source path specified
- **RAG-Ready**: Outputs structure compatible with embedding and vector store components

## Configuration

### Required Parameters

- `asset_name`: Name of the document collection asset

### Optional Parameters

- `source_path`: Path to directory containing documents (optional)
- `description`: Asset description
- `group_name`: Asset group for organization (default: "knowledge_base")
- `include_sample_metadata`: Include data preview in metadata (default: true)

## Usage

```yaml
type: dagster_component_templates.DocumentIngestionComponent
attributes:
  asset_name: knowledge_base_docs
  source_path: "/path/to/documents"
  description: "Documents for RAG/Q&A system"
```

## Supported File Types

- `.txt`: Plain text files
- `.md`: Markdown documents
- `.pdf`: PDF documents
- `.doc`, `.docx`: Microsoft Word documents
- `.html`: HTML files

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| document_id | int | Unique document identifier |
| title | string | Document title (filename) |
| content | string | Full document text content |
| source | string | File path to original document |
| file_size | int | File size in bytes |
| last_modified | datetime | File modification timestamp |
| file_type | string | File extension/type |

## Use Cases

- RAG systems
- Q&A applications
- Knowledge base indexing
- Semantic search
- AI assistant context

## Dependencies

- pandas>=1.5.0

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
