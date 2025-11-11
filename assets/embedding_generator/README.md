# Embedding Generator Asset

Generate vector embeddings for text using OpenAI, Sentence Transformers, Cohere, or Hugging Face with batch processing. Works seamlessly with Dagster's IO manager pattern - connect text/chunk assets and automatically generate embeddings!

## Overview

Perfect for:
- RAG (Retrieval-Augmented Generation) pipelines
- Semantic search systems
- Document similarity analysis
- Vector database population

**Compatible with:**
- Document Text Extractor (produces text strings)
- Text Chunker (produces list of text chunks or DataFrame)
- REST API Fetcher (with text output)
- Any asset producing string, list of strings, or DataFrame with text column

## Features

- **Multiple Providers**: OpenAI, Sentence Transformers, Cohere, Hugging Face
- **Batch Processing**: Efficient batching for large text collections
- **Multiple Formats**: Array, list, or DataFrame output
- **Normalization**: Optional L2 normalization
- **IO Manager Compatible**: Works seamlessly with upstream assets via visual connections

## Configuration

### Required
- **asset_name** (string)
- **provider** (string) - `"openai"`, `"sentence_transformers"`, `"cohere"`, `"huggingface"`
- **model** (string) - Model name

### Optional
- **text_input** (string/list) - Direct text input(s)
- **text_column** (string) - Column with text if DataFrame (default: `"text"`)
- **batch_size** (integer) - Batch size (default: `100`)
- **api_key** (string) - API key (use `${API_KEY}`)
- **normalize_embeddings** (boolean) - Normalize to unit length (default: `true`)
- **output_format** (string) - `"array"`, `"list"`, `"dataframe"` (default: `"array"`)
- **add_text_to_output** (boolean) - Include text in output (default: `true`)
- **save_to_file** (boolean), **output_path** (string)
- **description**, **group_name** (string)

## Input & Output

### Input Requirements
Accepts text/chunks from upstream assets via IO manager:
- **String**: Single text document
- **List of strings**: Multiple text chunks
- **DataFrame**: Must have a text column (default column name: `"text"`)

### Output Format
Based on `output_format` parameter:
- **array** (default): NumPy array of embeddings
- **list**: Python list of embeddings
- **dataframe**: Pandas DataFrame with `embedding` column (and optionally `text` column)

## Example Pipeline

### Complete Text → Chunks → Embeddings Pipeline

```yaml
# 1. Extract text from document
- type: DocumentTextExtractorComponent
  attributes:
    asset_name: document_text
    file_path: /path/to/document.pdf

# 2. Chunk text into smaller pieces
- type: TextChunkerComponent
  attributes:
    asset_name: document_chunks
    chunking_strategy: fixed_tokens
    chunk_size: 512
    output_format: dataframe

# 3. Generate embeddings for chunks
- type: EmbeddingGeneratorComponent
  attributes:
    asset_name: chunk_embeddings
    provider: openai
    model: text-embedding-3-small
    api_key: ${OPENAI_API_KEY}
    output_format: dataframe
```

**Visual Connections:**
```
document_text → document_chunks → chunk_embeddings
```

## Standalone Examples

### OpenAI Embeddings (Direct Input)
```yaml
type: dagster_component_templates.EmbeddingGeneratorComponent
attributes:
  asset_name: embeddings
  provider: openai
  model: text-embedding-3-small
  text_input: "${TEXT}"
  api_key: ${OPENAI_API_KEY}
```

### Local Sentence Transformers
```yaml
type: dagster_component_templates.EmbeddingGeneratorComponent
attributes:
  asset_name: local_embeddings
  provider: sentence_transformers
  model: all-MiniLM-L6-v2
  text_input: "${TEXT}"
  output_format: dataframe
```

### From Upstream Text Chunker
```yaml
# Text Chunker upstream
- type: TextChunkerComponent
  attributes:
    asset_name: chunks
    chunking_strategy: fixed_size
    chunk_size: 500

# Generate embeddings from chunks
- type: EmbeddingGeneratorComponent
  attributes:
    asset_name: chunk_embeddings
    provider: openai
    model: text-embedding-3-small
    api_key: ${OPENAI_API_KEY}
    output_format: list
```

## Provider Models

### OpenAI
- `text-embedding-3-small` (1536 dims, fast)
- `text-embedding-3-large` (3072 dims, high quality)

### Sentence Transformers
- `all-MiniLM-L6-v2` (384 dims, fast)
- `all-mpnet-base-v2` (768 dims, quality)

### Cohere
- `embed-english-v3.0`
- `embed-multilingual-v3.0`

## Requirements
- openai >= 1.0.0
- sentence-transformers >= 2.0.0
- cohere >= 4.0.0
- huggingface-hub >= 0.20.0
- pandas >= 2.0.0
- numpy >= 1.24.0

## License
MIT License
