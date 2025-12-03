# Document Chunker Component

Split documents into chunks for RAG (Retrieval Augmented Generation) and embeddings. Supports multiple strategies: fixed-size, semantic, recursive, sentence-based, and token-aware chunking.

## Overview

The Document Chunker Component prepares long documents for vector embeddings and retrieval by splitting them into appropriately-sized chunks. It preserves metadata and provides multiple chunking strategies optimized for different use cases.

## Features

- **Multiple Strategies**: Fixed, recursive, semantic, sentence-based, token-aware
- **Configurable Size & Overlap**: Control chunk size and overlap between chunks
- **Metadata Preservation**: Keep document IDs, titles, and custom metadata
- **Token-Aware Chunking**: Respect LLM token limits (GPT, Claude, etc.)
- **Sentence Boundary Detection**: Avoid splitting sentences mid-chunk
- **Small Chunk Merging**: Automatically merge chunks below minimum size
- **Batch Processing**: Process multiple documents efficiently
- **Chunk Statistics**: Track chunk sizes, counts, and distribution

## Use Cases

1. **RAG Pipelines**: Prepare documents for retrieval augmented generation
2. **Vector Embeddings**: Split documents before generating embeddings
3. **Knowledge Base Indexing**: Index long documents for search
4. **Document Q&A**: Enable question-answering over large documents
5. **Semantic Search**: Create searchable document chunks

## Prerequisites

- **Python Packages**: `pandas>=1.5.0`, `tiktoken>=0.5.0` (for token-aware chunking)
- Optional: `nltk` (for advanced sentence splitting), `spacy` (for semantic analysis)

## Configuration

### Basic Configuration

```yaml
type: dagster_component_templates.DocumentChunkerComponent
attributes:
  asset_name: document_chunks
  strategy: recursive
  chunk_size: 1000
  chunk_overlap: 200
  source_column: document_text
  metadata_columns: "doc_id,title"
```

### Chunking Strategies

#### 1. Recursive (Recommended)

Best general-purpose strategy. Splits by separators hierarchically:

```yaml
type: dagster_component_templates.DocumentChunkerComponent
attributes:
  asset_name: document_chunks
  strategy: recursive
  chunk_size: 1000
  chunk_overlap: 200
  source_column: text
  separators: "\\n\\n,\\n,. , "  # Try paragraphs, then sentences, then words
```

#### 2. Token-Aware (For LLMs)

Ensures chunks fit within LLM token limits:

```yaml
type: dagster_component_templates.DocumentChunkerComponent
attributes:
  asset_name: llm_ready_chunks
  strategy: token_aware
  chunk_size: 1000  # Token count, not characters
  chunk_overlap: 100
  tokenizer_model: "gpt-3.5-turbo"  # or cl100k_base
  source_column: document_text
  preserve_sentences: true
```

#### 3. Semantic (Paragraph-Based)

Keeps semantic units (paragraphs) together:

```yaml
type: dagster_component_templates.DocumentChunkerComponent
attributes:
  asset_name: semantic_chunks
  strategy: semantic
  chunk_size: 1500
  chunk_overlap: 100
  source_column: article_text
```

#### 4. Sentence-Based

Splits at sentence boundaries:

```yaml
type: dagster_component_templates.DocumentChunkerComponent
attributes:
  asset_name: sentence_chunks
  strategy: sentence
  chunk_size: 800
  chunk_overlap: 100
  sentence_tokenizer: nltk  # or simple, spacy
  source_column: text
```

#### 5. Fixed-Size

Simple character-based splitting:

```yaml
type: dagster_component_templates.DocumentChunkerComponent
attributes:
  asset_name: fixed_chunks
  strategy: fixed
  chunk_size: 1000
  chunk_overlap: 200
  preserve_sentences: true
  source_column: text
```

### With Metadata Preservation

Preserve document metadata across chunks:

```yaml
type: dagster_component_templates.DocumentChunkerComponent
attributes:
  asset_name: knowledge_base_chunks
  strategy: recursive
  chunk_size: 1200
  chunk_overlap: 200

  source_column: content
  output_column: chunk_text

  # Preserve these columns in each chunk
  metadata_columns: "doc_id,title,author,source,page_number,section"

  # Add chunk position metadata
  add_chunk_metadata: true

  # Quality control
  min_chunk_size: 150
  merge_small_chunks: true
  preserve_sentences: true
```

Output will include:
- Original metadata columns (doc_id, title, etc.)
- `chunk_index`: Position of chunk in document
- `total_chunks`: Total chunks from this document
- `chunk_size`: Size of this chunk
- `chunk_tokens`: Token count (if token_aware)

## Configuration Options

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `asset_name` | string | Name of the output asset |

### Chunking Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `strategy` | string | `recursive` | Chunking strategy: fixed, recursive, sentence, token_aware, semantic |
| `chunk_size` | int | `1000` | Target chunk size (characters or tokens) |
| `chunk_overlap` | int | `200` | Overlap between consecutive chunks |
| `min_chunk_size` | int | `100` | Minimum chunk size (merge smaller chunks) |

### Input/Output

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_column` | string | `text` | Column containing document text |
| `output_column` | string | `chunk` | Column name for chunk text |
| `metadata_columns` | string | `None` | Comma-separated columns to preserve |
| `add_chunk_metadata` | bool | `true` | Add chunk position/size metadata |

### Advanced Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `separators` | string | `None` | Custom separators (recursive only) |
| `sentence_tokenizer` | string | `simple` | Tokenizer: simple, nltk, spacy |
| `tokenizer_model` | string | `None` | Model for token counting (required for token_aware) |
| `preserve_sentences` | bool | `true` | Avoid splitting sentences |
| `merge_small_chunks` | bool | `true` | Merge chunks below min_chunk_size |

## Choosing a Strategy

| Strategy | Best For | Pros | Cons |
|----------|----------|------|------|
| **recursive** | General purpose | Respects structure, flexible | May split paragraphs |
| **token_aware** | LLM inputs | Respects token limits | Requires tokenizer |
| **semantic** | Coherent units | Keeps paragraphs intact | Variable chunk sizes |
| **sentence** | Clean boundaries | Never splits sentences | May be too small/large |
| **fixed** | Simple uniformity | Predictable sizes | May split mid-sentence |

## Chunk Size Guidelines

### For Embeddings

- **OpenAI text-embedding-3**: 8191 tokens max
- **Cohere embed-english-v3.0**: 512 tokens max
- **Sentence-transformers**: 256-512 tokens typical
- **Recommended**: 500-1000 characters (100-200 tokens)

### For RAG Context

- **GPT-3.5**: Max 4096 tokens, use chunks of 1000-2000 chars
- **GPT-4**: Max 8192-128K tokens, use chunks of 2000-4000 chars
- **Claude**: Max 200K tokens, use chunks of 3000-5000 chars
- **Recommended**: Size chunks to fit 3-5 chunks in context window

### For Search

- **Smaller chunks (500-1000 chars)**: Better precision, more specific matches
- **Larger chunks (2000-3000 chars)**: More context, better for complex queries

## Overlap Guidelines

- **High overlap (300-500)**: Better retrieval, more redundancy
- **Medium overlap (100-200)**: Balanced (recommended)
- **Low overlap (0-50)**: Less redundancy, more chunks

## Integration with Other Components

### Upstream Components

The Document Chunker requires a DataFrame with text:
- Document Text Extractor
- CSV File Ingestion
- Database Query
- API Ingestion components

### Downstream Components

Process chunks with:
- **Embeddings Generator**: Generate embeddings for each chunk
- **Vector Store Writer**: Store chunks in vector database
- **OpenAI/Anthropic LLM**: Process chunks individually
- **DataFrame Transformer**: Filter or transform chunks

## Example Pipelines

### RAG Pipeline

```yaml
# 1. Extract text from PDFs
- type: dagster_component_templates.DocumentTextExtractorComponent
  attributes:
    asset_name: extracted_documents
    file_path_column: pdf_path

# 2. Chunk documents
- type: dagster_component_templates.DocumentChunkerComponent
  attributes:
    asset_name: document_chunks
    strategy: recursive
    chunk_size: 1000
    chunk_overlap: 200
    source_column: text
    metadata_columns: "file_name,page_number"
    add_chunk_metadata: true

# 3. Generate embeddings
- type: dagster_component_templates.EmbeddingsGeneratorComponent
  attributes:
    asset_name: chunk_embeddings
    provider: openai
    model: text-embedding-3-small
    input_column: chunk

# 4. Store in vector database
- type: dagster_component_templates.VectorStoreWriterComponent
  attributes:
    asset_name: vector_store
    provider: pinecone
    index_name: documents
```

### Long Document Q&A

```yaml
# 1. Load research papers
- type: dagster_component_templates.CSVFileIngestionComponent
  attributes:
    asset_name: research_papers
    file_path: "data/papers.csv"

# 2. Chunk with token awareness
- type: dagster_component_templates.DocumentChunkerComponent
  attributes:
    asset_name: paper_chunks
    strategy: token_aware
    chunk_size: 2000  # Tokens
    chunk_overlap: 200
    tokenizer_model: gpt-4-turbo
    source_column: paper_text
    metadata_columns: "paper_id,title,authors,year"

# 3. Analyze each chunk
- type: dagster_component_templates.OpenAILLMComponent
  attributes:
    asset_name: paper_analysis
    model: gpt-4-turbo
    system_prompt: "Analyze this research paper section."
    input_column: chunk
    output_column: analysis
```

## Best Practices

1. **Test Strategies**: Try different strategies on sample data to find best fit
2. **Optimize Chunk Size**: Balance between context (larger) and precision (smaller)
3. **Use Overlap**: Overlap prevents losing context at boundaries (20-30% recommended)
4. **Preserve Metadata**: Always preserve document IDs for traceability
5. **Token-Aware for LLMs**: Use token_aware strategy when feeding to LLMs
6. **Monitor Statistics**: Check chunk size distribution in metadata
7. **Merge Small Chunks**: Enable merge_small_chunks to avoid tiny fragments

## Troubleshooting

### Chunks Too Large

- Decrease `chunk_size`
- Reduce `chunk_overlap`
- Check if documents have proper paragraph breaks

### Chunks Too Small

- Increase `chunk_size`
- Enable `merge_small_chunks`
- Check `min_chunk_size` setting

### Sentences Split

- Enable `preserve_sentences: true`
- Use `sentence` or `semantic` strategy
- Increase `chunk_size` to fit more sentences

### Too Many/Few Chunks

- Adjust `chunk_size` to control chunk count
- Check `min_chunk_size` if getting too many small chunks
- Review overlap setting (high overlap = more chunks)

### Token Count Issues

- Install tiktoken: `pip install tiktoken`
- Verify `tokenizer_model` matches your LLM
- Use `token_aware` strategy for accurate token counting

## Metadata

The component provides rich metadata:

- `strategy`: Chunking strategy used
- `total_chunks`: Total number of chunks created
- `source_documents`: Number of input documents
- `avg_chunks_per_doc`: Average chunks per document
- `avg_chunk_size`: Average chunk size
- `min_chunk_size`: Smallest chunk size
- `max_chunk_size`: Largest chunk size
- `target_chunk_size`: Configured target size
- `chunk_overlap`: Configured overlap

## Support

For issues or questions:
- Tiktoken Documentation: https://github.com/openai/tiktoken
- Dagster Documentation: https://docs.dagster.io
- Component Issues: File an issue in the component repository
