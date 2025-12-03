# Embeddings Generator Component

Generate vector embeddings for text using OpenAI, Cohere, Sentence Transformers, or Hugging Face. Supports batch processing, dimension reduction, similarity computation, and cost tracking.

## Overview

The Embeddings Generator Component creates dense vector representations of text that capture semantic meaning. These embeddings are essential for RAG systems, semantic search, clustering, and recommendation engines.

## Features

- **Multiple Providers**: OpenAI, Cohere, Sentence Transformers (local), Hugging Face
- **Batch Processing**: Efficient processing of large datasets
- **Dimension Reduction**: PCA and UMAP for smaller embeddings
- **Cosine Similarity**: Compute pairwise similarities
- **Normalization**: Unit-length embeddings for cosine similarity
- **Caching**: Cache embeddings to avoid recomputation
- **Cost Tracking**: Monitor API costs
- **Local Models**: No API required with Sentence Transformers

## Use Cases

1. **RAG Pipelines**: Embed document chunks for retrieval
2. **Semantic Search**: Find similar content based on meaning
3. **Clustering**: Group similar documents
4. **Duplicate Detection**: Find near-duplicate content
5. **Recommendation Systems**: Content-based recommendations
6. **Classification**: Use embeddings as features

## Prerequisites

- **Python Packages**: Based on provider
  - OpenAI: `openai>=1.0.0`
  - Cohere: `cohere>=4.0.0`
  - Sentence Transformers: `sentence-transformers>=2.0.0`
  - Hugging Face: `huggingface-hub>=0.16.0`
- **API Keys**: For OpenAI/Cohere (not needed for local models)

## Configuration

### OpenAI Embeddings

```yaml
type: dagster_component_templates.EmbeddingsGeneratorComponent
attributes:
  asset_name: openai_embeddings
  provider: openai
  model: text-embedding-3-small
  api_key: "${OPENAI_API_KEY}"
  input_column: chunk
  output_column: embedding
  batch_size: 100
  normalize_embeddings: true
  track_costs: true
```

### Local Sentence Transformers (No API)

```yaml
type: dagster_component_templates.EmbeddingsGeneratorComponent
attributes:
  asset_name: local_embeddings
  provider: sentence_transformers
  model: all-MiniLM-L6-v2  # or all-mpnet-base-v2
  input_column: text
  output_column: embedding
  batch_size: 32
  normalize_embeddings: true
```

### Cohere Embeddings

```yaml
type: dagster_component_templates.EmbeddingsGeneratorComponent
attributes:
  asset_name: cohere_embeddings
  provider: cohere
  model: embed-english-v3.0
  api_key: "${COHERE_API_KEY}"
  input_column: text
  output_column: embedding
  batch_size: 96
```

### With Dimension Reduction

Reduce embedding dimensions to save storage:

```yaml
type: dagster_component_templates.EmbeddingsGeneratorComponent
attributes:
  asset_name: reduced_embeddings
  provider: openai
  model: text-embedding-3-large  # 3072 dimensions
  api_key: "${OPENAI_API_KEY}"
  input_column: text
  output_column: embedding

  # Reduce from 3072 to 512 dimensions
  dimension_reduction: pca  # or umap
  target_dimensions: 512

  normalize_embeddings: true
```

### With Similarity Computation

Compute pairwise similarities:

```yaml
type: dagster_component_templates.EmbeddingsGeneratorComponent
attributes:
  asset_name: embeddings_with_similarity
  provider: sentence_transformers
  model: all-mpnet-base-v2
  input_column: text
  output_column: embedding

  # Compute similarities
  compute_similarity: true
  similarity_threshold: 0.7  # Only store similarities > 0.7
```

### With Caching

Cache embeddings to disk:

```yaml
type: dagster_component_templates.EmbeddingsGeneratorComponent
attributes:
  asset_name: cached_embeddings
  provider: openai
  model: text-embedding-3-small
  api_key: "${OPENAI_API_KEY}"
  input_column: chunk
  output_column: embedding

  # Enable caching
  cache_embeddings: true
  cache_path: "data/embeddings_cache.parquet"
```

## Model Recommendations

### OpenAI Models

| Model | Dimensions | Cost (per 1M tokens) | Best For |
|-------|-----------|---------------------|----------|
| text-embedding-3-small | 1536 | $0.02 | General purpose, cost-effective |
| text-embedding-3-large | 3072 | $0.13 | High quality, best performance |
| text-embedding-ada-002 | 1536 | $0.10 | Legacy, still good |

### Cohere Models

| Model | Dimensions | Best For |
|-------|-----------|----------|
| embed-english-v3.0 | 1024 | English text |
| embed-multilingual-v3.0 | 1024 | Multiple languages |

### Sentence Transformers (Local)

| Model | Dimensions | Speed | Best For |
|-------|-----------|-------|----------|
| all-MiniLM-L6-v2 | 384 | Fast | General purpose, resource-constrained |
| all-mpnet-base-v2 | 768 | Medium | Best quality local model |
| all-MiniLM-L12-v2 | 384 | Fast | Balance of speed and quality |

## Configuration Options

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `asset_name` | string | Name of output asset |
| `provider` | string | Provider: openai, cohere, sentence_transformers, huggingface |
| `model` | string | Model name |

### Input/Output

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_key` | string | `None` | API key (use `${VAR}`) |
| `input_column` | string | `text` | Column with text |
| `output_column` | string | `embedding` | Column for embeddings |

### Performance

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | int | `100` | Batch size for API calls |
| `rate_limit_delay` | float | `0.0` | Delay between batches (seconds) |
| `normalize_embeddings` | bool | `true` | Normalize to unit length |

### Advanced Features

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dimension_reduction` | string | `None` | Reduction method: pca, umap |
| `target_dimensions` | int | `None` | Target dimensions after reduction |
| `compute_similarity` | bool | `false` | Compute pairwise similarities |
| `similarity_threshold` | float | `None` | Min similarity to store |
| `cache_embeddings` | bool | `false` | Cache to file |
| `cache_path` | string | `None` | Cache file path |
| `track_costs` | bool | `true` | Track API costs |

## Cost Optimization

### 1. Use Cheaper Models

- OpenAI: `text-embedding-3-small` ($0.02/1M tokens) vs `text-embedding-3-large` ($0.13/1M)
- Consider local models (Sentence Transformers) for no API costs

### 2. Enable Caching

Cache embeddings to avoid recomputing:

```yaml
cache_embeddings: true
cache_path: "embeddings_cache.parquet"
```

### 3. Reduce Dimensions

Smaller embeddings = less storage:

```yaml
dimension_reduction: pca
target_dimensions: 512  # From 1536 or 3072
```

### 4. Batch Processing

Larger batches = fewer API calls:

```yaml
batch_size: 100  # Max for OpenAI
```

### Approximate Costs (OpenAI)

- 1,000 chunks × 100 tokens each = 100K tokens
- With `text-embedding-3-small`: $0.002
- With `text-embedding-3-large`: $0.013

## Integration with Other Components

### Upstream Components

Works with any component producing text:
- **Document Chunker**: Embed chunks
- **CSV File Ingestion**: Embed rows
- **Database Query**: Embed query results
- **OpenAI/Anthropic LLM**: Embed LLM outputs

### Downstream Components

Use embeddings with:
- **Vector Store Writer**: Store in Pinecone, Weaviate, Qdrant
- **Similarity Computation**: Find similar items
- **Clustering**: Group similar content
- **Classification**: Use as features

## Example Pipelines

### Complete RAG Pipeline

```yaml
# 1. Load documents
- type: dagster_component_templates.CSVFileIngestionComponent
  attributes:
    asset_name: documents
    file_path: "data/docs.csv"

# 2. Chunk documents
- type: dagster_component_templates.DocumentChunkerComponent
  attributes:
    asset_name: chunks
    strategy: recursive
    chunk_size: 1000
    chunk_overlap: 200
    source_column: text

# 3. Generate embeddings
- type: dagster_component_templates.EmbeddingsGeneratorComponent
  attributes:
    asset_name: embeddings
    provider: openai
    model: text-embedding-3-small
    api_key: "${OPENAI_API_KEY}"
    input_column: chunk
    output_column: embedding

# 4. Store in vector database
- type: dagster_component_templates.VectorStoreWriterComponent
  attributes:
    asset_name: vector_store
    provider: pinecone
    index_name: documents
```

### Duplicate Detection

```yaml
# 1. Load content
- type: dagster_component_templates.DatabaseQueryComponent
  attributes:
    asset_name: articles
    query: "SELECT id, title, content FROM articles"

# 2. Generate embeddings with similarity
- type: dagster_component_templates.EmbeddingsGeneratorComponent
  attributes:
    asset_name: article_embeddings
    provider: sentence_transformers
    model: all-mpnet-base-v2
    input_column: content
    output_column: embedding
    compute_similarity: true
    similarity_threshold: 0.9  # High similarity = likely duplicate
```

## Best Practices

1. **Choose Right Model**: Balance cost, quality, and speed
2. **Normalize Embeddings**: Always normalize for cosine similarity
3. **Enable Caching**: Cache for iterative development
4. **Batch Process**: Use appropriate batch sizes for your provider
5. **Monitor Costs**: Enable cost tracking for paid APIs
6. **Test Locally First**: Use Sentence Transformers for prototyping
7. **Reduce Dimensions**: Consider PCA/UMAP for large-scale systems

## Troubleshooting

### High Costs

- Switch to `text-embedding-3-small` or local models
- Enable caching
- Reduce number of embeddings (filter data first)

### Out of Memory

- Decrease `batch_size`
- Process data in chunks
- Use dimension reduction

### Slow Processing

- Increase `batch_size`
- Use local models (Sentence Transformers)
- Enable GPU for local models

### Rate Limits

- Increase `rate_limit_delay`
- Decrease `batch_size`
- Upgrade API tier

## Metadata

The component provides:

- `provider`: Provider used
- `model`: Model name
- `num_embeddings`: Number of embeddings generated
- `embedding_dimension`: Embedding dimensions
- `normalized`: Whether embeddings are normalized
- `total_tokens`: Token count (if applicable)
- `estimated_cost_usd`: Estimated cost
- `dimension_reduction`: Reduction method (if used)
- `similarity_computed`: Whether similarity was computed

## Support

For issues or questions:
- OpenAI Documentation: https://platform.openai.com/docs/guides/embeddings
- Sentence Transformers: https://www.sbert.net/
- Cohere Documentation: https://docs.cohere.com/
- Dagster Documentation: https://docs.dagster.io
