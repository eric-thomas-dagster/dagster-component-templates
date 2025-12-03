# Reranker Component

Rerank search results for improved relevance using Cohere Rerank API, cross-encoder models, and BM25 algorithms. Essential for optimizing RAG pipelines and semantic search systems.

## Overview

The Reranker Component implements two-stage retrieval by reranking initial search results from vector stores. While vector search is fast and scales well, reranking with more sophisticated models significantly improves relevance at the cost of slightly increased latency. This is a critical component for production RAG systems.

## Why Reranking Matters

**Two-Stage Retrieval Benefits:**
- **Higher Accuracy**: Cross-encoders and rerank models consider full query-document interaction
- **Better Rankings**: Move most relevant results to the top
- **Hybrid Search**: Combine semantic (vector) and lexical (BM25) signals
- **Cost Optimization**: Fast vector search retrieves candidates, expensive reranking refines top-k

**Performance Impact:**
- Typical improvement: 10-30% increase in relevance metrics (MRR, NDCG)
- Recommended for: Question answering, document search, knowledge retrieval
- Trade-off: Adds 50-200ms latency per query

## Features

- **Multiple Reranking Methods**:
  - Cohere Rerank API (state-of-the-art, managed service)
  - Cross-encoder models (sentence-transformers, local inference)
  - BM25 algorithm (keyword-based, zero-cost)

- **Flexible Scoring**:
  - Relevance score normalization
  - Combine with original vector search scores (weighted or multiply)
  - Configurable threshold filtering
  - Top-N selection

- **Production Ready**:
  - Batch processing support
  - Query grouping for efficiency
  - Rate limiting for API calls
  - Cost tracking (Cohere)
  - Comprehensive error handling

- **Integration**:
  - Works with any vector search output
  - Returns pandas DataFrame for downstream processing
  - Compatible with all vector stores

## Use Cases

### 1. RAG Pipeline Optimization
Improve document retrieval for LLM context:
```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: rag_reranked_chunks
  source_asset: vector_search_results
  method: cohere
  model: rerank-english-v2.0
  query_column: question
  text_column: chunk_text
  top_n: 5  # Only top 5 for LLM context
  api_key: "${COHERE_API_KEY}"
```

### 2. Semantic Search Enhancement
Boost search relevance for customer-facing search:
```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: product_search_reranked
  source_asset: product_vector_search
  method: cross_encoder
  model: cross-encoder/ms-marco-MiniLM-L-12-v2
  query_column: search_query
  text_column: product_description
  top_n: 20
  rerank_threshold: 0.4
```

### 3. Hybrid Search (Vector + Keyword)
Combine semantic and lexical signals:
```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: hybrid_search_results
  source_asset: vector_search_results
  method: bm25
  query_column: query
  text_column: document
  combine_scores: weighted
  score_weight: 0.6  # 60% BM25, 40% vector similarity
  top_n: 10
```

### 4. Question Answering
Find best answer from candidate passages:
```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: qa_reranked_passages
  source_asset: candidate_passages
  method: cross_encoder
  model: cross-encoder/quora-distilroberta-base
  query_column: question
  text_column: passage
  top_n: 3
  normalize_scores: true
```

## Configuration

### Basic Configuration

```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: reranked_results
  source_asset: search_results
  method: cohere
  query_column: query
  text_column: document
  api_key: "${COHERE_API_KEY}"
```

### Full Configuration Options

```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  # Required
  asset_name: reranked_results
  source_asset: search_results
  method: cohere  # cohere | cross_encoder | bm25
  
  # Method-Specific
  model: rerank-english-v2.0  # Model name (Cohere or cross-encoder)
  api_key: "${COHERE_API_KEY}"  # Required for Cohere
  
  # Column Mapping
  query_column: query
  text_column: document
  score_column: similarity_score  # Original scores (optional)
  output_score_column: rerank_score
  
  # Filtering & Selection
  top_n: 10  # Return top N after reranking
  rerank_threshold: 0.3  # Minimum score threshold
  max_chunks_per_doc: 1000  # Limit documents per rerank call
  
  # Score Processing
  normalize_scores: true
  combine_scores: weighted  # weighted | multiply | null
  score_weight: 0.7  # For weighted combination
  
  # Performance
  batch_size: 100  # For cross-encoder
  rate_limit_delay: 0.1  # Seconds between API calls
  return_documents: true
  
  # Tracking
  track_costs: true
  
  # Organization
  description: "Reranked search results"
  group_name: "reranking"
  include_sample_metadata: true
```

## Reranking Methods Comparison

### Cohere Rerank API

**Pros:**
- State-of-the-art accuracy (trained on massive query-document pairs)
- Managed service (no model hosting)
- Fast inference
- Multilingual support

**Cons:**
- Requires API key and internet connection
- Cost: ~$0.02 per 1000 searches
- Data leaves your infrastructure

**Best For:** Production RAG systems, customer-facing search

**Models:**
- `rerank-english-v2.0`: English, best accuracy
- `rerank-multilingual-v2.0`: 100+ languages

### Cross-Encoder Models

**Pros:**
- Run locally (no API required)
- No per-query cost
- Full control and privacy
- Many open-source models available

**Cons:**
- Slower inference than vector search
- Requires GPU for production scale
- Model hosting overhead

**Best For:** Privacy-sensitive applications, high query volume

**Popular Models:**
- `cross-encoder/ms-marco-MiniLM-L-12-v2`: Fast, good accuracy
- `cross-encoder/ms-marco-MiniLM-L-6-v2`: Faster, slightly lower accuracy
- `cross-encoder/ms-marco-electra-base`: Higher accuracy, slower

### BM25 Algorithm

**Pros:**
- Zero cost (no API, no GPU)
- Very fast
- Good for keyword-heavy queries
- Complements semantic search

**Cons:**
- Lower accuracy than neural methods
- No understanding of semantics
- Vocabulary-dependent

**Best For:** Hybrid search, keyword queries, cost-sensitive applications

## Prerequisites

### For Cohere Method
1. **Cohere API Key**: Sign up at [cohere.com](https://cohere.com/)
2. **Python Package**: `cohere>=4.0.0`

### For Cross-Encoder Method
1. **Python Package**: `sentence-transformers>=2.2.0`
2. **GPU Recommended**: For production workloads
3. **Model Download**: First run downloads model (~100MB-1GB)

### For BM25 Method
1. **Python Package**: `rank-bm25>=0.2.2`
2. **No API Key**: Runs locally

## Performance Tuning

### Optimize Latency

1. **Limit Reranking Candidates**:
```yaml
max_chunks_per_doc: 100  # Rerank only top 100 from vector search
```

2. **Use Faster Models**:
```yaml
method: cross_encoder
model: cross-encoder/ms-marco-MiniLM-L-6-v2  # Faster than L-12
```

3. **Reduce top_n**:
```yaml
top_n: 5  # Only need 5 for LLM context
```

### Optimize Cost (Cohere)

1. **Combine with Threshold**:
```yaml
rerank_threshold: 0.5  # Filter low-relevance docs early
```

2. **Cache Queries**: Implement caching upstream for repeated queries

3. **Consider Cross-Encoder**: One-time GPU cost vs per-query API cost

### Optimize Accuracy

1. **Combine Scores**:
```yaml
combine_scores: weighted
score_weight: 0.7  # Blend rerank + vector scores
```

2. **Use Best Model**:
```yaml
method: cohere
model: rerank-english-v2.0  # Best accuracy
```

3. **Increase Candidates**:
```yaml
max_chunks_per_doc: 200  # More candidates = better top-k
```

## Cost Estimates

### Cohere Rerank Pricing (as of 2024)
- **Price**: $0.02 per 1000 searches
- **Examples**:
  - 10,000 queries/day = $0.20/day = $6/month
  - 100,000 queries/day = $2/day = $60/month
  - 1M queries/day = $20/day = $600/month

### Cross-Encoder Hosting
- **GPU Cost**: $0.50-2.00/hour (cloud GPU)
- **Break-even**: ~25,000-100,000 queries/day
- **Recommendation**: Use Cohere for < 50k queries/day, self-host for more

### BM25
- **Cost**: $0 (runs on CPU)
- **Latency**: < 10ms per query
- **Best Combined**: With vector search for hybrid approach

## Integration Patterns

### Pattern 1: RAG Pipeline
```
Documents → Chunker → Embeddings → Vector Store
                                        ↓
Query → Embeddings → Vector Search (top 100)
                           ↓
                     Reranker (top 5)
                           ↓
                     LLM with Context
```

### Pattern 2: Hybrid Search
```
Query → Embeddings → Vector Search (top 100)
                           ↓
                     BM25 Reranker (combine scores)
                           ↓
                     Cross-Encoder Reranker (top 10)
                           ↓
                     Final Results
```

### Pattern 3: Progressive Refinement
```
Vector Search (top 500) → Fast Reranker (top 50) → Slow Reranker (top 10)
```

## Advanced Examples

### Multi-Query Reranking
Rerank results for multiple queries in one batch:
```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: batch_reranked
  source_asset: batch_search_results  # Multiple queries
  method: cohere
  query_column: query  # Can have different values per row
  text_column: document
  top_n: 10  # Top 10 per query
```

### Score Normalization & Combination
Blend rerank scores with original similarity scores:
```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: blended_results
  source_asset: vector_results
  method: cross_encoder
  score_column: cosine_similarity
  normalize_scores: true
  combine_scores: weighted
  score_weight: 0.6  # 60% rerank, 40% original
```

### Threshold-Based Filtering
Remove low-relevance results:
```yaml
type: dagster_component_templates.RerankerComponent
attributes:
  asset_name: filtered_results
  source_asset: search_results
  method: cohere
  rerank_threshold: 0.5  # Only keep scores >= 0.5
  top_n: null  # Return all above threshold
```

## Troubleshooting

### Issue: High Latency
**Solution**: Reduce `max_chunks_per_doc`, use faster model, or increase `batch_size`

### Issue: Low Relevance
**Solution**: Use Cohere or better cross-encoder model, combine scores with `weighted`

### Issue: High Cost (Cohere)
**Solution**: Implement query caching, use threshold filtering, or switch to cross-encoder

### Issue: Out of Memory (Cross-Encoder)
**Solution**: Reduce `batch_size`, use smaller model, or use GPU with more VRAM

### Issue: Scores Not in 0-1 Range
**Solution**: Enable `normalize_scores: true`

## Best Practices

1. **Always Rerank for RAG**: 10-30% accuracy improvement is worth slight latency increase
2. **Start with Cohere**: Easy to integrate, best accuracy, switch to self-hosted if needed
3. **Limit Candidates**: Rerank top 50-200 from vector search, not all results
4. **Combine Scores**: Use `weighted` combination for best results
5. **Monitor Costs**: Track Cohere usage, consider self-hosting at scale
6. **Test Models**: Cross-encoder model choice significantly impacts accuracy
7. **Use Thresholds**: Filter low-relevance results early in pipeline

## Related Components

- **vector_store_query / vector_search**: Provides candidates for reranking
- **embeddings_generator**: Creates embeddings for vector search
- **llm_chain**: Consumes reranked results for RAG
- **openai_llm / anthropic_llm**: Final LLM step after reranking

## References

- [Cohere Rerank Documentation](https://docs.cohere.com/docs/reranking)
- [Sentence-Transformers Cross-Encoders](https://www.sbert.net/examples/applications/cross-encoder/README.html)
- [BM25 Algorithm](https://en.wikipedia.org/wiki/Okapi_BM25)
- [Two-Stage Retrieval Paper](https://arxiv.org/abs/2004.04906)

## Changelog

### v1.0.0
- Initial release
- Support for Cohere Rerank API
- Support for cross-encoder models
- Support for BM25 algorithm
- Score combination and normalization
- Batch processing and query grouping
- Cost tracking
