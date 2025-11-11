# RAG Pipeline Asset

Complete Retrieval-Augmented Generation pipeline combining query embedding, vector search, and LLM generation to answer questions using your document knowledge base. Works seamlessly with Dagster's IO manager pattern - connect query assets and automatically generate answers!

## Overview

This asset component implements a full RAG (Retrieval-Augmented Generation) pipeline that:
1. Generates embeddings for user queries
2. Searches vector databases for relevant documents
3. Uses LLMs to generate contextual answers

**Compatible with:**
- Any asset producing string query text
- Any asset producing dict with 'query' or 'text' key
- REST API Fetcher (with query input)
- Can also accept direct query via `query` parameter

Perfect for:
- Question-answering systems
- Document search and Q&A
- Knowledge base assistants
- Context-aware chatbots
- Research and analysis tools

## Features

- **Complete Pipeline**: Embedding → Retrieval → Generation in one component
- **Multiple Vector Stores**: ChromaDB, Pinecone, Qdrant support
- **Multiple LLM Providers**: OpenAI, Anthropic
- **Multiple Embedding Providers**: OpenAI, Sentence Transformers
- **Source Attribution**: Include source documents in responses
- **Configurable Retrieval**: Control number of documents retrieved (top_k)
- **Temperature Control**: Fine-tune LLM creativity

## Configuration

### Required Parameters

- **asset_name** (string) - Name of the asset
- **query** (string) - User query/question
- **vector_store_provider** (string) - Vector store: `"chromadb"`, `"pinecone"`, `"qdrant"`
- **collection_name** (string) - Vector store collection name
- **llm_provider** (string) - LLM provider: `"openai"`, `"anthropic"`
- **llm_model** (string) - LLM model name

### Optional Parameters

- **embedding_provider** (string) - Embedding provider (default: `"openai"`)
- **embedding_model** (string) - Embedding model (default: `"text-embedding-3-small"`)
- **top_k** (integer) - Number of documents to retrieve (default: `5`)
- **vector_store_connection** (string) - Vector store connection string/path
- **llm_api_key** (string) - LLM API key (use `${API_KEY}`)
- **embedding_api_key** (string) - Embedding API key (use `${API_KEY}`)
- **include_sources** (boolean) - Include source documents (default: `true`)
- **temperature** (number) - LLM temperature (default: `0.7`)
- **description** (string) - Asset description
- **group_name** (string) - Asset group

## Input & Output

### Input Requirements
Accepts query from upstream assets via IO manager (optional if `query` parameter is provided):
- **String**: Query text directly
- **Dict**: Dictionary with 'query' or 'text' key containing the query
- **Alternative**: Can use `query` parameter instead of upstream input

### Output Format
Returns a dictionary with:
- `response`: Generated answer from LLM
- `query`: Original query text
- `sources`: List of source documents (if `include_sources: true`)
- `num_sources`: Count of retrieved documents

## Example Pipeline

### Complete RAG Query Pipeline

```yaml
# 1. Query input (hypothetical upstream component)
- type: RestApiFetcherComponent
  attributes:
    asset_name: user_query
    api_url: https://api.example.com/questions/latest
    output_format: dict

# 2. RAG Pipeline to answer query
- type: RAGPipelineComponent
  attributes:
    asset_name: rag_response
    vector_store_provider: chromadb
    collection_name: my_docs
    vector_store_connection: ./chroma_db
    llm_provider: openai
    llm_model: gpt-4
    llm_api_key: ${OPENAI_API_KEY}
    embedding_provider: openai
    embedding_model: text-embedding-3-small
    embedding_api_key: ${OPENAI_API_KEY}
    top_k: 5
    include_sources: true
```

**Visual Connections:**
```
user_query → rag_response
```

## Standalone Examples

### Basic RAG with ChromaDB and OpenAI

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: qa_response
  query: "What is machine learning?"
  vector_store_provider: chromadb
  collection_name: documents
  llm_provider: openai
  llm_model: gpt-4
  llm_api_key: ${OPENAI_API_KEY}
  embedding_api_key: ${OPENAI_API_KEY}
```

### With Custom Retrieval Count

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: detailed_answer
  query: "Explain quantum computing"
  vector_store_provider: chromadb
  collection_name: technical_docs
  llm_provider: openai
  llm_model: gpt-4
  top_k: 10  # Retrieve 10 most relevant documents
  llm_api_key: ${OPENAI_API_KEY}
  embedding_api_key: ${OPENAI_API_KEY}
```

### With Anthropic Claude

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: claude_rag
  query: "${USER_QUESTION}"
  vector_store_provider: chromadb
  collection_name: knowledge_base
  llm_provider: anthropic
  llm_model: claude-3-5-sonnet-20241022
  embedding_provider: openai
  llm_api_key: ${ANTHROPIC_API_KEY}
  embedding_api_key: ${OPENAI_API_KEY}
  temperature: 0.3
```

### With Pinecone Vector Store

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: pinecone_rag
  query: "How do I deploy to production?"
  vector_store_provider: pinecone
  collection_name: deployment-docs
  llm_provider: openai
  llm_model: gpt-4
  llm_api_key: ${OPENAI_API_KEY}
  embedding_api_key: ${PINECONE_API_KEY}
  top_k: 5
```

### With Local Sentence Transformers

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: local_rag
  query: "${QUESTION}"
  vector_store_provider: chromadb
  collection_name: documents
  llm_provider: openai
  llm_model: gpt-4
  embedding_provider: sentence_transformers
  embedding_model: all-MiniLM-L6-v2
  llm_api_key: ${OPENAI_API_KEY}
```

### Without Source Attribution

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: answer_only
  query: "What are the benefits of exercise?"
  vector_store_provider: chromadb
  collection_name: health_articles
  llm_provider: openai
  llm_model: gpt-4
  include_sources: false  # Don't include source documents
  llm_api_key: ${OPENAI_API_KEY}
  embedding_api_key: ${OPENAI_API_KEY}
```

## How RAG Works

### 1. Query Embedding

Your query is converted to a vector embedding:
```
"What is machine learning?" → [0.023, -0.145, 0.432, ...]
```

### 2. Vector Search

The embedding searches the vector database for similar documents:
```
Top 5 most relevant documents retrieved based on cosine similarity
```

### 3. Context Building

Retrieved documents are combined into context:
```
Context:
Document 1: Machine learning is a subset of AI...
Document 2: ML algorithms learn from data...
Document 3: Types of ML include supervised...
```

### 4. LLM Generation

The LLM generates an answer using the context:
```
Question: What is machine learning?
Context: [Retrieved documents]
Answer: Machine learning is a subset of artificial intelligence...
```

## Vector Store Configuration

### ChromaDB (Local)

```yaml
vector_store_provider: chromadb
vector_store_connection: ./chroma_db  # Local path
```

**Best for:** Development, small datasets, local testing

### Pinecone (Cloud)

```yaml
vector_store_provider: pinecone
collection_name: my-index
embedding_api_key: ${PINECONE_API_KEY}
```

**Best for:** Production, large scale, managed service

### Qdrant

```yaml
vector_store_provider: qdrant
vector_store_connection: localhost  # or cloud URL
embedding_api_key: ${QDRANT_API_KEY}
```

**Best for:** High performance, self-hosted or cloud

## Embedding Models

### OpenAI (Default)

```yaml
embedding_provider: openai
embedding_model: text-embedding-3-small  # Fast, economical
# or
embedding_model: text-embedding-3-large  # Higher quality
```

**Dimensions:** 1536 (small), 3072 (large)

### Sentence Transformers (Local)

```yaml
embedding_provider: sentence_transformers
embedding_model: all-MiniLM-L6-v2  # Fast, good quality
# or
embedding_model: all-mpnet-base-v2  # Higher quality
```

**Best for:** No API costs, privacy, offline use

## LLM Models

### OpenAI GPT-4

```yaml
llm_provider: openai
llm_model: gpt-4  # Best quality
# or
llm_model: gpt-4-turbo  # Faster, cheaper
# or
llm_model: gpt-3.5-turbo  # Fastest, cheapest
```

### Anthropic Claude

```yaml
llm_provider: anthropic
llm_model: claude-3-5-sonnet-20241022  # Balanced
# or
llm_model: claude-3-opus-20240229  # Highest quality
# or
llm_model: claude-3-haiku-20240307  # Fastest
```

## Tuning Retrieval

### Top-K Selection

Number of documents to retrieve:

```yaml
top_k: 3   # Fast, focused answers
top_k: 5   # Balanced (default)
top_k: 10  # Comprehensive, more context
top_k: 20  # Maximum context, slower
```

**Guidelines:**
- Simple questions: 3-5 documents
- Complex questions: 10-15 documents
- Research/analysis: 15-20 documents

### Temperature Control

Control answer creativity:

```yaml
temperature: 0.0  # Deterministic, factual
temperature: 0.3  # Slightly varied, consistent
temperature: 0.7  # Balanced (default)
temperature: 1.0  # Creative, diverse
```

**Guidelines:**
- Factual Q&A: 0.0-0.3
- General questions: 0.5-0.7
- Brainstorming: 0.8-1.0

## Common Use Cases

### 1. Documentation Q&A

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: docs_qa
  query: "${USER_QUESTION}"
  vector_store_provider: chromadb
  collection_name: product_docs
  llm_provider: openai
  llm_model: gpt-4
  top_k: 5
  temperature: 0.3
  llm_api_key: ${OPENAI_API_KEY}
  embedding_api_key: ${OPENAI_API_KEY}
  description: Answer questions from product documentation
  group_name: support
```

### 2. Research Assistant

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: research_answer
  query: "What are the latest findings on ${TOPIC}?"
  vector_store_provider: pinecone
  collection_name: research-papers
  llm_provider: anthropic
  llm_model: claude-3-5-sonnet-20241022
  top_k: 15
  temperature: 0.5
  llm_api_key: ${ANTHROPIC_API_KEY}
  embedding_api_key: ${PINECONE_API_KEY}
  include_sources: true
  group_name: research
```

### 3. Customer Support Bot

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: support_response
  query: "${CUSTOMER_QUESTION}"
  vector_store_provider: chromadb
  collection_name: support_articles
  llm_provider: openai
  llm_model: gpt-3.5-turbo
  top_k: 5
  temperature: 0.2
  llm_api_key: ${OPENAI_API_KEY}
  embedding_api_key: ${OPENAI_API_KEY}
  group_name: support
```

### 4. Code Documentation Search

```yaml
type: dagster_component_templates.RAGPipelineComponent
attributes:
  asset_name: code_docs_search
  query: "How do I use the ${FUNCTION_NAME} function?"
  vector_store_provider: chromadb
  collection_name: code_documentation
  llm_provider: openai
  llm_model: gpt-4
  top_k: 8
  temperature: 0.1
  llm_api_key: ${OPENAI_API_KEY}
  embedding_api_key: ${OPENAI_API_KEY}
  group_name: developer_tools
```

## Response Structure

### With Sources (default)

```json
{
  "query": "What is machine learning?",
  "answer": "Machine learning is a subset of artificial intelligence...",
  "num_sources": 5,
  "sources": [
    {
      "text": "Machine learning is...",
      "metadata": {"title": "ML Basics", "page": 1}
    },
    ...
  ]
}
```

### Without Sources

```json
{
  "query": "What is machine learning?",
  "answer": "Machine learning is a subset of artificial intelligence...",
  "num_sources": 5
}
```

## Building a Complete RAG System

### Step 1: Document Ingestion

Use Document Text Extractor to extract text from PDFs/documents.

### Step 2: Text Chunking

Use Text Chunker to split documents into chunks.

### Step 3: Generate Embeddings

Use Embedding Generator to create embeddings for chunks.

### Step 4: Store in Vector DB

Use Vector Store Writer to store embeddings.

### Step 5: Query with RAG Pipeline

Use RAG Pipeline component to answer questions!

## Environment Variables

```bash
# LLM APIs
export OPENAI_API_KEY="sk-..."
export ANTHROPIC_API_KEY="sk-ant-..."

# Vector Store APIs
export PINECONE_API_KEY="..."
export QDRANT_API_KEY="..."
```

## Error Handling

- **Empty Results**: Returns answer indicating no relevant documents found
- **API Errors**: Logged and raised with context
- **Invalid Query**: Handled gracefully with error message
- **Vector Store Connection**: Clear error messages for connection issues

## Metadata

Each materialization includes:
- Query text
- Number of sources retrieved
- Answer length in characters
- LLM and embedding models used

## Troubleshooting

### Issue: "No relevant documents found"

**Solution:**
- Check that vector store has documents
- Verify collection name is correct
- Try increasing `top_k`

### Issue: "Poor answer quality"

**Solution:**
- Increase `top_k` for more context
- Lower `temperature` for more factual answers
- Use better embedding model
- Improve document chunking strategy

### Issue: "Slow responses"

**Solution:**
- Reduce `top_k`
- Use faster LLM model (gpt-3.5-turbo, claude-haiku)
- Use local embeddings (sentence-transformers)
- Optimize vector store indices

### Issue: "Out of context answers"

**Solution:**
- Verify correct documents in vector store
- Check embedding model matches stored embeddings
- Review retrieved sources to ensure relevance

## Performance Tips

1. **Optimize Top-K**: Start with 5, adjust based on results
2. **Use Faster Models**: gpt-3.5-turbo for simple Q&A
3. **Local Embeddings**: Use sentence-transformers to avoid embedding API calls
4. **Cache Common Queries**: Cache frequent query results
5. **Batch Processing**: Process multiple queries in parallel

## Cost Optimization

1. **Embedding Costs**:
   - OpenAI: ~$0.0001/1K tokens
   - Sentence Transformers: Free (local)

2. **LLM Costs**:
   - GPT-3.5-turbo: ~$0.002/1K tokens
   - GPT-4: ~$0.03/1K tokens
   - Claude Haiku: ~$0.0025/1K tokens

3. **Vector Store**:
   - ChromaDB: Free (local)
   - Pinecone: Paid, scales with index size
   - Qdrant: Self-hosted (free) or cloud (paid)

## Security Best Practices

1. **Use Environment Variables**:
```yaml
llm_api_key: ${OPENAI_API_KEY}
embedding_api_key: ${OPENAI_API_KEY}
```

2. **Sanitize Queries**: Validate and sanitize user queries
3. **Rate Limiting**: Implement rate limits for API usage
4. **Access Control**: Restrict access to sensitive documents
5. **Audit Logging**: Log all queries for monitoring

## Requirements

- openai >= 1.0.0
- anthropic >= 0.18.0
- chromadb >= 0.4.0
- pinecone-client >= 3.0.0
- qdrant-client >= 1.7.0
- sentence-transformers >= 2.0.0

## Contributing

Found a bug or have a feature request?
- GitHub Issues: https://github.com/eric-thomas-dagster/dagster-component-templates/issues

## License

MIT License
