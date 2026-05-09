# Vertex AI Text Embeddings

Generate text embeddings via Vertex AI's `text-embedding-*` models. Drop-in shape parallel to `embeddings_generator`, `openai_embeddings`, and friends. Useful for RAG, semantic search, vector-store loaders.

```yaml
type: dagster_component_templates.VertexAITextEmbeddingsAssetComponent
attributes:
  asset_name: doc_embeddings
  upstream_asset_key: doc_texts
  credentials_path: ${GOOGLE_APPLICATION_CREDENTIALS}
  text_column: text
  output_column: embedding
  model_name: text-embedding-004
  task_type: RETRIEVAL_DOCUMENT
  group_name: ai
```

## Common models

| Model | Dim | Notes |
|---|---|---|
| `text-embedding-004` | 768 | Default. English-strong. |
| `text-embedding-005` | 768 | Successor. |
| `text-multilingual-embedding-002` | 768 | Multilingual. |
| `gemini-embedding-001` | 768 | Latest Gemini-family embedding model. |

`output_dimensionality` is supported for newer models — set it to `256` / `512` / `1024` to truncate (and save vector-store space).

## Task types

Vertex tunes embeddings for the intended downstream use. Pick one:

| Task type | When |
|---|---|
| `RETRIEVAL_DOCUMENT` | The text is a document being indexed for later retrieval. |
| `RETRIEVAL_QUERY` | The text is a search query. |
| `SEMANTIC_SIMILARITY` | Pairwise comparison. |
| `CLASSIFICATION` | Feeding into a classifier. |
| `CLUSTERING` | k-means / clustering input. |
| `QUESTION_ANSWERING` | Q+A retrieval. |
| `FACT_VERIFICATION` | Fact-check workflows. |
| `CODE_RETRIEVAL_QUERY` | Code search queries. |

For RAG: index docs with `RETRIEVAL_DOCUMENT`, query with `RETRIEVAL_QUERY` — Vertex will produce vectors that compose well together.

## Required SA roles

`roles/aiplatform.user` on the project. Plus the Vertex AI API has to be enabled.

## Sister components

- `embeddings_generator` — provider-agnostic via LiteLLM.
- `openai_llm` / `anthropic_llm` / `gemini_llm` — text generation peers (same SA + auth pattern).
- `vector_store_query` / `pinecone_asset` / `pgvector_asset` / `chromadb_asset` — load embeddings into a vector store downstream.
