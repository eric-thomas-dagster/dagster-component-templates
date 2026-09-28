"""ContextEngineeringPipelineComponent — one YAML builds a governed,
searchable, citeable knowledge base out of raw unstructured text.

Same "pipeline component" shape as `ml_pipeline`, `rag_pipeline`,
`warehouse_pipeline`, `polars_pipeline`: **one YAML file declares the
whole pipeline**, a `steps:` list defines the chain in reading order,
each step's output is a DataFrame the next step can read via `source:`
(or, if omitted, the immediately-prior step — you don't have to repeat
`source:` on every line).

This is the "build the knowledge base" half of what the industry calls
"context engineering" (dbt Labs' `dbt_context_engineering` package is
the reference example: chunk → embed → classify → semantic search,
governed the same way metrics are). It's designed to pair with the
existing `rag_pipeline` component for the "query the knowledge base"
half — this pipeline's `write_vector_store` step's output is exactly
what `rag_pipeline`'s `retrieve`/`hybrid_search` ops expect to query
against.

Example (support tickets → a searchable, classified knowledge base):

    type: dagster_component_templates.ContextEngineeringPipelineComponent
    attributes:
      asset_name: support_kb
      upstream_asset_key: raw_support_tickets
      id_column: ticket_id
      text_column: body
      metadata_columns: [customer_id, created_at]
      steps:
        - {id: chunks, op: chunk, chunk_size: 500, chunk_overlap: 50}
        - {id: classified, op: classify, candidate_labels: [billing, technical, shipping, other]}
        - {id: embedded, op: embed, provider: sentence_transformers, model: all-MiniLM-L6-v2}
        - {id: indexed, op: write_vector_store, provider: chromadb,
           connection_string: /tmp/context_kb, collection_name: support_tickets}

Standardization — that's the point, same as `ml_pipeline`. Every
context-engineering pipeline in the org uses the same YAML shape, the
same ops, the same output conventions.

Op coverage:

- chunk:             fixed-size chunking with sentence-boundary snapping
                      and configurable overlap; threads `id_column` (and
                      any `metadata_columns`) through to every chunk row
                      as a citation back to the source row.
- classify:           HuggingFace zero-shot classification (local, free,
                      no API key) tags each chunk with a category BEFORE
                      embedding -- this is dbt's "relevance vs. similarity"
                      trick: raw cosine similarity ranks short boilerplate
                      over long relevant analysis unless chunks are
                      pre-filtered by category first.
- embed:              sentence-transformers (local, free), OpenAI, or
                      Cohere.
- write_vector_store: ChromaDB (local, free), Pinecone, or Qdrant.

Every op is free/local by default (sentence-transformers + HuggingFace
zero-shot + ChromaDB) -- no API key required to run this end to end,
matching dbt's own stated philosophy that this kind of governance
tooling should be cheap to validate, not another paid call per test.
"""
import re
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy) OR .get_connection() (DB-API) -- works out of the box
    with duckdb_resource, postgres_resource, mysql_resource,
    snowflake_resource, bigquery_resource, and any custom resource
    implementing the same duck-typed interface. Falls back to a bare
    SQLAlchemy engine via `database_url_env_var` when no Dagster resource
    is registered -- same dual pattern already used by this repo's
    reverse_etl components (e.g. greenhouse_candidate_update). This is
    what makes the pipeline "broadly usable against DuckDB and other
    databases" rather than requiring the raw text to already be a
    materialized Dagster asset."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            # get_connection() is a @contextmanager (confirmed live against
            # dagster_duckdb.DuckDBResource) -- calling it without `with` hands
            # back a _GeneratorContextManager, not a connection, and pd.read_sql
            # fails with AttributeError. Must be entered via `with`.
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy) "
            f"or .get_connection() (DB-API); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
):
    """Construct a Dagster partitions_def from the canonical partition fields.
    Canonical implementation — copied as-is per FIELD_CONVENTIONS.md."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# ── chunk op ────────────────────────────────────────────────────────────

def _chunk_fixed(text: str, chunk_size: int, chunk_overlap: int, preserve_sentences: bool) -> List[str]:
    """Fixed-size chunking with overlap, optionally snapping to a nearby
    sentence boundary -- same approach as document_chunker's chunk_fixed
    (duplicated, not imported, per this repo's standalone-component
    convention)."""
    chunks: List[str] = []
    start = 0
    text_len = len(text)
    while start < text_len:
        end = start + chunk_size
        if preserve_sentences and end < text_len:
            window = text[max(0, end - 100): min(text_len, end + 100)]
            sentence_ends = [m.end() for m in re.finditer(r"[.!?]\s", window)]
            if sentence_ends:
                closest = min(sentence_ends, key=lambda p: abs((max(0, end - 100) + p) - end))
                end = max(0, end - 100) + closest
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= text_len:
            break
        start = max(end - chunk_overlap, start + 1)
    return chunks


def _do_chunk(df: pd.DataFrame, step: dict, id_column: str, text_column: str, metadata_columns: List[str], context) -> pd.DataFrame:
    chunk_size = step.get("chunk_size", 500)
    chunk_overlap = step.get("chunk_overlap", 50)
    preserve_sentences = step.get("preserve_sentences", True)
    out_text_col = step.get("output_column", "chunk_text")

    rows: List[Dict[str, Any]] = []
    skipped = 0
    for _, row in df.iterrows():
        source_id = row[id_column]
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        if not text.strip():
            skipped += 1
            continue
        chunks = _chunk_fixed(text, chunk_size, chunk_overlap, preserve_sentences)
        for i, chunk_text in enumerate(chunks):
            record: Dict[str, Any] = {
                id_column: source_id,
                "chunk_id": f"{source_id}_{i}",
                "chunk_index": i,
                "total_chunks": len(chunks),
                out_text_col: chunk_text,
            }
            for col in metadata_columns:
                if col in df.columns:
                    record[col] = row[col]
            rows.append(record)
    if skipped:
        context.log.warning(f"chunk: skipped {skipped} row(s) with empty {text_column!r}")
    return pd.DataFrame(rows)


# ── classify op ───────────────────────────────────────────────────────

def _do_classify(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Tag each chunk with a category before embedding/search -- two modes:

    - mode='zero_shot' (default): HuggingFace zero-shot classification --
      local, free, no API key. Same pattern as zero_shot_classifier
      (duplicated, not imported).
    - mode='llm': any litellm-supported model judges the category via a
      real completion call -- costs money per chunk, but can apply real
      judgment a fixed zero-shot label set can't (nuanced/ambiguous
      categories, multi-factor rules described in the prompt).
    """
    candidate_labels = step.get("candidate_labels")
    if not candidate_labels:
        raise ValueError("classify op requires `candidate_labels` (a list of category names).")
    text_column = step.get("text_column", "chunk_text")
    output_column = step.get("output_column", "category")
    batch_size = step.get("batch_size", 16)
    mode = step.get("mode", "zero_shot")

    if text_column not in df.columns:
        raise ValueError(f"classify op: text_column={text_column!r} not in upstream columns: {list(df.columns)}")

    out = df.copy()
    texts = out[text_column].fillna("").astype(str).tolist()

    if mode == "zero_shot":
        try:
            from transformers import pipeline
        except ImportError:
            raise ImportError("classify op mode='zero_shot' requires transformers: pip install transformers torch")
        model_name = step.get("model", "facebook/bart-large-mnli")
        multi_label = step.get("multi_label", False)
        output_scores = step.get("output_scores", False)

        context.log.info(f"classify: loading zero-shot classifier {model_name!r} for {len(df)} chunks")
        classifier = pipeline("zero-shot-classification", model=model_name)
        results: List[dict] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_results = classifier(batch, candidate_labels=candidate_labels, multi_label=multi_label)
            if isinstance(batch_results, dict):
                batch_results = [batch_results]
            results.extend(batch_results)

        out[output_column] = [r["labels"][0] for r in results]
        if output_scores:
            for label in candidate_labels:
                out[f"score_{label}"] = [dict(zip(r["labels"], r["scores"])).get(label, 0.0) for r in results]

    elif mode == "llm":
        import os
        import json
        try:
            from litellm import completion
        except ImportError:
            raise ImportError("classify op mode='llm' requires litellm: pip install litellm")
        model_name = step.get("model", "gpt-4o-mini")
        api_key_env_var = step.get("api_key_env_var", "OPENAI_API_KEY")
        llm_max_retries = step.get("llm_max_retries", 2)
        prompt_prefix = step.get("prompt_prefix")

        categories: List[Optional[str]] = []
        for text in texts:
            prompt_parts = []
            if prompt_prefix:
                prompt_parts.append(prompt_prefix)
            prompt_parts.append(
                f"Classify the following text into exactly one of these categories: {candidate_labels}\n\n"
                f"Text:\n{text}\n\n"
                'Return only a JSON object like {"category": "<one of the listed categories>"}.'
            )
            try:
                resp = completion(
                    model=model_name,
                    messages=[{"role": "user", "content": "\n\n".join(prompt_parts)}],
                    api_key=os.environ.get(api_key_env_var),
                    num_retries=llm_max_retries,
                )
                raw = resp.choices[0].message.content.strip()
                if raw.startswith("```"):
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                parsed = json.loads(raw)
                category = parsed.get("category") if isinstance(parsed, dict) else None
                if category not in candidate_labels:
                    category = None
            except Exception as e:
                context.log.warning(f"classify (llm): failed for a chunk: {e}")
                category = None
            categories.append(category)
        out[output_column] = categories
    else:
        raise ValueError(f"classify op: unsupported mode {mode!r}. Valid: 'zero_shot', 'llm'.")

    return out


# ── embed op ────────────────────────────────────────────────────────────

def _do_embed(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    provider = step.get("provider", "sentence_transformers")
    text_column = step.get("text_column", "chunk_text")
    output_column = step.get("output_column", "embedding")
    batch_size = step.get("batch_size", 32)
    model = step.get("model")

    if text_column not in df.columns:
        raise ValueError(f"embed op: text_column={text_column!r} not in upstream columns: {list(df.columns)}")

    out = df.copy()
    texts = out[text_column].astype(str).tolist()
    embeddings: List[List[float]] = []

    if provider == "sentence_transformers":
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError("embed op provider='sentence_transformers' requires: pip install sentence-transformers")
        model_name = model or "all-MiniLM-L6-v2"
        context.log.info(f"embed: loading sentence-transformers model {model_name!r} for {len(out)} chunks")
        model_obj = SentenceTransformer(model_name)
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = model_obj.encode(batch, normalize_embeddings=step.get("normalize_embeddings", True), show_progress_bar=False, batch_size=batch_size)
            embeddings.extend(batch_embeddings.tolist())

    elif provider == "openai":
        import os
        try:
            import openai
        except ImportError:
            raise ImportError("embed op provider='openai' requires: pip install openai")
        model_name = model or "text-embedding-3-small"
        api_key_env_var = step.get("api_key_env_var", "OPENAI_API_KEY")
        client = openai.OpenAI(api_key=os.environ.get(api_key_env_var))
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            resp = client.embeddings.create(model=model_name, input=batch)
            embeddings.extend([d.embedding for d in resp.data])

    elif provider == "cohere":
        import os
        try:
            import cohere
        except ImportError:
            raise ImportError("embed op provider='cohere' requires: pip install cohere")
        model_name = model or "embed-english-v3.0"
        api_key_env_var = step.get("api_key_env_var", "COHERE_API_KEY")
        client = cohere.Client(api_key=os.environ.get(api_key_env_var))
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            resp = client.embed(texts=batch, model=model_name, input_type="search_document")
            embeddings.extend(resp.embeddings)

    elif provider == "litellm":
        # Universal gateway -- one interface across OpenAI, Azure, Bedrock,
        # Vertex, Ollama (local), VoyageAI, Mistral, and everything else
        # litellm supports, instead of a separate SDK integration per
        # provider. model is litellm's own "<provider>/<model>" format,
        # e.g. "text-embedding-3-small", "azure/my-embed-deployment",
        # "ollama/nomic-embed-text", "bedrock/amazon.titan-embed-text-v2:0".
        import os
        try:
            import litellm
        except ImportError:
            raise ImportError("embed op provider='litellm' requires: pip install litellm")
        model_name = model or "text-embedding-3-small"
        api_key_env_var = step.get("api_key_env_var")
        api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            resp = litellm.embedding(model=model_name, input=batch, api_key=api_key)
            embeddings.extend([d["embedding"] for d in resp.data])
    else:
        raise ValueError(f"embed op: unsupported provider {provider!r}. Valid: sentence_transformers, openai, cohere, litellm.")

    out[output_column] = embeddings
    return out


# ── write_vector_store op ────────────────────────────────────────────────

def _do_write_vector_store(df: pd.DataFrame, step: dict, id_column: str, context) -> pd.DataFrame:
    provider = step.get("provider", "chromadb")
    text_column = step.get("text_column", "chunk_text")
    embedding_column = step.get("embedding_column", "embedding")
    chunk_id_column = step.get("id_column", "chunk_id" if "chunk_id" in df.columns else id_column)
    metadata_columns = step.get("metadata_columns") or [c for c in df.columns if c not in (text_column, embedding_column, chunk_id_column)]
    batch_size = step.get("batch_size", 100)

    for required in (text_column, embedding_column, chunk_id_column):
        if required not in df.columns:
            raise ValueError(f"write_vector_store op: column {required!r} not in upstream columns: {list(df.columns)}")

    if provider == "chromadb":
        import chromadb
        connection_string = step.get("connection_string") or "./chroma_db"
        collection_name = step.get("collection_name", "context_engineering_kb")
        client = chromadb.PersistentClient(path=connection_string)
        collection = client.get_or_create_collection(name=collection_name)

        ids = df[chunk_id_column].astype(str).tolist()
        embeddings = df[embedding_column].tolist()
        documents = df[text_column].astype(str).tolist()
        metadatas = df[[c for c in metadata_columns if c in df.columns]].astype(str).to_dict("records") if metadata_columns else None

        for i in range(0, len(df), batch_size):
            j = min(i + batch_size, len(df))
            collection.upsert(
                ids=ids[i:j],
                embeddings=embeddings[i:j],
                documents=documents[i:j],
                metadatas=metadatas[i:j] if metadatas else None,
            )
        context.log.info(f"write_vector_store: upserted {len(df)} chunks into chromadb collection {collection_name!r} at {connection_string!r}")

    elif provider == "pinecone":
        try:
            from pinecone import Pinecone
        except ImportError:
            raise ImportError("write_vector_store op provider='pinecone' requires: pip install pinecone-client")
        import os
        api_key_env_var = step.get("api_key_env_var", "PINECONE_API_KEY")
        pc = Pinecone(api_key=os.environ.get(api_key_env_var))
        index = pc.Index(step["collection_name"])
        vectors = []
        for _, row in df.iterrows():
            meta = {c: str(row[c]) for c in metadata_columns if c in df.columns}
            meta[text_column] = str(row[text_column])
            vectors.append({"id": str(row[chunk_id_column]), "values": row[embedding_column], "metadata": meta})
        for i in range(0, len(vectors), batch_size):
            index.upsert(vectors=vectors[i:i + batch_size])
        context.log.info(f"write_vector_store: upserted {len(df)} chunks into pinecone index {step['collection_name']!r}")

    elif provider == "qdrant":
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.models import PointStruct
        except ImportError:
            raise ImportError("write_vector_store op provider='qdrant' requires: pip install qdrant-client")
        import os
        api_key_env_var = step.get("api_key_env_var")
        client = QdrantClient(url=step.get("connection_string") or "localhost", api_key=os.environ.get(api_key_env_var) if api_key_env_var else None)
        points = []
        for idx, row in df.iterrows():
            meta = {c: str(row[c]) for c in metadata_columns if c in df.columns}
            meta[text_column] = str(row[text_column])
            meta[chunk_id_column] = str(row[chunk_id_column])
            points.append(PointStruct(id=idx, vector=row[embedding_column], payload=meta))
        for i in range(0, len(points), batch_size):
            client.upsert(collection_name=step["collection_name"], points=points[i:i + batch_size])
        context.log.info(f"write_vector_store: upserted {len(df)} chunks into qdrant collection {step['collection_name']!r}")
    else:
        raise ValueError(f"write_vector_store op: unsupported provider {provider!r}. Valid: chromadb, pinecone, qdrant.")

    return df


_FRAME_OPS = {
    "classify": lambda df, step, ctx: _do_classify(df, step, ctx),
    "embed": lambda df, step, ctx: _do_embed(df, step, ctx),
}


# ── execution_mode='sql' ────────────────────────────────────────────────
# Generates and runs ONE server-side query per warehouse so the corpus
# never leaves the database and there's no in-memory row limit -- the
# direct answer to "data never leaves the database, where we can help
# it" for warehouses with a native embedding/completion function.
# validation.level: code -- there is no live warehouse credential in
# this dev environment, so these code paths are structurally tested
# (the emitted SQL is asserted, not executed) rather than run for real.

def _sql_chunk_expr(dialect: str, text_expr: str, chunk_size: int, chunk_overlap: int) -> str:
    step = max(chunk_size - chunk_overlap, 1)
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER({text_expr}, 'none', {chunk_size}, {chunk_overlap})"
    if dialect == "bigquery":
        # BigQuery has no native recursive splitter; emit a GENERATE_ARRAY
        # of substrings at fixed stride -- coarser than Cortex's splitter,
        # documented as such in the README.
        return (
            f"ARRAY(SELECT SUBSTR({text_expr}, pos, {chunk_size}) "
            f"FROM UNNEST(GENERATE_ARRAY(1, LENGTH({text_expr}), {step})) AS pos)"
        )
    if dialect == "databricks":
        return (
            f"ARRAY(SELECT SUBSTR({text_expr}, pos, {chunk_size}) "
            f"FROM (SELECT explode(sequence(1, LENGTH({text_expr}), {step})) AS pos))"
        )
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_classify_expr(dialect: str, text_expr: str, candidate_labels: List[str]) -> str:
    labels_sql = ", ".join(f"'{label}'" for label in candidate_labels)
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.CLASSIFY_TEXT({text_expr}, ARRAY_CONSTRUCT({labels_sql}))"
    if dialect == "bigquery":
        return f"AI.CLASSIFY({text_expr}, [{labels_sql}])"
    if dialect == "databricks":
        return f"ai_classify({text_expr}, ARRAY({labels_sql}))"
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_embed_expr(dialect: str, text_expr: str, model: Optional[str]) -> str:
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model or 'snowflake-arctic-embed-m'}', {text_expr})"
    if dialect == "bigquery":
        return f"ML.GENERATE_EMBEDDING(MODEL `{model or 'embedding_model'}`, {text_expr})"
    if dialect == "databricks":
        return f"ai_query('{model or 'databricks-gte-large-en'}', {text_expr})"
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _build_sql_mode_query(
    steps_cfg: List[dict], source_cfg: dict, dialect: str, output_table: str,
    id_column: str, text_column: str, metadata_columns: List[str],
) -> str:
    """Build the single CREATE OR REPLACE TABLE statement implementing the
    chunk -> classify -> embed chain natively in the warehouse. Only the
    ops actually used matter for output shape; write_vector_store has no
    SQL equivalent (there's no in-warehouse vector index for these
    dialects that behaves like chromadb/pinecone/qdrant) so it's skipped
    in this mode -- point rag_pipeline's `retrieve` op straight at
    output_table instead (Snowflake Cortex Search / BigQuery vector
    search / Databricks Vector Search all query a table directly)."""
    chunk_step = next(s for s in steps_cfg if s["op"] == "chunk")
    classify_step = next((s for s in steps_cfg if s["op"] == "classify"), None)
    embed_step = next((s for s in steps_cfg if s["op"] == "embed"), None)

    chunk_size = chunk_step.get("chunk_size", 500)
    chunk_overlap = chunk_step.get("chunk_overlap", 50)
    chunk_expr = _sql_chunk_expr(dialect, text_column, chunk_size, chunk_overlap)

    meta_select = ", ".join(f"src.{c}" for c in metadata_columns)
    meta_select = f", {meta_select}" if meta_select else ""

    lines = [
        f"CREATE OR REPLACE TABLE {output_table} AS",
        "WITH chunked AS (",
        f"  SELECT src.{id_column} AS {id_column},",
        f"         chunk.value::string AS chunk_text,",
        f"         chunk.index AS chunk_index{meta_select}",
        f"  FROM ({source_cfg['sql']}) AS src,",
        f"       LATERAL FLATTEN(input => {chunk_expr}) AS chunk",
        ")",
        "SELECT chunked.*",
    ]
    select_extra = []
    if classify_step is not None:
        candidate_labels = classify_step.get("candidate_labels")
        if not candidate_labels:
            raise ValueError("classify step in execution_mode='sql' requires `candidate_labels`.")
        classify_expr = _sql_classify_expr(dialect, "chunked.chunk_text", candidate_labels)
        select_extra.append(f"       {classify_expr} AS category")
    if embed_step is not None:
        embed_expr = _sql_embed_expr(dialect, "chunked.chunk_text", embed_step.get("model"))
        select_extra.append(f"       {embed_expr} AS embedding")
    if select_extra:
        lines[-1] += ","
        lines.append(",\n".join(select_extra))
    lines.append("FROM chunked")
    return "\n".join(lines)


def _run_sql_mode(
    context, steps_cfg: List[dict], source_cfg: dict, dialect: str, output_table: str,
    id_column: str, text_column: str, metadata_columns: List[str],
) -> Output:
    sql = _build_sql_mode_query(steps_cfg, source_cfg, dialect, output_table, id_column, text_column, metadata_columns)
    resource_key = source_cfg["resource_key"]
    resource = getattr(context.resources, resource_key)

    context.log.info(f"execution_mode='sql' ({dialect}): running server-side chunk/classify/embed into {output_table}")
    if hasattr(resource, "get_engine"):
        engine = resource.get_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql(sql)
            row_count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {output_table}").scalar()
    elif hasattr(resource, "get_connection"):
        with resource.get_connection() as conn:
            conn.execute(sql)
            row_count = conn.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
    else:
        raise ValueError(f"resource {resource_key!r} must expose .get_engine() or .get_connection().")

    return Output(
        value=None,
        metadata={
            "dagster/row_count": MetadataValue.int(row_count),
            "execution_mode": MetadataValue.text("sql"),
            "sql_dialect": MetadataValue.text(dialect),
            "output_table": MetadataValue.text(output_table),
            "generated_sql": MetadataValue.md(f"```sql\n{sql}\n```"),
        },
    )


class ContextEngineeringPipelineComponent(Component, Model, Resolvable):
    """Chunk, classify, embed, and index raw unstructured text into a
    governed, searchable, citeable knowledge base -- one YAML, a
    `steps:` list, pairs with `rag_pipeline` for the query side.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame of raw text rows. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull raw text rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource>, sql: <query>}. "
            "resource_key must point at a resource exposing .get_engine() (SQLAlchemy) or "
            ".get_connection() (DB-API) -- works out of the box with duckdb_resource, "
            "postgres_resource, snowflake_resource, bigquery_resource, etc., so this pipeline "
            "runs the same way against DuckDB or any other supported warehouse. Alternatively "
            "set `database_url_env_var` to a bare SQLAlchemy connection string from an "
            "environment variable when no Dagster resource is registered. Mutually "
            "exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    id_column: Union[str, int] = Field(description="Column uniquely identifying each source row -- threaded through to every chunk as a citation back to the source.")
    text_column: Union[str, int] = Field(description="Column containing the raw text to chunk.")
    metadata_columns: Optional[List[str]] = Field(
        default=None,
        description="Additional source columns to carry forward onto every chunk row (e.g. customer_id, created_at) -- available to later steps and written as vector-store metadata for filtered search.",
    )
    max_source_rows: Optional[int] = Field(
        default=None,
        description=(
            "Safety cap on source rows processed per materialize (applied AFTER any SQL "
            "filtering, before chunking). A real corpus is typically far too large to process "
            "in one in-memory run -- pair this with partitioning (partition_type below) to "
            "bound each run to a slice (e.g. one day's new tickets) rather than the whole "
            "historical corpus. None (default) processes every row in the upstream/query "
            "result, which is only safe for a genuinely small corpus or a pre-filtered "
            "partition. Only applies to execution_mode='python' -- execution_mode='sql' runs "
            "the whole corpus through the warehouse's own distributed engine and doesn't need "
            "this cap."
        ),
    )
    execution_mode: str = Field(
        default="python",
        description=(
            "'python' (default): pulls rows into a DataFrame and calls chunk/classify/embed "
            "as Python/API calls -- works with every provider, but data leaves the database "
            "and the corpus must fit in memory (bound it with max_source_rows + partitioning). "
            "'sql': the whole chunk/classify/embed chain runs as ONE query executed by the "
            "warehouse's own distributed engine via a warehouse-native AI function -- data "
            "never leaves the database and there's no in-memory size limit, but only works "
            "for warehouses with a native embedding/completion function (set sql_dialect). "
            "DuckDB has no such function (same limitation dbt's own context-engineering "
            "package has) -- use 'python' with a duckdb_resource `source:` for DuckDB."
        ),
    )
    sql_dialect: Optional[str] = Field(
        default=None,
        description="Required when execution_mode='sql'. One of: 'snowflake_cortex', 'bigquery', 'databricks'. Selects which warehouse-native AI SQL functions to emit.",
    )
    output_table: Optional[str] = Field(
        default=None,
        description="Required when execution_mode='sql'. Fully-qualified destination table name the final chunk/category/embedding rows are written to, in the same database (CREATE OR REPLACE TABLE ... AS ...) -- the knowledge base never leaves the warehouse.",
    )
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered list of {id, op, ...op-specific fields} dicts. Each step's `source:` "
            "names a prior step's id to read from; if omitted, defaults to the immediately "
            "preceding step (so you don't have to repeat `source:` on every line). "
            "Valid ops: 'chunk' (must be first), 'classify', 'embed', 'write_vector_store'."
        )
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — list of team names or email addresses.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. Auto-inferred if unset.")

    include_preview_metadata: bool = Field(default=False, description="Include a preview of the output data in metadata.")
    preview_rows: int = Field(default=25, ge=1, le=500, description="Rows to include in the preview metadata when include_preview_metadata is True.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'static'/'dynamic'/None.")
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO), required for time-based types.")
    partition_values: Optional[str] = Field(default=None, description="Comma-separated values for static partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps_cfg = list(self.steps or [])
        if not steps_cfg:
            raise ValueError("context_engineering_pipeline requires at least one step in `steps:`.")
        if steps_cfg[0]["op"] != "chunk":
            raise ValueError("context_engineering_pipeline: the first step must be op='chunk'.")
        valid_ops = {"chunk", "classify", "embed", "write_vector_store"}
        for s in steps_cfg:
            if "id" not in s or "op" not in s:
                raise ValueError(f"every step needs {{id, op}}; got: {s}")
            if s["op"] not in valid_ops:
                raise ValueError(f"step {s['id']!r} op={s['op']!r} unsupported. Valid: {sorted(valid_ops)}")

        execution_mode = self.execution_mode
        if execution_mode not in ("python", "sql"):
            raise ValueError(f"execution_mode must be 'python' or 'sql', got {execution_mode!r}.")
        if execution_mode == "sql":
            if self.sql_dialect not in ("snowflake_cortex", "bigquery", "databricks"):
                raise ValueError(
                    "execution_mode='sql' requires sql_dialect to be one of: "
                    "'snowflake_cortex', 'bigquery', 'databricks' (DuckDB and plain "
                    "Postgres/MySQL have no native embedding function -- use "
                    "execution_mode='python' with a `source:` warehouse_query for those)."
                )
            if not self.output_table:
                raise ValueError("execution_mode='sql' requires `output_table` (the destination table for the resulting knowledge base).")
            if not self.source or self.source.get("kind") != "warehouse_query":
                raise ValueError("execution_mode='sql' requires `source: {kind: warehouse_query, resource_key: ..., sql: ...}` -- there's no DataFrame to hand to a SQL engine.")
        if self.upstream_asset_key and self.source:
            raise ValueError("context_engineering_pipeline: set exactly one of `upstream_asset_key` or `source`, not both.")
        if not self.upstream_asset_key and not self.source:
            raise ValueError("context_engineering_pipeline: set one of `upstream_asset_key` or `source`.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
        )

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            freshness_policy = FreshnessPolicy(maximum_lag_minutes=self.freshness_max_lag_minutes, cron_schedule=self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        id_column = self.id_column
        text_column = self.text_column
        metadata_columns = self.metadata_columns or []
        max_source_rows = self.max_source_rows
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        source_cfg = self.source
        sql_dialect = self.sql_dialect
        output_table = self.output_table

        _inferred_kinds = self.kinds or ["ai", "context-engineering"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        asset_kwargs: Dict[str, Any] = dict(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Context-engineering pipeline ({execution_mode} mode): {' → '.join(s['id'] for s in steps_cfg)}",
            group_name=self.group_name,
            tags=_all_tags,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        if self.upstream_asset_key:
            asset_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))}
        if source_cfg and source_cfg.get("resource_key"):
            asset_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

        if execution_mode == "sql":
            @asset(**asset_kwargs)
            def _sql_asset(context: AssetExecutionContext):
                return _run_sql_mode(context, steps_cfg, source_cfg, sql_dialect, output_table, id_column, text_column, metadata_columns)
            return Definitions(assets=[_sql_asset])

        @asset(**asset_kwargs)
        def _asset(context: AssetExecutionContext, **kwargs):
            upstream = kwargs.get("upstream")
            if upstream is not None:
                if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                    upstream = upstream.value
                if isinstance(upstream, dict):
                    _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            else:
                upstream = _ingest_warehouse_query(source_cfg, context)

            if id_column not in upstream.columns:
                raise ValueError(f"id_column={id_column!r} not in upstream: {list(upstream.columns)}")
            if text_column not in upstream.columns:
                raise ValueError(f"text_column={text_column!r} not in upstream: {list(upstream.columns)}")

            source_rows_available = len(upstream)
            if max_source_rows is not None and source_rows_available > max_source_rows:
                context.log.warning(
                    f"max_source_rows={max_source_rows} < {source_rows_available} available source rows -- "
                    f"truncating this run. Pair with partitioning to process the full corpus over multiple runs "
                    f"rather than silently dropping rows every time."
                )
                upstream = upstream.head(max_source_rows)

            state: Dict[str, pd.DataFrame] = {}
            step_metadata: Dict[str, Dict[str, Any]] = {}
            last_frame_id: Optional[str] = None

            for step in steps_cfg:
                import time as _time
                t0 = _time.time()
                step_id = step["id"]
                op = step["op"]
                source_id = step.get("source") or last_frame_id

                if op == "chunk":
                    df = _do_chunk(upstream, step, id_column, text_column, metadata_columns, context)
                elif op == "write_vector_store":
                    if source_id is None:
                        raise ValueError(f"step {step_id!r} (op=write_vector_store) has no source and is not preceded by another step.")
                    df = _do_write_vector_store(state[source_id], step, id_column, context)
                else:
                    if source_id is None:
                        raise ValueError(f"step {step_id!r} (op={op!r}) has no source and is not preceded by another step.")
                    df = _FRAME_OPS[op](state[source_id], step, context)

                state[step_id] = df
                last_frame_id = step_id
                elapsed = _time.time() - t0
                step_metadata[step_id] = {"op": op, "rows": len(df), "elapsed_seconds": round(elapsed, 3)}
                context.log.info(f"step {step_id!r} ({op}) → {len(df)} rows in {elapsed:.2f}s")

            final_df = state[steps_cfg[-1]["id"]]

            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(len(final_df)),
                "source_rows": MetadataValue.int(len(upstream)),
                "source_rows_available": MetadataValue.int(source_rows_available),
                "n_steps": MetadataValue.int(len(steps_cfg)),
                "step_chain": MetadataValue.text(" → ".join(s["id"] for s in steps_cfg)),
                "step_metadata": MetadataValue.json(step_metadata),
            }
            if include_preview and len(final_df) > 0:
                try:
                    _prev_cols = [c for c in final_df.columns if c != "embedding"]
                    _prev = final_df[_prev_cols]
                    _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False) or "")
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(value=final_df, metadata=metadata)

        return Definitions(assets=[_asset])
