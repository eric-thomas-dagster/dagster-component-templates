"""HybridKnowledgeSearchServerComponent -- combined vector + graph retrieval over one
corpus, joined by `chunk_id` (the shared chunk-id contract documented in
`knowledge_graph_builder`'s and `context_engineering_pipeline`'s own READMEs).

Same dual-mode shape as `knowledge_search_server`/`knowledge_graph_search_server`:
  1. An importable `HybridKnowledgeSearchResource`, registered under `resource_key`.
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.
`mcp_server_resource_key`, if set, ALSO registers a stdio MCP-server-shaped resource.

Config is two nested blocks -- `vector_store` (same fields as `knowledge_search_server`'s
own `KnowledgeSearchResource`) and `graph` (same fields as `knowledge_graph_search_server`'s
own `KnowledgeGraphSearchResource`) -- rather than this component's own flat fields, since
the two stores are genuinely independent configurations being composed, not this
component's own settings. Their read logic is duplicated here from those two components
(not imported across components, this repo's standalone-component convention).

`search(query, top_k, hops, mode)`:
  - mode='vector': vector search only -- `{"passages": [...]}`, no graph expansion.
  - mode='graph': starts from entities matching the query text (fuzzy name/alias search),
    expands `hops` steps, and returns every chunk those entities/their neighbors appear in,
    plus the supporting triples -- no vector step.
  - mode='hybrid' (default): vector search AND graph expansion (seeded by both the entities
    mentioned in the top vector hits AND entities matching the query text directly),
    merged and de-duplicated by chunk_id. Each passage's `paths` field names how it was
    found (`"vector"`, `"graph:hop_<N>:<entity>"`, or both). Graph-only passages (reached by
    traversal, not a direct vector hit) have `text: null` UNLESS `vector_store.provider`
    is `chromadb`, in which case a best-effort `collection.get(ids=[chunk_id])` backfills it
    (chromadb is the only provider here with a cheap get-by-id lookup; others are left null
    rather than guessed at).

Every search/expansion result is capped by `max_results`.

Overlap check: when `vector_store.provider == "chromadb"` and `graph.provider == "networkx"`
(the only combination where listing every id in both stores is cheap), the inspection asset
reports `overlap_with_knowledge_base`: the Jaccard overlap (|intersection| / |union|) between
the vector collection's chunk ids and the graph's chunk ids. Every other provider combination
reports `overlap_with_knowledge_base: null` with a `overlap_note` explaining why (no cheap
full-id-listing API for that combination) rather than silently guessing.
"""
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

import dagster as dg
from dagster import AssetExecutionContext, AssetKey, Definitions, MetadataValue, asset
from pydantic import Field

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN knowledge-search core (duplicated from knowledge_search_server's own
# _embed_query/_query_vector_store -- see tests/test_drift.py)
def _embed_query(query: str, provider: str, model: Optional[str], api_key_env_var: Optional[str]) -> list:
    if provider == "sentence_transformers":
        from sentence_transformers import SentenceTransformer
        model_name = model or "all-MiniLM-L6-v2"
        return SentenceTransformer(model_name).encode([query], normalize_embeddings=True, show_progress_bar=False)[0].tolist()
    if provider == "openai":
        import openai
        client = openai.OpenAI(api_key=os.environ.get(api_key_env_var or "OPENAI_API_KEY"))
        resp = client.embeddings.create(model=model or "text-embedding-3-small", input=[query])
        return resp.data[0].embedding
    if provider == "cohere":
        import cohere
        client = cohere.Client(api_key=os.environ.get(api_key_env_var or "COHERE_API_KEY"))
        resp = client.embed(texts=[query], model=model or "embed-english-v3.0", input_type="search_query")
        return resp.embeddings[0]
    if provider == "litellm":
        import litellm
        api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
        resp = litellm.embedding(model=model or "text-embedding-3-small", input=[query], api_key=api_key)
        return resp.data[0]["embedding"]
    raise ValueError(f"HybridKnowledgeSearch: unsupported embed_provider {provider!r}. Valid: sentence_transformers, openai, cohere, litellm.")


def _build_s3vectors_client(region_name: str) -> Any:
    import boto3
    return boto3.client("s3vectors", region_name=region_name)


class _DemoS3VectorsClient:
    def __init__(self, path: str):
        self._path = path

    def query_vectors(self, vectorBucketName, indexName, topK, queryVector, filter=None, returnMetadata=True, returnDistance=True, **_kw):
        import json
        import math
        if not os.path.exists(self._path):
            return {"vectors": []}
        with open(self._path) as f:
            data = json.load(f)
        index = (data.get("indexes") or {}).get(indexName, {"vectors": {}})
        query = queryVector["float32"]
        query_norm = math.sqrt(sum(x * x for x in query)) or 1.0
        scored = []
        for key, entry in index["vectors"].items():
            metadata = entry.get("metadata", {})
            if filter and not all(metadata.get(k) == v for k, v in filter.items()):
                continue
            vec = entry["data"]["float32"]
            dot = sum(a * b for a, b in zip(query, vec))
            vec_norm = math.sqrt(sum(x * x for x in vec)) or 1.0
            scored.append((1.0 - (dot / (query_norm * vec_norm)), key, metadata))
        scored.sort(key=lambda t: t[0])
        results = []
        for distance, key, metadata in scored[:topK]:
            item = {"key": key}
            if returnMetadata:
                item["metadata"] = metadata
            if returnDistance:
                item["distance"] = distance
            results.append(item)
        return {"vectors": results}


def _query_vector_store(embedding, *, provider, connection_string, collection_name, top_k, api_key_env_var,
                         vector_bucket_name=None, aws_region=None, demo_mode=False) -> list:
    if provider == "chromadb":
        import chromadb
        client = chromadb.PersistentClient(path=connection_string or "./chroma_db")
        collection = client.get_collection(name=collection_name)
        res = collection.query(query_embeddings=[embedding], n_results=top_k)
        out = []
        for i in range(len(res["ids"][0])):
            out.append({
                "id": res["ids"][0][i],
                "metadata": (res.get("metadatas") or [[]])[0][i] if res.get("metadatas") else {},
                "document": (res.get("documents") or [[]])[0][i] if res.get("documents") else None,
                "score": None,
            })
        return out
    if provider == "pinecone":
        from pinecone import Pinecone
        pc = Pinecone(api_key=os.environ.get(api_key_env_var or "PINECONE_API_KEY"))
        index = pc.Index(collection_name)
        resp = index.query(vector=embedding, top_k=top_k, include_metadata=True)
        return [{"id": m["id"], "metadata": m.get("metadata") or {}, "document": None, "score": m.get("score")} for m in resp["matches"]]
    if provider == "qdrant":
        from qdrant_client import QdrantClient
        client = QdrantClient(url=connection_string or "localhost", api_key=os.environ.get(api_key_env_var) if api_key_env_var else None)
        hits = client.search(collection_name=collection_name, query_vector=embedding, limit=top_k)
        return [{"id": h.id, "metadata": h.payload or {}, "document": None, "score": h.score} for h in hits]
    if provider == "faiss":
        import faiss
        import pickle
        import numpy as np
        index_path = connection_string or f"./{collection_name}.faiss"
        metadata_path = index_path.replace(".faiss", "_metadata.pkl")
        faiss_index = faiss.read_index(index_path)
        query_vector = np.array([embedding]).astype("float32")
        distances, indices = faiss_index.search(query_vector, top_k)
        with open(metadata_path, "rb") as f:
            metadata = pickle.load(f)
        return [
            {"id": int(doc_idx), "metadata": metadata[doc_idx] if doc_idx < len(metadata) else {}, "document": None, "score": float(distances[0][i])}
            for i, doc_idx in enumerate(indices[0])
        ]
    if provider == "s3vectors":
        if not vector_bucket_name:
            raise ValueError("HybridKnowledgeSearch: provider='s3vectors' requires vector_bucket_name.")
        client = _DemoS3VectorsClient(connection_string or f"./{vector_bucket_name}_{collection_name}_s3vectors_demo.json") if demo_mode else _build_s3vectors_client(aws_region)
        resp = client.query_vectors(
            vectorBucketName=vector_bucket_name, indexName=collection_name, topK=top_k,
            queryVector={"float32": [float(x) for x in embedding]}, returnMetadata=True, returnDistance=True,
        )
        return [{"id": v["key"], "metadata": v.get("metadata") or {}, "document": None, "score": v.get("distance")} for v in resp.get("vectors", [])]
    raise ValueError(f"HybridKnowledgeSearch: unsupported provider {provider!r}. Valid: chromadb, pinecone, qdrant, faiss, s3vectors.")
# END knowledge-search core


# BEGIN receipt-warehouse-ddl (canonical; inlined verbatim, byte-identical, by every component that creates or reads the receipt tables -- drift-tested)
RECEIPT_TABLE_NAMES = ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events", "context_item_scores")
RECEIPT_DIALECTS = ("bigquery", "snowflake", "databricks", "postgres", "mysql", "duckdb", "sqlite")


def receipt_table(dataset: str, name: str, prefix: str = "", dialect: str = "bigquery") -> str:
    """Table reference for a receipt table. `dataset` is the namespace: `project.dataset` (bigquery), `database.schema` (snowflake),
    `catalog.schema` (databricks), `schema` (postgres/duckdb), `database` (mysql); "" for sqlite or the connection default."""
    if name not in RECEIPT_TABLE_NAMES:
        raise ValueError(f"unknown receipt table {name!r}; valid: {RECEIPT_TABLE_NAMES}")
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    parts = [p for p in dataset.strip("`").split(".") if p] + [f"{prefix}{name}"]
    if dialect == "bigquery":
        return "`" + ".".join(parts) + "`"          # BigQuery reads `a.b.c` as one path
    if dialect in ("databricks", "mysql"):
        return ".".join(f"`{p}`" for p in parts)
    if dialect in ("postgres", "duckdb", "sqlite"):
        return ".".join(f'"{p}"' for p in parts)
    return ".".join(parts)                          # snowflake: unquoted, case-insensitive


_RECEIPT_TYPES = {  # (string, long string, integer, float, timestamp)
    "bigquery": ("STRING", "STRING", "INT64", "FLOAT64", "TIMESTAMP"),
    "snowflake": ("VARCHAR", "VARCHAR", "BIGINT", "DOUBLE", "TIMESTAMP_TZ"),
    "databricks": ("STRING", "STRING", "BIGINT", "DOUBLE", "TIMESTAMP"),
    "postgres": ("TEXT", "TEXT", "BIGINT", "DOUBLE PRECISION", "TIMESTAMPTZ"),
    "mysql": ("VARCHAR(255)", "LONGTEXT", "BIGINT", "DOUBLE", "DATETIME(6)"),   # UTC by convention
    "duckdb": ("VARCHAR", "VARCHAR", "BIGINT", "DOUBLE", "TIMESTAMPTZ"),
    "sqlite": ("TEXT", "TEXT", "INTEGER", "REAL", "TEXT"),                       # ISO-8601 UTC strings
}


def receipt_table_ddl(dataset: str, prefix: str = "", dialect: str = "bigquery") -> list:
    """CREATE TABLE IF NOT EXISTS statements for the receipt tables (same columns as the SQLite store, per resources/context_receipt_store/SPEC.md).
    Event payloads stay JSON text in `payload_json`; priors and feedback are computed from them (SQL: JSON_VALUE and friends, or in Python)."""
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    S, L, I, F, T = _RECEIPT_TYPES[dialect]
    K = "`rank`" if dialect == "mysql" else "rank"        # RANK is reserved in MySQL 8
    t = lambda n: receipt_table(dataset, n, prefix, dialect)  # noqa: E731
    return [
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipts')} (
  receipt_id {S} NOT NULL, created_at {T} NOT NULL, context_name {S} NOT NULL, context_version {S}, policy_version {S},
  caller_id {S}, purpose {S}, query_hash {S}, eligible_count {I}, excluded_count {I}, dagster_asset_key {S},
  dagster_run_id {S}, trace_id {S}, mcp_session_id {S}, mcp_request_id {S}, dagster_partition_key {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_items')} (
  receipt_id {S} NOT NULL, item_id {S} NOT NULL, source_asset_key {S}, source_version {S}, source_partition {S},
  source_code_version {S}, {K} {I}, score {F}, selection {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_excluded')} (
  receipt_id {S} NOT NULL, item_id {S} NOT NULL, reason {S} NOT NULL, source_asset_key {S}, source_version {S},
  source_partition {S}, source_code_version {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_events')} (
  event_id {S} NOT NULL, receipt_id {S} NOT NULL, event_type {S} NOT NULL, occurred_at {T} NOT NULL, payload_json {L} NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_item_scores')} (
  item_id {S} NOT NULL, retrievals {I} NOT NULL, outcome_weight {F} NOT NULL, outcome_sum {F} NOT NULL, prior {F} NOT NULL,
  updated_at {T} NOT NULL)""",
    ]


# Columns added after the first release: (table, column, kind). A deployed table created from an older DDL lacks them, so
# creators probe each one and run the ALTER only when the probe fails (never a destructive step; existing rows read NULL).
RECEIPT_ADDED_COLUMNS = (("context_receipts", "dagster_partition_key", "string"),)


def receipt_column_upgrades(dataset: str, prefix: str = "", dialect: str = "bigquery") -> list:
    """``[(probe_sql, alter_sql)]`` per late-added column: run ``probe_sql``; if it raises (column missing), run ``alter_sql``."""
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    S, L, I, F, T = _RECEIPT_TYPES[dialect]
    out = []
    for table, column, kind in RECEIPT_ADDED_COLUMNS:
        ref = receipt_table(dataset, table, prefix, dialect)
        out.append((f"SELECT {column} FROM {ref} WHERE 1 = 0", f"ALTER TABLE {ref} ADD COLUMN {column} {S if kind == 'string' else L}"))
    return out
# END receipt-warehouse-ddl


# Outcome signals -> default value in [-1, 1] (same vocabulary as context_receipt_store's own
# OUTCOME_SIGNALS; unknown signals need an explicit value, which record_feedback's 3-arg tool
# signature doesn't expose -- so an unlisted signal here is simply rejected, not silently
# defaulted to 0).
OUTCOME_SIGNALS = {
    "thumbs_up": 1.0,
    "thumbs_down": -1.0,
    "accepted": 0.5,
    "edited": -0.3,
    "escalated": -0.7,
    "reopened": -0.7,
    "closed_won": 1.0,
    "closed_lost": -0.5,
    "reformulated": -0.2,
}
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _new_receipt_id() -> str:
    """Time-sortable ULID, generated client-side -- same scheme as context_receipt_store's own
    `new_receipt_id()` (copied, not imported, per this repo's self-contained-component
    convention), so receipt ids from this server sort/compare the same way."""
    value = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 31])
        value >>= 5
    return "".join(reversed(chars))


class _ReceiptWriter:
    """Minimal, SQLite-only receipt writer for recording retrievals/outcomes directly from this
    MCP server's own stdio subprocess. Deliberately NOT the full `ContextReceiptStore` (that
    class also supports bigquery/sql backends, a usage sensor, context policy enforcement, and
    learned-prior rerank scoring) -- just enough to record_retrieval/record_outcome against a
    local file, using the SAME canonical receipt-table DDL/names a real
    `ContextReceiptStoreComponent(backend='sqlite', database_path=...)` resource reads. Point
    both at the same file and that resource's own sensors/asset checks see receipts recorded
    here. This exists only because the stdio MCP subprocess has no Dagster
    context.resources available at tool-call time (confirmed: `main()` below constructs
    `HybridKnowledgeSearchResource` from plain env vars, never from a Dagster resource
    registry)."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_tables()

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_tables(self) -> None:
        with self._connect() as conn:
            for stmt in receipt_table_ddl("", dialect="sqlite"):
                conn.execute(stmt)

    def record_retrieval(
        self, *, context_name: str, items: List[Dict[str, Any]],
        eligible_count: Optional[int] = None, purpose: Optional[str] = None, query_hash: Optional[str] = None,
    ) -> str:
        receipt_id = _new_receipt_id()
        created_at = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO context_receipts (
                    receipt_id, created_at, context_name, context_version, policy_version,
                    caller_id, purpose, query_hash, eligible_count, excluded_count,
                    dagster_asset_key, dagster_run_id, trace_id, mcp_session_id, mcp_request_id, dagster_partition_key
                ) VALUES (?, ?, ?, NULL, NULL, NULL, ?, ?, ?, 0, NULL, NULL, NULL, NULL, NULL, NULL)""",
                (receipt_id, created_at, context_name, purpose, query_hash, eligible_count),
            )
            conn.executemany(
                """INSERT INTO context_receipt_items (
                    receipt_id, item_id, source_asset_key, source_version, source_partition,
                    source_code_version, rank, score, selection
                ) VALUES (?, ?, ?, ?, NULL, ?, ?, ?, 'ranked')""",
                [
                    (
                        receipt_id, item["item_id"], item.get("source_asset_key"), item.get("source_version"),
                        item.get("source_code_version"), item.get("rank"), item.get("score"),
                    )
                    for item in items
                ],
            )
        return receipt_id

    def record_outcome(
        self, receipt_id: str, *, signal: str, value: Optional[float] = None,
        source: Optional[str] = None, outcome_ref: Optional[str] = None,
    ) -> None:
        if value is None:
            if signal not in OUTCOME_SIGNALS:
                raise ValueError(f"unknown signal {signal!r} requires an explicit value")
            value = OUTCOME_SIGNALS[signal]
        payload = {"signal": signal, "value": float(value), "source": source, "outcome_ref": outcome_ref}
        with self._connect() as conn:
            exists = conn.execute("SELECT 1 FROM context_receipts WHERE receipt_id = ?", (receipt_id,)).fetchone()
            if exists is None:
                raise KeyError(f"unknown context receipt: {receipt_id}")
            conn.execute(
                "INSERT INTO context_receipt_events (event_id, receipt_id, event_type, occurred_at, payload_json) "
                "VALUES (?, ?, 'outcome', ?, ?)",
                (str(uuid.uuid4()), receipt_id, datetime.now(timezone.utc).isoformat(),
                 json.dumps(payload, sort_keys=True, separators=(",", ":"))),
            )

    def counts(self) -> Dict[str, int]:
        """Best-effort {"retrievals": n, "outcomes": n} -- used by the inspection asset."""
        with self._connect() as conn:
            retrievals = conn.execute("SELECT COUNT(*) FROM context_receipts").fetchone()[0]
            outcomes = conn.execute("SELECT COUNT(*) FROM context_receipt_events WHERE event_type = 'outcome'").fetchone()[0]
        return {"retrievals": retrievals, "outcomes": outcomes}


# BEGIN knowledge-graph-store core (duplicated from knowledge_graph_builder's/
# knowledge_graph_search_server's own graph-store I/O -- read-only subset -- see tests/test_drift.py)
def _build_networkx_graph(path: str):
    import json
    import networkx as nx
    if not os.path.exists(path):
        return nx.MultiDiGraph()
    with open(path) as f:
        data = json.load(f)
    return nx.node_link_graph(data, directed=True, multigraph=True, edges="edges")


def _build_neo4j_driver(uri: str, username: Optional[str], password: Optional[str]):
    from neo4j import GraphDatabase
    auth = (username, password) if username else None
    return GraphDatabase.driver(uri, auth=auth)


def _build_falkordb_graph(host: str, port: int, username: Optional[str], password: Optional[str], graph_name: str):
    from falkordb import FalkorDB
    db = FalkorDB(host=host, port=port, username=username, password=password)
    return db.select_graph(graph_name)


def _run_graph_query(provider: str, cfg: dict, query: str, params: dict) -> List[Dict[str, Any]]:
    if provider == "neo4j":
        uri = cfg.get("uri")
        if not uri:
            raise ValueError("HybridKnowledgeSearch: graph.provider='neo4j' requires `uri`.")
        username = os.environ.get(cfg["username_env_var"]) if cfg.get("username_env_var") else None
        password = os.environ.get(cfg["password_env_var"]) if cfg.get("password_env_var") else None
        driver = _build_neo4j_driver(uri, username, password)
        try:
            with driver.session(database=cfg.get("database", "neo4j"), default_access_mode="READ") as session:
                result = session.run(query, params)
                return [dict(r) for r in result]
        finally:
            driver.close()
    if provider == "falkordb":
        host = cfg.get("host", "localhost")
        port = int(cfg.get("port", 6379))
        username = os.environ.get(cfg["username_env_var"]) if cfg.get("username_env_var") else None
        password = os.environ.get(cfg["password_env_var"]) if cfg.get("password_env_var") else None
        g = _build_falkordb_graph(host, port, username, password, cfg.get("graph_name", "knowledge_graph"))
        result = g.query(query, params)
        columns = getattr(result, "header", None)
        if columns:
            col_names = [c[1] if isinstance(c, (list, tuple)) else c for c in columns]
            return [dict(zip(col_names, row)) for row in result.result_set]
        return [dict(zip(params.keys(), row)) for row in result.result_set]
    raise ValueError(f"unsupported provider {provider!r}")
# END knowledge-graph-store core


# BEGIN sql_tables read path -- duplicated from knowledge_graph_search_server's own copy (this
# repo's standalone-component convention). Same 3 dialects as knowledge_graph_builder's
# execution_mode='sql'. get_chunks_for_entity/get_graph_context are hybrid-specific additions:
# the sql_tables edges schema keeps only ONE aggregate `evidence` string per (subject, relation,
# object) triple (not a per-chunk array like networkx/neo4j/falkordb's `evidences`), so
# get_graph_context's evidence for a given chunk_id is that triple's one aggregate value, not
# necessarily the exact snippet from that specific chunk -- a real, documented scope cut (same
# spirit as execution_mode='sql' having no resolve/alias-merging step).
def _sql_str(value: Any) -> str:
    s = str(value).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
    return f"'{s}'"


def _sql_qualify(name: str, dialect: str) -> str:
    return f"`{name}`" if dialect == "bigquery" else name


def _sql_tables_names(output_table: str, dialect: str) -> Tuple[str, str]:
    return _sql_qualify(f"{output_table}_nodes", dialect), _sql_qualify(f"{output_table}_edges", dialect)


def _build_snowflake_connection(cfg: dict):
    account = os.environ.get(cfg.get("snowflake_account_env_var") or "SNOWFLAKE_ACCOUNT")
    user = os.environ.get(cfg.get("snowflake_user_env_var") or "SNOWFLAKE_USER")
    password = os.environ.get(cfg.get("snowflake_password_env_var") or "SNOWFLAKE_PASSWORD")
    if not account or not user:
        raise ValueError("HybridKnowledgeSearch: graph.provider='sql_tables' sql_dialect='snowflake_cortex' requires snowflake_account_env_var and snowflake_user_env_var to be set in the environment.")
    import snowflake.connector
    kwargs: Dict[str, Any] = {"account": account, "user": user}
    if password:
        kwargs["password"] = password
    if cfg.get("snowflake_database"):
        kwargs["database"] = cfg["snowflake_database"]
    if cfg.get("snowflake_schema"):
        kwargs["schema"] = cfg["snowflake_schema"]
    if cfg.get("snowflake_warehouse"):
        kwargs["warehouse"] = cfg["snowflake_warehouse"]
    return snowflake.connector.connect(**kwargs)


def _build_bigquery_client(cfg: dict):
    from google.cloud import bigquery
    return bigquery.Client(project=cfg.get("bigquery_project_id"))


def _build_databricks_connection(cfg: dict):
    host = os.environ.get(cfg.get("databricks_host_env_var") or "DATABRICKS_HOST")
    token = os.environ.get(cfg.get("databricks_token_env_var") or "DATABRICKS_TOKEN")
    http_path = cfg.get("databricks_http_path")
    if not host or not token or not http_path:
        raise ValueError("HybridKnowledgeSearch: graph.provider='sql_tables' sql_dialect='databricks' requires databricks_host_env_var/databricks_token_env_var (set in the environment) and databricks_http_path.")
    from databricks import sql as databricks_sql
    return databricks_sql.connect(server_hostname=host, http_path=http_path, access_token=token)


def _maybe_json_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        import json
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return []
    return list(value)


def _run_sql_tables_query(cfg: dict, sql: str) -> List[Dict[str, Any]]:
    dialect = cfg.get("sql_dialect")
    if dialect == "snowflake_cortex":
        conn = _build_snowflake_connection(cfg)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    if dialect == "bigquery":
        client = _build_bigquery_client(cfg)
        result = client.query(sql).result()
        return [dict(row.items()) for row in result]
    if dialect == "databricks":
        conn = _build_databricks_connection(cfg)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    raise ValueError(f"HybridKnowledgeSearch: graph.provider='sql_tables' requires sql_dialect to be one of 'snowflake_cortex', 'bigquery', 'databricks'; got {dialect!r}.")


def _sql_array_fns(dialect: str) -> Dict[str, Any]:
    if dialect == "bigquery":
        return {"ctor": lambda x: f"[{x}]", "empty": "CAST([] AS ARRAY<STRING>)", "append": lambda arr, x: f"ARRAY_CONCAT({arr}, [{x}])", "contains": lambda arr, x: f"{x} IN UNNEST({arr})"}
    if dialect == "databricks":
        return {"ctor": lambda x: f"array({x})", "empty": "CAST(array() AS ARRAY<STRING>)", "append": lambda arr, x: f"concat({arr}, array({x}))", "contains": lambda arr, x: f"array_contains({arr}, {x})"}
    return {"ctor": lambda x: f"ARRAY_CONSTRUCT({x})", "empty": "ARRAY_CONSTRUCT()", "append": lambda arr, x: f"ARRAY_APPEND({arr}, {x})",
            "contains": lambda arr, x: f"ARRAY_CONTAINS(({x})::VARIANT, {arr})"}


def _build_sql_neighbors_query(dialect: str, edges_tbl: str, name: str, hops: int, relation: Optional[str]) -> str:
    # Databricks' real `) MAX RECURSION LEVEL n` safety clause is omitted -- not load-bearing (the
    # `depth < hops` predicate already bounds recursion tighter) and sqlglot 28.0.0 has no parser
    # support for it (confirmed directly), unlike everything else validated here.
    fns = _sql_array_fns(dialect)
    qname = _sql_str(name)
    other = "(CASE WHEN e.source = n.name THEN e.target ELSE e.source END)"
    relation_filter = f"e.relation = {_sql_str(relation)}" if relation else "TRUE"
    return (
        f"WITH RECURSIVE neighbors (name, depth, path) AS (\n"
        f"  SELECT {qname}, 0, {fns['ctor'](qname)}\n"
        f"  UNION ALL\n"
        f"  SELECT {other}, n.depth + 1, {fns['append']('n.path', other)}\n"
        f"  FROM {edges_tbl} AS e JOIN neighbors AS n ON (e.source = n.name OR e.target = n.name)\n"
        f"  WHERE n.depth < {int(hops)} AND {relation_filter} AND NOT {fns['contains']('n.path', other)}\n"
        f")\n"
        f"SELECT name, MIN(depth) AS depth FROM neighbors WHERE name != {qname} GROUP BY name ORDER BY depth"
    )


def _build_sql_path_query(dialect: str, edges_tbl: str, source: str, target: str, max_hops: int) -> str:
    fns = _sql_array_fns(dialect)
    qsource, qtarget = _sql_str(source), _sql_str(target)
    other = "(CASE WHEN e.source = p.name THEN e.target ELSE e.source END)"
    return (
        f"WITH RECURSIVE paths (name, depth, path, rels) AS (\n"
        f"  SELECT {qsource}, 0, {fns['ctor'](qsource)}, {fns['empty']}\n"
        f"  UNION ALL\n"
        f"  SELECT {other}, p.depth + 1, {fns['append']('p.path', other)}, {fns['append']('p.rels', 'e.relation')}\n"
        f"  FROM {edges_tbl} AS e JOIN paths AS p ON (e.source = p.name OR e.target = p.name)\n"
        f"  WHERE p.depth < {int(max_hops)} AND NOT {fns['contains']('p.path', other)}\n"
        f")\n"
        f"SELECT path, rels FROM paths WHERE name = {qtarget} ORDER BY depth LIMIT 1"
    )


def _build_sql_search_entities_query(nodes_tbl: str, query: str, type: Optional[str], limit: int) -> str:
    q = _sql_str(f"%{query.lower()}%")
    type_filter = f"AND type = {_sql_str(type)}" if type is not None else ""
    return f"SELECT id, name, type, mentions FROM {nodes_tbl} WHERE LOWER(name) LIKE {q} {type_filter} ORDER BY mentions DESC LIMIT {int(limit)}"


def _build_sql_entity_relations_queries(edges_tbl: str, name: str) -> Tuple[str, str]:
    qname = _sql_str(name)
    out_q = f"SELECT target AS other, relation, confidence, mention_count, evidence FROM {edges_tbl} WHERE source = {qname}"
    in_q = f"SELECT source AS other, relation, confidence, mention_count, evidence FROM {edges_tbl} WHERE target = {qname}"
    return out_q, in_q


def _build_sql_chunks_for_entity_query(edges_tbl: str, name: str) -> str:
    qname = _sql_str(name)
    return f"SELECT chunk_ids FROM {edges_tbl} WHERE source = {qname} OR target = {qname}"


def _build_sql_graph_context_query(dialect: str, edges_tbl: str, chunk_id: str) -> str:
    fns = _sql_array_fns(dialect)
    return (f"SELECT source AS subject, target AS object, relation, confidence, evidence FROM {edges_tbl} "
            f"WHERE {fns['contains']('chunk_ids', _sql_str(chunk_id))}")
# END sql_tables read path


_NETWORKX_GRAPH_CACHE: Dict[int, Any] = {}


def _drop_none(d: Dict[str, Any]) -> Dict[str, Any]:
    """Dagster's config system rejects a None value for an individual entry of a
    Dict[str, Any] ConfigurableResource field ("Null map member not caught in validation")
    even though the field itself may be None/absent -- so every optional, unset sub-field
    must be dropped from the dict entirely before it reaches HybridKnowledgeSearchResource,
    rather than passed through as an explicit null."""
    return {k: v for k, v in d.items() if v is not None}


class HybridKnowledgeSearchResource(dg.ConfigurableResource):
    """Combined vector + graph retrieval over one corpus joined by chunk_id."""

    vector_store: Dict[str, Any] = Field(default_factory=dict, description=(
        "Same fields as knowledge_search_server's KnowledgeSearchResource: provider "
        "(chromadb|pinecone|qdrant|faiss|s3vectors, default chromadb), connection_string, "
        "collection_name (required), api_key_env_var, embed_provider (default "
        "sentence_transformers), embed_model, embed_api_key_env_var, text_metadata_key "
        "(default chunk_text), vector_bucket_name, aws_region, demo_mode."
    ))
    graph: Dict[str, Any] = Field(default_factory=dict, description=(
        "Same fields as knowledge_graph_search_server's KnowledgeGraphSearchResource: "
        "provider (networkx|neo4j|falkordb, default networkx), path, uri, "
        "username_env_var, password_env_var, database, host, port, "
        "graph_name (default knowledge_graph)."
    ))
    knowledge_base: Optional[str] = Field(default=None, description="Optional UI-linking label.")
    max_results: int = Field(default=50, description="Cap on rows returned by any list-returning method.")
    receipts_db_path: Optional[str] = Field(
        default=None,
        description=(
            "SQLite file path. When set, every search() call with a vector component "
            "(mode='vector' or 'hybrid') records a retrieval receipt for its vector-sourced "
            "passages (chunk ids and scores only, never the query or chunk text) and returns "
            "receipt_id; mode='graph' alone records nothing (no ranked vector retrieval to "
            "attribute a receipt to). Quarantined chunks (feedback_quarantined metadata) are "
            "always dropped from vector results, whether or not this is set. See "
            "KnowledgeSearchResource.receipts_db_path's own docs (knowledge_search_server) for "
            "why this is a plain file path rather than a Dagster resource_key."
        ),
    )

    def _vector_cfg(self) -> dict:
        return {
            "provider": self.vector_store.get("provider", "chromadb"),
            "connection_string": self.vector_store.get("connection_string"),
            "collection_name": self.vector_store.get("collection_name"),
            "api_key_env_var": self.vector_store.get("api_key_env_var"),
            "embed_provider": self.vector_store.get("embed_provider", "sentence_transformers"),
            "embed_model": self.vector_store.get("embed_model"),
            "embed_api_key_env_var": self.vector_store.get("embed_api_key_env_var"),
            "text_metadata_key": self.vector_store.get("text_metadata_key", "chunk_text"),
            "vector_bucket_name": self.vector_store.get("vector_bucket_name"),
            "aws_region": self.vector_store.get("aws_region"),
            "demo_mode": self.vector_store.get("demo_mode", True),
        }

    def _graph_cfg(self) -> dict:
        return {
            "provider": self.graph.get("provider", "networkx"),
            "path": self.graph.get("path"),
            "uri": self.graph.get("uri"),
            "username_env_var": self.graph.get("username_env_var"),
            "password_env_var": self.graph.get("password_env_var"),
            "database": self.graph.get("database", "neo4j"),
            "host": self.graph.get("host", "localhost"),
            "port": self.graph.get("port", 6379),
            "graph_name": self.graph.get("graph_name", "knowledge_graph"),
            "sql_dialect": self.graph.get("sql_dialect"),
            "output_table": self.graph.get("output_table"),
            "snowflake_account_env_var": self.graph.get("snowflake_account_env_var", "SNOWFLAKE_ACCOUNT"),
            "snowflake_user_env_var": self.graph.get("snowflake_user_env_var", "SNOWFLAKE_USER"),
            "snowflake_password_env_var": self.graph.get("snowflake_password_env_var", "SNOWFLAKE_PASSWORD"),
            "snowflake_database": self.graph.get("snowflake_database"),
            "snowflake_schema": self.graph.get("snowflake_schema", "PUBLIC"),
            "snowflake_warehouse": self.graph.get("snowflake_warehouse"),
            "databricks_host_env_var": self.graph.get("databricks_host_env_var", "DATABRICKS_HOST"),
            "databricks_token_env_var": self.graph.get("databricks_token_env_var", "DATABRICKS_TOKEN"),
            "databricks_http_path": self.graph.get("databricks_http_path"),
            "bigquery_project_id": self.graph.get("bigquery_project_id"),
        }

    def _sql_tables(self, gcfg: dict) -> Tuple[str, str]:
        if not gcfg.get("output_table"):
            raise ValueError("HybridKnowledgeSearch: graph.provider='sql_tables' requires graph.output_table.")
        if gcfg.get("sql_dialect") not in ("snowflake_cortex", "bigquery", "databricks"):
            raise ValueError(f"HybridKnowledgeSearch: graph.provider='sql_tables' requires graph.sql_dialect to be one of 'snowflake_cortex', 'bigquery', 'databricks'; got {gcfg.get('sql_dialect')!r}.")
        return _sql_tables_names(gcfg["output_table"], gcfg["sql_dialect"])

    def _nx_graph(self):
        cfg = self._graph_cfg()
        key = id(self)
        if key not in _NETWORKX_GRAPH_CACHE:
            path = cfg["path"] or f"./{cfg['graph_name']}.json"
            _NETWORKX_GRAPH_CACHE[key] = _build_networkx_graph(path)
        return _NETWORKX_GRAPH_CACHE[key]

    # ─── vector primitive ───────────────────────────────────────────────

    def _vector_search(self, query: str, top_k: int) -> Tuple[List[Dict[str, Any]], int]:
        """Returns (hits, eligible_count). `eligible_count` is the raw hit count BEFORE
        quarantine filtering -- quarantined chunks (`feedback_quarantined` metadata, the same
        field `write_vector_store`'s receipts policy stamps) are dropped here, honoring
        quarantine the same way the FastMCP gateway's own KnowledgeSearch.search() does,
        independent of whether receipts are configured on this server at all."""
        vcfg = self._vector_cfg()
        if not vcfg["collection_name"]:
            raise ValueError("HybridKnowledgeSearch: vector_store.collection_name is required.")
        embedding = _embed_query(query, vcfg["embed_provider"], vcfg["embed_model"], vcfg["embed_api_key_env_var"])
        raw_hits = _query_vector_store(
            embedding, provider=vcfg["provider"], connection_string=vcfg["connection_string"],
            collection_name=vcfg["collection_name"], top_k=top_k, api_key_env_var=vcfg["api_key_env_var"],
            vector_bucket_name=vcfg["vector_bucket_name"], aws_region=vcfg["aws_region"], demo_mode=vcfg["demo_mode"],
        )
        eligible_count = len(raw_hits)
        hits = [h for h in raw_hits if not (h.get("metadata") or {}).get("feedback_quarantined")]
        out = []
        for h in hits:
            meta = h.get("metadata") or {}
            text = h.get("document") if h.get("document") is not None else meta.get(vcfg["text_metadata_key"])
            out.append({
                "chunk_id": meta.get("chunk_id", h["id"]), "text": text, "score": h.get("score"),
                "source": meta.get("source_asset_key"), "doc_id": meta.get("source_id"),
                "source_version": meta.get("source_version"), "source_code_version": meta.get("source_code_version"),
            })
        return out, eligible_count

    def _backfill_text_from_chromadb(self, chunk_id: str) -> Optional[str]:
        vcfg = self._vector_cfg()
        if vcfg["provider"] != "chromadb" or not vcfg["collection_name"]:
            return None
        try:
            import chromadb
            client = chromadb.PersistentClient(path=vcfg["connection_string"] or "./chroma_db")
            collection = client.get_collection(name=vcfg["collection_name"])
            res = collection.get(ids=[chunk_id], include=["documents", "metadatas"])
            if res["ids"]:
                doc = (res.get("documents") or [None])[0]
                if doc is not None:
                    return doc
                meta = (res.get("metadatas") or [{}])[0] or {}
                return meta.get(vcfg["text_metadata_key"])
        except Exception:  # noqa: BLE001 -- best-effort only
            return None
        return None

    # ─── graph primitives (same shape as knowledge_graph_search_server) ──

    def search_entities(self, query: str, type: Optional[str] = None) -> dict:
        gcfg = self._graph_cfg()
        q = query.lower()
        if gcfg["provider"] == "sql_tables":
            nodes_tbl, _ = self._sql_tables(gcfg)
            rows = _run_sql_tables_query(gcfg, _build_sql_search_entities_query(nodes_tbl, query, type, self.max_results))
            return {"entities": [{"name": r["name"], "type": r["type"], "mention_count": r["mentions"], "aliases": None} for r in rows]}
        if gcfg["provider"] == "networkx":
            g = self._nx_graph()
            hits = []
            for name, data in g.nodes(data=True):
                aliases = data.get("aliases") or []
                if q in name.lower() or any(q in a.lower() for a in aliases):
                    if type is not None and data.get("type") != type:
                        continue
                    hits.append({"name": name, "type": data.get("type"), "mention_count": data.get("mention_count"), "aliases": aliases})
            return {"entities": hits[: self.max_results]}
        records = _run_graph_query(
            gcfg["provider"], gcfg,
            "MATCH (n {graph: $graph_name}) WHERE (toLower(n.name) CONTAINS $q "
            "OR any(a IN n.aliases WHERE toLower(a) CONTAINS $q)) AND ($type IS NULL OR n.type = $type) "
            "RETURN n.name AS name, n.type AS type, n.mention_count AS mention_count, n.aliases AS aliases LIMIT $limit",
            {"graph_name": gcfg["graph_name"], "q": q, "type": type, "limit": self.max_results},
        )
        return {"entities": records}

    def get_entity(self, name: str) -> dict:
        gcfg = self._graph_cfg()
        if gcfg["provider"] == "sql_tables":
            nodes_tbl, edges_tbl = self._sql_tables(gcfg)
            node_rows = _run_sql_tables_query(gcfg, f"SELECT type, mentions FROM {nodes_tbl} WHERE id = {_sql_str(name)}")
            if not node_rows:
                return {"error": f"entity {name!r} not found"}
            out_q, in_q = _build_sql_entity_relations_queries(edges_tbl, name)
            out_rows, in_rows = _run_sql_tables_query(gcfg, out_q), _run_sql_tables_query(gcfg, in_q)
            relations = (
                [{"direction": "out", "relation": r["relation"], "other": r["other"], "confidence": r["confidence"], "mention_count": r["mention_count"], "evidence": r["evidence"]} for r in out_rows]
                + [{"direction": "in", "relation": r["relation"], "other": r["other"], "confidence": r["confidence"], "mention_count": r["mention_count"], "evidence": r["evidence"]} for r in in_rows]
            )
            return {"name": name, "type": node_rows[0]["type"], "aliases": None, "mention_count": node_rows[0]["mentions"], "relations": relations}
        if gcfg["provider"] == "networkx":
            g = self._nx_graph()
            if not g.has_node(name):
                return {"error": f"entity {name!r} not found"}
            data = g.nodes[name]
            relations = []
            for u, v, edata in g.out_edges(name, data=True):
                relations.append({"direction": "out", "relation": edata.get("relation"), "other": v, "confidence": edata.get("confidence"), "mention_count": edata.get("mention_count"), "evidence": (edata.get("evidences") or [None])[0]})
            for u, v, edata in g.in_edges(name, data=True):
                relations.append({"direction": "in", "relation": edata.get("relation"), "other": u, "confidence": edata.get("confidence"), "mention_count": edata.get("mention_count"), "evidence": (edata.get("evidences") or [None])[0]})
            return {"name": name, "type": data.get("type"), "aliases": data.get("aliases"), "mention_count": data.get("mention_count"), "relations": relations}
        out_rows = _run_graph_query(
            gcfg["provider"], gcfg,
            "MATCH (n {name: $name, graph: $graph_name}) OPTIONAL MATCH (n)-[r]->(m) "
            "RETURN n.type AS type, n.aliases AS aliases, n.mention_count AS mention_count, "
            "'out' AS direction, r.relation AS relation, m.name AS other, r.confidence AS confidence, "
            "r.mention_count AS rel_mention_count, r.evidences[0] AS evidence",
            {"name": name, "graph_name": gcfg["graph_name"]},
        )
        in_rows = _run_graph_query(
            gcfg["provider"], gcfg,
            "MATCH (n {name: $name, graph: $graph_name})<-[r]-(m) "
            "RETURN 'in' AS direction, r.relation AS relation, m.name AS other, r.confidence AS confidence, "
            "r.mention_count AS rel_mention_count, r.evidences[0] AS evidence",
            {"name": name, "graph_name": gcfg["graph_name"]},
        )
        if not out_rows:
            return {"error": f"entity {name!r} not found"}
        base = out_rows[0]
        relations = [
            {"direction": r["direction"], "relation": r["relation"], "other": r["other"], "confidence": r["confidence"], "mention_count": r.get("rel_mention_count"), "evidence": r["evidence"]}
            for r in out_rows + in_rows if r.get("other") is not None
        ]
        return {"name": name, "type": base["type"], "aliases": base["aliases"], "mention_count": base["mention_count"], "relations": relations}

    def get_neighbors(self, name: str, hops: int = 1, relation: Optional[str] = None) -> dict:
        gcfg = self._graph_cfg()
        hops = max(1, min(int(hops), 3))
        if gcfg["provider"] == "sql_tables":
            nodes_tbl, edges_tbl = self._sql_tables(gcfg)
            rows = _run_sql_tables_query(gcfg, _build_sql_neighbors_query(gcfg["sql_dialect"], edges_tbl, name, hops, relation))
            names = [r["name"] for r in rows]
            types: Dict[str, Any] = {}
            if names:
                names_sql = ", ".join(_sql_str(n) for n in names)
                types = {r["id"]: r["type"] for r in _run_sql_tables_query(gcfg, f"SELECT id, type FROM {nodes_tbl} WHERE id IN ({names_sql})")}
            results = [{"name": r["name"], "type": types.get(r["name"]), "distance": int(r["depth"])} for r in rows]
            return {"neighbors": results[: self.max_results]}
        if gcfg["provider"] == "networkx":
            g = self._nx_graph()
            if not g.has_node(name):
                return {"error": f"entity {name!r} not found"}
            undirected = g.to_undirected(as_view=True)
            visited = {name: 0}
            frontier = [name]
            for d in range(1, hops + 1):
                next_frontier = []
                for node in frontier:
                    for nbr in undirected.neighbors(node):
                        if nbr in visited:
                            continue
                        if relation is not None:
                            edges_between = list(g.get_edge_data(node, nbr, default={}).values()) + list(g.get_edge_data(nbr, node, default={}).values())
                            if not any(e.get("relation") == relation for e in edges_between):
                                continue
                        visited[nbr] = d
                        next_frontier.append(nbr)
                frontier = next_frontier
            results = [{"name": n, "type": g.nodes[n].get("type"), "distance": d} for n, d in sorted(visited.items(), key=lambda kv: kv[1]) if n != name]
            return {"neighbors": results[: self.max_results]}
        query = (
            f"MATCH p=(n {{name: $name, graph: $graph_name}})-[r*1..{hops}]-(m) "
            "WHERE $relation IS NULL OR all(rel IN r WHERE rel.relation = $relation) "
            "RETURN DISTINCT m.name AS name, m.type AS type, length(p) AS distance ORDER BY distance LIMIT $limit"
        )
        records = _run_graph_query(gcfg["provider"], gcfg, query, {"name": name, "graph_name": gcfg["graph_name"], "relation": relation, "limit": self.max_results})
        return {"neighbors": records}

    def find_path(self, source: str, target: str, max_hops: int = 4) -> dict:
        gcfg = self._graph_cfg()
        max_hops = max(1, min(int(max_hops), 6))
        if gcfg["provider"] == "sql_tables":
            _, edges_tbl = self._sql_tables(gcfg)
            rows = _run_sql_tables_query(gcfg, _build_sql_path_query(gcfg["sql_dialect"], edges_tbl, source, target, max_hops))
            if not rows:
                return {"path": None}
            return {"path": _maybe_json_list(rows[0]["path"]), "relations": _maybe_json_list(rows[0]["rels"])}
        if gcfg["provider"] == "networkx":
            import networkx as nx
            g = self._nx_graph()
            if not g.has_node(source) or not g.has_node(target):
                return {"error": "source or target entity not found"}
            undirected = g.to_undirected(as_view=True)
            try:
                path = nx.shortest_path(undirected, source, target)
            except nx.NetworkXNoPath:
                return {"path": None}
            if len(path) - 1 > max_hops:
                return {"path": None}
            relations = []
            for u, v in zip(path, path[1:]):
                edges_between = list(g.get_edge_data(u, v, default={}).values()) + list(g.get_edge_data(v, u, default={}).values())
                relations.append(edges_between[0].get("relation") if edges_between else None)
            return {"path": path, "relations": relations}
        query = (
            f"MATCH p=shortestPath((a {{name: $source, graph: $graph_name}})-[*1..{max_hops}]-(b {{name: $target, graph: $graph_name}})) "
            "RETURN [n IN nodes(p) | n.name] AS path, [r IN relationships(p) | r.relation] AS relations"
        )
        records = _run_graph_query(gcfg["provider"], gcfg, query, {"source": source, "target": target, "graph_name": gcfg["graph_name"]})
        if not records:
            return {"path": None}
        return {"path": records[0]["path"], "relations": records[0]["relations"]}

    # ─── hybrid-specific primitives ───────────────────────────────────────

    def get_chunks_for_entity(self, name: str) -> dict:
        gcfg = self._graph_cfg()
        if gcfg["provider"] == "sql_tables":
            nodes_tbl, edges_tbl = self._sql_tables(gcfg)
            if not _run_sql_tables_query(gcfg, f"SELECT id FROM {nodes_tbl} WHERE id = {_sql_str(name)}"):
                return {"error": f"entity {name!r} not found"}
            rows = _run_sql_tables_query(gcfg, _build_sql_chunks_for_entity_query(edges_tbl, name))
            chunk_ids: set = set()
            for r in rows:
                chunk_ids.update(_maybe_json_list(r["chunk_ids"]))
            return {"entity": name, "chunk_ids": sorted(chunk_ids)[: self.max_results]}
        if gcfg["provider"] == "networkx":
            g = self._nx_graph()
            if not g.has_node(name):
                return {"error": f"entity {name!r} not found"}
            chunk_ids: set = set()
            for _, _, data in g.out_edges(name, data=True):
                chunk_ids.update(data.get("chunk_ids") or [])
            for _, _, data in g.in_edges(name, data=True):
                chunk_ids.update(data.get("chunk_ids") or [])
            return {"entity": name, "chunk_ids": sorted(chunk_ids)[: self.max_results]}
        records = _run_graph_query(
            gcfg["provider"], gcfg,
            "MATCH (n {name: $name, graph: $graph_name})-[r]-() RETURN DISTINCT r.chunk_ids AS chunk_ids",
            {"name": name, "graph_name": gcfg["graph_name"]},
        )
        chunk_ids = sorted({cid for row in records for cid in (row.get("chunk_ids") or [])})
        return {"entity": name, "chunk_ids": chunk_ids[: self.max_results]}

    def get_graph_context(self, chunk_id: str) -> dict:
        gcfg = self._graph_cfg()
        if gcfg["provider"] == "sql_tables":
            _, edges_tbl = self._sql_tables(gcfg)
            rows = _run_sql_tables_query(gcfg, _build_sql_graph_context_query(gcfg["sql_dialect"], edges_tbl, chunk_id))
            entities = sorted({r["subject"] for r in rows} | {r["object"] for r in rows})
            return {"chunk_id": chunk_id, "entities": entities, "triples": rows}
        if gcfg["provider"] == "networkx":
            g = self._nx_graph()
            triples = []
            for u, v, data in g.edges(data=True):
                for i, cid in enumerate(data.get("chunk_ids") or []):
                    if cid == chunk_id:
                        triples.append({
                            "subject": u, "relation": data.get("relation"), "object": v,
                            "confidence": data.get("confidence"), "evidence": (data.get("evidences") or [None])[i],
                        })
            entities = sorted({t["subject"] for t in triples} | {t["object"] for t in triples})
            return {"chunk_id": chunk_id, "entities": entities, "triples": triples}
        records = _run_graph_query(
            gcfg["provider"], gcfg,
            "MATCH (a)-[r {graph: $graph_name}]->(b) WHERE $chunk_id IN r.chunk_ids "
            "WITH a, b, r, [i IN range(0, size(r.chunk_ids) - 1) WHERE r.chunk_ids[i] = $chunk_id][0] AS idx "
            "RETURN a.name AS subject, b.name AS object, r.relation AS relation, r.confidence AS confidence, r.evidences[idx] AS evidence",
            {"chunk_id": chunk_id, "graph_name": gcfg["graph_name"]},
        )
        entities = sorted({r["subject"] for r in records} | {r["object"] for r in records})
        return {"chunk_id": chunk_id, "entities": entities, "triples": records}

    # ─── composed hybrid search ────────────────────────────────────────

    def search(self, query: str, top_k: int = 5, hops: int = 1, mode: Literal["hybrid", "vector", "graph"] = "hybrid") -> dict:
        if mode not in ("hybrid", "vector", "graph"):
            raise ValueError(f"mode must be 'hybrid', 'vector', or 'graph', got {mode!r}")
        passages: Dict[str, Dict[str, Any]] = {}
        triples: List[Dict[str, Any]] = []
        seen_triple_keys: set = set()

        def _passage(cid: str) -> dict:
            return passages.setdefault(cid, {"chunk_id": cid, "text": None, "score": None, "source": None, "doc_id": None, "paths": []})

        def _add_triples_for_chunk(cid: str, path_label: str) -> None:
            ctx = self.get_graph_context(cid)
            for t in ctx.get("triples", []):
                key = (t["subject"], t["relation"], t["object"], cid)
                if key not in seen_triple_keys:
                    seen_triple_keys.add(key)
                    triples.append({**t, "chunk_id": cid, "path": path_label})

        vector_hits: List[Dict[str, Any]] = []
        eligible_count: Optional[int] = None
        if mode in ("vector", "hybrid"):
            vector_hits, eligible_count = self._vector_search(query, top_k)
            for h in vector_hits:
                rec = _passage(h["chunk_id"])
                for field in ("text", "score", "source", "doc_id"):
                    if h.get(field) is not None:
                        rec[field] = h[field]
                rec["paths"].append("vector")

        if mode in ("graph", "hybrid"):
            seed_entities = {e["name"] for e in self.search_entities(query).get("entities", [])}
            for cid in list(passages):
                ctx = self.get_graph_context(cid)
                seed_entities.update(ctx.get("entities", []))

            visited: set = set()
            frontier = set(seed_entities)
            for d in range(hops + 1):
                next_frontier: set = set()
                for name in frontier:
                    if name in visited:
                        continue
                    visited.add(name)
                    for cid in self.get_chunks_for_entity(name).get("chunk_ids", []):
                        rec = _passage(cid)
                        label = f"graph:hop_{d}:{name}"
                        if label not in rec["paths"]:
                            rec["paths"].append(label)
                        _add_triples_for_chunk(cid, f"graph:hop_{d}")
                    if d < hops:
                        for nb in self.get_neighbors(name, hops=1).get("neighbors", []):
                            if nb["name"] not in visited:
                                next_frontier.add(nb["name"])
                frontier = next_frontier

        if mode == "hybrid":
            for cid in list(passages):
                if not any(p.startswith("graph") for p in passages[cid]["paths"]):
                    _add_triples_for_chunk(cid, "vector")

        for rec in passages.values():
            if rec["text"] is None:
                rec["text"] = self._backfill_text_from_chromadb(rec["chunk_id"])

        ordered = sorted(passages.values(), key=lambda r: (r["score"] is None, -(r["score"] or 0)))
        result: Dict[str, Any] = {"passages": ordered[: self.max_results], "triples": triples[: self.max_results]}
        if self.receipts_db_path and vector_hits:
            items = [
                {
                    "item_id": h["chunk_id"], "source_asset_key": h.get("source"),
                    "source_version": h.get("source_version"), "source_code_version": h.get("source_code_version"),
                    "rank": rank, "score": h.get("score"),
                }
                for rank, h in enumerate(vector_hits)
            ]
            result["receipt_id"] = _ReceiptWriter(self.receipts_db_path).record_retrieval(
                context_name=self._vector_cfg()["collection_name"],
                items=items,
                eligible_count=eligible_count,
                query_hash=hashlib.sha256(query.encode("utf-8")).hexdigest(),
            )
        return result

    def record_feedback(self, receipt_id: str, signal: str, user_id: Optional[str] = None) -> dict:
        """Records an outcome (thumbs_up/thumbs_down/accepted/edited/escalated/reopened/
        closed_won/closed_lost/reformulated -- see OUTCOME_SIGNALS) against a prior search()
        call's receipt_id. `user_id`, when given, becomes the outcome's `outcome_ref`: a later
        call with the SAME user_id for the SAME receipt supersedes the earlier one when
        scoring, so a user changing their mind isn't double-counted."""
        if not self.receipts_db_path:
            raise ValueError("record_feedback: receipts_db_path is not configured on this server.")
        _ReceiptWriter(self.receipts_db_path).record_outcome(receipt_id, signal=signal, outcome_ref=user_id)
        return {"recorded": True}

    # ─── corpus-overlap check (build-time / inspection-asset metadata) ──

    def check_overlap(self) -> dict:
        """Jaccard overlap between the vector store's chunk ids and the graph's chunk ids --
        only computed when both sides expose a cheap full-id listing (chromadb for the vector
        side; networkx or sql_tables for the graph side); otherwise returns overlap_fraction:
        None with a note explaining why."""
        vcfg, gcfg = self._vector_cfg(), self._graph_cfg()
        if vcfg["provider"] != "chromadb" or gcfg["provider"] not in ("networkx", "sql_tables"):
            return {
                "overlap_fraction": None, "vector_chunk_count": None, "graph_chunk_count": None,
                "overlap_note": (
                    f"overlap check needs a cheap full-id listing on both sides; "
                    f"vector_store.provider={vcfg['provider']!r} + graph.provider={gcfg['provider']!r} "
                    "is not a supported combination (chromadb + networkx|sql_tables)."
                ),
            }
        try:
            import chromadb
            client = chromadb.PersistentClient(path=vcfg["connection_string"] or "./chroma_db")
            collection = client.get_collection(name=vcfg["collection_name"])
            vector_ids = set(collection.get(include=[])["ids"])
        except Exception as e:  # noqa: BLE001
            return {"overlap_fraction": None, "vector_chunk_count": None, "graph_chunk_count": None, "overlap_note": f"could not read vector store: {type(e).__name__}: {e}"}
        graph_ids: set = set()
        if gcfg["provider"] == "sql_tables":
            _, edges_tbl = self._sql_tables(gcfg)
            for r in _run_sql_tables_query(gcfg, f"SELECT chunk_ids FROM {edges_tbl}"):
                graph_ids.update(_maybe_json_list(r["chunk_ids"]))
        else:
            g = self._nx_graph()
            for _, _, data in g.edges(data=True):
                graph_ids.update(data.get("chunk_ids") or [])
        union = vector_ids | graph_ids
        overlap = len(vector_ids & graph_ids) / len(union) if union else None
        return {
            "overlap_fraction": overlap, "vector_chunk_count": len(vector_ids), "graph_chunk_count": len(graph_ids),
            "overlap_note": "Jaccard overlap (|intersection| / |union|) of chunk ids present in both stores.",
        }


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined per this repo's convention, same
    field shape as knowledge_search_server's/knowledge_graph_search_server's own copy
    (checked in tests/test_drift.py)."""

    name: str
    type: Literal["http", "sse", "stdio"] = "stdio"
    command: Optional[List[str]] = None
    url: Optional[str] = None
    env: Optional[Dict[str, str]] = None
    headers: Optional[Dict[str, str]] = None
    headers_env: Optional[Dict[str, str]] = None
    live_env_passthrough: Optional[List[str]] = None

    def model_dump(self, *args, **kwargs):
        dumped = super().model_dump(*args, **kwargs)
        passthrough = dumped.pop("live_env_passthrough", None)
        if dumped.get("type") == "stdio" and passthrough:
            env = dict(dumped.get("env") or {})
            for var_name in passthrough:
                value = os.environ.get(var_name)
                if value:
                    env[var_name] = value
            dumped["env"] = env
        return dumped


def _this_module_name() -> str:
    if __spec__ is not None:
        return __spec__.name
    raise RuntimeError(
        "hybrid_knowledge_search_server's stdio MCP server must be run as "
        "`python -m <dotted.path.to.this.module>`, not `python component.py` directly -- "
        "the spawned server's own command needs a real importable module name "
        "(available via __spec__.name). See README.md."
    )


_VECTOR_ENV_KEYS = {
    "provider": "HYBRID_KG_SEARCH_VECTOR_PROVIDER", "connection_string": "HYBRID_KG_SEARCH_VECTOR_CONNECTION_STRING",
    "collection_name": "HYBRID_KG_SEARCH_VECTOR_COLLECTION_NAME", "api_key_env_var": "HYBRID_KG_SEARCH_VECTOR_API_KEY_ENV_VAR",
    "embed_provider": "HYBRID_KG_SEARCH_EMBED_PROVIDER", "embed_model": "HYBRID_KG_SEARCH_EMBED_MODEL",
    "embed_api_key_env_var": "HYBRID_KG_SEARCH_EMBED_API_KEY_ENV_VAR", "text_metadata_key": "HYBRID_KG_SEARCH_TEXT_METADATA_KEY",
    "vector_bucket_name": "HYBRID_KG_SEARCH_VECTOR_BUCKET_NAME", "aws_region": "HYBRID_KG_SEARCH_AWS_REGION",
    "demo_mode": "HYBRID_KG_SEARCH_DEMO_MODE",
}
_GRAPH_ENV_KEYS = {
    "provider": "HYBRID_KG_SEARCH_GRAPH_PROVIDER", "path": "HYBRID_KG_SEARCH_GRAPH_PATH", "uri": "HYBRID_KG_SEARCH_GRAPH_URI",
    "username_env_var": "HYBRID_KG_SEARCH_GRAPH_USERNAME_ENV_VAR", "password_env_var": "HYBRID_KG_SEARCH_GRAPH_PASSWORD_ENV_VAR",
    "database": "HYBRID_KG_SEARCH_GRAPH_DATABASE", "host": "HYBRID_KG_SEARCH_GRAPH_HOST", "port": "HYBRID_KG_SEARCH_GRAPH_PORT",
    "graph_name": "HYBRID_KG_SEARCH_GRAPH_NAME",
    "sql_dialect": "HYBRID_KG_SEARCH_GRAPH_SQL_DIALECT", "output_table": "HYBRID_KG_SEARCH_GRAPH_OUTPUT_TABLE",
    "snowflake_account_env_var": "HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_ACCOUNT_ENV_VAR",
    "snowflake_user_env_var": "HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_USER_ENV_VAR",
    "snowflake_password_env_var": "HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_PASSWORD_ENV_VAR",
    "snowflake_database": "HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_DATABASE", "snowflake_schema": "HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_SCHEMA",
    "snowflake_warehouse": "HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_WAREHOUSE",
    "databricks_host_env_var": "HYBRID_KG_SEARCH_GRAPH_DATABRICKS_HOST_ENV_VAR",
    "databricks_token_env_var": "HYBRID_KG_SEARCH_GRAPH_DATABRICKS_TOKEN_ENV_VAR",
    "databricks_http_path": "HYBRID_KG_SEARCH_GRAPH_DATABRICKS_HTTP_PATH",
    "bigquery_project_id": "HYBRID_KG_SEARCH_GRAPH_BIGQUERY_PROJECT_ID",
}


class HybridKnowledgeSearchServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a `HybridKnowledgeSearchResource` (default key `hybrid_knowledge_search`)
    and, always, a small inspection asset reporting its settings + the corpus-overlap check.
    Run THIS FILE as the stdio MCP server for agentic_pipeline (`python -m
    dagster_community_components.assets.ai.hybrid_knowledge_search_server.component`) --
    see README.md."""

    resource_key: str = Field(default="hybrid_knowledge_search", description="Dagster resource key the in-process HybridKnowledgeSearchResource is registered under.")
    mcp_server_resource_key: Optional[str] = Field(default=None, description="If set, ALSO registers a stdio MCP-server-shaped resource under this key.")
    vector_store: Dict[str, Any] = Field(default_factory=dict, description="Same fields as knowledge_search_server's KnowledgeSearchResource (provider, connection_string, collection_name [required], api_key_env_var, embed_provider, embed_model, embed_api_key_env_var, text_metadata_key, vector_bucket_name, aws_region, demo_mode).")
    graph: Dict[str, Any] = Field(default_factory=dict, description="Same fields as knowledge_graph_search_server's KnowledgeGraphSearchResource (provider, path, uri, username_env_var, password_env_var, database, host, port, graph_name).")
    knowledge_base: Optional[str] = Field(default=None, description="Optional UI-linking label.")
    max_results: int = Field(default=50, description="Cap on rows returned by any list-returning method.")
    receipts_db_path: Optional[str] = Field(
        default=None,
        description=(
            "SQLite file path. When set, search() records a retrieval receipt per call with a "
            "vector component (mode='vector' or 'hybrid') and a new record_feedback(receipt_id, "
            "signal, user_id?) tool is exposed. Quarantined chunks are always dropped from "
            "vector results, whether or not this is set. See HybridKnowledgeSearchResource."
            "receipts_db_path's own docs for why this is a plain file path, not a Dagster "
            "resource_key -- the stdio MCP subprocess that answers searches has no Dagster "
            "context to resolve one from."
        ),
    )
    inspection_asset_name: str = Field(default="hybrid_knowledge_search_tools", description="Asset key for the inspection asset reporting this server's configured settings and the corpus-overlap check.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, Any]] = Field(default=None, description="Extra asset tags, applied to the inspection asset.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners",), csv=(), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Hybrid Knowledge Search Server", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        if not self.vector_store.get("collection_name"):
            raise ValueError("HybridKnowledgeSearchServerComponent: vector_store.collection_name is required.")
        if self.graph.get("provider", "networkx") == "neo4j" and not self.graph.get("uri"):
            raise ValueError("HybridKnowledgeSearchServerComponent: graph.provider='neo4j' requires graph.uri.")
        if self.graph.get("provider") == "sql_tables":
            if not self.graph.get("output_table"):
                raise ValueError("HybridKnowledgeSearchServerComponent: graph.provider='sql_tables' requires graph.output_table.")
            if self.graph.get("sql_dialect") not in ("snowflake_cortex", "bigquery", "databricks"):
                raise ValueError("HybridKnowledgeSearchServerComponent: graph.provider='sql_tables' requires graph.sql_dialect to be one of 'snowflake_cortex', 'bigquery', 'databricks'.")
        resource = HybridKnowledgeSearchResource(
            vector_store=_drop_none(self.vector_store), graph=_drop_none(self.graph),
            knowledge_base=self.knowledge_base, max_results=self.max_results,
            receipts_db_path=self.receipts_db_path,
        )
        resources: Dict[str, Any] = {self.resource_key: resource}

        if self.mcp_server_resource_key:
            mcp_env: Dict[str, str] = {"HYBRID_KG_SEARCH_MAX_RESULTS": str(self.max_results)}
            if self.receipts_db_path:
                mcp_env["HYBRID_KG_SEARCH_RECEIPTS_DB_PATH"] = self.receipts_db_path
            for field, env_key in _VECTOR_ENV_KEYS.items():
                v = self.vector_store.get(field)
                if v is not None:
                    mcp_env[env_key] = str(v)
            for field, env_key in _GRAPH_ENV_KEYS.items():
                v = self.graph.get(field)
                if v is not None:
                    mcp_env[env_key] = str(v)
            passthrough = [v for v in (self.vector_store.get("api_key_env_var"), self.vector_store.get("embed_api_key_env_var"),
                                       self.graph.get("username_env_var"), self.graph.get("password_env_var"),
                                       self.graph.get("snowflake_account_env_var"), self.graph.get("snowflake_user_env_var"),
                                       self.graph.get("snowflake_password_env_var"), self.graph.get("databricks_host_env_var"),
                                       self.graph.get("databricks_token_env_var")) if v]
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio",
                command=[sys.executable, "-m", _this_module_name()], env=mcp_env, live_env_passthrough=passthrough,
            )

        tags = dict(self.asset_tags or {})
        tags.setdefault("dagster-community/hybrid-knowledge-search", self.knowledge_base or self.resource_key)

        vector_store_cfg, graph_cfg, knowledge_base = dict(self.vector_store), dict(self.graph), self.knowledge_base
        receipts_db_path = self.receipts_db_path

        @asset(
            key=AssetKey.from_user_string(self.inspection_asset_name),
            group_name=self.group_name,
            description=self.description or "Reports the hybrid-knowledge-search server's configured settings and corpus-overlap check.",
            owners=self.owners or [],
            tags=tags,
        )
        def _inspection_asset(context: AssetExecutionContext):
            overlap = resource.check_overlap()
            tool_names = [
                "search", "search_entities", "get_entity", "get_neighbors", "find_path",
                "get_chunks_for_entity", "get_graph_context",
            ]
            tool_descriptions = {
                "search": "Hybrid (or vector-only / graph-only) search: passages + supporting triples, merged and de-duplicated by chunk_id.",
                "search_entities": "Fuzzy name/alias search over graph entities.",
                "get_entity": "Exact-name entity lookup: attributes, relations, evidence.",
                "get_neighbors": "Entities within N hops (<=3) of a given entity.",
                "find_path": "Shortest path (<=6 hops) between two entities.",
                "get_chunks_for_entity": "Every chunk_id an entity appears in.",
                "get_graph_context": "Entities + triples associated with a given chunk_id.",
            }
            if receipts_db_path:
                tool_names.append("record_feedback")
                tool_descriptions["record_feedback"] = (
                    "Record an outcome (thumbs_up/thumbs_down/accepted/edited/escalated/reopened/"
                    "closed_won/closed_lost/reformulated) against a prior search's receipt_id, "
                    "optionally scoped by user_id."
                )
            md = {
                "vector_provider": MetadataValue.text(vector_store_cfg.get("provider", "chromadb")),
                "vector_collection_name": MetadataValue.text(vector_store_cfg.get("collection_name") or ""),
                "graph_provider": MetadataValue.text(graph_cfg.get("provider", "networkx")),
                "graph_name": MetadataValue.text(graph_cfg.get("graph_name", "knowledge_graph")),
                "overlap_with_knowledge_base": (MetadataValue.float(overlap["overlap_fraction"]) if overlap["overlap_fraction"] is not None else MetadataValue.null()),
                "overlap_note": MetadataValue.text(overlap["overlap_note"]),
                "chunk_count_vector": (MetadataValue.int(overlap["vector_chunk_count"]) if overlap["vector_chunk_count"] is not None else MetadataValue.null()),
                "chunk_count_graph": (MetadataValue.int(overlap["graph_chunk_count"]) if overlap["graph_chunk_count"] is not None else MetadataValue.null()),
                "tool_names": MetadataValue.json(tool_names),
                "tool_descriptions": MetadataValue.json(tool_descriptions),
            }
            if knowledge_base:
                md["knowledge_base"] = MetadataValue.text(knowledge_base)
            if receipts_db_path:
                md["receipts_db_path"] = MetadataValue.text(receipts_db_path)
                if os.path.exists(receipts_db_path):
                    counts = _ReceiptWriter(receipts_db_path).counts()
                    md["retrievals_recorded"] = MetadataValue.int(counts["retrievals"])
                    md["outcomes_recorded"] = MetadataValue.int(counts["outcomes"])
                else:
                    md["retrievals_recorded"] = MetadataValue.int(0)
                    md["outcomes_recorded"] = MetadataValue.int(0)
            context.add_output_metadata(md)

        return Definitions(assets=[_inspection_asset], resources=resources)


# ── stdio MCP server entrypoint:  python -m <dotted.path.to.this.module>  ───────────────
def build_mcp_server(resource: HybridKnowledgeSearchResource, server_name: str = "hybrid-knowledge-search"):
    try:
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server(server_name)

    @mcp.tool()
    def search(query: str, top_k: int = 5, hops: int = 1, mode: str = "hybrid") -> dict:
        """Hybrid (or vector-only / graph-only, via `mode`) search over the corpus. Returns
        {"passages": [{chunk_id, text, score, source, doc_id, paths}, ...], "triples": [...]}."""
        return _safe(resource.search, query, top_k, hops, mode)

    @mcp.tool()
    def search_entities(query: str, type: Optional[str] = None) -> dict:
        """Fuzzy name/alias search over graph entities."""
        return _safe(resource.search_entities, query, type)

    @mcp.tool()
    def get_entity(name: str) -> dict:
        """Exact-name entity lookup: attributes, relations, evidence."""
        return _safe(resource.get_entity, name)

    @mcp.tool()
    def get_neighbors(name: str, hops: int = 1, relation: Optional[str] = None) -> dict:
        """Entities within `hops` (capped at 3) of `name`."""
        return _safe(resource.get_neighbors, name, hops, relation)

    @mcp.tool()
    def find_path(source: str, target: str, max_hops: int = 4) -> dict:
        """Shortest path (capped at 6 hops) between two entities."""
        return _safe(resource.find_path, source, target, max_hops)

    @mcp.tool()
    def get_chunks_for_entity(name: str) -> dict:
        """Every chunk_id a given entity appears in."""
        return _safe(resource.get_chunks_for_entity, name)

    @mcp.tool()
    def get_graph_context(chunk_id: str) -> dict:
        """Entities + triples associated with a given chunk_id."""
        return _safe(resource.get_graph_context, chunk_id)

    if resource.receipts_db_path:
        @mcp.tool()
        def record_feedback(receipt_id: str, signal: str, user_id: Optional[str] = None) -> dict:
            """Record an outcome against a prior search() call's receipt_id: signal is one of
            thumbs_up, thumbs_down, accepted, edited, escalated, reopened, closed_won,
            closed_lost, reformulated. user_id (optional) scopes the outcome so a later call
            with the same user_id for the same receipt supersedes the earlier one instead of
            double-counting a changed mind."""
            return _safe(resource.record_feedback, receipt_id, signal, user_id)

    return mcp


def _safe(fn, *args, **kwargs) -> dict:
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
        return {"error": f"{type(e).__name__}: {e}"}


def main(argv: Optional[List[str]] = None) -> None:
    """Env (vector_store.*): HYBRID_KG_SEARCH_VECTOR_PROVIDER (default chromadb),
    _VECTOR_CONNECTION_STRING, _VECTOR_COLLECTION_NAME (required), _VECTOR_API_KEY_ENV_VAR,
    _EMBED_PROVIDER (default sentence_transformers), _EMBED_MODEL, _EMBED_API_KEY_ENV_VAR,
    _TEXT_METADATA_KEY (default chunk_text), _VECTOR_BUCKET_NAME, _AWS_REGION, _DEMO_MODE.
    Env (graph.*): HYBRID_KG_SEARCH_GRAPH_PROVIDER (default networkx), _GRAPH_PATH, _GRAPH_URI,
    _GRAPH_USERNAME_ENV_VAR, _GRAPH_PASSWORD_ENV_VAR, _GRAPH_DATABASE, _GRAPH_HOST, _GRAPH_PORT,
    _GRAPH_NAME. provider='sql_tables': _GRAPH_SQL_DIALECT, _GRAPH_OUTPUT_TABLE,
    _GRAPH_SNOWFLAKE_ACCOUNT_ENV_VAR/_USER_ENV_VAR/_PASSWORD_ENV_VAR/_DATABASE/_SCHEMA/_WAREHOUSE,
    _GRAPH_DATABRICKS_HOST_ENV_VAR/_TOKEN_ENV_VAR/_HTTP_PATH, _GRAPH_BIGQUERY_PROJECT_ID.
    HYBRID_KG_SEARCH_MAX_RESULTS (default 50).
    HYBRID_KG_SEARCH_RECEIPTS_DB_PATH (optional -- enables receipt recording + record_feedback)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")
    collection_name = os.environ.get("HYBRID_KG_SEARCH_VECTOR_COLLECTION_NAME")
    if not collection_name:
        raise SystemExit("HYBRID_KG_SEARCH_VECTOR_COLLECTION_NAME must be set")
    vector_store = {
        "provider": os.environ.get("HYBRID_KG_SEARCH_VECTOR_PROVIDER", "chromadb"),
        "connection_string": os.environ.get("HYBRID_KG_SEARCH_VECTOR_CONNECTION_STRING"),
        "collection_name": collection_name,
        "api_key_env_var": os.environ.get("HYBRID_KG_SEARCH_VECTOR_API_KEY_ENV_VAR"),
        "embed_provider": os.environ.get("HYBRID_KG_SEARCH_EMBED_PROVIDER", "sentence_transformers"),
        "embed_model": os.environ.get("HYBRID_KG_SEARCH_EMBED_MODEL"),
        "embed_api_key_env_var": os.environ.get("HYBRID_KG_SEARCH_EMBED_API_KEY_ENV_VAR"),
        "text_metadata_key": os.environ.get("HYBRID_KG_SEARCH_TEXT_METADATA_KEY", "chunk_text"),
        "vector_bucket_name": os.environ.get("HYBRID_KG_SEARCH_VECTOR_BUCKET_NAME"),
        "aws_region": os.environ.get("HYBRID_KG_SEARCH_AWS_REGION"),
        "demo_mode": os.environ.get("HYBRID_KG_SEARCH_DEMO_MODE", "1") != "0",
    }
    graph = {
        "provider": os.environ.get("HYBRID_KG_SEARCH_GRAPH_PROVIDER", "networkx"),
        "path": os.environ.get("HYBRID_KG_SEARCH_GRAPH_PATH"),
        "uri": os.environ.get("HYBRID_KG_SEARCH_GRAPH_URI"),
        "username_env_var": os.environ.get("HYBRID_KG_SEARCH_GRAPH_USERNAME_ENV_VAR"),
        "password_env_var": os.environ.get("HYBRID_KG_SEARCH_GRAPH_PASSWORD_ENV_VAR"),
        "database": os.environ.get("HYBRID_KG_SEARCH_GRAPH_DATABASE", "neo4j"),
        "host": os.environ.get("HYBRID_KG_SEARCH_GRAPH_HOST", "localhost"),
        "port": int(os.environ.get("HYBRID_KG_SEARCH_GRAPH_PORT", "6379")),
        "graph_name": os.environ.get("HYBRID_KG_SEARCH_GRAPH_NAME", "knowledge_graph"),
        "sql_dialect": os.environ.get("HYBRID_KG_SEARCH_GRAPH_SQL_DIALECT"),
        "output_table": os.environ.get("HYBRID_KG_SEARCH_GRAPH_OUTPUT_TABLE"),
        "snowflake_account_env_var": os.environ.get("HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_ACCOUNT_ENV_VAR", "SNOWFLAKE_ACCOUNT"),
        "snowflake_user_env_var": os.environ.get("HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_USER_ENV_VAR", "SNOWFLAKE_USER"),
        "snowflake_password_env_var": os.environ.get("HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_PASSWORD_ENV_VAR", "SNOWFLAKE_PASSWORD"),
        "snowflake_database": os.environ.get("HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_DATABASE"),
        "snowflake_schema": os.environ.get("HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_SCHEMA", "PUBLIC"),
        "snowflake_warehouse": os.environ.get("HYBRID_KG_SEARCH_GRAPH_SNOWFLAKE_WAREHOUSE"),
        "databricks_host_env_var": os.environ.get("HYBRID_KG_SEARCH_GRAPH_DATABRICKS_HOST_ENV_VAR", "DATABRICKS_HOST"),
        "databricks_token_env_var": os.environ.get("HYBRID_KG_SEARCH_GRAPH_DATABRICKS_TOKEN_ENV_VAR", "DATABRICKS_TOKEN"),
        "databricks_http_path": os.environ.get("HYBRID_KG_SEARCH_GRAPH_DATABRICKS_HTTP_PATH"),
        "bigquery_project_id": os.environ.get("HYBRID_KG_SEARCH_GRAPH_BIGQUERY_PROJECT_ID"),
    }
    resource = HybridKnowledgeSearchResource(
        vector_store=_drop_none(vector_store), graph=_drop_none(graph),
        max_results=int(os.environ.get("HYBRID_KG_SEARCH_MAX_RESULTS", "50")),
        receipts_db_path=os.environ.get("HYBRID_KG_SEARCH_RECEIPTS_DB_PATH"),
    )
    build_mcp_server(resource).run(transport="stdio")


if __name__ == "__main__":
    main()
