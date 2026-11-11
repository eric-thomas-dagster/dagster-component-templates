"""`KnowledgeSearch` -- the gateway-side counterpart to `vector_store_writer`/
`context_engineering_pipeline`'s `write_vector_store` step and `ReceiptMiddleware`
(`fastmcp_receipt_middleware.py`). A gateway process (e.g. a FastMCP server) wires these
three together: `KnowledgeSearch.search()` embeds a query with the SAME provider/model the
index was built with, queries the vector store, drops quarantined chunks, optionally
reorders by learned item priors (`rerank`/`rerank_with_priors`), and returns both the
chunks a tool can answer with AND a `_receipt` dict shaped for `ContextReceiptStore.
record_retrieval(**receipt)` -- which `ReceiptMiddleware` calls automatically when a tool's
result carries a `_receipt` key, so a tool implementation never has to call the receipt
store directly (the "tools stay receipt-agnostic" design `fastmcp_receipt_middleware.py`
documents).

`_receipt` never contains the raw query text or any chunk's text/content -- only item IDs,
source-asset/version references, rank, and score (the same constraint `record_retrieval`
itself enforces). The query is reduced to `query_hash` (sha256), one-way.

Verified vector-store metadata field names this relies on (same fields `write_vector_store`
stamps when `stamp_provenance`/`receipts` are configured, confirmed against
`assets/ai/context_engineering_pipeline/component.py` and `assets/ai/vector_store_writer/
component.py` earlier this session): `chunk_id` or the row's provider-native id, `source_
asset_key`, `source_version`, `source_code_version`, and `feedback_quarantined`/
`feedback_prior` (only present once a `receipts` policy has run at least once -- absent on
a fresh index, treated as not-quarantined/no-prior).
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Mapping, Optional, Sequence

try:
    from .component import rerank as rerank_with_priors
except ImportError:
    # Loaded standalone (e.g. via importlib.util.spec_from_file_location, this repo's test
    # convention) rather than as part of the installed package -- no relative-import context
    # to resolve `.component` against. Load component.py the same bare way instead.
    import importlib.util as _importlib_util
    from pathlib import Path as _Path

    _spec = _importlib_util.spec_from_file_location(
        "context_receipt_store_component", _Path(__file__).parent / "component.py"
    )
    _component_mod = _importlib_util.module_from_spec(_spec)
    _spec.loader.exec_module(_component_mod)
    rerank_with_priors = _component_mod.rerank


def _embed_query(query: str, provider: str, model: Optional[str], api_key_env_var: Optional[str]) -> List[float]:
    """Embeds ONE query string with the same provider/model an index was built with --
    mirrors `context_engineering_pipeline`'s own `_do_embed_raw` provider dispatch (single
    query, not batched, since a gateway answers one question at a time)."""
    import os

    if provider == "sentence_transformers":
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError("embed_provider='sentence_transformers' requires: pip install sentence-transformers")
        model_name = model or "all-MiniLM-L6-v2"
        return SentenceTransformer(model_name).encode([query], normalize_embeddings=True, show_progress_bar=False)[0].tolist()

    if provider == "openai":
        try:
            import openai
        except ImportError:
            raise ImportError("embed_provider='openai' requires: pip install openai")
        client = openai.OpenAI(api_key=os.environ.get(api_key_env_var or "OPENAI_API_KEY"))
        resp = client.embeddings.create(model=model or "text-embedding-3-small", input=[query])
        return resp.data[0].embedding

    if provider == "cohere":
        try:
            import cohere
        except ImportError:
            raise ImportError("embed_provider='cohere' requires: pip install cohere")
        client = cohere.Client(api_key=os.environ.get(api_key_env_var or "COHERE_API_KEY"))
        resp = client.embed(texts=[query], model=model or "embed-english-v3.0", input_type="search_query")
        return resp.embeddings[0]

    if provider == "litellm":
        try:
            import litellm
        except ImportError:
            raise ImportError("embed_provider='litellm' requires: pip install litellm")
        api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
        resp = litellm.embedding(model=model or "text-embedding-3-small", input=[query], api_key=api_key)
        return resp.data[0]["embedding"]

    raise ValueError(f"KnowledgeSearch: unsupported embed_provider {provider!r}. Valid: sentence_transformers, openai, cohere, litellm.")


def _query_vector_store(
    embedding: List[float], *, provider: str, connection_string: Optional[str], collection_name: str,
    top_k: int, api_key_env_var: Optional[str],
) -> List[Dict[str, Any]]:
    """Queries chromadb/pinecone/qdrant for the nearest `top_k` vectors. Returns a list of
    `{"id", "metadata", "distance_or_score"}` dicts -- the raw provider result, not yet
    quarantine-filtered or reranked (`KnowledgeSearch.search` does that)."""
    import os

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

    raise ValueError(f"KnowledgeSearch: unsupported provider {provider!r}. Valid: chromadb, pinecone, qdrant.")


class KnowledgeSearch:
    """Gateway-side helper so a FastMCP (or any other) tool needs no hand-written vector
    search code. Construct once per knowledge base; call `.search(query)` per request.

        knowledge = KnowledgeSearch(
            provider="chromadb", connection_string="./chroma_db", collection_name="kb",
            embed_provider="sentence_transformers", embed_model="all-MiniLM-L6-v2",
        )

        @mcp.tool()
        async def search_knowledge(query: str, caller_id: str, purpose: str) -> dict:
            return knowledge.search(query, context_name="kb", caller_id=caller_id, purpose=purpose)
    """

    def __init__(
        self,
        *,
        provider: str,
        connection_string: Optional[str] = None,
        collection_name: str,
        embed_provider: str = "sentence_transformers",
        embed_model: Optional[str] = None,
        api_key_env_var: Optional[str] = None,
        embed_api_key_env_var: Optional[str] = None,
        text_metadata_key: str = "text",
        exclude_quarantined: bool = True,
    ):
        self.provider = provider
        self.connection_string = connection_string
        self.collection_name = collection_name
        self.embed_provider = embed_provider
        self.embed_model = embed_model
        self.api_key_env_var = api_key_env_var
        self.embed_api_key_env_var = embed_api_key_env_var
        self.text_metadata_key = text_metadata_key
        self.exclude_quarantined = exclude_quarantined

    def search(
        self,
        query: str,
        *,
        context_name: str,
        top_k: int = 10,
        purpose: Optional[str] = None,
        priors: Optional[Mapping[str, Mapping[str, Any]]] = None,
        prior_weight: float = 0.2,
    ) -> Dict[str, Any]:
        """Returns `{"chunks": [...], "_receipt": {...}}`. `chunks` carries real chunk text
        for the tool/agent to answer with; `_receipt` never does -- pass it straight through
        as a tool's `_receipt` key and `ReceiptMiddleware` records it automatically."""
        embedding = _embed_query(query, self.embed_provider, self.embed_model, self.embed_api_key_env_var)
        raw_hits = _query_vector_store(
            embedding, provider=self.provider, connection_string=self.connection_string,
            collection_name=self.collection_name, top_k=top_k, api_key_env_var=self.api_key_env_var,
        )
        eligible_count = len(raw_hits)

        hits = raw_hits
        if self.exclude_quarantined:
            hits = [h for h in hits if not (h.get("metadata") or {}).get("feedback_quarantined")]

        candidates = [{"item_id": h["id"], "score": h.get("score") if h.get("score") is not None else 0.0} for h in hits]
        if priors:
            ordered = rerank_with_priors(candidates, priors, top_k=top_k, prior_weight=prior_weight)
            order = [c["item_id"] for c in ordered]
            hits = sorted(hits, key=lambda h: order.index(h["id"]) if h["id"] in order else len(order))[:top_k]
        else:
            hits = hits[:top_k]

        chunks = []
        items = []
        for rank, h in enumerate(hits):
            meta = h.get("metadata") or {}
            text = h.get("document") or meta.get(self.text_metadata_key)
            chunks.append({
                "chunk_id": meta.get("chunk_id", h["id"]),
                "text": text,
                "metadata": {k: v for k, v in meta.items() if k != self.text_metadata_key},
            })
            items.append({
                "item_id": meta.get("chunk_id", h["id"]),
                "source_asset_key": meta.get("source_asset_key"),
                "source_version": meta.get("source_version"),
                "source_code_version": meta.get("source_code_version"),
                "rank": rank,
                "score": h.get("score"),
            })

        return {
            "chunks": chunks,
            "_receipt": {
                "context_name": context_name,
                "purpose": purpose,
                "query_hash": hashlib.sha256(query.encode("utf-8")).hexdigest(),
                "eligible_count": eligible_count,
                "items": items,
            },
        }
