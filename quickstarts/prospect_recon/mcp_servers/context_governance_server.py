"""FastMCP server: the governance boundary for the prospect-recon context KB.

Positioning (the leadership-facing "three products, three jobs" story -- see
quickstarts/prospect_recon/README.md): Prefect builds the demo project (Demo
Factory, unmodified, out of scope here); Dagster builds and shows the context
-- the asset graph, materialization history, the governed knowledge base
itself (prospect_recon_context_kb); FastMCP governs ACCESS to that context and
is the system of record for receipts. Concretely: `prospect_recon_context_kb`
(ContextEngineeringPipelineComponent) and `prospect_recon_research`
(AgenticPipelineComponent's tool_use_loop) keep their own existing, already-
tested, in-process `receipts:` wiring for BUILD-time provenance (chunking,
classification, the research tool-call trail) -- that's real, fast, and
there's no reason to break it. This server is the NEW governance boundary for
the GENERATION/consumption-time receipt: the moment prospect_recon_brief
actually consumes the governed KB to produce an externally-shipped artifact
(the brief committed to dagster-demo-factory). That moment goes through an MCP
tool call instead of a direct in-process resource call, which is what "FastMCP
governs access to context and writes receipts" means here -- a real access
boundary, not just a relabeling of the same in-process call.

Wraps (does not reimplement) the real, tested `ContextReceiptStore`
(resources/context_receipt_store/component.py) via `dagster_community_components`'
registry import -- same backend/db file prospect_recon_context_kb's
`context_receipts` resource instance already writes to
(data/prospect-recon-receipts.sqlite3), so every consumer sees one shared
receipt history regardless of which side (component or this server) wrote it.

Tools:
  retrieve_context(context_name, query, top_k=5) -- embeds `query` (litellm,
    the same model prospect_recon_context_kb's `embed` step uses) and queries
    the chromadb collection prospect_recon_context_kb writes to, recording the
    retrieval as a receipt. Not currently called by this quickstart's pipeline
    (prospect_recon_brief gets the KB's full DataFrame directly via normal
    Dagster asset lineage, which is simpler and already tested) -- exposed so
    a future consumer (a chat/ops tool, a different pipeline) has a governed
    read path into this KB without going around the receipt store.
  write_receipt(context_name, item_ids, caller_id, policy_version, purpose) --
    the generation-time provenance write prospect_recon_brief calls.
  record_outcome(receipt_id, signal, value=None) -- proxies
    ContextReceiptStore.record_outcome (e.g. "brief_committed",
    "demo_deployed" once Demo Factory's own outcome is known).

Run standalone for a smoke test: `python3 context_governance_server.py` (stdio transport).
"""
import os

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("prospect-recon-context-governance")

_PROJECT_ROOT = os.environ.get(
    "PROSPECT_RECON_PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)
_DEFAULT_RECEIPTS_DB = os.path.join(_PROJECT_ROOT, "data", "prospect-recon-receipts.sqlite3")
_DEFAULT_CHROMA_PATH = os.path.join(_PROJECT_ROOT, "data", "chroma")
_DEFAULT_COLLECTION = "prospect_recon"


def _receipt_store():
    # ContextReceiptStore is a plain, reusable class (resources/context_receipt_store/
    # component.py) -- registered in dagster_community_components' registry, so this
    # reuses the real, tested implementation rather than reimplementing SQLite access.
    from dagster_community_components import ContextReceiptStore

    db_path = os.environ.get("PROSPECT_RECON_RECEIPTS_DB", _DEFAULT_RECEIPTS_DB)
    return ContextReceiptStore(db_path)


@mcp.tool()
def write_receipt(
    context_name: str,
    item_ids: list[str],
    caller_id: str = "",
    policy_version: str = "",
    purpose: str = "",
) -> dict:
    """Record that `caller_id` consumed `item_ids` from `context_name` for `purpose`
    (e.g. "brief_generation"). Returns {receipt_id}. This is the governance checkpoint:
    every consumption of the prospect-recon KB that produces an external artifact should
    call this instead of writing to the receipt store directly."""
    store = _receipt_store()
    receipt_id = store.record_retrieval(
        context_name=context_name,
        items=[{"item_id": iid} for iid in item_ids],
        caller_id=caller_id or None,
        policy_version=policy_version or None,
        purpose=purpose or None,
    )
    return {"receipt_id": receipt_id}


@mcp.tool()
def record_outcome(receipt_id: str, signal: str, value: float | None = None) -> dict:
    """Append an outcome (e.g. "brief_committed", "demo_deployed") to a receipt this
    server (or the pipeline's own in-process receipts: config) previously wrote."""
    store = _receipt_store()
    store.record_outcome(receipt_id, signal=signal, value=value)
    return {"ok": True}


@mcp.tool()
def retrieve_context(context_name: str, query: str, top_k: int = 5) -> dict:
    """Embed `query` and search the prospect-recon chromadb collection
    (prospect_recon_context_kb's own `write_vector_store` index), recording the retrieval
    as a receipt. Returns {receipt_id, items: [{item_id, text, metadata, score}]}."""
    import chromadb
    import litellm

    model = os.environ.get("PROSPECT_RECON_RESEARCH_EMBED_MODEL", "text-embedding-3-small")
    embed_resp = litellm.embedding(model=model, input=[query], api_key=os.environ.get("OPENAI_API_KEY"))
    query_embedding = embed_resp.data[0]["embedding"]

    chroma_path = os.environ.get("PROSPECT_RECON_CHROMA_PATH", _DEFAULT_CHROMA_PATH)
    collection_name = os.environ.get("PROSPECT_RECON_CHROMA_COLLECTION", _DEFAULT_COLLECTION)
    collection = chromadb.PersistentClient(path=chroma_path).get_collection(collection_name)
    res = collection.query(query_embeddings=[query_embedding], n_results=top_k)

    ids = (res.get("ids") or [[]])[0]
    docs = (res.get("documents") or [[]])[0]
    metas = (res.get("metadatas") or [[]])[0]
    dists = (res.get("distances") or [[]])[0]
    items = [
        {"item_id": iid, "text": doc, "metadata": meta, "score": 1.0 - dist if dist is not None else None}
        for iid, doc, meta, dist in zip(ids, docs, metas, dists)
    ]

    store = _receipt_store()
    receipt_id = store.record_retrieval(
        context_name=context_name,
        items=[{"item_id": it["item_id"]} for it in items],
        purpose="retrieve_context",
    )
    return {"receipt_id": receipt_id, "items": items}


if __name__ == "__main__":
    mcp.run(transport="stdio")
