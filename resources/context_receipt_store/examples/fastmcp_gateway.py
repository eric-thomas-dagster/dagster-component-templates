"""FastMCP context gateway that receipts every retrieval.

Run:  python fastmcp_gateway.py   (stdio)  or  fastmcp run fastmcp_gateway.py

The gateway is the choke point: policy is evaluated here, so the receipt's
`excluded` list is a fact about what was withheld, not a reconstruction. The
Dagster side supplies the context (chunks, versions, source asset keys); swap
CORPUS for your vector store / Dagster-materialized index.
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from fastmcp import FastMCP

from dagster_community_components import AsyncReceiptWriter, ContextReceiptStore, rerank_with_priors

CONTEXT_NAME = "account_support_context"
POLICY_VERSION = "support-policy-v1"
MAX_AGE = timedelta(days=30)
TOP_K = 3

_NOW = datetime.now(timezone.utc)
# Stand-in for a Dagster-built index: each chunk carries the version of the
# materialization it came from and the asset it was derived from.
CORPUS: list[dict[str, Any]] = [
    {"id": "tkt-1:0", "text": "Acme reported renewal blockers: SSO migration.", "account": "acme",
     "asset": "support_tickets", "version": "mat-41", "updated": _NOW - timedelta(days=2), "pii": False},
    {"id": "tkt-2:0", "text": "Acme CFO phone 555-0100, prefers email.", "account": "acme",
     "asset": "crm_accounts", "version": "mat-17", "updated": _NOW - timedelta(days=1), "pii": True},
    {"id": "tkt-3:0", "text": "Old Acme pricing complaint (2024).", "account": "acme",
     "asset": "support_tickets", "version": "mat-12", "updated": _NOW - timedelta(days=400), "pii": False},
    {"id": "tkt-4:0", "text": "Acme renewal blockers: procurement delay.", "account": "acme",
     "asset": "support_tickets", "version": "mat-41", "updated": _NOW - timedelta(days=5), "pii": False},
    {"id": "tkt-9:0", "text": "Globex renewal blockers: budget freeze.", "account": "globex",
     "asset": "support_tickets", "version": "mat-41", "updated": _NOW - timedelta(days=3), "pii": False},
]


def build_gateway(store: ContextReceiptStore, explore_fraction: float = 0.1) -> FastMCP:
    mcp = FastMCP("context-gateway")
    # Hot path: the receipt ID is assigned here and writes are fire-and-forget, so the
    # agent never waits on the store. Dagster's sensor reads the store on the cold path.
    receipts = AsyncReceiptWriter(store)

    @mcp.tool
    def search_context(query: str, account: str, caller_id: str, purpose: str) -> dict:
        """Retrieve context for an account under policy; returns chunks + receipt_id."""
        keywords = {w.lower().strip("?.,") for w in query.split()}
        retrieved, excluded = [], []
        for chunk in CORPUS:
            ref = {"item_id": chunk["id"], "source_asset_key": chunk["asset"],
                   "source_version": chunk["version"]}
            if chunk["account"] != account:
                excluded.append({**ref, "reason": "out_of_scope"})
            elif chunk["pii"]:
                excluded.append({**ref, "reason": "pii"})
            elif _NOW - chunk["updated"] > MAX_AGE:
                excluded.append({**ref, "reason": "stale"})
            else:
                hits = len(keywords & {w.lower().strip("?.,") for w in chunk["text"].split()})
                retrieved.append((hits, chunk, ref))
        # Rank by keyword hits adjusted by learned priors (written back by Dagster's
        # sensor from outcomes); reserve a slice of traffic for exploration.
        priors = store.get_item_scores([r[1]["id"] for r in retrieved])
        ranked = rerank_with_priors(
            [{"item_id": c["id"], "score": float(h), "chunk": c, "ref": ref} for h, c, ref in retrieved],
            priors, top_k=TOP_K, explore_fraction=explore_fraction,
        )
        kept = [(r["score"], r["chunk"], {**r["ref"], "selection": r["selection"]}) for r in ranked]
        kept_ids = {c["id"] for _, c, _ in kept}
        excluded += [{**ref, "reason": "over_budget"} for _, c, ref in retrieved if c["id"] not in kept_ids]

        receipt_id = receipts.record_retrieval(
            context_name=CONTEXT_NAME,
            policy_version=POLICY_VERSION,
            caller_id=caller_id,
            purpose=purpose,
            query_hash=hashlib.sha256(query.encode()).hexdigest(),
            eligible_count=len(CORPUS),
            items=[{**ref, "rank": rank, "score": float(hits)}
                   for rank, (hits, _, ref) in enumerate(kept)],
            excluded=excluded,
        )
        return {
            "receipt_id": receipt_id,
            "chunks": [{"id": c["id"], "text": c["text"]} for _, c, _ in kept],
        }

    @mcp.tool
    def attach_output(receipt_id: str, output_id: str, model: str, output_text: str) -> str:
        """Link the agent's final output to its receipt (stores a hash, not the text)."""
        receipts.attach_output(
            receipt_id, output_id=output_id, model=model,
            output_hash=hashlib.sha256(output_text.encode()).hexdigest(),
        )
        return "ok"

    @mcp.tool
    def record_feedback(receipt_id: str, signal: str, user_id: str) -> str:
        """Attach an outcome (thumbs_up, thumbs_down, edited, escalated, ...) to a receipt."""
        receipts.record_outcome(receipt_id, signal=signal, source="gateway", outcome_ref=user_id)
        return "ok"

    @mcp.tool
    def explain(receipt_id: str) -> dict:
        """Answer 'why did the AI say this?' for a receipt ID."""
        receipts.flush()  # make sure queued writes for this receipt are durable
        return store.get_receipt(receipt_id) or {"error": "unknown receipt"}

    @mcp.tool
    def impacted_by(source_asset_key: str) -> list[str]:
        """Recall: every receipt that used a chunk derived from this asset."""
        receipts.flush()
        return store.find_by_source_asset(source_asset_key)

    return mcp


mcp = build_gateway(ContextReceiptStore(os.environ.get("CONTEXT_RECEIPTS_DB", "./context-receipts.sqlite3")))

if __name__ == "__main__":
    mcp.run()
