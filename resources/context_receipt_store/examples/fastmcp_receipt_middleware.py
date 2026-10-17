"""FastMCP middleware that receipts context tools with no FastMCP changes.

Any tool can opt in by returning a dict with a ``_receipt`` key holding the
``record_retrieval`` kwargs (items, excluded, context_name, ...). The middleware
records the receipt, strips ``_receipt`` and adds ``receipt_id`` to the result,
so the ID travels with the context to the agent. Tools that don't opt in are
untouched. Unlike fastmcp_gateway.py the tools stay receipt-agnostic.
"""

from __future__ import annotations

from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.tools.base import ToolResult

from dagster_community_components import ContextReceiptStore


class ReceiptMiddleware(Middleware):
    def __init__(self, receipts: ContextReceiptStore):
        self.receipts = receipts

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        result = await call_next(context)
        data = result.structured_content
        if not isinstance(data, dict) or "_receipt" not in data:
            return result
        data = dict(data)
        receipt_args = dict(data.pop("_receipt"))
        receipt_args.setdefault("caller_id", (context.message.arguments or {}).get("caller_id"))
        # Tie the receipt to the MCP call so the tool call can be traced to its context.
        fctx = context.fastmcp_context
        if fctx is not None:
            for field, attr in (("mcp_request_id", "request_id"), ("mcp_session_id", "session_id")):
                try:
                    receipt_args.setdefault(field, getattr(fctx, attr))
                except Exception:  # not every transport exposes both ids
                    pass
        data["receipt_id"] = self.receipts.record_retrieval(**receipt_args)
        # `_meta` is the MCP channel a client or UI can carry alongside the answer, so a later
        # thumbs-up/down (or any outcome) can be posted against this receipt id.
        return ToolResult(structured_content=data, meta={"context_receipt_id": data["receipt_id"]})
