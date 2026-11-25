"""Real stdio subprocess round trip (same transport agentic_pipeline/slack_chat_tools etc. use)
-- not just calling the registered function in-process -- since the SDK's tool dispatch only
preserves a raised exception's own message when it recognizes the exception as ITS OWN
ToolError (see _sdk_tool_error_cls in component.py); getting this wrong (e.g. raising some
other same-named ToolError) still produces isError=True but replaces the message with a
useless generic 'Error executing tool <name>'. Mirrors dagster_instance_tools/tests/
test_mcp_stdio.py and report_tools_server/tests/test_report_tools_server.py's identical check.

record_feedback (not search_knowledge) is the failure path exercised here: it needs no
embedding model / vector-store provider at all, just KNOWLEDGE_SEARCH_RECEIPTS_DB_PATH set, so
it stays fast and dependency-free (no chromadb/sentence-transformers needed in CI) while still
exercising a real, deterministic failure (an unknown receipt_id -> KeyError inside
_ReceiptWriter.record_outcome)."""
import asyncio
import os
import sys

import pytest


def test_stdio_server_bad_receipt_id_is_a_real_isError_result_with_the_original_message(tmp_path):
    pytest.importorskip("mcp")
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    server = str(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "component.py"))
    receipts_db_path = str(tmp_path / "receipts.sqlite")
    params = StdioServerParameters(
        command=sys.executable,
        args=[server],
        env={
            **os.environ,
            "KNOWLEDGE_SEARCH_COLLECTION_NAME": "kb_test",
            "KNOWLEDGE_SEARCH_RECEIPTS_DB_PATH": receipts_db_path,
        },
    )

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                names = sorted(t.name for t in (await s.list_tools()).tools)
                bad = await s.call_tool("record_feedback", {"receipt_id": "nope-not-a-real-receipt", "signal": "thumbs_up"})
                return names, bad

    names, bad = asyncio.run(asyncio.wait_for(go(), 60))
    assert names == ["record_feedback", "search_knowledge"]
    # A tool-level failure (unknown receipt_id) must come back as a REAL isError result, not a
    # quiet non-error {"error": ...} dict an agent loop's failed-call accounting would miss.
    assert getattr(bad, "is_error", None) or getattr(bad, "isError", None)
    assert "unknown context receipt" in bad.content[0].text
