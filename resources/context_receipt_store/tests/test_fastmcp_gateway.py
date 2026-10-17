import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastmcp")
from fastmcp import Client  # noqa: E402


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_gateway_receipts_retrieval_exclusions_and_output(tmp_path, monkeypatch):
    monkeypatch.setenv("CONTEXT_RECEIPTS_DB", str(tmp_path / "gw.sqlite3"))
    root = Path(__file__).parents[1]
    store_mod = _load("ctx_store_component", root / "component.py")
    # The example imports the public package path; alias it to the local file.
    pkg = type(sys)("dagster_community_components")
    pkg.ContextReceiptStore = store_mod.ContextReceiptStore
    monkeypatch.setitem(sys.modules, "dagster_community_components", pkg)
    gw = _load("ctx_gateway_example", root / "examples" / "fastmcp_gateway.py")
    store = store_mod.ContextReceiptStore(str(tmp_path / "gw.sqlite3"))

    async def run():
        async with Client(gw.mcp) as client:
            res = (await client.call_tool("search_context", {
                "query": "renewal blockers", "account": "acme",
                "caller_id": "agent-1", "purpose": "summarize"})).data
            await client.call_tool("attach_output", {
                "receipt_id": res["receipt_id"], "output_id": "out-1",
                "model": "m", "output_text": "answer"})
            return res

    res = asyncio.run(run())
    assert [c["id"] for c in res["chunks"]] == ["tkt-1:0"]
    receipt = store.get_receipt(res["receipt_id"])
    reasons = {e["item_id"]: e["reason"] for e in receipt["excluded"]}
    assert reasons == {"tkt-2:0": "pii", "tkt-3:0": "stale", "tkt-9:0": "out_of_scope"}
    assert receipt["eligible_count"] == 4 and receipt["excluded_count"] == 3
    assert [e["event_type"] for e in receipt["events"]] == ["output_attached"]
    assert "answer" not in str(receipt)
    assert store.find_by_source_asset("support_tickets") == [res["receipt_id"]]


def test_middleware_receipts_opted_in_tools_only(tmp_path, monkeypatch):
    from fastmcp import FastMCP

    root = Path(__file__).parents[1]
    store_mod = _load("ctx_store_component2", root / "component.py")
    pkg = type(sys)("dagster_community_components")
    pkg.ContextReceiptStore = store_mod.ContextReceiptStore
    monkeypatch.setitem(sys.modules, "dagster_community_components", pkg)
    mw = _load("ctx_mw_example", root / "examples" / "fastmcp_receipt_middleware.py")
    store = store_mod.ContextReceiptStore(str(tmp_path / "mw.sqlite3"))

    app = FastMCP("t")
    app.add_middleware(mw.ReceiptMiddleware(store))

    @app.tool
    def lookup(caller_id: str) -> dict:
        return {"chunks": ["x"], "_receipt": {
            "context_name": "c", "items": [{"item_id": "a", "source_asset_key": "s"}],
            "excluded": [{"item_id": "b", "reason": "pii"}]}}

    @app.tool
    def plain() -> dict:
        return {"v": 1}

    async def run():
        async with Client(app) as client:
            return (await client.call_tool("lookup", {"caller_id": "agent"})).data, \
                   (await client.call_tool("plain", {})).data

    got, plain = asyncio.run(run())
    assert "_receipt" not in got and got["chunks"] == ["x"]
    receipt = store.get_receipt(got["receipt_id"])
    assert receipt["caller_id"] == "agent" and receipt["excluded"][0]["reason"] == "pii"
    assert plain == {"v": 1}
