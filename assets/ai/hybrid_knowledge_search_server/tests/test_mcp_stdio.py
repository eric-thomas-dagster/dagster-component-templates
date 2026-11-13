"""Launch component.py as a real stdio MCP server and call it with the real `mcp` client
library -- not a mock. Required for every MCP-importing component (see
tests/test_mcp_components_inventory.py)."""
import asyncio
import json
import os
import sys

import pytest

from .test_hybrid_knowledge_search_server import CHUNK_C1, CHUNK_C2, _build_chroma, _build_graph

mcp = pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

HERE = __import__("pathlib").Path(__file__).resolve().parent
SERVER = str(HERE.parent / "component.py")


@pytest.fixture()
def paths(tmp_path):
    chroma_path, graph_path = str(tmp_path / "chroma"), str(tmp_path / "kg.json")
    _build_chroma(chroma_path, "support_kb")
    _build_graph(graph_path)
    return chroma_path, graph_path


def _run(coro_fn, env_extra):
    env = {**os.environ, **env_extra}
    params = StdioServerParameters(command=sys.executable, args=[SERVER], env=env)

    async def go():
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                return await coro_fn(s)
    return asyncio.run(asyncio.wait_for(go(), 90))


def _json(res):
    return json.loads(res.content[0].text)


def test_stdio_server_lists_and_serves_tools(paths):
    chroma_path, graph_path = paths

    async def body(s):
        names = sorted(t.name for t in (await s.list_tools()).tools)
        hybrid = _json(await s.call_tool("search", {"query": "Alice works at Acme", "mode": "hybrid"}))
        entity = _json(await s.call_tool("get_entity", {"name": "Alice"}))
        chunks = _json(await s.call_tool("get_chunks_for_entity", {"name": "Alice"}))
        return names, hybrid, entity, chunks

    names, hybrid, entity, chunks = _run(body, {
        "HYBRID_KG_SEARCH_VECTOR_PROVIDER": "chromadb", "HYBRID_KG_SEARCH_VECTOR_CONNECTION_STRING": chroma_path,
        "HYBRID_KG_SEARCH_VECTOR_COLLECTION_NAME": "support_kb",
        "HYBRID_KG_SEARCH_GRAPH_PROVIDER": "networkx", "HYBRID_KG_SEARCH_GRAPH_PATH": graph_path,
    })
    assert names == sorted(["search", "search_entities", "get_entity", "get_neighbors", "find_path", "get_chunks_for_entity", "get_graph_context"])
    assert any(p["chunk_id"] == "c1" for p in hybrid["passages"])
    assert entity["type"] == "PERSON"
    assert chunks["chunk_ids"] == ["c1"]


def test_stdio_server_requires_collection_name():
    import subprocess
    env = {k: v for k, v in os.environ.items() if k != "HYBRID_KG_SEARCH_VECTOR_COLLECTION_NAME"}
    p = subprocess.run([sys.executable, SERVER], env=env, capture_output=True, text=True, timeout=60)
    assert p.returncode != 0 and "HYBRID_KG_SEARCH_VECTOR_COLLECTION_NAME" in p.stderr
