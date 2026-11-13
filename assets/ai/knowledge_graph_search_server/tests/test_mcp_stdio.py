"""Launch component.py as a real stdio MCP server and call it with the real `mcp` client
library -- not a mock. Required for every MCP-importing component (see
tests/test_mcp_components_inventory.py)."""
import asyncio
import json
import os
import sys

import networkx as nx
import pytest

mcp = pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

HERE = __import__("pathlib").Path(__file__).resolve().parent
SERVER = str(HERE.parent / "component.py")


@pytest.fixture()
def graph_path(tmp_path):
    path = str(tmp_path / "kg.json")
    g = nx.MultiDiGraph()
    g.add_node("Alice", type="PERSON", aliases=["Alice"], mention_count=2)
    g.add_node("Acme Corp", type="ORG", aliases=["Acme Corp"], mention_count=1)
    g.add_edge("Alice", "Acme Corp", relation="works_at", confidence=0.9, mention_count=1,
               chunk_ids=["c1"], source_ids=["d1"], content_hashes=["h1"], evidences=["Alice works at Acme Corp."])
    data = nx.node_link_data(g, edges="edges")
    with open(path, "w") as f:
        json.dump(data, f)
    return path


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


def test_stdio_server_lists_and_serves_tools(graph_path):
    async def body(s):
        names = sorted(t.name for t in (await s.list_tools()).tools)
        entity = _json(await s.call_tool("get_entity", {"name": "Alice"}))
        search = _json(await s.call_tool("search_entities", {"query": "acme"}))
        top = _json(await s.call_tool("top_entities", {}))
        return names, entity, search, top

    names, entity, search, top = _run(body, {"KNOWLEDGE_GRAPH_SEARCH_PROVIDER": "networkx", "KNOWLEDGE_GRAPH_SEARCH_PATH": graph_path})
    assert names == sorted(["search_entities", "get_entity", "get_neighbors", "find_path", "top_entities"])
    assert entity["type"] == "PERSON"
    assert entity["relations"][0]["other"] == "Acme Corp"
    assert {e["name"] for e in search["entities"]} == {"Acme Corp"}
    assert top["entities"][0]["name"] == "Alice"


def test_stdio_server_get_neighbors_and_find_path(graph_path):
    async def body(s):
        neighbors = _json(await s.call_tool("get_neighbors", {"name": "Alice", "hops": 1}))
        path = _json(await s.call_tool("find_path", {"source": "Alice", "target": "Acme Corp"}))
        return neighbors, path

    neighbors, path = _run(body, {"KNOWLEDGE_GRAPH_SEARCH_PROVIDER": "networkx", "KNOWLEDGE_GRAPH_SEARCH_PATH": graph_path})
    assert {n["name"] for n in neighbors["neighbors"]} == {"Acme Corp"}
    assert path["path"] == ["Alice", "Acme Corp"]


def test_stdio_server_unknown_entity_returns_error_not_crash(graph_path):
    async def body(s):
        return _json(await s.call_tool("get_entity", {"name": "Nobody"}))

    res = _run(body, {"KNOWLEDGE_GRAPH_SEARCH_PROVIDER": "networkx", "KNOWLEDGE_GRAPH_SEARCH_PATH": graph_path})
    assert "error" in res
