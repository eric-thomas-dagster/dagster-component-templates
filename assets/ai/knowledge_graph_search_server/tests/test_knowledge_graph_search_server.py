"""Committed tests for KnowledgeGraphSearchServerComponent. Builds a real small networkx
graph file (the exact node-link JSON shape KnowledgeGraphBuilderComponent's write_graph
step writes) and queries it through the real resource -- nothing mocked for the
provider='networkx' path, which is this component's free/local default."""
import json

import dagster as dg
import networkx as nx
import pytest

from .conftest import load_component_module

CLS = "KnowledgeGraphSearchServerComponent"
RESOURCE_CLS = "KnowledgeGraphSearchResource"


@pytest.fixture()
def mod():
    return load_component_module()


def _write_graph_file(path: str) -> None:
    g = nx.MultiDiGraph()
    g.add_node("Alice", type="PERSON", aliases=["Alice", "Alice Smith"], mention_count=3)
    g.add_node("Acme Corp", type="ORG", aliases=["Acme Corp", "ACME"], mention_count=2)
    g.add_node("Bob", type="PERSON", aliases=["Bob"], mention_count=1)
    g.add_edge("Alice", "Acme Corp", relation="works_at", confidence=0.9, mention_count=2,
               chunk_ids=["c1", "c2"], source_ids=["d1", "d2"], content_hashes=["h1", "h2"],
               evidences=["Alice works at Acme Corp.", "Alice is employed by Acme Corp."])
    g.add_edge("Bob", "Acme Corp", relation="works_at", confidence=0.7, mention_count=1,
               chunk_ids=["c3"], source_ids=["d3"], content_hashes=["h3"], evidences=["Bob works at Acme Corp too."])
    data = nx.node_link_data(g, edges="edges")
    with open(path, "w") as f:
        json.dump(data, f)


def _resource(mod, path):
    cls = getattr(mod, RESOURCE_CLS)
    return cls(provider="networkx", path=path, graph_name="test_kg")


def test_search_entities_matches_name_and_alias(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.search_entities("acme")
    assert {e["name"] for e in res["entities"]} == {"Acme Corp"}
    res2 = r.search_entities("smith")  # matches the alias, not the canonical name
    assert {e["name"] for e in res2["entities"]} == {"Alice"}


def test_search_entities_type_filter(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.search_entities("a", type="ORG")
    assert {e["name"] for e in res["entities"]} == {"Acme Corp"}


def test_get_entity_returns_relations_with_evidence(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.get_entity("Alice")
    assert res["type"] == "PERSON"
    assert res["mention_count"] == 3
    rels = res["relations"]
    assert len(rels) == 1
    assert rels[0]["relation"] == "works_at"
    assert rels[0]["other"] == "Acme Corp"
    assert rels[0]["evidence"] == "Alice works at Acme Corp."


def test_get_entity_not_found(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    assert "error" in r.get_entity("Nobody")


def test_get_neighbors_one_hop(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.get_neighbors("Alice", hops=1)
    assert {n["name"] for n in res["neighbors"]} == {"Acme Corp"}


def test_get_neighbors_two_hops_reaches_bob_via_acme(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.get_neighbors("Alice", hops=2)
    assert {n["name"] for n in res["neighbors"]} == {"Acme Corp", "Bob"}


def test_find_path_between_alice_and_bob(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.find_path("Alice", "Bob")
    assert res["path"] == ["Alice", "Acme Corp", "Bob"]


def test_find_path_no_path_when_entities_not_found(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    assert "error" in r.find_path("Alice", "Nobody")


def test_top_entities_ranked_by_mention_count(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    res = r.top_entities()
    names = [e["name"] for e in res["entities"]]
    assert names[0] == "Alice"  # mention_count 3, the highest


def test_graph_file_cache_is_loaded_once_per_resource_instance(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    r = _resource(mod, path)
    g1 = r._graph()
    g2 = r._graph()
    assert g1 is g2  # same in-memory object -- confirms the cache, not a re-read per call


# ─── component build_defs: resource + inspection asset + optional mcp server ─────────────

def test_build_defs_registers_resource_and_inspection_asset(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    _write_graph_file(path)
    cls = getattr(mod, CLS)
    comp = cls(resource_key="kg_search", provider="networkx", path=path, graph_name="test_kg")
    defs = comp.build_defs(None)
    assert "kg_search" in defs.resources
    result = dg.materialize(list(defs.assets), resources=defs.resources)
    assert result.success
    md = result.asset_materializations_for_node("knowledge_graph_search_tools")[0].metadata
    assert md["provider"].value == "networkx"
    assert md["tool_names"].value == ["search_entities", "get_entity", "get_neighbors", "find_path", "top_entities"]


def test_build_defs_registers_mcp_server_resource_with_env_passthrough(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    cls = getattr(mod, CLS)
    comp = cls(provider="neo4j", uri="bolt://localhost:7687", username_env_var="NEO4J_USER",
               password_env_var="NEO4J_PASS", mcp_server_resource_key="kg_mcp")
    defs = comp.build_defs(None)
    mcp_resource = defs.resources["kg_mcp"]
    assert mcp_resource.type == "stdio"
    assert mcp_resource.live_env_passthrough == ["NEO4J_USER", "NEO4J_PASS"]
    assert mcp_resource.env["KNOWLEDGE_GRAPH_SEARCH_URI"] == "bolt://localhost:7687"


def test_build_defs_neo4j_requires_uri(mod):
    cls = getattr(mod, CLS)
    comp = cls(provider="neo4j")
    with pytest.raises(ValueError, match="requires `uri`"):
        comp.build_defs(None)


# ─── MCP tool wrapper: errors surface as data, never crash the agent loop ────────────────

def test_safe_wrapper_returns_error_dict_instead_of_raising(mod, tmp_path):
    path = str(tmp_path / "kg.json")
    r = _resource(mod, path)  # file doesn't exist -> empty graph, not an exception
    res = mod._safe(r.get_entity, "Anyone")
    assert res == {"error": "entity 'Anyone' not found"}


# ─── provider='sql_tables': recursive-CTE graph traversal over knowledge_graph_builder's
# execution_mode='sql' output tables ──────────────────────────────────────────────────────
# Snowflake/Databricks/BigQuery SQL is checked for real syntactic validity with
# sqlglot.parse(sql, read=dialect). Databricks' real `) MAX RECURSION LEVEL n` safety clause is
# deliberately omitted (not load-bearing -- the `depth < hops` predicate already bounds recursion
# tighter than that) since sqlglot 28.0.0 has no parser support for it (confirmed directly). The
# recursive traversal LOGIC itself (direction-agnostic join, cycle prevention via a path array,
# depth-bounded BFS, shortest-path tie-breaking) is executed for real against DuckDB, using
# DuckDB's own list_concat/list_contains in place of the real dialects' ARRAY_CONCAT/ARRAY_AGG
# equivalents -- same stand-in technique knowledge_graph_builder's own tests use.

from .conftest import requires_duckdb, requires_sqlglot  # noqa: E402


@requires_sqlglot
@pytest.mark.parametrize("dialect,read", [
    ("snowflake_cortex", "snowflake"), ("bigquery", "bigquery"), ("databricks", "databricks"),
])
def test_sql_tables_queries_are_valid_sql_for_dialect(mod, dialect, read):
    import sqlglot
    nodes_tbl, edges_tbl = mod._sql_tables_names("kg", dialect)
    for sql in (
        mod._build_sql_search_entities_query(nodes_tbl, "acme", "ORG", 50),
        mod._build_sql_top_entities_query(nodes_tbl, None, 50),
        mod._build_sql_neighbors_query(dialect, edges_tbl, "Alice", 2, None),
        mod._build_sql_neighbors_query(dialect, edges_tbl, "Alice", 2, "works_at"),
        mod._build_sql_path_query(dialect, edges_tbl, "Alice", "Bob", 4),
        *mod._build_sql_entity_relations_queries(edges_tbl, "Alice"),
    ):
        sqlglot.parse(sql, read=read)


@requires_duckdb
def test_recursive_neighbors_logic_direction_agnostic_cycle_safe_depth_bounded(mod):
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE TABLE edges (source STRING, target STRING, relation STRING)")
    con.execute("INSERT INTO edges VALUES ('A','B','r1'), ('B','C','r2'), ('C','D','r3'), ('A','A','self')")
    sql = """
    WITH RECURSIVE neighbors (name, depth, path) AS (
      SELECT 'A', 0, ['A']
      UNION ALL
      SELECT (CASE WHEN e.source = n.name THEN e.target ELSE e.source END), n.depth + 1,
             list_concat(n.path, [(CASE WHEN e.source = n.name THEN e.target ELSE e.source END)])
      FROM edges AS e JOIN neighbors AS n ON (e.source = n.name OR e.target = n.name)
      WHERE n.depth < 2 AND TRUE AND NOT list_contains(n.path, (CASE WHEN e.source = n.name THEN e.target ELSE e.source END))
    )
    SELECT name, MIN(depth) AS depth FROM neighbors WHERE name != 'A' GROUP BY name ORDER BY depth
    """
    rows = con.execute(sql).fetchall()
    assert rows == [("B", 1), ("C", 2)]  # D is 3 hops away (beyond the bound); the self-loop never appears


@requires_duckdb
def test_recursive_path_logic_finds_shortest_not_first(mod):
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE TABLE edges (source STRING, target STRING, relation STRING)")
    con.execute("INSERT INTO edges VALUES ('A','B','r1'), ('B','C','r2'), ('C','D','r3'), ('A','D','shortcut')")
    sql = """
    WITH RECURSIVE paths (name, depth, path, rels) AS (
      SELECT 'A', 0, ['A'], CAST([] AS STRING[])
      UNION ALL
      SELECT (CASE WHEN e.source = p.name THEN e.target ELSE e.source END), p.depth + 1,
             list_concat(p.path, [(CASE WHEN e.source = p.name THEN e.target ELSE e.source END)]),
             list_concat(p.rels, [e.relation])
      FROM edges AS e JOIN paths AS p ON (e.source = p.name OR e.target = p.name)
      WHERE p.depth < 4 AND NOT list_contains(p.path, (CASE WHEN e.source = p.name THEN e.target ELSE e.source END))
    )
    SELECT path, rels FROM paths WHERE name = 'D' ORDER BY depth LIMIT 1
    """
    rows = con.execute(sql).fetchall()
    assert rows == [(["A", "D"], ["shortcut"])]


class _FakeSqlRows:
    """Stand-in for _run_sql_tables_query, so resource-method dispatch/shaping is tested without
    a real warehouse connection (connection-building itself is covered by
    test_sql_connection_builders_* below)."""

    def __init__(self, by_sql_substring):
        self.by_sql_substring = by_sql_substring
        self.calls = []

    def __call__(self, cfg, sql):
        self.calls.append(sql)
        for substr, rows in self.by_sql_substring.items():
            if substr in sql:
                return rows
        raise AssertionError(f"no fake rows registered for SQL: {sql}")


def _sql_resource(mod, **kwargs):
    cls = getattr(mod, RESOURCE_CLS)
    return cls(provider="sql_tables", sql_dialect="snowflake_cortex", output_table="kg", **kwargs)


def test_sql_tables_requires_output_table_and_dialect(mod):
    cls = getattr(mod, RESOURCE_CLS)
    r = cls(provider="sql_tables")
    with pytest.raises(ValueError, match="output_table"):
        r.search_entities("x")
    r2 = cls(provider="sql_tables", output_table="kg", sql_dialect="postgres")
    with pytest.raises(ValueError, match="sql_dialect"):
        r2.search_entities("x")


def test_sql_tables_search_entities_shapes_rows(mod, monkeypatch):
    fake = _FakeSqlRows({"kg_nodes": [{"id": "Acme", "name": "Acme", "type": "ORG", "mentions": 3}]})
    monkeypatch.setattr(mod, "_run_sql_tables_query", fake)
    r = _sql_resource(mod)
    assert r.search_entities("acme") == {"entities": [{"name": "Acme", "type": "ORG", "mention_count": 3, "aliases": None}]}


def test_sql_tables_top_entities_shapes_rows(mod, monkeypatch):
    fake = _FakeSqlRows({"kg_nodes": [{"name": "Acme", "type": "ORG", "mentions": 3}]})
    monkeypatch.setattr(mod, "_run_sql_tables_query", fake)
    r = _sql_resource(mod)
    assert r.top_entities() == {"entities": [{"name": "Acme", "type": "ORG", "mention_count": 3}]}


def test_sql_tables_get_entity_not_found(mod, monkeypatch):
    fake = _FakeSqlRows({"kg_nodes": []})
    monkeypatch.setattr(mod, "_run_sql_tables_query", fake)
    r = _sql_resource(mod)
    assert r.get_entity("Nobody") == {"error": "entity 'Nobody' not found"}


def test_sql_tables_get_entity_found_with_relations(mod, monkeypatch):
    def _rows(cfg, sql):
        if "SELECT type, mentions" in sql:
            return [{"type": "PERSON", "mentions": 2}]
        if "source = " in sql:
            return [{"other": "Acme", "relation": "works_at", "confidence": 0.9, "mention_count": 2, "evidence": "ev1"}]
        if "target = " in sql:
            return []
        raise AssertionError(sql)
    monkeypatch.setattr(mod, "_run_sql_tables_query", _rows)
    r = _sql_resource(mod)
    result = r.get_entity("Alice")
    assert result["type"] == "PERSON"
    assert result["mention_count"] == 2
    assert result["relations"] == [{"direction": "out", "relation": "works_at", "other": "Acme", "confidence": 0.9, "mention_count": 2, "evidence": "ev1"}]


def test_sql_tables_get_neighbors_joins_types(mod, monkeypatch):
    def _rows(cfg, sql):
        if "neighbors" in sql:
            return [{"name": "Acme", "depth": 1}]
        if "WHERE id IN" in sql:
            return [{"id": "Acme", "type": "ORG"}]
        raise AssertionError(sql)
    monkeypatch.setattr(mod, "_run_sql_tables_query", _rows)
    r = _sql_resource(mod)
    assert r.get_neighbors("Alice", hops=1) == {"neighbors": [{"name": "Acme", "type": "ORG", "distance": 1}]}


def test_sql_tables_find_path_none_when_no_rows(mod, monkeypatch):
    fake = _FakeSqlRows({"paths": []})
    monkeypatch.setattr(mod, "_run_sql_tables_query", fake)
    r = _sql_resource(mod)
    assert r.find_path("Alice", "Bob") == {"path": None}


def test_sql_tables_find_path_parses_json_array_strings(mod, monkeypatch):
    # Snowflake's connector returns ARRAY columns as JSON-encoded strings, not native lists
    # (confirmed against snowflake-connector-python's own documented/long-standing behavior) --
    # _maybe_json_list must handle that, not just a native list.
    fake = _FakeSqlRows({"paths": [{"path": '["Alice", "Acme"]', "rels": '["works_at"]'}]})
    monkeypatch.setattr(mod, "_run_sql_tables_query", fake)
    r = _sql_resource(mod)
    assert r.find_path("Alice", "Acme") == {"path": ["Alice", "Acme"], "relations": ["works_at"]}


def test_sql_tables_find_path_handles_native_list(mod, monkeypatch):
    fake = _FakeSqlRows({"paths": [{"path": ["Alice", "Acme"], "rels": ["works_at"]}]})
    monkeypatch.setattr(mod, "_run_sql_tables_query", fake)
    r = _sql_resource(mod)
    assert r.find_path("Alice", "Acme") == {"path": ["Alice", "Acme"], "relations": ["works_at"]}


def test_maybe_json_list_handles_none_and_malformed(mod):
    assert mod._maybe_json_list(None) == []
    assert mod._maybe_json_list("not json") == []
    assert mod._maybe_json_list(["a", "b"]) == ["a", "b"]


# ─── direct vendor-connection builders (kwargs construction, no real network call) ────────

def test_snowflake_connection_requires_account_and_user(mod, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    with pytest.raises(ValueError, match="snowflake_account_env_var"):
        mod._build_snowflake_connection({"snowflake_account_env_var": "SNOWFLAKE_ACCOUNT", "snowflake_user_env_var": "SNOWFLAKE_USER"})


def test_snowflake_connection_builds_expected_kwargs(mod, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct1")
    monkeypatch.setenv("SNOWFLAKE_USER", "user1")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw1")
    captured = {}

    class _FakeSnowflakeConnector:
        @staticmethod
        def connect(**kwargs):
            captured.update(kwargs)
            return "connection"

    import sys
    import types
    fake_module = types.ModuleType("snowflake")
    fake_module.connector = _FakeSnowflakeConnector
    monkeypatch.setitem(sys.modules, "snowflake", fake_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector", _FakeSnowflakeConnector)

    conn = mod._build_snowflake_connection({
        "snowflake_account_env_var": "SNOWFLAKE_ACCOUNT", "snowflake_user_env_var": "SNOWFLAKE_USER",
        "snowflake_password_env_var": "SNOWFLAKE_PASSWORD", "snowflake_database": "DB", "snowflake_schema": "SCH",
        "snowflake_warehouse": "WH",
    })
    assert conn == "connection"
    assert captured == {"account": "acct1", "user": "user1", "password": "pw1", "database": "DB", "schema": "SCH", "warehouse": "WH"}


def test_databricks_connection_requires_host_token_and_http_path(mod, monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "host1")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    with pytest.raises(ValueError, match="databricks_http_path"):
        mod._build_databricks_connection({"databricks_host_env_var": "DATABRICKS_HOST", "databricks_token_env_var": "DATABRICKS_TOKEN"})
