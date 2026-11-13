"""KnowledgeGraphSearchServerComponent -- lets an agent search a knowledge graph built by
KnowledgeGraphBuilderComponent, via search_entities/get_entity/get_neighbors/find_path/
top_entities tools returning plain JSON.

Same dual-mode shape as `knowledge_search_server`/`dagster_workflow_tools`:
  1. An importable `KnowledgeGraphSearchResource` (graph-read logic duplicated here from
     `knowledge_graph_builder`'s own graph-store I/O -- not imported across components,
     this repo's standalone-component convention; structurally checked in tests/test_drift.py)
     registered under `resource_key`.
  2. A self-contained stdio MCP server: `python -m <dotted.path.to.this.module>`.
`mcp_server_resource_key`, if set, ALSO registers a stdio MCP-server-shaped resource (same
convention as `knowledge_search_server`'s own `mcp_server_resource_key`) so any
mcp_servers-aware component can reference this server via `server_resource_key`.

Backends: `provider='networkx'` loads the graph file ONCE at server start and serves every
tool call from that in-memory graph (restart the server to pick up a rebuilt graph -- there is
no live-reload). `provider='neo4j'` / `'falkordb'` issue a live, read-only query per tool call
instead (neo4j: `session(default_access_mode=READ)`; falkordb has no separate read-only session
mode in its Python client, so this is enforced only by never issuing a write clause in the
Cypher below) -- not exercised against a live server in this repo's own test suite (no
account/service available here); reviewed directly against the `neo4j` driver and
`falkordb-py`'s documented `select_graph`/`query` API, same caveat as the builder component.

Always emits one small inspection asset (`inspection_asset_name`), tagged
`dagster-community/knowledge-graph-search`, reporting the configured provider/graph settings,
mirroring `knowledge_search_server`'s own inspection-asset precedent.
"""
import logging
import os
import sys
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


# BEGIN knowledge-graph-store core (duplicated from knowledge_graph_builder's own write_graph/
# reuse_from helpers -- read-only subset -- see tests/test_drift.py)
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
            raise ValueError("KnowledgeGraphSearch: provider='neo4j' requires `uri`.")
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


# BEGIN sql_tables read path -- recursive-CTE graph traversal over knowledge_graph_builder's
# execution_mode='sql' output tables (<output_table>_nodes/<output_table>_edges). Same 3
# dialects as the builder's own SQL mode. get_neighbors/find_path use `WITH RECURSIVE` (BigQuery
# requires the RECURSIVE keyword; Snowflake accepts it but doesn't require it; Databricks
# requires Runtime 17.1+ -- GA Aug 2025, confirmed via Databricks' own docs -- older runtimes
# reject the keyword outright, with no graceful degradation available in SQL itself). Cycle
# prevention and the returned path are both carried as an array column through the recursion
# (ARRAY_APPEND/ARRAY_CONCAT/concat, per dialect) -- a documented, standard technique for all 3,
# not a repo-specific trick. No alias matching (the sql_tables node schema has no aliases
# column -- that's the python-mode 'resolve' step, not available in SQL mode).
def _sql_str(value: Any) -> str:
    s = str(value).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
    return f"'{s}'"


def _sql_qualify(name: str, dialect: str) -> str:
    return f"`{name}`" if dialect == "bigquery" else name


def _sql_tables_names(output_table: str, dialect: str) -> "Tuple[str, str]":
    return _sql_qualify(f"{output_table}_nodes", dialect), _sql_qualify(f"{output_table}_edges", dialect)


def _build_snowflake_connection(cfg: dict):
    import snowflake.connector
    account = os.environ.get(cfg.get("snowflake_account_env_var") or "SNOWFLAKE_ACCOUNT")
    user = os.environ.get(cfg.get("snowflake_user_env_var") or "SNOWFLAKE_USER")
    password = os.environ.get(cfg.get("snowflake_password_env_var") or "SNOWFLAKE_PASSWORD")
    if not account or not user:
        raise ValueError("KnowledgeGraphSearch: provider='sql_tables' sql_dialect='snowflake_cortex' requires snowflake_account_env_var and snowflake_user_env_var to be set in the environment.")
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
        raise ValueError("KnowledgeGraphSearch: provider='sql_tables' sql_dialect='databricks' requires databricks_host_env_var/databricks_token_env_var (set in the environment) and databricks_http_path.")
    from databricks import sql as databricks_sql
    return databricks_sql.connect(server_hostname=host, http_path=http_path, access_token=token)


def _maybe_json_list(value: Any) -> List[Any]:
    """Array-typed columns come back as a native list from most connectors, but defensively
    handle a JSON-encoded string too (wire-format details vary by connector/result-format
    version) rather than assume one or the other."""
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
    raise ValueError(f"KnowledgeGraphSearch: provider='sql_tables' requires sql_dialect to be one of 'snowflake_cortex', 'bigquery', 'databricks'; got {dialect!r}.")


def _sql_array_fns(dialect: str) -> Dict[str, Any]:
    # "empty" is explicitly typed ARRAY<STRING> (not a bare `[]`/`array()`): BigQuery/Databricks
    # infer an untyped empty array/literal's element type from context, and a recursive CTE's
    # anchor and recursive terms must agree column-for-column -- an inferred type that doesn't
    # match the recursive term's real STRING elements (relation names) raises a type-mismatch
    # error (confirmed with a DuckDB stand-in; sqlglot's parse-only check can't catch this since
    # it's a type error, not a syntax error). Snowflake's ARRAY holds heterogeneous VARIANT
    # elements, so ARRAY_CONSTRUCT() has no such ambiguity.
    if dialect == "bigquery":
        return {"ctor": lambda x: f"[{x}]", "empty": "CAST([] AS ARRAY<STRING>)", "append": lambda arr, x: f"ARRAY_CONCAT({arr}, [{x}])", "contains": lambda arr, x: f"{x} IN UNNEST({arr})"}
    if dialect == "databricks":
        return {"ctor": lambda x: f"array({x})", "empty": "CAST(array() AS ARRAY<STRING>)", "append": lambda arr, x: f"concat({arr}, array({x}))", "contains": lambda arr, x: f"array_contains({arr}, {x})"}
    return {"ctor": lambda x: f"ARRAY_CONSTRUCT({x})", "empty": "ARRAY_CONSTRUCT()", "append": lambda arr, x: f"ARRAY_APPEND({arr}, {x})",
            "contains": lambda arr, x: f"ARRAY_CONTAINS(({x})::VARIANT, {arr})"}


def _build_sql_neighbors_query(dialect: str, edges_tbl: str, name: str, hops: int, relation: Optional[str]) -> str:
    # Databricks also supports a `) MAX RECURSION LEVEL n` safety clause (confirmed against its own
    # docs), but it's not load-bearing here -- the `n.depth < hops` predicate below already bounds
    # recursion to a SMALLER depth than that platform's own default (100) ever would, for every
    # hops/max_hops value this component allows (capped at 3/6) -- and it's omitted since sqlglot
    # 28.0.0 has no parser support for it (confirmed directly), so it can't be sqlglot-validated
    # the way every other statement in this file is.
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
    return (f"SELECT id, name, type, mentions FROM {nodes_tbl} "
            f"WHERE LOWER(name) LIKE {q} {type_filter} "
            f"ORDER BY mentions DESC LIMIT {int(limit)}")


def _build_sql_top_entities_query(nodes_tbl: str, type: Optional[str], limit: int) -> str:
    type_filter = f"WHERE type = {_sql_str(type)} " if type is not None else ""
    return f"SELECT name, type, mentions FROM {nodes_tbl} {type_filter}ORDER BY mentions DESC LIMIT {int(limit)}"


def _build_sql_entity_relations_queries(edges_tbl: str, name: str) -> "Tuple[str, str]":
    qname = _sql_str(name)
    out_q = f"SELECT target AS other, relation, confidence, mention_count, evidence FROM {edges_tbl} WHERE source = {qname}"
    in_q = f"SELECT source AS other, relation, confidence, mention_count, evidence FROM {edges_tbl} WHERE target = {qname}"
    return out_q, in_q
# END sql_tables read path


def _check_required_env_var(var_name: Optional[str], *, label: str) -> None:
    if var_name and not os.environ.get(var_name):
        raise ValueError(
            f"KnowledgeGraphSearch: {label} env var {var_name!r} is not set in this process's "
            "environment. If this is a stdio MCP server, confirm it's listed in "
            "live_env_passthrough so its value is copied in from the spawning process at run time."
        )


class KnowledgeGraphSearchResource(dg.ConfigurableResource):
    """Searches a knowledge graph built by KnowledgeGraphBuilderComponent. `provider='networkx'`
    loads the graph file once, at resource construction, into memory (`_load()` is called lazily
    on first use and cached on the instance -- a pydantic ConfigurableResource can't hold extra
    mutable state as a declared field without it becoming part of the schema, so the cache lives
    in a module-level dict keyed by id(self) instead)."""

    provider: Literal["networkx", "neo4j", "falkordb", "sql_tables"] = Field(default="networkx", description="Graph store the knowledge graph lives in.")
    path: Optional[str] = Field(default=None, description="networkx only: local JSON file path (written by KnowledgeGraphBuilderComponent's write_graph step).")
    uri: Optional[str] = Field(default=None, description="neo4j only: bolt/neo4j connection URI.")
    username_env_var: Optional[str] = Field(default=None, description="neo4j/falkordb: env var holding the username.")
    password_env_var: Optional[str] = Field(default=None, description="neo4j/falkordb: env var holding the password.")
    database: str = Field(default="neo4j", description="neo4j only: database name.")
    host: str = Field(default="localhost", description="falkordb only: Redis host.")
    port: int = Field(default=6379, description="falkordb only: Redis port.")
    graph_name: str = Field(default="knowledge_graph", description="neo4j/falkordb: the `graph` tag / FalkorDB graph key the builder wrote under. networkx: only used to name the default file path when `path` is unset.")
    max_results: int = Field(default=50, description="Cap on rows returned by search_entities/get_neighbors/top_entities.")
    sql_dialect: Optional[str] = Field(default=None, description="sql_tables only: 'snowflake_cortex', 'bigquery', or 'databricks' -- same 3 dialects as knowledge_graph_builder's own execution_mode='sql'.")
    output_table: Optional[str] = Field(default=None, description="sql_tables only: the base table name knowledge_graph_builder's write_graph step wrote under (reads <output_table>_nodes/<output_table>_edges).")
    snowflake_account_env_var: str = Field(default="SNOWFLAKE_ACCOUNT", description="sql_tables + sql_dialect='snowflake_cortex': env var holding the Snowflake account identifier.")
    snowflake_user_env_var: str = Field(default="SNOWFLAKE_USER", description="sql_tables + sql_dialect='snowflake_cortex': env var holding the Snowflake username.")
    snowflake_password_env_var: Optional[str] = Field(default="SNOWFLAKE_PASSWORD", description="sql_tables + sql_dialect='snowflake_cortex': env var holding the Snowflake password.")
    snowflake_database: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='snowflake_cortex': database name.")
    snowflake_schema: Optional[str] = Field(default="PUBLIC", description="sql_tables + sql_dialect='snowflake_cortex': schema name.")
    snowflake_warehouse: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='snowflake_cortex': virtual warehouse. Uses the user default if unset.")
    databricks_host_env_var: str = Field(default="DATABRICKS_HOST", description="sql_tables + sql_dialect='databricks': env var holding the workspace URL.")
    databricks_token_env_var: str = Field(default="DATABRICKS_TOKEN", description="sql_tables + sql_dialect='databricks': env var holding a personal access token.")
    databricks_http_path: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='databricks': SQL warehouse HTTP path, e.g. /sql/1.0/warehouses/<id>.")
    bigquery_project_id: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='bigquery': GCP project for the BQ job. Defaults to the ambient credentials' project.")

    def _sql_cfg(self) -> dict:
        return {
            "sql_dialect": self.sql_dialect, "output_table": self.output_table,
            "snowflake_account_env_var": self.snowflake_account_env_var, "snowflake_user_env_var": self.snowflake_user_env_var,
            "snowflake_password_env_var": self.snowflake_password_env_var, "snowflake_database": self.snowflake_database,
            "snowflake_schema": self.snowflake_schema, "snowflake_warehouse": self.snowflake_warehouse,
            "databricks_host_env_var": self.databricks_host_env_var, "databricks_token_env_var": self.databricks_token_env_var,
            "databricks_http_path": self.databricks_http_path, "bigquery_project_id": self.bigquery_project_id,
        }

    def _cfg(self) -> dict:
        return {
            "path": self.path, "uri": self.uri, "username_env_var": self.username_env_var,
            "password_env_var": self.password_env_var, "database": self.database,
            "host": self.host, "port": self.port, "graph_name": self.graph_name,
        }

    def _graph(self):
        cache = _NETWORKX_GRAPH_CACHE
        key = id(self)
        if key not in cache:
            path = self.path or f"./{self.graph_name}.json"
            cache[key] = _build_networkx_graph(path)
        return cache[key]

    def _sql_tables(self) -> "Tuple[str, str]":
        if not self.output_table:
            raise ValueError("KnowledgeGraphSearch: provider='sql_tables' requires `output_table` (the base name knowledge_graph_builder's write_graph step wrote under).")
        if self.sql_dialect not in ("snowflake_cortex", "bigquery", "databricks"):
            raise ValueError(f"KnowledgeGraphSearch: provider='sql_tables' requires sql_dialect to be one of 'snowflake_cortex', 'bigquery', 'databricks'; got {self.sql_dialect!r}.")
        return _sql_tables_names(self.output_table, self.sql_dialect)

    def search_entities(self, query: str, type: Optional[str] = None) -> dict:
        q = query.lower()
        if self.provider == "sql_tables":
            nodes_tbl, _ = self._sql_tables()
            sql = _build_sql_search_entities_query(nodes_tbl, query, type, self.max_results)
            rows = _run_sql_tables_query(self._sql_cfg(), sql)
            return {"entities": [{"name": r["name"], "type": r["type"], "mention_count": r["mentions"], "aliases": None} for r in rows]}
        if self.provider == "networkx":
            g = self._graph()
            hits = []
            for name, data in g.nodes(data=True):
                aliases = data.get("aliases") or []
                if q in name.lower() or any(q in a.lower() for a in aliases):
                    if type is not None and data.get("type") != type:
                        continue
                    hits.append({"name": name, "type": data.get("type"), "mention_count": data.get("mention_count"), "aliases": aliases})
            return {"entities": hits[: self.max_results]}
        records = _run_graph_query(
            self.provider, self._cfg(),
            "MATCH (n {graph: $graph_name}) WHERE (toLower(n.name) CONTAINS $q "
            "OR any(a IN n.aliases WHERE toLower(a) CONTAINS $q)) AND ($type IS NULL OR n.type = $type) "
            "RETURN n.name AS name, n.type AS type, n.mention_count AS mention_count, n.aliases AS aliases LIMIT $limit",
            {"graph_name": self.graph_name, "q": q, "type": type, "limit": self.max_results},
        )
        return {"entities": records}

    def get_entity(self, name: str) -> dict:
        if self.provider == "sql_tables":
            nodes_tbl, edges_tbl = self._sql_tables()
            node_rows = _run_sql_tables_query(self._sql_cfg(), f"SELECT type, mentions FROM {nodes_tbl} WHERE id = {_sql_str(name)}")
            if not node_rows:
                return {"error": f"entity {name!r} not found"}
            out_q, in_q = _build_sql_entity_relations_queries(edges_tbl, name)
            out_rows = _run_sql_tables_query(self._sql_cfg(), out_q)
            in_rows = _run_sql_tables_query(self._sql_cfg(), in_q)
            relations = (
                [{"direction": "out", "relation": r["relation"], "other": r["other"], "confidence": r["confidence"], "mention_count": r["mention_count"], "evidence": r["evidence"]} for r in out_rows]
                + [{"direction": "in", "relation": r["relation"], "other": r["other"], "confidence": r["confidence"], "mention_count": r["mention_count"], "evidence": r["evidence"]} for r in in_rows]
            )
            return {"name": name, "type": node_rows[0]["type"], "aliases": None, "mention_count": node_rows[0]["mentions"], "relations": relations}
        if self.provider == "networkx":
            g = self._graph()
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
            self.provider, self._cfg(),
            "MATCH (n {name: $name, graph: $graph_name}) OPTIONAL MATCH (n)-[r]->(m) "
            "RETURN n.type AS type, n.aliases AS aliases, n.mention_count AS mention_count, "
            "'out' AS direction, r.relation AS relation, m.name AS other, r.confidence AS confidence, "
            "r.mention_count AS rel_mention_count, r.evidences[0] AS evidence",
            {"name": name, "graph_name": self.graph_name},
        )
        in_rows = _run_graph_query(
            self.provider, self._cfg(),
            "MATCH (n {name: $name, graph: $graph_name})<-[r]-(m) "
            "RETURN 'in' AS direction, r.relation AS relation, m.name AS other, r.confidence AS confidence, "
            "r.mention_count AS rel_mention_count, r.evidences[0] AS evidence",
            {"name": name, "graph_name": self.graph_name},
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
        hops = max(1, min(int(hops), 3))
        if self.provider == "sql_tables":
            nodes_tbl, edges_tbl = self._sql_tables()
            sql = _build_sql_neighbors_query(self.sql_dialect, edges_tbl, name, hops, relation)
            rows = _run_sql_tables_query(self._sql_cfg(), sql)
            names = [r["name"] for r in rows]
            types = {}
            if names:
                names_sql = ", ".join(_sql_str(n) for n in names)
                types = {r["id"]: r["type"] for r in _run_sql_tables_query(self._sql_cfg(), f"SELECT id, type FROM {nodes_tbl} WHERE id IN ({names_sql})")}
            results = [{"name": r["name"], "type": types.get(r["name"]), "distance": int(r["depth"])} for r in rows]
            return {"neighbors": results[: self.max_results]}
        if self.provider == "networkx":
            g = self._graph()
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
            results = [
                {"name": n, "type": g.nodes[n].get("type"), "distance": d}
                for n, d in sorted(visited.items(), key=lambda kv: kv[1]) if n != name
            ]
            return {"neighbors": results[: self.max_results]}
        query = (
            f"MATCH p=(n {{name: $name, graph: $graph_name}})-[r*1..{hops}]-(m) "
            "WHERE $relation IS NULL OR all(rel IN r WHERE rel.relation = $relation) "
            "RETURN DISTINCT m.name AS name, m.type AS type, length(p) AS distance "
            "ORDER BY distance LIMIT $limit"
        )
        records = _run_graph_query(self.provider, self._cfg(), query, {"name": name, "graph_name": self.graph_name, "relation": relation, "limit": self.max_results})
        return {"neighbors": records}

    def find_path(self, source: str, target: str, max_hops: int = 4) -> dict:
        max_hops = max(1, min(int(max_hops), 6))
        if self.provider == "sql_tables":
            _, edges_tbl = self._sql_tables()
            sql = _build_sql_path_query(self.sql_dialect, edges_tbl, source, target, max_hops)
            rows = _run_sql_tables_query(self._sql_cfg(), sql)
            if not rows:
                return {"path": None}
            path = _maybe_json_list(rows[0]["path"])
            rels = _maybe_json_list(rows[0]["rels"])
            return {"path": path, "relations": rels}
        if self.provider == "networkx":
            import networkx as nx
            g = self._graph()
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
        records = _run_graph_query(self.provider, self._cfg(), query, {"source": source, "target": target, "graph_name": self.graph_name})
        if not records:
            return {"path": None}
        return {"path": records[0]["path"], "relations": records[0]["relations"]}

    def top_entities(self, type: Optional[str] = None) -> dict:
        if self.provider == "sql_tables":
            nodes_tbl, _ = self._sql_tables()
            sql = _build_sql_top_entities_query(nodes_tbl, type, self.max_results)
            rows = _run_sql_tables_query(self._sql_cfg(), sql)
            return {"entities": [{"name": r["name"], "type": r["type"], "mention_count": r["mentions"]} for r in rows]}
        if self.provider == "networkx":
            g = self._graph()
            rows = [
                {"name": n, "type": data.get("type"), "mention_count": data.get("mention_count", 0)}
                for n, data in g.nodes(data=True) if type is None or data.get("type") == type
            ]
            rows.sort(key=lambda r: -(r["mention_count"] or 0))
            return {"entities": rows[: self.max_results]}
        records = _run_graph_query(
            self.provider, self._cfg(),
            "MATCH (n {graph: $graph_name}) WHERE $type IS NULL OR n.type = $type "
            "RETURN n.name AS name, n.type AS type, n.mention_count AS mention_count "
            "ORDER BY n.mention_count DESC LIMIT $limit",
            {"graph_name": self.graph_name, "type": type, "limit": self.max_results},
        )
        return {"entities": records}


_NETWORKX_GRAPH_CACHE: Dict[int, Any] = {}


class _McpServerShapeResource(dg.ConfigurableResource):
    """Minimal MCPServerResource-shaped resource -- inlined per this repo's convention, same
    field shape as knowledge_search_server's/dagster_workflow_tools' own copy (checked in
    tests/test_drift.py)."""

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
        "knowledge_graph_search_server's stdio MCP server must be run as "
        "`python -m <dotted.path.to.this.module>`, not `python component.py` directly "
        "-- the spawned server's own command needs a real importable module name "
        "(available via __spec__.name). See README.md."
    )


class KnowledgeGraphSearchServerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Registers a `KnowledgeGraphSearchResource` (default key `knowledge_graph_search`) and,
    always, a small inspection asset reporting its settings. Run THIS FILE as the stdio MCP
    server for agentic_pipeline (`python -m dagster_community_components.assets.ai.
    knowledge_graph_search_server.component`) -- see README.md."""

    resource_key: str = Field(default="knowledge_graph_search", description="Dagster resource key the in-process KnowledgeGraphSearchResource is registered under.")
    mcp_server_resource_key: Optional[str] = Field(
        default=None,
        description=(
            "If set, ALSO registers a stdio MCP-server-shaped resource under this key, built from "
            "this component's own settings (python -m this module, with KNOWLEDGE_GRAPH_SEARCH_* "
            "env vars). Any mcp_servers-aware component can then reference it via server_resource_key."
        ),
    )
    provider: Literal["networkx", "neo4j", "falkordb", "sql_tables"] = Field(default="networkx", description="Graph store the knowledge graph lives in.")
    path: Optional[str] = Field(default=None, description="networkx only: local JSON file path (written by KnowledgeGraphBuilderComponent's write_graph step).")
    uri: Optional[str] = Field(default=None, description="neo4j only: bolt/neo4j connection URI.")
    username_env_var: Optional[str] = Field(default=None, description="neo4j/falkordb: env var holding the username.")
    password_env_var: Optional[str] = Field(default=None, description="neo4j/falkordb: env var holding the password.")
    database: str = Field(default="neo4j", description="neo4j only: database name.")
    host: str = Field(default="localhost", description="falkordb only: Redis host.")
    port: int = Field(default=6379, description="falkordb only: Redis port.")
    graph_name: str = Field(default="knowledge_graph", description="neo4j/falkordb: the `graph` tag / FalkorDB graph key the builder wrote under. networkx: only used to name the default file path when `path` is unset.")
    max_results: int = Field(default=50, description="Cap on rows returned by search_entities/get_neighbors/top_entities.")
    sql_dialect: Optional[str] = Field(default=None, description="sql_tables only: 'snowflake_cortex', 'bigquery', or 'databricks' -- same 3 dialects as knowledge_graph_builder's own execution_mode='sql'.")
    output_table: Optional[str] = Field(default=None, description="sql_tables only: the base table name knowledge_graph_builder's write_graph step wrote under (reads <output_table>_nodes/<output_table>_edges).")
    snowflake_account_env_var: str = Field(default="SNOWFLAKE_ACCOUNT", description="sql_tables + sql_dialect='snowflake_cortex': env var holding the Snowflake account identifier.")
    snowflake_user_env_var: str = Field(default="SNOWFLAKE_USER", description="sql_tables + sql_dialect='snowflake_cortex': env var holding the Snowflake username.")
    snowflake_password_env_var: Optional[str] = Field(default="SNOWFLAKE_PASSWORD", description="sql_tables + sql_dialect='snowflake_cortex': env var holding the Snowflake password.")
    snowflake_database: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='snowflake_cortex': database name.")
    snowflake_schema: Optional[str] = Field(default="PUBLIC", description="sql_tables + sql_dialect='snowflake_cortex': schema name.")
    snowflake_warehouse: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='snowflake_cortex': virtual warehouse. Uses the user default if unset.")
    databricks_host_env_var: str = Field(default="DATABRICKS_HOST", description="sql_tables + sql_dialect='databricks': env var holding the workspace URL.")
    databricks_token_env_var: str = Field(default="DATABRICKS_TOKEN", description="sql_tables + sql_dialect='databricks': env var holding a personal access token.")
    databricks_http_path: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='databricks': SQL warehouse HTTP path, e.g. /sql/1.0/warehouses/<id>.")
    bigquery_project_id: Optional[str] = Field(default=None, description="sql_tables + sql_dialect='bigquery': GCP project for the BQ job. Defaults to the ambient credentials' project.")
    inspection_asset_name: str = Field(default="knowledge_graph_search_tools", description="Asset key for the inspection asset reporting this server's configured settings.")
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
        return ComponentFormConfig(label="Knowledge Graph Search Server", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> Definitions:
        if self.provider == "neo4j" and not self.uri:
            raise ValueError("KnowledgeGraphSearchServerComponent: provider='neo4j' requires `uri`.")
        if self.provider == "sql_tables":
            if not self.output_table:
                raise ValueError("KnowledgeGraphSearchServerComponent: provider='sql_tables' requires `output_table`.")
            if self.sql_dialect not in ("snowflake_cortex", "bigquery", "databricks"):
                raise ValueError("KnowledgeGraphSearchServerComponent: provider='sql_tables' requires sql_dialect to be one of 'snowflake_cortex', 'bigquery', 'databricks'.")
        resource = KnowledgeGraphSearchResource(
            provider=self.provider, path=self.path, uri=self.uri, username_env_var=self.username_env_var,
            password_env_var=self.password_env_var, database=self.database, host=self.host, port=self.port,
            graph_name=self.graph_name, max_results=self.max_results,
            sql_dialect=self.sql_dialect, output_table=self.output_table,
            snowflake_account_env_var=self.snowflake_account_env_var, snowflake_user_env_var=self.snowflake_user_env_var,
            snowflake_password_env_var=self.snowflake_password_env_var, snowflake_database=self.snowflake_database,
            snowflake_schema=self.snowflake_schema, snowflake_warehouse=self.snowflake_warehouse,
            databricks_host_env_var=self.databricks_host_env_var, databricks_token_env_var=self.databricks_token_env_var,
            databricks_http_path=self.databricks_http_path, bigquery_project_id=self.bigquery_project_id,
        )
        resources: Dict[str, Any] = {self.resource_key: resource}

        if self.mcp_server_resource_key:
            mcp_env = {
                "KNOWLEDGE_GRAPH_SEARCH_PROVIDER": self.provider,
                "KNOWLEDGE_GRAPH_SEARCH_GRAPH_NAME": self.graph_name,
                "KNOWLEDGE_GRAPH_SEARCH_DATABASE": self.database,
                "KNOWLEDGE_GRAPH_SEARCH_HOST": self.host,
                "KNOWLEDGE_GRAPH_SEARCH_PORT": str(self.port),
                "KNOWLEDGE_GRAPH_SEARCH_MAX_RESULTS": str(self.max_results),
            }
            if self.path:
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_PATH"] = self.path
            if self.uri:
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_URI"] = self.uri
            if self.username_env_var:
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_USERNAME_ENV_VAR"] = self.username_env_var
            if self.password_env_var:
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_PASSWORD_ENV_VAR"] = self.password_env_var
            if self.provider == "sql_tables":
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_SQL_DIALECT"] = self.sql_dialect
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_OUTPUT_TABLE"] = self.output_table
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_ACCOUNT_ENV_VAR"] = self.snowflake_account_env_var
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_USER_ENV_VAR"] = self.snowflake_user_env_var
                if self.snowflake_password_env_var:
                    mcp_env["KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_PASSWORD_ENV_VAR"] = self.snowflake_password_env_var
                if self.snowflake_database:
                    mcp_env["KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_DATABASE"] = self.snowflake_database
                if self.snowflake_schema:
                    mcp_env["KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_SCHEMA"] = self.snowflake_schema
                if self.snowflake_warehouse:
                    mcp_env["KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_WAREHOUSE"] = self.snowflake_warehouse
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_DATABRICKS_HOST_ENV_VAR"] = self.databricks_host_env_var
                mcp_env["KNOWLEDGE_GRAPH_SEARCH_DATABRICKS_TOKEN_ENV_VAR"] = self.databricks_token_env_var
                if self.databricks_http_path:
                    mcp_env["KNOWLEDGE_GRAPH_SEARCH_DATABRICKS_HTTP_PATH"] = self.databricks_http_path
                if self.bigquery_project_id:
                    mcp_env["KNOWLEDGE_GRAPH_SEARCH_BIGQUERY_PROJECT_ID"] = self.bigquery_project_id
            passthrough = [v for v in (
                self.username_env_var, self.password_env_var,
                self.snowflake_account_env_var if self.provider == "sql_tables" else None,
                self.snowflake_user_env_var if self.provider == "sql_tables" else None,
                self.snowflake_password_env_var if self.provider == "sql_tables" else None,
                self.databricks_host_env_var if self.provider == "sql_tables" else None,
                self.databricks_token_env_var if self.provider == "sql_tables" else None,
            ) if v]
            resources[self.mcp_server_resource_key] = _McpServerShapeResource(
                name=self.mcp_server_resource_key, type="stdio",
                command=[sys.executable, "-m", _this_module_name()], env=mcp_env,
                live_env_passthrough=passthrough,
            )

        tags = dict(self.asset_tags or {})
        tags.setdefault("dagster-community/knowledge-graph-search", self.graph_name)

        provider, graph_name, path_ = self.provider, self.graph_name, self.path

        @asset(
            key=AssetKey.from_user_string(self.inspection_asset_name),
            group_name=self.group_name,
            description=self.description or "Reports the knowledge-graph-search server's configured provider/graph settings.",
            owners=self.owners or [],
            tags=tags,
        )
        def _inspection_asset(context: AssetExecutionContext):
            md = {
                "provider": MetadataValue.text(provider),
                "graph_name": MetadataValue.text(graph_name),
                "tool_names": MetadataValue.json(["search_entities", "get_entity", "get_neighbors", "find_path", "top_entities"]),
                "tool_descriptions": MetadataValue.json({
                    "search_entities": "Fuzzy name/alias search over entities, optionally filtered by type.",
                    "get_entity": "Exact-name lookup: an entity's attributes, relations, and evidence.",
                    "get_neighbors": "Entities within N hops (<=3) of a given entity, optionally filtered by relation.",
                    "find_path": "Shortest path (<=6 hops) between two entities.",
                    "top_entities": "Entities ranked by mention_count, optionally filtered by type.",
                }),
            }
            if path_:
                md["path"] = MetadataValue.text(path_)
            context.add_output_metadata(md)

        return Definitions(assets=[_inspection_asset], resources=resources)


# ── stdio MCP server entrypoint:  python -m <dotted.path.to.this.module>  ───────────────
def build_mcp_server(resource: KnowledgeGraphSearchResource, server_name: str = "knowledge-graph-search"):
    try:
        from mcp.server.mcpserver import MCPServer as _Server
    except ImportError:
        from mcp.server.fastmcp import FastMCP as _Server

    mcp = _Server(server_name)

    @mcp.tool()
    def search_entities(query: str, type: Optional[str] = None) -> dict:
        """Fuzzy name/alias search over entities. Returns {"entities": [{name, type, mention_count,
        aliases}, ...]} on success, or {"error": "..."}."""
        return _safe(resource.search_entities, query, type)

    @mcp.tool()
    def get_entity(name: str) -> dict:
        """Exact-name lookup. Returns {name, type, aliases, mention_count, relations: [{direction,
        relation, other, confidence, mention_count, evidence}, ...]} or {"error": "..."}."""
        return _safe(resource.get_entity, name)

    @mcp.tool()
    def get_neighbors(name: str, hops: int = 1, relation: Optional[str] = None) -> dict:
        """Entities within `hops` (capped at 3) of `name`, optionally filtered by relation. Returns
        {"neighbors": [{name, type, distance}, ...]} or {"error": "..."}."""
        return _safe(resource.get_neighbors, name, hops, relation)

    @mcp.tool()
    def find_path(source: str, target: str, max_hops: int = 4) -> dict:
        """Shortest path (capped at 6 hops) between two entities. Returns {"path": [names] or null,
        "relations": [relation per hop]} or {"error": "..."}."""
        return _safe(resource.find_path, source, target, max_hops)

    @mcp.tool()
    def top_entities(type: Optional[str] = None) -> dict:
        """Entities ranked by mention_count, optionally filtered by type. Returns {"entities": [...]}."""
        return _safe(resource.top_entities, type)

    return mcp


def _safe(fn, *args, **kwargs) -> dict:
    try:
        return fn(*args, **kwargs)
    except Exception as e:  # noqa: BLE001 -- surface as data, the agent loop must not crash
        return {"error": f"{type(e).__name__}: {e}"}


def main(argv: Optional[List[str]] = None) -> None:
    """Env: KNOWLEDGE_GRAPH_SEARCH_PROVIDER (default networkx), KNOWLEDGE_GRAPH_SEARCH_PATH
    (networkx), KNOWLEDGE_GRAPH_SEARCH_URI/_USERNAME_ENV_VAR/_PASSWORD_ENV_VAR/_DATABASE (neo4j),
    KNOWLEDGE_GRAPH_SEARCH_HOST/_PORT (falkordb), KNOWLEDGE_GRAPH_SEARCH_GRAPH_NAME,
    KNOWLEDGE_GRAPH_SEARCH_MAX_RESULTS (default 50). provider='sql_tables':
    KNOWLEDGE_GRAPH_SEARCH_SQL_DIALECT, _OUTPUT_TABLE, _SNOWFLAKE_ACCOUNT_ENV_VAR/_USER_ENV_VAR/
    _PASSWORD_ENV_VAR/_DATABASE/_SCHEMA/_WAREHOUSE, _DATABRICKS_HOST_ENV_VAR/_TOKEN_ENV_VAR/
    _HTTP_PATH, _BIGQUERY_PROJECT_ID."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(message)s")
    resource = KnowledgeGraphSearchResource(
        provider=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_PROVIDER", "networkx"),
        path=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_PATH"),
        uri=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_URI"),
        username_env_var=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_USERNAME_ENV_VAR"),
        password_env_var=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_PASSWORD_ENV_VAR"),
        database=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_DATABASE", "neo4j"),
        host=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_HOST", "localhost"),
        port=int(os.environ.get("KNOWLEDGE_GRAPH_SEARCH_PORT", "6379")),
        graph_name=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_GRAPH_NAME", "knowledge_graph"),
        max_results=int(os.environ.get("KNOWLEDGE_GRAPH_SEARCH_MAX_RESULTS", "50")),
        sql_dialect=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_SQL_DIALECT"),
        output_table=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_OUTPUT_TABLE"),
        snowflake_account_env_var=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_ACCOUNT_ENV_VAR", "SNOWFLAKE_ACCOUNT"),
        snowflake_user_env_var=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_USER_ENV_VAR", "SNOWFLAKE_USER"),
        snowflake_password_env_var=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_PASSWORD_ENV_VAR", "SNOWFLAKE_PASSWORD"),
        snowflake_database=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_DATABASE"),
        snowflake_schema=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_SCHEMA", "PUBLIC"),
        snowflake_warehouse=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_SNOWFLAKE_WAREHOUSE"),
        databricks_host_env_var=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_DATABRICKS_HOST_ENV_VAR", "DATABRICKS_HOST"),
        databricks_token_env_var=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_DATABRICKS_TOKEN_ENV_VAR", "DATABRICKS_TOKEN"),
        databricks_http_path=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_DATABRICKS_HTTP_PATH"),
        bigquery_project_id=os.environ.get("KNOWLEDGE_GRAPH_SEARCH_BIGQUERY_PROJECT_ID"),
    )
    build_mcp_server(resource).run(transport="stdio")


if __name__ == "__main__":
    main()
