"""A generic fake warehouse connection for Snowflake and Databricks SQL, backed by DuckDB (sibling of bq_fake.py).

It EXECUTES the real SQL the code under test sends: each statement is parsed with sqlglot in the given READ dialect ('snowflake' /
'databricks' -- SQL that is not valid in that dialect fails the test), transpiled to DuckDB and run. This checks syntax and the
semantics of OUR logic (joins, windows, filters, JSON extraction, scoping); it is not Snowflake / Databricks themselves.

sqlglot's snowflake/databricks -> duckdb transpile mistranslates a handful of constructs. Each is patched NARROWLY in `_patch_duckdb`
(on the transpiled text, never on the SQL under test), with the reason next to it. Where a construct has no DuckDB counterpart at all
(the warehouse AI / splitter functions; the AI ones are call-counting UDFs from ai_stubs.py) a macro or UDF stands in so the surrounding statement can run:
  - SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER  -> `cortex_split`, a fixed-stride splitter (Cortex's real splitter is smarter)
  - Snowflake IS_ARRAY / Databricks FROM_JSON(.., 'array<string>') -> `is_array` / `from_json_strings`, JSON-array helpers
Three-part names (DB.SCH.t / `cat`.`sch`.`t`) lose their first part: a DuckDB schema stands in for the catalog.schema pair's schema.
"""
import atexit
import contextlib
import re
import weakref
from typing import Any, List

import duckdb
import sqlglot
from sqlglot import exp

from . import ai_stubs

_OPEN: "weakref.WeakSet" = weakref.WeakSet()


@atexit.register
def _close_all():  # close DuckDB connections before interpreter teardown (avoids native-thread / mutex races at exit)
    for w in list(_OPEN):
        w.close()


def _patch_duckdb(sql: str, read: str) -> str:
    # sqlglot renders JSON string extraction as CAST(JSON(x) -> '$.k' AS TEXT), which keeps the JSON quotes ('"cited"'); Snowflake's
    # ::STRING / ::FLOAT of a variant gives the bare value. Use ->> (unquoted text) and DOUBLE (Snowflake FLOAT is 64-bit; sqlglot said REAL).
    sql = re.sub(r"CAST\(JSON\((\w+)\) -> ('[^']*') AS (?:TEXT|REAL)\)",
                 lambda m: f"({m.group(1)} ->> {m.group(2)})" if "AS TEXT" in m.group(0) else f"CAST({m.group(1)} ->> {m.group(2)} AS DOUBLE)", sql)
    # Snowflake TO_ARRAY(variant array) is the array itself; sqlglot renders a one-element wrapper [ids].
    sql = sql.replace("CASE WHEN ids IS NULL THEN NULL ELSE [ids] END", "from_json_strings(ids::VARCHAR)")
    # VARIANT has no DuckDB type; ARRAY_CONTAINS(elem::VARIANT, arr) is just ARRAY_CONTAINS(arr, elem) on strings
    sql = sql.replace(" AS VARIANT)", " AS VARCHAR)")
    # LATERAL FLATTEN(input => f(x)) AS chunk: rendered as an invalid `, CROSS JOIN UNNEST(input => ...)`; value/index are 0-based in Snowflake
    sql = re.sub(r",\s+CROSS JOIN UNNEST\(input => (.*?)\) AS chunk\(SEQ, KEY, PATH, INDEX, VALUE, THIS\)",
                 r" CROSS JOIN UNNEST(\1) WITH ORDINALITY AS chunk(value, idx1)", sql)
    sql = sql.replace("chunk.index AS chunk_index", "(chunk.idx1 - 1) AS chunk_index")
    # Databricks `LATERAL VIEW posexplode(arr) chunks AS chunk_index, chunk_text`: rendered as UNNEST .. WITH ORDINALITY AS chunks(chunk_index,
    # chunk_text) -- column order reversed (ordinality comes last) and 1-based (posexplode is 0-based).
    sql = sql.replace("AS chunks(chunk_index, chunk_text)", "AS chunks(chunk_text, idx1)")
    sql = sql.replace("chunks.chunk_index AS chunk_index", "(chunks.idx1 - 1) AS chunk_index")
    # Spark from_json(str, 'array<string>') takes a Spark schema string; DuckDB's takes a DuckDB one
    sql = sql.replace("FROM_JSON(ids, 'array<string>')", "from_json_strings(ids)")
    return sql


class FakeWarehouse:
    def __init__(self, read: str):
        assert read in ("snowflake", "databricks")
        self.read = read
        self.con = duckdb.connect()
        self.queries: List[str] = []
        _OPEN.add(self)
        self.con.execute("SET TimeZone = 'UTC'")
        self.con.execute("CREATE MACRO cortex_split(t, m, size, ov) AS list_filter(list_transform("
                         "generate_series(1, greatest(length(t), 1), greatest(size - ov, 1)), p -> substr(t, p, size)), x -> length(trim(x)) > 0)")
        self.ai = ai_stubs.register(self.con)        # call-counting stand-ins for the Cortex / ai_query / ai_classify functions
        self.con.execute("CREATE MACRO is_array(x) AS json_type(x) = 'ARRAY'")
        self.con.execute("CREATE MACRO from_json_strings(x) AS CAST(CAST(x AS JSON) AS VARCHAR[])")

    def to_duckdb(self, sql: str) -> str:
        # Delta `INSERT INTO t REPLACE WHERE cond SELECT ..` (atomic replace of the matching rows) has no DuckDB form: emulated as DELETE + INSERT.
        # The atomicity is NOT modelled; only the resulting rows are.
        m = re.match(r"\s*INSERT INTO (\S+) REPLACE WHERE (.*?) SELECT (.*)$", sql, re.S)
        if m:
            sql = f"DELETE FROM {m.group(1)} WHERE {m.group(2)}; INSERT INTO {m.group(1)} SELECT {m.group(3)}"
        sql = ai_stubs.rewrite_text(sql.replace("SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(", "cortex_split("))
        out = []
        for stmt in sqlglot.parse(sql, read=self.read):             # raises ParseError for SQL that is not valid in the dialect
            if stmt is None:
                continue
            for t in stmt.find_all(exp.Table):
                if t.args.get("catalog"):
                    t.set("catalog", None)
            out.append(_patch_duckdb(stmt.sql(dialect="duckdb"), self.read))
        return "; ".join(out)

    def run(self, sql: str):
        self.queries.append(sql)
        return self.con.execute(self.to_duckdb(sql))

    def rows(self, sql: str) -> List[dict]:
        cur = self.run(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def close(self):
        with contextlib.suppress(Exception):
            self.con.close()


# ── the interfaces _sql_session understands ──────────────────────────────────────────────────────────────

class _Cursor:
    """snowflake-connector style: cursor.execute(sql) then fetchall()."""
    def __init__(self, wh): self.wh, self._cur = wh, None
    def execute(self, sql):
        self._cur = self.wh.run(sql)
        return self
    def fetchall(self): return self._cur.fetchall()


class _CursorOnlyConnection:
    def __init__(self, wh): self.wh = wh
    def cursor(self): return _Cursor(self.wh)


class CursorStyleResource:
    """Like dagster-snowflake's SnowflakeResource: `with resource.get_connection() as conn`, conn has .cursor() only (no .execute)."""
    def __init__(self, wh): self.wh = wh
    @contextlib.contextmanager
    def get_connection(self):
        yield _CursorOnlyConnection(self.wh)


class _EngineConn:
    def __init__(self, wh): self.wh = wh
    def exec_driver_sql(self, sql): return self.wh.run(sql)


class _Engine:
    def __init__(self, wh): self.wh = wh
    @contextlib.contextmanager
    def begin(self): yield _EngineConn(self.wh)


class EngineStyleResource:
    """SQLAlchemy engine style: `resource.get_engine().begin()` -> conn.exec_driver_sql(sql).fetchall()."""
    def __init__(self, wh): self.wh = wh
    def get_engine(self): return _Engine(self.wh)


class ConnectionStyleResource:
    """DB-API connection with .execute (e.g. databricks-sql / duckdb-like): `with resource.get_connection() as conn`."""
    def __init__(self, wh): self.wh = wh
    @contextlib.contextmanager
    def get_connection(self):
        class C:
            execute = staticmethod(self.wh.run)
        yield C()
