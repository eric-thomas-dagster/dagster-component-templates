"""Seeded fuzzing of the SQL string-literal escapers and the partition-placeholder binder (deterministic: fixed seed, a few thousand cases).

``_standard_sql_str`` (single quotes doubled) is executed as ``SELECT <literal>`` in REAL DuckDB and must return the exact original string.
``_sql_str`` (backslash escapes; BigQuery / Databricks / Snowflake) is parsed with sqlglot in each of those dialects and the literal value
extracted: that proves the escaping against sqlglot's model of each tokenizer, not against the engines themselves. NUL is excluded for DuckDB
(its parser rejects U+0000 in SQL text) and noted separately. Injection payloads must stay inside one literal."""
import random
from types import SimpleNamespace

import pytest

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")
from sqlglot import exp  # noqa: E402

from .conftest import load_component_module  # noqa: E402

SEED = 20261019
ALPHABET = ["'", "''", "\\", "\\'", "\n", "\r", "\t", "%", "%s", "%(x)s", ":name", "::", "--", "/*", "*/", "{", "}", "{{", "}}", "{partition_key}",
            '"', "`", ";", " ", "a", "Z", "0", "é", "日本", "😀", " ", "'); DROP TABLE x; --", "' OR '1'='1", "\\\\", "$$", "?"]
INJECTIONS = ["'); DROP TABLE x; --", "' OR '1'='1", "\\'; DELETE FROM t; --", "'/*", "x' UNION SELECT 1 --", "\\"]


@pytest.fixture(scope="module")
def mod():
    return load_component_module()


def strings(n, *, nul=False, seed=SEED):
    rng = random.Random(seed)
    alphabet = ALPHABET + (["\0"] if nul else [])
    out = list(INJECTIONS) + ["", "'", "\\", "\\\\'", "''''"]
    for _ in range(n):
        out.append("".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12))))
    return out


def literal_values(sql, dialect):
    tree = sqlglot.parse_one(sql, read=dialect)
    return tree, [lit.this for lit in tree.find_all(exp.Literal) if lit.is_string]


def test_standard_sql_str_round_trips_in_real_duckdb(mod):
    con = duckdb.connect()
    for s in strings(3000):
        assert con.execute("SELECT " + mod._standard_sql_str(s)).fetchone()[0] == s, repr(s)


def test_standard_sql_str_injection_stays_inside_one_literal(mod):
    con = duckdb.connect()
    con.execute("CREATE TABLE x(a INT)")
    for s in INJECTIONS:
        stmts = con.execute("SELECT " + mod._standard_sql_str(s) + " AS v").fetchall()
        assert stmts == [(s,)]
    assert con.execute("SELECT COUNT(*) FROM x").fetchone()[0] == 0           # still exists, nothing ran
    assert mod._standard_sql_str(5) == "'5'" and mod._standard_sql_str(None) == "'None'"


@pytest.mark.parametrize("dialect", ["bigquery", "databricks", "snowflake"])
def test_sql_str_round_trips_in_sqlglot_dialects(mod, dialect):
    for s in strings(1500, nul=True):
        tree, values = literal_values("SELECT " + mod._sql_str(s) + " AS v", dialect)
        assert values == [s], (dialect, s, mod._sql_str(s))
        assert len(list(tree.find_all(exp.Select))) == 1 and not list(tree.find_all(exp.Drop))


def test_sql_str_composes_with_other_text(mod):
    # the literal is embedded mid-statement in real use: a trailing backslash must not swallow the closing quote
    for s in ["\\", "a\\", "\\\\", "end\\'"]:
        for dialect in ("bigquery", "databricks", "snowflake"):
            tree, values = literal_values(f"SELECT {mod._sql_str(s)} AS a, {mod._sql_str('next')} AS b", dialect)
            assert values == [s, "next"], (dialect, s)


def ctx(key):
    return SimpleNamespace(has_partition_key=True, has_partition_key_range=False, partition_key=key,
                           log=SimpleNamespace(warning=lambda *a, **k: None))


def test_partition_binder_round_trips_random_keys_in_duckdb(mod):
    con = duckdb.connect()
    sql = "SELECT {partition_key} AS k, '{{literal}}' AS l, 'a:b' AS c WHERE {partition_key} = {partition_key}"
    for key in strings(1500):
        out = mod._bind_partition_placeholders(sql, ctx(key), mod._standard_sql_str)
        row = con.execute(out).fetchone()
        assert row == (key, "{literal}", "a:b"), (key, out)


def test_partition_binder_invariants(mod):
    q = mod._standard_sql_str
    assert mod._bind_partition_placeholders("SELECT 1 WHERE x = {{y}}", ctx("k"), q) == "SELECT 1 WHERE x = {y}"
    with pytest.raises(ValueError, match="quoted string literal"):
        mod._bind_partition_placeholders("SELECT '{partition_key}'", ctx("k"), q)
    with pytest.raises(ValueError, match="not available"):
        mod._bind_partition_placeholders("SELECT {nope}", ctx("k"), q)
    # a key that itself looks like a placeholder or an escaped brace is data, never re-expanded
    for key in ["{partition_key}", "{{x}}", "}}{{", "{partition_start}"]:
        out = mod._bind_partition_placeholders("SELECT {partition_key} AS k", ctx(key), q)
        assert duckdb.connect().execute(out).fetchone()[0] == key
    with pytest.raises(ValueError, match="no partition key"):
        mod._bind_partition_placeholders("SELECT {partition_key}", SimpleNamespace(has_partition_key=False, has_partition_key_range=False), q)


def test_partition_binder_with_the_backslash_escaper_in_sqlglot(mod):
    for key in strings(600, nul=True):
        out = mod._bind_partition_placeholders("SELECT {partition_key} AS k", ctx(key), mod._sql_str)
        for dialect in ("bigquery", "snowflake", "databricks"):
            assert literal_values(out, dialect)[1] == [key], (dialect, key)
