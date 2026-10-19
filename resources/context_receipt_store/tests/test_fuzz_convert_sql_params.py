"""Seeded fuzzing (fixed seed, deterministic, a few thousand cases per style) of ``_convert_sql_params``, the ``:name`` -> PEP-249 paramstyle rewriter.

Random statements mix placeholders, quoted literals that LOOK like parameters (':x', '?', '%s', '--', '/*'), comments (whose text also looks
like parameters), CAST()s and IN-lists; random values (str with quotes/backslashes/newlines/unicode, int, float, NaN/inf, None, bool). Each
statement is converted for every paramstyle and EXECUTED in real sqlite3 (qmark / numeric / named natively; format / pyformat by the driver's own
rule: client-side ``%`` substitution of SQL literals) and real DuckDB (qmark); the result must equal the values we meant to bind, and (when it has
no ``:word`` inside literals, which SQLAlchemy's text() would read as a bind) equal SQLAlchemy ``text()``'s result for the same values.
Invariants: placeholder count == parameters used, text outside placeholders is unchanged."""
import math
import random
import re
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from .backends import load_module  # noqa: E402

SEED = 20261019
STYLES = ["qmark", "numeric", "named", "format", "pyformat"]
TEXT_PIECES = ["a", "Zz", "0", " ", "'", "''", "\\", "\n", "\t", "%", "%s", "%(p0)s", ":p0", ":x", "::", "?", "--", "/*", "*/", "é", "日本", "😀", '"', "`", ";", "$1", "\\'",
               "'); DROP TABLE x; --"]
COMMENT_PIECES = [p for p in TEXT_PIECES if p not in ("\n", "*/")]


@pytest.fixture(scope="module")
def mod():
    return load_module("context_receipt_store_fuzz_convert")


def rtext(rng, pieces=TEXT_PIECES, n=8):
    return "".join(rng.choice(pieces) for _ in range(rng.randint(0, n)))


def junk(rng, pieces):
    """Comment text that cannot close the comment by accident (random pieces can concatenate into ``*/``)."""
    t = rtext(rng, pieces)
    while "*/" in t:
        t = t.replace("*/", "* /")
    return t


def rvalue(rng, strings_only=False):
    kind = rng.choice(["str", "str", "int", "float", "none", "bool", "nan"] if not strings_only else ["str", "none"])
    return {"str": lambda: rtext(rng), "int": lambda: rng.randint(-2 ** 40, 2 ** 40), "float": lambda: rng.choice([0.5, -2.25, 1e-3, 1e15]),
            "none": lambda: None, "bool": lambda: rng.random() < 0.5, "nan": lambda: rng.choice([float("nan"), float("inf"), float("-inf")])}[kind]()


def expect(v):
    if isinstance(v, float) and not math.isfinite(v):
        return None
    return int(v) if isinstance(v, bool) else v


def case(rng, colon_free=False, no_nested_comment=False):
    """-> (sql, params, expected_row, n_placeholders_used).
    ``colon_free``: nothing that SQLAlchemy's text() would read as a bind (it ignores quotes: ':x' inside a literal IS a bind for it, and so is a
    literal ``%(name)s`` for positional dialects). ``no_nested_comment``: PostgreSQL and DuckDB NEST /* /* */ */, our converter (like SQLite) does not."""
    pieces = [p for p in TEXT_PIECES if ":" not in p and "%(" not in p] if colon_free else TEXT_PIECES
    cpieces = [p for p in COMMENT_PIECES if ":" not in p and "%(" not in p] if colon_free else COMMENT_PIECES
    if no_nested_comment:
        cpieces = [p for p in cpieces if p != "/*"]
    cols, sk, params, want, used = [], [], {}, [], 0
    for i in range(rng.randint(1, 5)):
        kind = rng.choice(["param", "param", "lit", "cast", "in"])
        deco = rng.choice(["", f"/* {junk(rng, cpieces)} */ ", ""])
        if kind == "param":
            params[f"p{i}"] = v = rvalue(rng)
            cols.append(f"{deco}:p{i} AS c{i}")
            sk.append(f"{deco}\0 AS c{i}")
            want.append(expect(v))
            used += 1
        elif kind == "cast":
            params[f"p{i}"] = v = rvalue(rng, strings_only=True)
            cols.append(f"{deco}CAST(:p{i} AS TEXT) AS c{i}")
            sk.append(f"{deco}CAST(\0 AS TEXT) AS c{i}")
            want.append(v)
            used += 1
        elif kind == "lit":
            t = rtext(rng, pieces)
            cols.append(f"{deco}'{t.replace(chr(39), chr(39) * 2)}' AS c{i}")
            sk.append(cols[-1])
            want.append(t)
        else:
            v = rtext(rng, ["a", "b", "'", "%", "é", "x"], 3)
            ids = [rtext(rng, ["a", "b", "'", "%", "é", "x"], 3) for _ in range(rng.randint(0, 4))]
            params[f"p{i}"], params[f"ids{i}"] = v, ids
            cols.append(f"{deco}CASE WHEN :p{i} IN :ids{i} THEN 1 ELSE 0 END AS c{i}")
            sk.append(f"{deco}CASE WHEN \0 IN " + ("(" + ", ".join(["\0"] * len(ids)) + ")" if ids else "(NULL)") + f" THEN 1 ELSE 0 END AS c{i}")
            want.append(1 if v in ids else 0)
            used += 1 + len(ids)
            continue
    tail = rng.choice(["", f" -- {junk(rng, cpieces)}\n", f" /* {junk(rng, cpieces)} */"])
    case.skeleton = "SELECT " + ", ".join(sk) + tail      # the statement with every placeholder token replaced by NUL
    return "SELECT " + ", ".join(cols) + tail, params, want, used


def lit(v):
    v = expect(v)
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return repr(v)


def run(mod, con, sql, params, style):
    converted, bound = mod._convert_sql_params(sql, params, style)
    cur = con.cursor()
    if style in ("format", "pyformat"):
        # what psycopg2/pymysql/snowflake do: substitute SQL literals with Python %-formatting (so a bare % in the SQL would raise here)
        rendered = converted % (tuple(lit(v) for v in bound) if style == "format" else {k: lit(v) for k, v in bound.items()})
        cur.execute(rendered)
    else:
        cur.execute(converted, bound)
    return list(cur.fetchone()), converted, bound


@pytest.mark.parametrize("style", STYLES)
def test_executes_in_real_sqlite_and_returns_the_bound_values(mod, style):
    rng = random.Random(SEED)
    con = sqlite3.connect(":memory:")
    for _ in range(1500):
        sql, params, want, used = case(rng)
        got, converted, bound = run(mod, con, sql, params, style)
        assert got == want, (style, sql, params, converted)
        assert len(bound) == used, (style, sql, bound)                    # one placeholder per parameter used (lists expand)


def test_executes_in_real_duckdb_qmark(mod):
    duckdb = pytest.importorskip("duckdb")
    rng = random.Random(SEED + 1)
    con = duckdb.connect()
    for _ in range(1500):
        sql, params, want, used = case(rng, no_nested_comment=True)
        converted, bound = mod._convert_sql_params(sql, params, "qmark")
        row = list(con.execute(converted, bound).fetchone())
        assert all(g == w or (isinstance(g, (int, float)) and isinstance(w, (int, float)) and abs(g - w) < 1e-9) for g, w in zip(row, want)) and len(row) == len(want), (sql, params, row, want)


@pytest.mark.parametrize("style", ["qmark", "named"])
def test_equals_sqlalchemy_text_when_no_colon_looks_like_a_bind(mod, style):
    sa = pytest.importorskip("sqlalchemy")
    rng = random.Random(SEED + 2)
    engine = sa.create_engine("sqlite://")
    con = sqlite3.connect(":memory:")
    for _ in range(800):
        sql, params, want, _used = case(rng, colon_free=True)
        ours, _, _ = run(mod, con, sql, params, style)
        lists = {k: v for k, v in params.items() if isinstance(v, list)}
        clean = {k: expect(v) for k, v in params.items() if k not in lists}
        stmt = sa.text(sql.replace("%", "%")).bindparams(*[sa.bindparam(k, value=v, expanding=True) for k, v in lists.items()], **clean)
        with engine.connect() as c:
            theirs = list(c.execute(stmt).fetchone())
        assert ours == theirs == want, (sql, params, ours, theirs)


@pytest.mark.parametrize("style, token", [("qmark", "?"), ("format", "%s")])
def test_text_outside_placeholders_is_unchanged(mod, style, token):
    """The converted statement equals the skeleton (the SQL with each placeholder replaced by the style's marker): quoted literals, comments,
    casts and look-alike tokens inside them are byte-identical; format doubles every other ``%``."""
    rng = random.Random(SEED + 3)
    for _ in range(2000):
        sql, params, _want, _used = case(rng)
        skeleton = case.skeleton
        converted, bound = mod._convert_sql_params(sql, params, style)
        expect_sql = (skeleton.replace("%", "%%") if style == "format" else skeleton).replace("\0", token)
        assert converted == expect_sql, (sql, converted, expect_sql)
        assert len(bound) == skeleton.count("\0")


def test_unknown_name_and_collisions_are_errors_not_silent(mod):
    with pytest.raises(ValueError, match="no value"):
        mod._convert_sql_params("SELECT :missing", {}, "qmark")
    with pytest.raises(ValueError, match="collide"):
        mod._convert_sql_params("SELECT 1 WHERE a IN :ids", {"ids": ["x"], "ids_0": 1}, "pyformat")
    rng = random.Random(SEED + 4)
    for _ in range(300):
        s = "".join(rng.choice(["'", '"', "`", "--", "/*", ":", "a", " ", "%"]) for _ in range(rng.randint(0, 12)))
        for style in STYLES:
            try:
                mod._convert_sql_params(s, {}, style)
            except ValueError:
                pass                                   # a stray :name with no value is the only documented failure
