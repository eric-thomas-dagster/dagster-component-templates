"""Seeded fuzzing (fixed seed) of agentic_pipeline's `database`-source placeholder binder ``_bind_source_sql``: the bound SQL goes through
``sqlalchemy.text()`` into a REAL SQLite database, and the partition value must come back byte-for-byte, with injection payloads staying inside
the literal. Colons are escaped as ``\\:`` for text(); that round trip is what is checked."""
import random

import pytest

sa = pytest.importorskip("sqlalchemy")

from .conftest import load_component_module  # noqa: E402

SEED = 20261019
ALPHABET = ["'", "''", "\\", "\\'", "\n", "\t", "%", ":", "::", ":name", "\\:", "--", "/*", "{", "}", "{{", "}}", "{partition_key}", '"', "`", ";", " ", "a", "9",
            "é", "😀", "'); DROP TABLE x; --", "' OR '1'='1"]
INJECTIONS = ["'); DROP TABLE x; --", "' OR '1'='1", "x' UNION SELECT 1 --", "a:b", ":name", "12:30:00", "\\", "\\:", "a\\:b"]


@pytest.fixture(scope="module")
def mod():
    return load_component_module()


@pytest.fixture(scope="module")
def engine():
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.exec_driver_sql("CREATE TABLE x(a INT)")
    return e


def keys(n):
    rng = random.Random(SEED)
    return list(INJECTIONS) + ["", "'", "''"] + ["".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 10))) for _ in range(n)]


def read_back(engine, sql):
    with engine.connect() as c:
        return c.execute(sa.text(sql)).fetchone()


def test_partition_key_round_trips_through_sqlalchemy_text(mod, engine):
    bad = []
    for key in keys(1500):
        sql = mod._bind_source_sql("SELECT {partition_key} AS k, '{{lit}}' AS l", key, None, None)
        try:
            row = read_back(engine, sql)
        except Exception as exc:  # noqa: BLE001
            bad.append((key, type(exc).__name__))
            continue
        if tuple(row) != (key, "{lit}"):
            bad.append((key, tuple(row)))
    assert not bad, bad[:8]


def test_injection_payloads_stay_in_the_literal(mod, engine):
    for key in INJECTIONS:
        sql = mod._bind_source_sql("SELECT {partition_key} AS k", key, None, None)
        assert read_back(engine, sql)[0] == key
    with engine.connect() as c:
        assert c.exec_driver_sql("SELECT COUNT(*) FROM x").fetchone()[0] == 0


def test_window_and_dimension_placeholders(mod, engine):
    sql = mod._bind_source_sql("SELECT {partition.region} AS r, {partition_start} AS s, {partition_end} AS e", "k", {"region": "o'brien"},
                               {"partition_start": "2026-01-01T00:00:00+00:00", "partition_end": "2026-01-02T00:00:00+00:00"})
    assert tuple(read_back(engine, sql)) == ("o'brien", "2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00")


def test_binder_rejections_and_brace_escapes(mod):
    assert mod._bind_source_sql("SELECT 1 WHERE x = {{y}}", "k", None, None) == "SELECT 1 WHERE x = {y}"
    with pytest.raises(ValueError, match="quoted string literal"):
        mod._bind_source_sql("SELECT '{partition_key}'", "k", None, None)
    with pytest.raises(ValueError, match="not available"):
        mod._bind_source_sql("SELECT {partition_key}", None, None, None)
