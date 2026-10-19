"""Seeded fuzzing (fixed seed, deterministic) of receipt_outcome_ingestion: the mapping (value_map / scale) must turn ANY cell into a value in
[-1, 1], a deliberate skip, or a rejected row (_Bad) -- never an exception that would abort the whole ingest -- and the ``{since}`` SQL literal
escaper must round-trip in real DuckDB."""
import random

import pytest

from .conftest import load_component_module

SEED = 20261019


@pytest.fixture(scope="module")
def mod():
    return load_component_module()


def cells(rng):
    return [lambda: rng.randint(-10, 10), lambda: rng.uniform(-1e3, 1e3), lambda: rng.random() * 2 - 1, lambda: float("nan"), lambda: float("inf"),
            lambda: -float("inf"), lambda: 1e308, lambda: str(rng.randint(-10, 10)), lambda: f" {rng.randint(0, 5)} ", lambda: "abc", lambda: "",
            lambda: True, lambda: False, lambda: None, lambda: "1e999", lambda: "NaN", lambda: "0x10", lambda: [1], lambda: {"a": 1}, lambda: b"3"]


def run_one(mod, row, mapping, vmap=None, skip=()):
    try:
        return mod._resolve_outcome(row, mapping, {mod._norm_key(k): v for k, v in (vmap or {}).items()}, set(skip))
    except mod._Bad:
        return "BAD"


def test_scale_and_value_map_never_raise_and_stay_in_range(mod):
    rng = random.Random(SEED)
    pool = cells(rng)
    for _ in range(4000):
        lo = rng.choice([0, 1, -5, 0.5, -1e6])
        hi = lo + rng.choice([1, 4, 1e-9, 1e6])
        mapping = {"receipt_id": "rid", "value": "v"}
        if rng.random() < 0.7:
            mapping["scale"] = {"min": lo, "max": hi}
        vmap = rng.choice([{}, {"good": 1, "bad": -1, "meh": 0.25, "up": "thumbs_up", "5": 1}])
        cell = rng.choice(pool)()
        out = run_one(mod, {"rid": "r1", "v": cell}, mapping, vmap, skip=rng.choice([(), ("skip", "6")]))
        if out in (None, "BAD"):
            continue
        rid, sig, val, ref = out
        assert -1.0 <= val <= 1.0, (cell, mapping, out)
        assert rid == "r1" and isinstance(sig, str)


def test_scale_is_linear_and_exact_at_the_ends(mod):
    mapping = {"receipt_id": "rid", "value": "v", "scale": {"min": 1, "max": 5}}
    rng = random.Random(SEED)
    for _ in range(500):
        x = rng.uniform(1, 5)
        val = run_one(mod, {"rid": "r", "v": x}, mapping)[2]
        assert val == pytest.approx(-1 + 2 * (x - 1) / 4) and -1 <= val <= 1
    assert run_one(mod, {"rid": "r", "v": 1}, mapping)[2] == -1.0 and run_one(mod, {"rid": "r", "v": 5}, mapping)[2] == 1.0
    for outside in (0.999, 5.001, -3, 1e300, float("nan"), float("inf")):
        assert run_one(mod, {"rid": "r", "v": outside}, mapping) == "BAD"


def test_without_scale_out_of_range_numbers_are_rejected(mod):
    mapping = {"receipt_id": "rid", "value": "v"}
    rng = random.Random(SEED)
    for _ in range(500):
        x = rng.uniform(-3, 3)
        out = run_one(mod, {"rid": "r", "v": x}, mapping)
        assert (out == "BAD") == (not -1 <= x <= 1)


@pytest.mark.parametrize("cell", [10 ** 400, -(10 ** 400), float("inf"), float("-inf"), float("nan"), "inf", "-inf", "nan", "Infinity", "1e999", "9" * 400])
def test_huge_integer_cell_is_a_rejected_row_not_a_crash(mod, cell):
    assert run_one(mod, {"rid": "r", "v": cell}, {"receipt_id": "rid", "value": "v"}) == "BAD"


def test_sql_literal_round_trips_in_real_duckdb(mod):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    rng = random.Random(SEED)
    alphabet = ["'", "''", "\\", "\n", "\r", "%", ":since", "--", "/*", "{since}", '"', "😀", "é", ";", " ", "a", "0", "'; DROP TABLE x; --"]
    for s in ["2026-01-01T00:00:00+00:00", "'); DROP TABLE x; --", "' OR '1'='1", ""] + ["".join(rng.choice(alphabet) for _ in range(rng.randint(0, 10))) for _ in range(2000)]:
        assert con.execute("SELECT " + mod._sql_literal(s)).fetchone()[0] == s, repr(s)
