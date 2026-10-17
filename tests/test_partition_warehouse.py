"""Partitioned warehouse transforms read only their slice of the source and replace only their slice of the output.

Real SQL against DuckDB: each transform runs partitioned over a three-day source and is compared with the same transform
run unpartitioned on just that day's rows."""
import importlib.util
import re
from pathlib import Path

import dagster as dg
import pytest
import sqlalchemy

ROOT = Path(__file__).resolve().parent.parent
BEGIN, END = "# BEGIN warehouse partition helpers", "# END warehouse partition helpers"
DAYS = ["2026-01-01", "2026-01-02", "2026-01-03"]
COMMON = dict(dialect="duckdb", partition_date_column="order_date")
CASES = {
    "dedup": ("WarehouseDedupComponent", dict(subset=["customer_id"], order_by=["updated_at"], descending=True)),
    "filter": ("WarehouseFilterComponent", dict(predicate="amount > 10")),
    "formula": ("WarehouseFormulaComponent", dict(expressions={"net": "amount * 0.9"}, keep_existing=True)),
    "multi_field_formula": ("WarehouseMultiFieldFormulaComponent", dict(expression="UPPER(TRIM({col}))", columns=["name"], output_mode="replace")),
    "multi_row_formula": ("WarehouseMultiRowFormulaComponent", dict(partition_by=["customer_id"], order_by=["updated_at"],
                                                                  expressions={"running": {"kind": "running_total", "col": "amount"}})),
    "summarize": ("WarehouseSummarizeComponent", dict(group_by=["order_date", "category"], aggregations={"revenue": {"col": "amount", "agg": "sum"}})),
    "top_n_per_group": ("WarehouseTopNPerGroupComponent", dict(group_by=["category"], sort_by="amount", n=2, ascending=False, rank_column="rnk")),
    "join": ("WarehouseJoinComponent", dict(right_table="raw.customers", how="left", on_columns=["customer_id"],
                                            select_cols=["_l.order_id", "_l.order_date", "_l.amount", "_r.customer_name"])),
    "union": ("WarehouseUnionComponent", dict(select_cols=["order_id", "customer_id", "amount", "order_date"], distinct=False)),
}


def _load(name):
    p = ROOT / f"assets/transforms/warehouse_{name}/component.py"
    spec = importlib.util.spec_from_file_location(f"wh_{name}", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _engine(tmp_path):
    return sqlalchemy.create_engine(f"duckdb:///{tmp_path / 'w.duckdb'}")


def _seed(engine, rows_by_day):
    with engine.begin() as c:
        c.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")
        c.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS analytics")
        for t in ("orders", "orders_a", "orders_b"):
            c.exec_driver_sql(f"DROP TABLE IF EXISTS raw.{t}")
            c.exec_driver_sql(f"CREATE TABLE raw.{t} (order_id INT, customer_id INT, order_date TIMESTAMP, updated_at TIMESTAMP, "
                              f"amount DOUBLE, category VARCHAR, name VARCHAR)")
        c.exec_driver_sql("DROP TABLE IF EXISTS raw.customers")
        c.exec_driver_sql("CREATE TABLE raw.customers AS SELECT * FROM (VALUES (0, 'zero'), (1, 'one'), (2, 'two')) t(customer_id, customer_name)")
        n = 0
        for day, k in rows_by_day.items():
            for i in range(k):
                n += 1
                for t in ("orders", "orders_a", "orders_b"):
                    c.exec_driver_sql(
                        f"INSERT INTO raw.{t} VALUES ({n}, {i % 3}, TIMESTAMP '{day} {8 + i}:00:00', TIMESTAMP '{day} {8 + i}:30:00', "
                        f"{10.0 * (i + 1) + (1 if t == 'orders_b' else 0)}, '{'ab'[i % 2]}', ' n{n} ')")


def _attrs(name, tmp_path, src, out, **extra):
    cls, spec = CASES[name]
    attrs = dict(asset_name=f"{name}_out", database_url=f"duckdb:///{tmp_path / 'w.duckdb'}", output_table=out, **COMMON, **spec, **extra)
    if name == "union":
        attrs["upstream_tables"] = [f"{src}_a", f"{src}_b"]
    elif name == "join":
        attrs["left_table"] = src
    else:
        attrs["upstream_table"] = src
    return cls, attrs


def _run(name, tmp_path, src, out, partition_key=None, **extra):
    cls, attrs = _attrs(name, tmp_path, src, out, **extra)
    comp = getattr(_load(name), cls)(**attrs)
    asset = next(a for a in comp.build_defs(None).assets)
    r = dg.materialize([asset], partition_key=partition_key, raise_on_error=False)
    assert r.success, r
    return r


def _rows(engine, table, where=""):
    with engine.connect() as c:
        return sorted(tuple(map(str, r)) for r in c.exec_driver_sql(f"SELECT * FROM {table} {where}").fetchall())


def _day_where(day):
    return f"WHERE order_date >= TIMESTAMP '{day}' AND order_date < TIMESTAMP '{day}' + INTERVAL 1 DAY"


def _scratch(engine):
    with engine.connect() as c:
        return [r[0] for r in c.exec_driver_sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%_dagster_part_%'")]


PART = dict(partition_type="daily", partition_start="2026-01-01")


@pytest.mark.parametrize("name", list(CASES))
def test_a_partition_equals_the_unpartitioned_run_over_that_days_rows(name, tmp_path):
    engine = _engine(tmp_path)
    _seed(engine, {d: 5 for d in DAYS})
    for day in DAYS:
        _run(name, tmp_path, "raw.orders", "analytics.out", partition_key=day, **PART)
        with engine.begin() as c:  # reference: the same transform, unpartitioned, over only this day's rows
            for t in ("orders", "orders_a", "orders_b"):
                c.exec_driver_sql(f"DROP TABLE IF EXISTS raw.day_{t}")
                c.exec_driver_sql(f"CREATE TABLE raw.day_{t} AS SELECT * FROM raw.{t} {_day_where(day)}")
        _run(name, tmp_path, "raw.day_orders", f"analytics.ref_{day.replace('-', '_')}")
    for day in DAYS:
        got = _rows(engine, "analytics.out", _day_where(day))
        want = _rows(engine, f"analytics.ref_{day.replace('-', '_')}")
        assert got == want and got, (name, day)
    assert _scratch(engine) == []


@pytest.mark.parametrize("name", list(CASES))
def test_rerunning_one_partition_replaces_only_its_rows(name, tmp_path):
    engine = _engine(tmp_path)
    _seed(engine, {d: 5 for d in DAYS})
    for day in DAYS:
        _run(name, tmp_path, "raw.orders", "analytics.out", partition_key=day, **PART)
    before = {d: _rows(engine, "analytics.out", _day_where(d)) for d in DAYS}
    assert all(before.values())
    _run(name, tmp_path, "raw.orders", "analytics.out", partition_key=DAYS[1], **PART)  # idempotent
    assert {d: _rows(engine, "analytics.out", _day_where(d)) for d in DAYS} == before
    with engine.begin() as c:  # the source for day 2 changes: only day 2 may change
        for t in ("orders", "orders_a", "orders_b"):
            c.exec_driver_sql(f"UPDATE raw.{t} SET amount = amount + 100 WHERE order_date >= TIMESTAMP '{DAYS[1]}' AND order_date < TIMESTAMP '{DAYS[2]}'")
    _run(name, tmp_path, "raw.orders", "analytics.out", partition_key=DAYS[1], **PART)
    after = {d: _rows(engine, "analytics.out", _day_where(d)) for d in DAYS}
    assert after[DAYS[0]] == before[DAYS[0]] and after[DAYS[2]] == before[DAYS[2]]
    if name != "multi_field_formula":  # that one only normalizes text, so amounts are not part of its output change
        assert after[DAYS[1]] != before[DAYS[1]]


def test_unpartitioned_runs_are_unchanged_and_misconfiguration_fails_early(tmp_path):
    engine = _engine(tmp_path)
    _seed(engine, {d: 3 for d in DAYS})
    _run("filter", tmp_path, "raw.orders", "analytics.plain")
    assert len(_rows(engine, "analytics.plain")) == 6  # amount > 10 keeps 2 of 3 rows a day and _scratch(engine) == []
    cls, attrs = _attrs("filter", tmp_path, "raw.orders", "analytics.x", **PART)
    attrs.pop("partition_date_column")
    with pytest.raises(ValueError, match="partition_date_column"):
        getattr(_load("filter"), cls)(**attrs).build_defs(None)
    cls, attrs = _attrs("filter", tmp_path, "raw.orders", "analytics.x", partition_type="multi", partition_start="2026-01-01")
    with pytest.raises(ValueError, match="multi"):
        getattr(_load("filter"), cls)(**attrs).build_defs(None)


def test_an_output_without_the_partition_column_is_refused_and_leaves_nothing_behind(tmp_path):
    engine = _engine(tmp_path)
    _seed(engine, {d: 3 for d in DAYS})
    cls, attrs = _attrs("summarize", tmp_path, "raw.orders", "analytics.bad", **PART)
    attrs["group_by"] = ["category"]  # the date column is not kept in the output
    asset = next(a for a in getattr(_load("summarize"), cls)(**attrs).build_defs(None).assets)
    r = dg.materialize([asset], partition_key=DAYS[0], raise_on_error=False)
    assert not r.success
    assert _scratch(engine) == []


def test_helper_copies_are_identical():
    blocks = {re.search(rf"{BEGIN}.*?{END}", (ROOT / f"assets/transforms/warehouse_{n}/component.py").read_text(), re.S).group(0) for n in CASES}
    assert len(blocks) == 1
