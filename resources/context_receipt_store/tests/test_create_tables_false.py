"""`create_tables=False` (the user manages the tables): no DDL/ALTER is ever run, but a table from an older release that lacks a
late-added column fails with an actionable error listing the exact ALTER, once-per-instance probe only."""
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from backends import load_module, make_store  # noqa: E402
from test_partition_key import _item, _old_ddl  # noqa: E402

KINDS_WH = ["bigquery-fake", "sql-duckdb", "sql-sqlite"]


@pytest.fixture()
def mod():
    return load_module("context_receipt_store_create_tables_false_under_test")


def _run(store, sql):
    if hasattr(store, "_client_factory"):
        store._client_factory().query(sql).result()
    else:
        import sqlalchemy as sa
        with store._engine.begin() as conn:
            conn.execute(sa.text(sql))


def _ddl(mod, store, old):
    if hasattr(store, "_client_factory"):
        args = (store.dataset, store.table_prefix, "bigquery")
    else:
        args = (store.schema, store.table_prefix, store.dialect)
    return _old_ddl(mod, *args) if old else mod.receipt_table_ddl(*args)


def _manual(kind, mod, tmp_path, monkeypatch, old):
    store = make_store(kind, mod, tmp_path, monkeypatch)
    for ddl in _ddl(mod, store, old):
        _run(store, ddl)
    if hasattr(store, "_client_factory"):
        manual = mod.BigQueryContextReceiptStore(store.dataset, client_factory=store._client_factory, table_prefix=store.table_prefix, create_tables=False)
    else:
        manual = mod.SqlContextReceiptStore(store._engine, schema=store.schema, table_prefix=store.table_prefix, dialect=store.dialect, create_tables=False)
    return manual, store


def _alter_for(mod, store):
    if hasattr(store, "_client_factory"):
        (_, alter), = mod.receipt_column_upgrades(store.dataset, store.table_prefix)
    else:
        (_, alter), = mod.receipt_column_upgrades(store.schema, store.table_prefix, store.dialect)
    return alter


@pytest.mark.parametrize("kind", KINDS_WH)
def test_old_table_gives_clear_error_then_works_after_alter(kind, mod, tmp_path, monkeypatch):
    manual, _ = _manual(kind, mod, tmp_path, monkeypatch, old=True)
    alter = _alter_for(mod, manual)
    with pytest.raises(RuntimeError) as err:
        manual.record_retrieval(context_name="rag", receipt_id="r1", items=[_item("a")], dagster_partition_key="2024-01-01")
    assert alter in str(err.value) and "ADD COLUMN dagster_partition_key" in str(err.value)
    with pytest.raises(RuntimeError):                       # still failing (not cached as ready); and the read path says the same
        manual.get_receipt("r1")
    _run(manual, alter)                                     # the user runs the printed statement
    manual.record_retrieval(context_name="rag", receipt_id="r1", items=[_item("a")], dagster_partition_key="2024-01-01")
    assert manual.get_receipt("r1")["dagster_partition_key"] == "2024-01-01"


@pytest.mark.parametrize("kind", KINDS_WH)
def test_up_to_date_table_costs_one_probe_per_instance(kind, mod, tmp_path, monkeypatch):
    manual, _ = _manual(kind, mod, tmp_path, monkeypatch, old=False)
    if hasattr(manual, "_client_factory"):
        client = manual._client_factory()
        before = len(client.queries)
        manual.record_retrieval(context_name="rag", receipt_id="r1", items=[_item("a")])
        manual.record_retrieval(context_name="rag", receipt_id="r2", items=[_item("a")])
        manual.get_receipt("r1")
        probes = [q for q in client.queries[before:] if re.match(r"\s*SELECT dagster_partition_key FROM .* WHERE 1 = 0", q)]
        assert len(probes) == 1
        assert not [q for q in client.queries[before:] if re.match(r"\s*(CREATE|ALTER)", q)]
    else:
        import sqlalchemy as sa
        seen = []
        sa.event.listen(manual._engine, "before_cursor_execute", lambda c, cur, stmt, *a: seen.append(stmt))
        manual.record_retrieval(context_name="rag", receipt_id="r1", items=[_item("a")])
        manual.record_retrieval(context_name="rag", receipt_id="r2", items=[_item("a")])
        manual.get_receipt("r1")
        assert len([q for q in seen if "WHERE 1 = 0" in q]) == 1
        assert not [q for q in seen if re.match(r"\s*(CREATE|ALTER)", q)]
