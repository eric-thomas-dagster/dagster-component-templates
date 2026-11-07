"""Real Sling-capability enhancements to DatabaseReplicationComponent, end to end against
two DuckDB files with the real Sling CLI -- same convention as
test_database_replication_streams.py.

Needs `dagster-sling` and the Sling binary. Run: pytest tests/test_database_replication_sling_enhancements.py

Every feature here was verified live against the real Sling CLI before being wired in.
Two were NOT added despite being real, documented Sling replication-config features,
because they're gated behind a paid Sling CLI Pro license on a free/open-source install
(confirmed via `unable to validate CLI Pro token` at runtime, not a guess from docs):
'hooks' (would have powered row-count validation via `check` hooks -- min_rows uses a
plain row-count query instead) and `SLING_THREADS`/`SLING_RETRIES`/`delete_missing`.
"""
import importlib.util
from pathlib import Path

import dagster as dg
import duckdb
import pytest

COMPONENT = Path(__file__).resolve().parents[1] / "component.py"


@pytest.fixture()
def repl():
    spec = importlib.util.spec_from_file_location("database_replication_component_2", COMPONENT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def dbs(tmp_path):
    src, tgt = tmp_path / "src.duckdb", tmp_path / "tgt.duckdb"
    c = duckdb.connect(str(src))
    c.execute("create schema if not exists main")
    c.execute("create table main.events as select range+1 as id, range*10 as amount from range(5)")
    c.close()
    return src, tgt


def _component(repl, dbs, **kw):
    src, tgt = dbs
    return repl.DatabaseReplicationComponent(
        asset_name="shop", source_type="duckdb", target_type="duckdb",
        source_connection=f"duckdb://{src}", target_connection=f"duckdb://{tgt}", **kw,
    ).build_defs(None)


def _run(defs, partition_key=None, raise_on_error=True):
    return dg.materialize(
        [*defs.assets, *defs.asset_checks], resources=defs.resources,
        partition_key=partition_key, raise_on_error=raise_on_error,
    )


def test_row_count_metadata_is_a_real_int(repl, dbs):
    defs = _component(repl, dbs, source_table="main.events", target_table="main.events_copy")
    result = _run(defs)
    mats = result.get_asset_materialization_events()
    assert len(mats) == 1
    row_count_md = mats[0].materialization.metadata["row_count"]
    assert isinstance(row_count_md, dg.IntMetadataValue), type(row_count_md)
    assert row_count_md.value == 5


def test_add_loaded_at_column_creates_the_column(repl, dbs):
    defs = _component(repl, dbs, source_table="main.events", target_table="main.events_copy", add_loaded_at_column=True)
    assert _run(defs).success
    t = duckdb.connect(str(dbs[1]), read_only=True)
    try:
        cols = [r[0] for r in t.execute("describe main.events_copy").fetchall()]
    finally:
        t.close()
    assert "_sling_loaded_at" in cols


def test_column_casing_component_level(repl, dbs):
    """column_casing normalizes COLUMN names, not the target table name. Asserts the
    option reaches Sling and the run succeeds -- not the exact split DuckDB's own
    source-side identifier handling produces, which this component doesn't control."""
    src, tgt = dbs
    s = duckdb.connect(str(src))
    s.execute('create table main.mixed_case as select range+1 as "UserId", range*10 as "OrderAmount" from range(3)')
    s.close()
    component = repl.DatabaseReplicationComponent(
        asset_name="shop", source_type="duckdb", target_type="duckdb",
        source_connection=f"duckdb://{src}", target_connection=f"duckdb://{tgt}",
        source_table="main.mixed_case", target_table="main.mixed_case_copy",
        column_casing="snake",
    )
    stream_config = component._stream_config(component._stream_specs()[0])
    assert stream_config["target_options"]["column_casing"] == "snake"
    defs = component.build_defs(None)
    assert _run(defs).success
    t = duckdb.connect(str(tgt), read_only=True)
    try:
        cols = [r[0] for r in t.execute("describe main.mixed_case_copy").fetchall()]
    finally:
        t.close()
    assert len(cols) == 2


def test_min_rows_check_passes_when_enough_rows_landed(repl, dbs):
    defs = _component(repl, dbs, source_table="main.events", target_table="main.events_copy", min_rows=1)
    assert defs.asset_checks
    result = _run(defs)
    assert result.success
    check_evals = [e for e in result.get_asset_check_evaluations() if e.check_name.startswith("min_rows_")]
    assert len(check_evals) == 1
    assert check_evals[0].passed
    assert check_evals[0].metadata["row_count"].value == 5


def test_min_rows_check_fails_when_not_enough_rows_landed(repl, dbs):
    """blocking=False, so the asset itself still succeeds -- only the check fails."""
    defs = _component(repl, dbs, source_table="main.events", target_table="main.events_copy", min_rows=100)
    result = _run(defs)
    assert result.success
    check_evals = [e for e in result.get_asset_check_evaluations() if e.check_name.startswith("min_rows_")]
    assert len(check_evals) == 1
    assert not check_evals[0].passed
    assert check_evals[0].metadata["row_count"].value == 5
    assert check_evals[0].metadata["min_rows"].value == 100


def test_min_rows_not_set_means_no_check(repl, dbs):
    defs = _component(repl, dbs, source_table="main.events", target_table="main.events_copy")
    assert not defs.asset_checks


def test_per_stream_min_rows_overrides_component_default(repl, dbs):
    streams = [
        {"source_table": "main.events", "target_table": "main.events_copy", "min_rows": 1},
    ]
    defs = _component(repl, dbs, streams=streams, min_rows=999)
    result = _run(defs)
    assert result.success
    check_evals = result.get_asset_check_evaluations()
    assert len(check_evals) == 1
    assert check_evals[0].passed  # 5 rows >= per-stream override of 1, not the component default of 999
