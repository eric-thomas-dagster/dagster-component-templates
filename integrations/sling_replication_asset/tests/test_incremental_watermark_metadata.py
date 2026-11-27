"""Real end-to-end tests against the actual Sling CLI and two DuckDB files -- same convention
as assets/ingestion/database_replication/tests/test_database_replication_sling_enhancements.py.

Covers item #12 from the migration-skills consolidated wishlist: `sling_replication_asset`
didn't surface a per-table incremental watermark anywhere. Sling itself has no separate
per-stream state file by default -- for `mode: incremental` it queries MAX(update_key) on the
TARGET table to pick each run's starting point (confirmed against Sling's own docs:
https://docs.slingdata.io/examples/database-to-database/incremental, and against this
machine's `~/.sling/.sling.db`, which holds no per-stream state at all). So the component now
queries that same value back after each materialization and attaches it as asset metadata.

Needs `dagster-sling` and the Sling binary. Run: pytest tests/test_incremental_watermark_metadata.py
"""
import importlib.util
import textwrap
from pathlib import Path

import dagster as dg
import duckdb
import pytest

COMPONENT = Path(__file__).resolve().parents[1] / "component.py"


@pytest.fixture()
def repl():
    spec = importlib.util.spec_from_file_location("sling_replication_asset_component_test", COMPONENT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def dbs(tmp_path):
    src, tgt = tmp_path / "src.duckdb", tmp_path / "tgt.duckdb"
    c = duckdb.connect(str(src))
    c.execute("create schema if not exists main")
    c.execute(
        "create table main.events as "
        "select * from (values "
        "(1, 100), (2, 150), (3, 300)"
        ") as t(id, updated_at)"
    )
    c.close()
    return src, tgt


def _replication_yaml(tmp_path, mode="incremental"):
    cfg = tmp_path / "replication.yaml"
    body = f"""
        source: SRC_DB
        target: TGT_DB
        defaults:
          mode: {mode}
          object: 'main.{{stream_table}}'
        streams:
          main.events:
            primary_key: [id]
            update_key: updated_at
    """
    cfg.write_text(textwrap.dedent(body))
    return cfg


def _connections(dbs):
    from dagster_sling import SlingConnectionResource

    src, tgt = dbs
    return [
        SlingConnectionResource(name="SRC_DB", type="duckdb", connection_string=f"duckdb://{src}"),
        SlingConnectionResource(name="TGT_DB", type="duckdb", connection_string=f"duckdb://{tgt}"),
    ]


def _run(defs, dbs):
    from dagster_sling import SlingResource

    return dg.materialize(
        [*defs.assets],
        resources={"sling": SlingResource(connections=_connections(dbs))},
        raise_on_error=True,
    )


def test_incremental_watermark_metadata_matches_real_max(repl, dbs, tmp_path):
    cfg = _replication_yaml(tmp_path)
    defs = repl.SlingReplicationAssetComponent(replication_config_path=str(cfg)).build_defs(None)
    result = _run(defs, dbs)
    mats = result.get_asset_materialization_events()
    assert len(mats) == 1
    md = mats[0].materialization.metadata
    assert "incremental_watermark" in md, md
    assert md["incremental_watermark"].value == "300"
    assert md["update_key"].value == "updated_at"


def test_full_refresh_mode_has_no_watermark_metadata(repl, dbs, tmp_path):
    cfg = _replication_yaml(tmp_path, mode="full-refresh")
    defs = repl.SlingReplicationAssetComponent(replication_config_path=str(cfg)).build_defs(None)
    result = _run(defs, dbs)
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert "incremental_watermark" not in md
    assert "update_key" not in md


def test_emit_incremental_watermarks_false_disables_it(repl, dbs, tmp_path):
    cfg = _replication_yaml(tmp_path)
    defs = repl.SlingReplicationAssetComponent(
        replication_config_path=str(cfg), emit_incremental_watermarks=False
    ).build_defs(None)
    result = _run(defs, dbs)
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert "incremental_watermark" not in md


def test_second_run_watermark_advances_after_new_rows(repl, dbs, tmp_path):
    cfg = _replication_yaml(tmp_path)
    defs = repl.SlingReplicationAssetComponent(replication_config_path=str(cfg)).build_defs(None)

    first = _run(defs, dbs)
    first_md = first.get_asset_materialization_events()[0].materialization.metadata
    assert first_md["incremental_watermark"].value == "300"

    src, _tgt = dbs
    c = duckdb.connect(str(src))
    c.execute("insert into main.events values (4, 500)")
    c.close()

    second = _run(defs, dbs)
    second_md = second.get_asset_materialization_events()[0].materialization.metadata
    assert second_md["incremental_watermark"].value == "500"
