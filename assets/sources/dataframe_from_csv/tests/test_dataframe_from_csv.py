"""Regression test: this source component used to raise NameError
(undefined `upstream_asset_key` in the column-lineage metadata block) on
EVERY materialization. Uses a real CSV file; nothing to mock."""
import dagster as dg

from .conftest import load_component_module


def test_materializes_real_csv_without_nameerror(tmp_path):
    mod = load_component_module()
    f = tmp_path / "x.csv"
    f.write_text("a,b\n1,2\n3,4\n")
    comp = mod.DataframeFromCsvComponent(asset_name="x", file_path=str(f))
    r = dg.materialize(comp.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()})
    assert r.success
    assert len(r.output_for_node("x")) == 2


def test_partitioned_run_still_works(tmp_path):
    mod = load_component_module()
    f = tmp_path / "x.csv"
    f.write_text("a\n1\n")
    comp = mod.DataframeFromCsvComponent(asset_name="x", file_path=str(f),
                                         partition_type="daily", partition_start="2024-01-01")
    r = dg.materialize(comp.build_defs(None).assets, partition_key="2024-01-02",
                       resources={"io_manager": dg.InMemoryIOManager()})
    assert r.success
