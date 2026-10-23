"""Real, committed tests for backend='polars'.

Added as a genuine Polars 2.0 alternative to `polars_io_manager`: that
component wraps the upstream `dagster-polars` package's
`PolarsParquetIOManager`, which crashes on any read under Polars 2.0
(`ArgumentRemovedError: rechunk ... removed in version 2.0`, reproduced
directly against the currently-released dagster-polars 0.27.12 + polars
2.0.0; fix is community-integrations#359, still unmerged). This backend
reads/writes via `DataFrame.write_parquet`/`pl.read_parquet` directly --
eager, no `scan_parquet`, no `dagster-polars` dependency at all -- so it
never goes near that bug.
"""
import dagster as dg
import pytest

pl = pytest.importorskip("polars")


def test_polars_backend_round_trips_a_real_dataframe(tmp_path, mod):
    @dg.asset
    def upstream() -> pl.DataFrame:
        return pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    @dg.asset
    def downstream(upstream: pl.DataFrame) -> pl.DataFrame:
        assert isinstance(upstream, pl.DataFrame)
        return upstream.with_columns((pl.col("a") * 2).alias("a2"))

    io_manager = mod.LocalParquetIOManager(base_dir=str(tmp_path), backend="polars")
    result = dg.materialize([upstream, downstream], resources={"io_manager": io_manager})
    assert result.success

    written = pl.read_parquet(tmp_path / "downstream.parquet")
    assert written["a2"].to_list() == [2, 4, 6]


def test_polars_backend_partition_range_round_trips(tmp_path, mod):
    parts = dg.DailyPartitionsDefinition("2024-01-01", end_offset=2)
    tags = {
        "dagster/asset_partition_range_start": "2024-01-01",
        "dagster/asset_partition_range_end": "2024-01-02",
    }

    @dg.asset(partitions_def=parts, backfill_policy=dg.BackfillPolicy.single_run())
    def upstream(context):
        return {k: pl.DataFrame({"day": [k], "v": [1]}) for k in context.partition_keys}

    io_manager = mod.LocalParquetIOManager(base_dir=str(tmp_path), backend="polars")
    result = dg.materialize([upstream], tags=tags, resources={"io_manager": io_manager})
    assert result.success
    assert (tmp_path / "upstream" / "2024-01-01.parquet").exists()
    assert (tmp_path / "upstream" / "2024-01-02.parquet").exists()


def test_polars_backend_rejects_a_pandas_dataframe_in_a_range_write(tmp_path, mod):
    import pandas as pd

    parts = dg.DailyPartitionsDefinition("2024-01-01", end_offset=1)
    tags = {
        "dagster/asset_partition_range_start": "2024-01-01",
        "dagster/asset_partition_range_end": "2024-01-01",
    }

    @dg.asset(partitions_def=parts, backfill_policy=dg.BackfillPolicy.single_run())
    def bad(context):
        return {k: pd.DataFrame({"v": [1]}) for k in context.partition_keys}

    io_manager = mod.LocalParquetIOManager(base_dir=str(tmp_path), backend="polars")
    with pytest.raises(Exception) as ei:
        dg.materialize([bad], tags=tags, resources={"io_manager": io_manager})
    chain, e = [], ei.value
    while e is not None:
        chain.append(str(e))
        e = e.__cause__
    assert any("must be a DataFrame" in m for m in chain), chain


def test_component_build_defs_rejects_an_invalid_backend(mod):
    comp = mod.LocalParquetIOManagerComponent(backend="arrow")
    with pytest.raises(ValueError, match="backend must be 'pandas' or 'polars'"):
        comp.build_defs(None)


def test_component_build_defs_with_polars_backend_wires_a_real_resource(tmp_path, mod):
    comp = mod.LocalParquetIOManagerComponent(base_dir=str(tmp_path), backend="polars")
    defs = comp.build_defs(None)
    io_manager = defs.resources["io_manager"]
    assert io_manager.backend == "polars"
