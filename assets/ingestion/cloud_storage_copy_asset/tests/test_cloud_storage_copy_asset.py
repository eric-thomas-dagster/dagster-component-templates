"""Committed tests for CloudStorageCopyAssetComponent.

binary/dataframe-file-sink modes run against real local files (fsspec's local backend, no mocking
needed -- it's the same mechanism s3://gs://abfss:// would use). The table sink runs against a
REAL file-backed local DuckDB connection (the sink's actual register()/execute() fast path, not
mocked), same pattern rest_api_ingestion's own sink tests use.
"""
import json

import dagster as dg
import duckdb
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class FakeDuckDBResource:
    def __init__(self, conn):
        self._conn = conn

    def get_connection(self):
        return self._conn


def _make_component(mod, **overrides):
    kwargs = dict(asset_key="copy/test_asset")
    kwargs.update(overrides)
    return mod.CloudStorageCopyAssetComponent(**kwargs)


def _materialize(component, resources=None, **kw):
    asset_def = component.build_defs(None).assets[0]
    return dg.materialize([asset_def], resources=resources or {}, **kw)


def test_binary_copy_preserves_bytes_exactly(mod, tmp_path):
    src = tmp_path / "in.bin"
    src.write_bytes(b"\x00\x01hello\xffworld")
    sink = tmp_path / "out.bin"
    component = _make_component(mod, source_path=str(src), sink_path=str(sink))
    result = _materialize(component)
    assert result.success
    assert sink.read_bytes() == src.read_bytes()
    md = result.asset_materializations_for_node(component.build_defs(None).assets[0].op.name)[0].metadata
    assert md["copy_mode"].value == "binary"
    assert md["bytes_copied"].value == len(src.read_bytes())


def test_dataframe_mode_converts_csv_to_parquet(mod, tmp_path):
    import pandas as pd
    src = tmp_path / "in.csv"
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_csv(src, index=False)
    sink = tmp_path / "out.parquet"
    component = _make_component(mod, source_path=str(src), source_format="csv", copy_mode="dataframe", sink_path=str(sink), sink_format="parquet")
    result = _materialize(component)
    assert result.success
    out = pd.read_parquet(sink)
    assert out["a"].tolist() == [1, 2]
    assert out["b"].tolist() == ["x", "y"]


def test_dataframe_mode_auto_detects_source_format(mod, tmp_path):
    import pandas as pd
    src = tmp_path / "in.json"
    pd.DataFrame({"a": [1]}).to_json(src, orient="records")
    sink = tmp_path / "out.csv"
    component = _make_component(mod, source_path=str(src), copy_mode="dataframe", sink_path=str(sink), sink_format="csv")
    result = _materialize(component)
    assert result.success
    assert "a" in sink.read_text()


def test_sink_table_uses_the_real_duckdb_fast_path(mod, tmp_path):
    import pandas as pd
    src = tmp_path / "in.csv"
    pd.DataFrame({"id": [1, 2, 3]}).to_csv(src, index=False)
    db_path = str(tmp_path / "sink.duckdb")
    conn = duckdb.connect(db_path)
    component = _make_component(
        mod, source_path=str(src), source_format="csv", copy_mode="dataframe",
        sink_table={"resource_key": "duckdb_sink", "table": "copied"},
    )
    result = _materialize(component, resources={"duckdb_sink": FakeDuckDBResource(conn)})
    assert result.success
    verify = duckdb.connect(db_path)
    rows = verify.execute("SELECT id FROM copied ORDER BY id").fetchall()
    verify.close()
    assert rows == [(1,), (2,), (3,)]


def test_partition_key_is_templated_into_source_and_sink_paths(mod, tmp_path):
    (tmp_path / "2024-01-01.bin").write_bytes(b"day1")
    (tmp_path / "2024-01-02.bin").write_bytes(b"day2")
    component = _make_component(
        mod, source_path=str(tmp_path / "{partition_key}.bin"), sink_path=str(tmp_path / "out" / "{partition_key}.bin"),
        partition_type="static", partition_values=["2024-01-01", "2024-01-02"],
    )
    asset_def = component.build_defs(None).assets[0]
    result = dg.materialize([asset_def], partition_key="2024-01-02")
    assert result.success
    assert (tmp_path / "out" / "2024-01-02.bin").read_bytes() == b"day2"


def test_dry_run_touches_nothing(mod, tmp_path):
    src = tmp_path / "in.bin"
    src.write_bytes(b"data")
    sink = tmp_path / "out.bin"
    component = _make_component(mod, source_path=str(src), sink_path=str(sink), dry_run=True)
    result = _materialize(component)
    assert result.success
    assert not sink.exists()


def test_requires_exactly_one_of_sink_path_or_sink_table(mod):
    with pytest.raises(ValueError, match="exactly one"):
        _make_component(mod, source_path="x")
    with pytest.raises(ValueError, match="exactly one"):
        _make_component(mod, source_path="x", sink_path="y", sink_table={"resource_key": "r", "table": "t"})


def test_sink_table_requires_dataframe_mode(mod):
    with pytest.raises(ValueError, match="copy_mode='dataframe'"):
        _make_component(mod, source_path="x", copy_mode="binary", sink_table={"resource_key": "r", "table": "t"})


def test_dataframe_mode_with_sink_path_requires_sink_format(mod):
    with pytest.raises(ValueError, match="sink_format"):
        _make_component(mod, source_path="x", copy_mode="dataframe", sink_path="y")


def test_azure_storage_options_only_built_for_azure_schemes(mod):
    assert mod._azure_storage_options("s3://bucket/key", account_name_env_var="X") is None
    assert mod._azure_storage_options("/local/path", account_name_env_var="X") is None


def test_azure_storage_options_built_from_env_vars(mod, monkeypatch):
    monkeypatch.setenv("AZ_ACCT", "myaccount")
    monkeypatch.setenv("AZ_KEY", "secretkey")
    opts = mod._azure_storage_options("abfss://container@myaccount.dfs.core.windows.net/x", account_name_env_var="AZ_ACCT", account_key_env_var="AZ_KEY")
    assert opts == {"account_name": "myaccount", "account_key": "secretkey"}


def test_azure_storage_options_connection_string_takes_precedence_over_account_key(mod, monkeypatch):
    monkeypatch.setenv("AZ_CONN", "conn-str")
    monkeypatch.setenv("AZ_KEY", "should-not-be-used")
    opts = mod._azure_storage_options("abfs://c@a.dfs.core.windows.net/x", account_key_env_var="AZ_KEY", connection_string_env_var="AZ_CONN")
    assert opts == {"connection_string": "conn-str"}


def test_wildcard_copies_every_matching_file_preserving_relative_path(mod, tmp_path):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "a.csv").write_bytes(b"a")
    (src / "b.csv").write_bytes(b"b")
    (src / "skip.txt").write_bytes(b"nope")
    (src / "sub" / "c.csv").write_bytes(b"c")
    sink = tmp_path / "dst"
    component = _make_component(mod, source_path=str(src), wildcard="*.csv", sink_path=str(sink))
    result = _materialize(component)
    assert result.success
    assert (sink / "a.csv").read_bytes() == b"a"
    assert (sink / "b.csv").read_bytes() == b"b"
    assert not (sink / "sub" / "c.csv").exists()  # not recursive
    md = result.asset_materializations_for_node(component.build_defs(None).assets[0].op.name)[0].metadata
    assert md["files_copied"].value == 2


def test_wildcard_recursive_includes_subdirectories(mod, tmp_path):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "a.csv").write_bytes(b"a")
    (src / "sub" / "c.csv").write_bytes(b"c")
    sink = tmp_path / "dst"
    component = _make_component(mod, source_path=str(src), wildcard="*.csv", recursive=True, sink_path=str(sink))
    result = _materialize(component)
    assert result.success
    assert (sink / "a.csv").read_bytes() == b"a"
    assert (sink / "sub" / "c.csv").read_bytes() == b"c"


def test_wildcard_requires_binary_mode_and_sink_path(mod):
    with pytest.raises(ValueError, match="wildcard requires"):
        _make_component(mod, source_path="x", wildcard="*.csv", copy_mode="dataframe", sink_path="y", sink_format="csv")
    with pytest.raises(ValueError, match="wildcard requires"):
        _make_component(mod, source_path="x", wildcard="*.csv", sink_table={"resource_key": "r", "table": "t"}, copy_mode="dataframe")
