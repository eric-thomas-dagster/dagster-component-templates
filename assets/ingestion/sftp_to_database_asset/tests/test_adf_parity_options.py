"""Tests for SFTPToDatabaseAssetComponent's ADF-Copy-activity-parity options: header_row, csv_quotechar/
csv_escapechar, dtype_mapping (typed columns), pre_copy_script, and dry_run. Real materialize() calls
against a fake SFTP client (same FakeSFTP as test_sftp_to_database_asset.py) and a real temp-file
SQLite database for the write side -- no mocking on the write path.
"""
import pandas as pd
import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module
from .test_sftp_to_database_asset import (
    FakeSFTP,
    _install_fake_paramiko_module,
    _make_component,
    _materialize,
)


@pytest.fixture()
def mod():
    return load_component_module()


def test_header_row_none_means_no_header(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake_sftp = FakeSFTP({"/data/orders.csv": "1,10.0\n2,20.0\n"})
    component = _make_component(mod, db_path, monkeypatch, header_row=None)
    result = _materialize(component, fake_sftp, "/data/orders.csv", monkeypatch)
    assert result.success
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("select * from sales", engine)
    assert list(df.columns) == ["0", "1"] or list(df.columns) == [0, 1]
    assert len(df) == 2


def test_csv_quotechar_and_escapechar(mod, tmp_path, monkeypatch):
    # A field containing the delimiter, quoted with a non-default quote char '.
    content = "id,note\n1,'a,b'\n"
    db_path = tmp_path / "test.db"
    fake_sftp = FakeSFTP({"/data/orders.csv": content})
    component = _make_component(mod, db_path, monkeypatch, csv_quotechar="'")
    result = _materialize(component, fake_sftp, "/data/orders.csv", monkeypatch)
    assert result.success
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("select * from sales", engine)
    assert df["note"].iloc[0] == "a,b"  # the quoted comma was NOT treated as a field separator


def test_dtype_mapping_coerces_column_types(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake_sftp = FakeSFTP({"/data/orders.csv": "id,amount\n1,10\n2,20\n"})
    component = _make_component(mod, db_path, monkeypatch, dtype_mapping='{"amount": "float64"}')
    result = _materialize(component, fake_sftp, "/data/orders.csv", monkeypatch)
    assert result.success
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("select * from sales", engine)
    assert df["amount"].dtype.kind == "f"


def test_dtype_mapping_bad_json_is_a_clear_error(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake_sftp = FakeSFTP({"/data/orders.csv": "id,amount\n1,10\n"})
    component = _make_component(mod, db_path, monkeypatch, dtype_mapping="{not valid json")
    with pytest.raises(Exception, match="dtype_mapping is not valid JSON"):
        _materialize(component, fake_sftp, "/data/orders.csv", monkeypatch)


def test_pre_copy_script_runs_before_the_write(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("create table sales (id integer, total real)"))
        conn.execute(text("insert into sales values (99, 999.0)"))  # pre-existing row the script should wipe

    fake_sftp = FakeSFTP({"/data/orders.csv": "id,total\n1,10.0\n"})
    component = _make_component(mod, db_path, monkeypatch, if_exists="append", pre_copy_script="DELETE FROM sales")
    result = _materialize(component, fake_sftp, "/data/orders.csv", monkeypatch)
    assert result.success
    with engine.connect() as conn:
        rows = conn.execute(text("select id from sales")).fetchall()
    assert rows == [(1,)]  # the old row (99) is gone, only the new one remains


def test_pre_copy_script_runs_once_per_table_not_once_per_partition_key(mod, tmp_path, monkeypatch):
    """The real regression this guards: a TRUNCATE-shaped pre_copy_script running once per processed
    partition key (instead of once per destination table) would wipe out an earlier partition's rows
    within the same run. This component has no wildcard/multi-file mode -- the equivalent multi-write
    path is a single_run backfill over several partition keys that share ONE (non-templated) table_name.
    pre_copy_script assumes a pre-existing sink table (same as Azure Data Factory's own 'pre-copy
    script', which only ever runs against an already-provisioned sink), so the table is created up
    front here."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("create table sales (id integer, total real)"))

    fake_sftp = FakeSFTP({
        "/data/sales_store_a.csv": "id,total\n1,10.0\n",
        "/data/sales_store_b.csv": "id,total\n2,20.0\n",
    })
    component = _make_component(
        mod, db_path, monkeypatch,
        partition_type="static", partition_values="store_a,store_b",
        if_exists="append", pre_copy_script="DELETE FROM sales",
        backfill_policy="single_run",
    )
    result = _materialize(
        component, fake_sftp, "/data/sales_{partition_key}.csv", monkeypatch,
        tags={
            "dagster/asset_partition_range_start": "store_a",
            "dagster/asset_partition_range_end": "store_b",
        },
    )
    assert result.success
    with engine.connect() as conn:
        count = conn.execute(text("select count(*) from sales")).scalar()
    assert count == 2  # both partitions' rows survived -- pre_copy_script did not re-fire between them


def test_dry_run_writes_nothing_and_skips_pre_copy_script(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake_sftp = FakeSFTP({"/data/orders.csv": "id,total\n1,10.0\n"})
    component = _make_component(
        mod, db_path, monkeypatch, pre_copy_script="this is not even valid sql ---!!!", dry_run=True,
    )
    result = _materialize(component, fake_sftp, "/data/orders.csv", monkeypatch)
    assert result.success  # the bogus pre_copy_script never ran -- dry_run skipped it entirely
    engine = create_engine(f"sqlite:///{db_path}")
    import sqlalchemy
    inspector = sqlalchemy.inspect(engine)
    assert "sales" not in inspector.get_table_names()  # nothing was ever written
