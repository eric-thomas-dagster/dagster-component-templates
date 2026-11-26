"""Tests for ADLSToDatabaseAssetComponent's ADF-Copy-activity-parity options: header_row, csv_quotechar/
csv_escapechar, dtype_mapping (typed columns), pre_copy_script, and dry_run. Real materialize() calls
against a fake Azure blob client (same FakeBlobServiceClient as test_adls_to_database_asset.py) and a real
temp-file SQLite database for the write side -- no mocking on the write path.
"""
import pandas as pd
import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module
from .test_adls_to_database_asset import (
    FakeBlobServiceClient,
    _install_fake_azure_module,
    _make_component,
    _materialize,
)


@pytest.fixture()
def mod():
    return load_component_module()


def test_header_row_none_means_no_header(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake = FakeBlobServiceClient({"my-container/orders.csv": "1,10.0\n2,20.0\n"})
    component = _make_component(mod, db_path, monkeypatch, container_name="my-container", blob_name="orders.csv", header_row=None)
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    from unittest.mock import patch
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake):
        result = dg.materialize([asset_def])
    assert result.success
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("select * from sales", engine)
    assert list(df.columns) == ["0", "1"] or list(df.columns) == [0, 1]
    assert len(df) == 2


def test_csv_quotechar_and_escapechar(mod, tmp_path, monkeypatch):
    # A field containing the delimiter, quoted with a non-default quote char '.
    content = "id,note\n1,'a,b'\n"
    db_path = tmp_path / "test.db"
    fake = FakeBlobServiceClient({"my-container/orders.csv": content})
    component = _make_component(
        mod, db_path, monkeypatch, container_name="my-container", blob_name="orders.csv",
        csv_quotechar="'",
    )
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    from unittest.mock import patch
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake):
        result = dg.materialize([asset_def])
    assert result.success
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("select * from sales", engine)
    assert df["note"].iloc[0] == "a,b"  # the quoted comma was NOT treated as a field separator


def test_dtype_mapping_coerces_column_types(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake = FakeBlobServiceClient({"my-container/orders.csv": "id,amount\n1,10\n2,20\n"})
    component = _make_component(
        mod, db_path, monkeypatch, container_name="my-container", blob_name="orders.csv",
        dtype_mapping='{"amount": "float64"}',
    )
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    from unittest.mock import patch
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake):
        result = dg.materialize([asset_def])
    assert result.success
    engine = create_engine(f"sqlite:///{db_path}")
    df = pd.read_sql("select * from sales", engine)
    assert df["amount"].dtype.kind == "f"


def test_dtype_mapping_bad_json_is_a_clear_error(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake = FakeBlobServiceClient({"my-container/orders.csv": "id,amount\n1,10\n"})
    component = _make_component(
        mod, db_path, monkeypatch, container_name="my-container", blob_name="orders.csv",
        dtype_mapping="{not valid json",
    )
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    from unittest.mock import patch
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake):
        with pytest.raises(Exception, match="dtype_mapping is not valid JSON"):
            dg.materialize([asset_def])


def test_pre_copy_script_runs_before_the_write(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("create table sales (id integer, total real)"))
        conn.execute(text("insert into sales values (99, 999.0)"))  # pre-existing row the script should wipe

    fake = FakeBlobServiceClient({"my-container/orders.csv": "id,total\n1,10.0\n"})
    component = _make_component(
        mod, db_path, monkeypatch, container_name="my-container", blob_name="orders.csv",
        if_exists="append", pre_copy_script="DELETE FROM sales",
    )
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    from unittest.mock import patch
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake):
        result = dg.materialize([asset_def])
    assert result.success
    with engine.connect() as conn:
        rows = conn.execute(text("select id from sales")).fetchall()
    assert rows == [(1,)]  # the old row (99) is gone, only the new one remains


def test_pre_copy_script_runs_once_per_table_not_once_per_file_in_wildcard_mode(mod, tmp_path, monkeypatch):
    """The real regression this guards: a TRUNCATE-shaped pre_copy_script running once per matched blob
    (instead of once per destination table) would wipe out an earlier file's rows within the same run.
    pre_copy_script assumes a pre-existing sink table (same as Azure Data Factory's own 'pre-copy script',
    which only ever runs against an already-provisioned sink), so the table is created up front here."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("create table sales (id integer, total real)"))

    fake = FakeBlobServiceClient({
        "my-container/data/a.csv": "id,total\n1,10.0\n",
        "my-container/data/b.csv": "id,total\n2,20.0\n",
    })
    component = _make_component(
        mod, db_path, monkeypatch, container_name="my-container", blob_name="data",
        wildcard="*.csv", if_exists="append", pre_copy_script="DELETE FROM sales",
    )
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    from unittest.mock import patch
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake):
        result = dg.materialize([asset_def])
    assert result.success
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        count = conn.execute(text("select count(*) from sales")).scalar()
    assert count == 2  # both files' rows survived -- pre_copy_script did not re-fire between them


def test_dry_run_writes_nothing_and_skips_pre_copy_script(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake = FakeBlobServiceClient({"my-container/orders.csv": "id,total\n1,10.0\n"})
    component = _make_component(
        mod, db_path, monkeypatch, container_name="my-container", blob_name="orders.csv",
        pre_copy_script="this is not even valid sql ---!!!", dry_run=True,
    )
    _install_fake_azure_module(monkeypatch)
    asset_def = component.build_defs(context=None).assets[0]
    from unittest.mock import patch
    with patch("azure.storage.blob.BlobServiceClient.from_connection_string", return_value=fake):
        result = dg.materialize([asset_def])
    assert result.success  # the bogus pre_copy_script never ran -- dry_run skipped it entirely
    engine = create_engine(f"sqlite:///{db_path}")
    import sqlalchemy
    inspector = sqlalchemy.inspect(engine)
    assert "sales" not in inspector.get_table_names()  # nothing was ever written
