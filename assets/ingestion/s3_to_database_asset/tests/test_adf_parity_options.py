"""Tests for S3ToDatabaseAssetComponent's ADF-Copy-activity-parity options: header_row,
csv_quotechar/csv_escapechar, pre_copy_script, and dry_run -- ported from the equivalent
adls_to_database_asset fix (see assets/ingestion/adls_to_database_asset/tests/test_adf_parity_options.py).
Also covers the real bug this port fixed: dtype_mapping/column_mapping JSON parsing used to silently
swallow a bad-JSON error (`except json.JSONDecodeError: pass`) instead of raising a clear one.

Real dg.materialize() calls against a fake boto3 S3 client (same FakeS3Client as
test_s3_to_database_asset.py) and a real temp-file SQLite database for the write side -- no mocking
on the write path.

Note: unlike adls_to_database_asset (which writes once per matched blob), this component already
reads every matched/partitioned object into a list of DataFrames and writes ONCE, after
concatenating them -- so pre_copy_script is still tracked via a `_pre_copy_done` set (for parity
and future-proofing) but, given the current single-write architecture, only ever fires once per
run regardless. The wildcard regression test below still guards the thing that actually matters:
that a destructive pre_copy_script doesn't wipe out any matched file's rows before the combined
write lands.
"""
from unittest.mock import patch

import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module
from .test_s3_to_database_asset import FakeS3Client, _make_component, _materialize_static


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "test.db"


def test_header_row_none_means_no_header(mod, db_path):
    fake = FakeS3Client({"data/sales.csv": "1,10.0\n2,20.0\n"})
    component = _make_component(
        mod, db_path, s3_bucket="my-bucket", s3_key="data/sales.csv", header_row=None,
    )
    result = _materialize_static(component, fake)
    assert result.success

    engine = create_engine(f"sqlite:///{db_path}")
    import pandas as pd
    df = pd.read_sql("select * from orders", engine)
    assert list(df.columns) == ["0", "1"] or list(df.columns) == [0, 1]
    assert len(df) == 2


def test_csv_quotechar_and_escapechar(mod, db_path):
    # A field containing the delimiter, quoted with a non-default quote char '.
    content = "id,note\n1,'a,b'\n"
    fake = FakeS3Client({"data/sales.csv": content})
    component = _make_component(
        mod, db_path, s3_bucket="my-bucket", s3_key="data/sales.csv", csv_quotechar="'",
    )
    result = _materialize_static(component, fake)
    assert result.success

    engine = create_engine(f"sqlite:///{db_path}")
    import pandas as pd
    df = pd.read_sql("select * from orders", engine)
    assert df["note"].iloc[0] == "a,b"  # the quoted comma was NOT treated as a field separator


def test_pre_copy_script_runs_before_the_write(mod, db_path):
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("create table orders (id integer, total real)"))
        conn.execute(text("insert into orders values (99, 999.0)"))  # pre-existing row the script should wipe

    fake = FakeS3Client({"data/sales.csv": "id,total\n1,10.0\n"})
    component = _make_component(
        mod, db_path, s3_bucket="my-bucket", s3_key="data/sales.csv",
        pre_copy_script="DELETE FROM orders",
    )
    result = _materialize_static(component, fake)
    assert result.success

    with engine.connect() as conn:
        rows = conn.execute(text("select id from orders")).fetchall()
    assert rows == [(1,)]  # the old row (99) is gone, only the new one remains


def test_pre_copy_script_runs_once_per_table_not_once_per_file_in_wildcard_mode(mod, db_path):
    """The real regression this guards: a TRUNCATE/DELETE-shaped pre_copy_script running once per
    matched object (instead of once per destination table) would wipe out an earlier file's rows
    before they ever reach the combined write. pre_copy_script assumes a pre-existing sink table
    (same as Azure Data Factory's own 'pre-copy script', which only ever runs against an
    already-provisioned sink), so the table is created up front here."""
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        conn.execute(text("create table orders (id integer, total real)"))

    fake = FakeS3Client({
        "incoming/a.csv": "id,total\n1,10.0\n",
        "incoming/b.csv": "id,total\n2,20.0\n",
    })
    component = _make_component(
        mod, db_path, s3_bucket="my-bucket", s3_key="incoming", wildcard="*.csv",
        pre_copy_script="DELETE FROM orders",
    )
    result = _materialize_static(component, fake)
    assert result.success

    with engine.connect() as conn:
        count = conn.execute(text("select count(*) from orders")).scalar()
    assert count == 2  # both files' rows survived -- pre_copy_script did not wipe either one


def test_dry_run_writes_nothing_and_skips_pre_copy_script(mod, db_path):
    fake = FakeS3Client({"data/sales.csv": "id,total\n1,10.0\n"})
    component = _make_component(
        mod, db_path, s3_bucket="my-bucket", s3_key="data/sales.csv",
        pre_copy_script="this is not even valid sql ---!!!", dry_run=True,
    )
    result = _materialize_static(component, fake)
    assert result.success  # the bogus pre_copy_script never ran -- dry_run skipped it entirely

    import sqlalchemy
    engine = create_engine(f"sqlite:///{db_path}")
    inspector = sqlalchemy.inspect(engine)
    assert "orders" not in inspector.get_table_names()  # nothing was ever written


def test_dtype_mapping_bad_json_is_a_clear_error(mod, db_path):
    """Real bug fixed: dtype_mapping parsing used to do `except json.JSONDecodeError: pass`, silently
    discarding the bad config and proceeding with dtype_mapping=None instead of telling the user their
    JSON was invalid. Parsing happens in build_defs() (component build time), not inside the asset
    body, so the fixed ValueError raises right there."""
    component = _make_component(
        mod, db_path, s3_bucket="my-bucket", s3_key="data/sales.csv",
        dtype_mapping="{not valid json",
    )
    with pytest.raises(ValueError, match="dtype_mapping is not valid JSON"):
        component.build_defs(context=None)


def test_column_mapping_bad_json_is_a_clear_error(mod, db_path):
    """Same bug, same fix, for column_mapping's JSON parsing."""
    component = _make_component(
        mod, db_path, s3_bucket="my-bucket", s3_key="data/sales.csv",
        column_mapping="{not valid json",
    )
    with pytest.raises(ValueError, match="column_mapping is not valid JSON"):
        component.build_defs(context=None)
