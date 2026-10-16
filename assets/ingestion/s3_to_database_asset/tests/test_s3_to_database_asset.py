"""Committed tests for S3ToDatabaseAssetComponent, covering the
backfill_policy='single_run' support added alongside the rest of the
repo's backfill_policy rollout (see assets/ai/synthetic_data_generator and
the 31 ingestion "Tier B" components fixed earlier).

Unlike those dlt-based connectors, this component writes directly to SQL
via df.to_sql() and returns a bare MaterializeResult with no IO-managed
value -- so a single_run backfill here has no multi-partition-output IO
manager constraint to work around; the real risk was purely in the
asset's OWN partition-key handling (it read exactly one S3 key per call,
templated from a single context.partition_key, which doesn't exist for a
multi-partition single_run backfill).

Mocks ONLY boto3.s3 (a fake client exposing .get_object(Bucket, Key), the
one method this component actually calls) and uses a real temp-file
SQLite database for the write side -- every other behavior (partition
date/key templating, CSV parsing, concatenation across a backfill range,
real SQL write) is exercised for real through dg.materialize().
"""
import tempfile
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "test.db"


class FakeS3Client:
    """Maps an S3 key to canned CSV bytes; records every get_object call."""

    def __init__(self, files: dict):
        self.files = files
        self.get_object_calls = []

    def get_object(self, Bucket, Key):
        self.get_object_calls.append({"Bucket": Bucket, "Key": Key})
        if Key not in self.files:
            raise KeyError(f"no such fake S3 key: {Key}")
        return {"Body": BytesIO(self.files[Key].encode())}


def _make_component(mod, db_path, **overrides):
    kwargs = dict(
        asset_name="s3_orders",
        database_url=f"sqlite:///{db_path}",
        table_name="orders",
        if_exists="append",
    )
    kwargs.update(overrides)
    return mod.S3ToDatabaseAssetComponent(**kwargs)


def _materialize(component, fake_s3, bucket, key, partition_key=None, tags=None):
    asset_def = component.build_defs(context=None).assets[0]
    with patch("boto3.client", return_value=fake_s3):
        return dg.materialize(
            [asset_def],
            run_config={"ops": {component.asset_name: {"config": {"s3_bucket": bucket, "s3_key": key}}}},
            partition_key=partition_key,
            tags=tags or {},
        )


def test_single_partition_reads_one_file_and_writes_it(mod, db_path):
    fake = FakeS3Client({"data/sales_2024-01-02.csv": "id,total\n1,10.0\n2,20.0\n"})
    component = _make_component(
        mod, db_path, partition_type="daily", partition_start="2024-01-01",
    )
    result = _materialize(
        component, fake, "my-bucket", "data/sales_{partition_date}.csv",
        partition_key="2024-01-02",
    )
    assert result.success
    assert len(fake.get_object_calls) == 1

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        rows = conn.execute(text("select count(*) from orders")).scalar()
    assert rows == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, db_path):
    component = _make_component(
        mod, db_path, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, db_path):
    component = _make_component(mod, db_path, partition_type="daily", partition_start="2024-01-01")
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_reads_one_file_per_day_and_writes_the_combined_total(mod, db_path):
    """Real bug before the fix: the asset read exactly ONE S3 key per call,
    templated from a single context.partition_key -- which doesn't exist
    (raises) for a multi-partition single_run backfill (has_partition_key
    is False; has_partition_key_range is True instead). Confirmed fixed:
    a 3-day single_run backfill now reads all 3 days' files and writes
    their combined total in one SQL write."""
    fake = FakeS3Client({
        "data/sales_2024-01-02.csv": "id,total\n1,10.0\n2,20.0\n",
        "data/sales_2024-01-03.csv": "id,total\n3,30.0\n",
        "data/sales_2024-01-04.csv": "id,total\n4,40.0\n5,50.0\n6,60.0\n",
    })
    component = _make_component(
        mod, db_path, partition_type="daily", partition_start="2024-01-01",
        backfill_policy="single_run",
    )
    result = _materialize(
        component, fake, "my-bucket", "data/sales_{partition_date}.csv",
        tags={
            "dagster/asset_partition_range_start": "2024-01-02",
            "dagster/asset_partition_range_end": "2024-01-04",
        },
    )
    assert result.success
    assert len(fake.get_object_calls) == 3
    assert {c["Key"] for c in fake.get_object_calls} == {
        "data/sales_2024-01-02.csv", "data/sales_2024-01-03.csv", "data/sales_2024-01-04.csv",
    }

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        rows = conn.execute(text("select count(*) from orders")).scalar()
    assert rows == 6  # 2 + 1 + 3
