"""Committed tests for SFTPToDatabaseAssetComponent's backfill_policy='single_run'
support, added alongside the rest of the repo's backfill_policy rollout.
Same shape as adls_to_database_asset/gcs_to_database_asset's own tests (see
those files for the full rationale): remote_path AND table_name can both
vary per partition key, so a single_run backfill processes and writes
each partition independently rather than concatenating into one frame --
but reuses ONE SSH connection across the whole range instead of
reconnecting per file.

Mocks ONLY paramiko.SSHClient (a fake client exposing .connect(),
.open_sftp() -> object with .getfo(remote_path, buf), .close()) and uses
a real temp-file SQLite database for the write side.
"""
import sys
import types
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from sqlalchemy import create_engine, text

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _install_fake_paramiko_module(fake_sftp):
    """The real `paramiko` package isn't installed in this test environment
    -- inject a minimal fake module into sys.modules so the component's
    lazy `import paramiko` resolves to our fake instead of failing on
    import."""
    paramiko_mod = types.ModuleType("paramiko")
    paramiko_mod.AutoAddPolicy = MagicMock

    class _FakeSSHClient:
        def set_missing_host_key_policy(self, policy):
            pass

        def connect(self, host, **kwargs):
            pass

        def open_sftp(self):
            return fake_sftp

        def close(self):
            pass

    paramiko_mod.SSHClient = _FakeSSHClient
    sys.modules["paramiko"] = paramiko_mod


class FakeSFTP:
    """Maps a remote path to canned file bytes; records every .getfo() call."""

    def __init__(self, files: dict):
        self.files = files
        self.getfo_calls = []
        self.closed = False

    def getfo(self, remote_path, buf):
        self.getfo_calls.append(remote_path)
        buf.write(self.files[remote_path].encode())

    def close(self):
        self.closed = True


def _make_component(mod, db_path, monkeypatch, **overrides):
    monkeypatch.setenv("FAKE_SFTP_HOST", "sftp.example.com")
    monkeypatch.setenv("FAKE_SFTP_USER", "demo")
    kwargs = dict(
        asset_name="sftp_sales",
        host_env_var="FAKE_SFTP_HOST",
        username_env_var="FAKE_SFTP_USER",
        database_url=f"sqlite:///{db_path}",
        table_name="sales",
        if_exists="append",
    )
    kwargs.update(overrides)
    return mod.SFTPToDatabaseAssetComponent(**kwargs)


def _materialize(component, fake_sftp, remote_path, filename="data.csv", partition_key=None, tags=None):
    _install_fake_paramiko_module(fake_sftp)
    asset_def = component.build_defs(context=None).assets[0]
    return dg.materialize(
        [asset_def],
        run_config={"ops": {component.asset_name: {"config": {"remote_path": remote_path, "filename": filename}}}},
        partition_key=partition_key,
        tags=tags or {},
    )


def test_single_partition_reads_one_file_and_writes_it(mod, tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    fake_sftp = FakeSFTP({"/data/sales_store_a.csv": "id,total\n1,10.0\n2,20.0\n"})
    component = _make_component(
        mod, db_path, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
    )
    result = _materialize(component, fake_sftp, "/data/sales_{partition_key}.csv", partition_key="store_a")
    assert result.success
    assert len(fake_sftp.getfo_calls) == 1
    assert fake_sftp.closed

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        rows = conn.execute(text("select count(*) from sales")).scalar()
    assert rows == 2


def test_backfill_policy_single_run_is_wired_onto_the_asset(mod, tmp_path, monkeypatch):
    component = _make_component(
        mod, tmp_path / "test.db", monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
        backfill_policy="single_run",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()


def test_backfill_policy_unset_by_default(mod, tmp_path, monkeypatch):
    component = _make_component(
        mod, tmp_path / "test.db", monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
    )
    asset_def = component.build_defs(context=None).assets[0]
    assert asset_def.backfill_policy is None


def test_single_run_backfill_reuses_one_ssh_connection_and_writes_each_table(mod, tmp_path, monkeypatch):
    """Real bug before the fix: the asset read exactly ONE remote file
    (templated from a single context.partition_key) per call, which
    doesn't exist for a multi-partition single_run backfill. Confirmed
    fixed: a 3-key single_run backfill now reads each store's own file
    over a SINGLE reused SSH connection and writes it to that store's OWN
    table -- not concatenated into one shared table, since the
    destination itself varies per key here."""
    db_path = tmp_path / "test.db"
    fake_sftp = FakeSFTP({
        "/data/sales_store_a.csv": "id,total\n1,10.0\n2,20.0\n",
        "/data/sales_store_b.csv": "id,total\n3,30.0\n",
        "/data/sales_store_c.csv": "id,total\n4,40.0\n5,50.0\n6,60.0\n",
    })
    component = _make_component(
        mod, db_path, monkeypatch,
        partition_type="static", partition_values="store_a,store_b,store_c",
        table_name="sales_{partition_key}",
        backfill_policy="single_run",
    )
    result = _materialize(
        component, fake_sftp, "/data/sales_{partition_key}.csv",
        tags={
            "dagster/asset_partition_range_start": "store_a",
            "dagster/asset_partition_range_end": "store_c",
        },
    )
    assert result.success
    assert len(fake_sftp.getfo_calls) == 3  # one SFTP session, 3 file reads
    assert fake_sftp.closed

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        assert conn.execute(text("select count(*) from sales_store_a")).scalar() == 2
        assert conn.execute(text("select count(*) from sales_store_b")).scalar() == 1
        assert conn.execute(text("select count(*) from sales_store_c")).scalar() == 3
