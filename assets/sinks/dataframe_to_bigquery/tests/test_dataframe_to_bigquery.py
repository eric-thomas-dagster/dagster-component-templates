"""Committed tests for DataframeToBigqueryComponent: backfill_policy (no resource_key on this sink).

google-cloud-bigquery is not installed, so a minimal fake `google.cloud.bigquery` (Client,
LoadJobConfig) is injected -- only that submodule (and `google.oauth2.service_account` in the
credentials test) via monkeypatch.setitem + setattr, both undone at test end. The Client records
every load; the component runs for real under dg.materialize().
"""
import sys
import types

import dagster as dg
import google.cloud
import google.oauth2
import pandas as pd
import pytest

from .conftest import (
    PartitionedMemoryIOManager,
    daily_upstream,
    load_component_module,
    materialize_upstream_days,
)

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-02",
    "dagster/asset_partition_range_end": "2024-01-04",
}
TABLE_ID = "proj.ds.out"


class Recorder:
    def __init__(self):
        self.clients = []
        self.loads = []  # (df, table_id, write_disposition)
        self.creds_files = []


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def bq(monkeypatch):
    rec = Recorder()
    bigquery = types.ModuleType("google.cloud.bigquery")

    class LoadJobConfig:
        def __init__(self, write_disposition=None, **kw):
            self.write_disposition = write_disposition

    class Job:
        def result(self):
            return None

    class Client:
        def __init__(self, credentials=None, location=None, **kw):
            self.credentials, self.location = credentials, location
            rec.clients.append(self)

        def load_table_from_dataframe(self, df, table_id, job_config=None):
            rec.loads.append((df.copy(), table_id, job_config.write_disposition))
            return Job()

        def get_table(self, table_id):
            return types.SimpleNamespace(num_rows=sum(len(d) for d, _, _ in rec.loads))

    bigquery.Client, bigquery.LoadJobConfig = Client, LoadJobConfig
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery)
    monkeypatch.setattr(google.cloud, "bigquery", bigquery, raising=False)

    sa = types.ModuleType("google.oauth2.service_account")

    class Credentials:
        @staticmethod
        def from_service_account_file(path):
            rec.creds_files.append(path)
            return f"creds:{path}"

    sa.Credentials = Credentials
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", sa)
    monkeypatch.setattr(google.oauth2, "service_account", sa, raising=False)
    return rec


def _written_ids(rec):
    assert len(rec.loads) == 1
    return sorted(rec.loads[0][0]["id"].tolist())


def _comp(mod, **kw):
    kw.setdefault("asset_name", "to_bq")
    kw.setdefault("upstream_asset_key", "orders_flat")
    kw.setdefault("table_id", TABLE_ID)
    return mod.DataframeToBigqueryComponent(**kw)


def _flat_upstream():
    @dg.asset(name="orders_flat")
    def orders_flat() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})
    return orders_flat


def test_unpartitioned_load_and_adc_credentials(mod, bq):
    sink = _comp(mod, location="EU").build_defs(None).assets[0]
    assert sink.backfill_policy is None
    r = dg.materialize([_flat_upstream(), sink])
    assert r.success
    (client,) = bq.clients
    assert client.credentials is None and client.location == "EU"
    assert bq.loads[0][1:] == (TABLE_ID, "WRITE_TRUNCATE")
    assert _written_ids(bq) == [1, 2, 3]


def test_service_account_credentials_env_var(mod, bq, monkeypatch):
    monkeypatch.setenv("BQ_CREDS", "/path/sa.json")
    sink = _comp(mod, credentials_env_var="BQ_CREDS").build_defs(None).assets[0]
    assert dg.materialize([_flat_upstream(), sink]).success
    assert bq.creds_files == ["/path/sa.json"]
    assert bq.clients[0].credentials == "creds:/path/sa.json"


def _partitioned(mod, **kw):
    return _comp(mod, upstream_asset_key="orders_src", partition_type="daily",
                 partition_start="2024-01-01", partition_date_column="order_date", **kw)


def test_backfill_policy_wired_and_off_by_default(mod, bq):
    assert _partitioned(mod).build_defs(None).assets[0].backfill_policy is None
    single = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    assert single.backfill_policy == dg.BackfillPolicy.single_run()
    multi = _partitioned(mod, backfill_policy="multi_run",
                         backfill_policy_max_partitions_per_run=2).build_defs(None).assets[0]
    assert multi.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=2)


def test_single_partition_run_writes_only_that_days_rows(mod, bq):
    up = daily_upstream()
    sink = _partitioned(mod).build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03"])
    r = dg.materialize([up, sink], selection=[sink.key], partition_key="2024-01-03",
                       instance=instance, resources={"io_manager": io})
    assert r.success
    assert _written_ids(bq) == [201, 202]


def test_single_run_backfill_range_writes_every_key_in_range_once(mod, bq):
    up = daily_upstream()
    sink = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    instance, io = dg.DagsterInstance.ephemeral(), PartitionedMemoryIOManager()
    materialize_upstream_days(up, instance, io, ["2024-01-02", "2024-01-03", "2024-01-04"])
    r = dg.materialize([up, sink], selection=[sink.key], instance=instance,
                       resources={"io_manager": io}, tags=RANGE_TAGS)
    assert r.success
    assert len(bq.clients) == 1  # one run, one client, one load job
    assert _written_ids(bq) == [101, 102, 201, 202, 301, 302]
    assert r.asset_materializations_for_node("to_bq")[0].metadata["row_count"].value == 6


def test_range_filter_actually_runs_on_a_partition_agnostic_upstream(mod, bq):
    """Upstream is NOT partitioned and hands over all 3 days; only the component's own range
    filter can narrow the rows to the 2-day range."""
    @dg.asset(name="orders_src")
    def all_days() -> pd.DataFrame:
        return pd.concat([
            pd.DataFrame({"id": [d * 10 + 1, d * 10 + 2],
                          "order_date": [f"2024-01-0{d} 09:00:00", f"2024-01-0{d} 18:00:00"]})
            for d in (2, 3, 4)
        ], ignore_index=True)

    sink = _partitioned(mod, backfill_policy="single_run").build_defs(None).assets[0]
    r = dg.materialize([all_days, sink], tags={
        "dagster/asset_partition_range_start": "2024-01-03",
        "dagster/asset_partition_range_end": "2024-01-04"})
    assert r.success
    assert _written_ids(bq) == [31, 32, 41, 42]
