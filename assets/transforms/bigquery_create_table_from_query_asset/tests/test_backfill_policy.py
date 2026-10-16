"""BigQueryCreateTableFromQueryAssetComponent: backfill_policy + range
placeholders. Only the BigQuery client (paid network call) is faked; the
component itself runs for real under dg.materialize(), with a single_run
backfill simulated by the dagster/asset_partition_range_* run tags."""
import sys
import types

import dagster as dg
import pytest

from .conftest import load_component_module

RANGE_TAGS = {
    "dagster/asset_partition_range_start": "2024-01-01",
    "dagster/asset_partition_range_end": "2024-01-02",
}


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def ddls(monkeypatch):
    captured = []

    class _Job:
        total_bytes_billed = 0
        total_bytes_processed = 0
        slot_millis = 0
        cache_hit = False

        def result(self):
            return []

    class _Client:
        def __init__(self, *a, **k):
            pass

        def query(self, ddl):
            captured.append(ddl)
            return _Job()

    bq = types.ModuleType("google.cloud.bigquery")
    bq.Client = _Client
    sa = types.ModuleType("google.oauth2.service_account")
    sa.Credentials = types.SimpleNamespace(from_service_account_info=lambda d: object())
    gcloud = types.ModuleType("google.cloud")
    gcloud.bigquery = bq
    oauth2 = types.ModuleType("google.oauth2")
    oauth2.service_account = sa
    google = types.ModuleType("google")
    google.cloud = gcloud
    google.oauth2 = oauth2
    for name, m in [("google", google), ("google.cloud", gcloud), ("google.cloud.bigquery", bq),
                    ("google.oauth2", oauth2), ("google.oauth2.service_account", sa)]:
        monkeypatch.setitem(sys.modules, name, m)
    return captured


WINDOW_Q = "SELECT * FROM src WHERE d >= '{partition_time_window_start}' AND d < '{partition_time_window_end}'"


def _comp(mod, query=WINDOW_Q, **kw):
    args = dict(asset_name="out_asset", credentials={"project_id": "p"},
                destination_table_id="p.ds.out", materialization="view", query=query,
                partition_type="daily", partition_start="2024-01-01")
    args.update(kw)
    return mod.BigQueryCreateTableFromQueryAssetComponent(**args)


def _run(comp, partition_key=None, tags=None):
    assets = comp.build_defs(None).assets
    return assets, dg.materialize(assets, resources={"io_manager": dg.InMemoryIOManager()}, partition_key=partition_key, tags=tags or {})


def test_backfill_policy_wiring(mod, ddls):
    assert _comp(mod, backfill_policy="single_run").build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    assert _comp(mod, backfill_policy="multi_run", backfill_policy_max_partitions_per_run=5).build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=5)
    assert _comp(mod).build_defs(None).assets[0].backfill_policy is None
    assert _comp(mod, partition_type=None, partition_start=None, backfill_policy="single_run").build_defs(None).assets[0].backfill_policy is None


def test_single_partition_still_substitutes_partition_key(mod, ddls):
    _, result = _run(_comp(mod, query="SELECT * FROM src WHERE d = '{partition_key}'"), partition_key="2024-01-02")
    assert result.success
    assert "d = '2024-01-02'" in ddls[-1]


def test_single_run_range_formats_query_with_whole_range_window(mod, ddls):
    """Pre-fix a range run left partition_key unset (has_partition_key is False)
    so the placeholders could not be supplied at all."""
    _, result = _run(_comp(mod, backfill_policy="single_run"), tags=RANGE_TAGS)
    assert result.success
    assert "d >= '2024-01-01T00:00:00+00:00'" in ddls[-1]
    assert "d < '2024-01-03T00:00:00+00:00'" in ddls[-1]


def test_range_run_with_partition_key_placeholder_fails_loudly(mod, ddls):
    comp = _comp(mod, query="SELECT * FROM src WHERE d = '{partition_key}'", backfill_policy="single_run")
    with pytest.raises(Exception, match="partition_key"):
        dg.materialize(comp.build_defs(None).assets, resources={"io_manager": dg.InMemoryIOManager()}, tags=RANGE_TAGS)
    assert ddls == []
