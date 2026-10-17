"""CloudWatch metrics, IMAP and the ad-spend standardizer use the run's partition window."""
import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path

import dagster as dg
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DAY = "2026-01-03"
PART = dict(partition_type="daily", partition_start="2026-01-01")


def _load(rel, monkeypatch):
    folder = ROOT / rel
    spec = importlib.util.spec_from_file_location("bs_" + folder.name, folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    return mod


def _run(asset, partition_key=None, **kw):
    return dg.materialize([asset], partition_key=partition_key, raise_on_error=False, **kw)


def test_cloudwatch_metrics_query_the_partition_window(monkeypatch):
    calls = []

    class Client:
        def get_metric_data(self, **kw):
            calls.append(kw)
            return {"MetricDataResults": [{"Timestamps": [], "Values": []}]}

    boto3 = types.ModuleType("boto3")
    boto3.client = lambda *a, **k: Client()
    monkeypatch.setitem(sys.modules, "boto3", boto3)
    mod = _load("assets/sources/aws_cloudwatch_metrics_query", monkeypatch)
    comp = mod.AwsCloudwatchMetricsQueryComponent(asset_name="cw", region="us-east-1", namespace="AWS/Lambda", metric_name="Invocations", **PART)
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))
    assert _run(asset, partition_key=DAY).success
    assert calls[0]["StartTime"] == datetime(2026, 1, 3) and calls[0]["EndTime"] == datetime(2026, 1, 4)
    plain = mod.AwsCloudwatchMetricsQueryComponent(asset_name="cw2", region="us-east-1", namespace="AWS/Lambda", metric_name="Invocations", range_minutes=30)
    assert _run(next(a for a in plain.build_defs(None).assets if hasattr(a, "node_def"))).success
    assert (calls[1]["EndTime"] - calls[1]["StartTime"]).total_seconds() == 30 * 60  # unpartitioned: unchanged


def test_imap_search_gets_the_partition_days_in_imap_format(monkeypatch):
    seen = []

    class Conn:
        def login(self, *a): ...
        def select(self, *a): ...
        def search(self, charset, criteria):
            seen.append(criteria)
            return "OK", [b""]
        def close(self): ...
        def logout(self): ...

    import imaplib

    monkeypatch.setattr(imaplib, "IMAP4_SSL", lambda *a, **k: Conn())
    monkeypatch.setenv("IMAP_USER", "u")
    monkeypatch.setenv("IMAP_APP_PASSWORD", "p")
    mod = _load("assets/ingestion/imap_inbox_source", monkeypatch)
    comp = mod.ImapInboxSourceComponent(asset_name="mail", host="h", username_env_var="IMAP_USER", password_env_var="IMAP_APP_PASSWORD",
                                        search_criteria='SINCE {partition_imap_since} BEFORE {partition_imap_before} SUBJECT "{partition_date}"', **PART)
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))
    assert _run(asset, partition_key=DAY).success
    assert seen[0] == 'SINCE 03-Jan-2026 BEFORE 04-Jan-2026 SUBJECT "2026-01-03"'
    plain = mod.ImapInboxSourceComponent(asset_name="mail2", host="h", username_env_var="IMAP_USER", password_env_var="IMAP_APP_PASSWORD", search_criteria="UNSEEN")
    assert _run(next(a for a in plain.build_defs(None).assets if hasattr(a, "node_def"))).success
    assert seen[1] == "UNSEEN"


def test_ad_spend_standardizer_keeps_only_the_partitions_rows(monkeypatch):
    mod = _load("assets/analytics/ad_spend_standardizer", monkeypatch)
    rows = pd.DataFrame({"date": ["2026-01-02", "2026-01-02", "2026-01-03", "2026-01-03", "2026-01-04"],
                         "campaign_id": ["a", "b", "a", "b", "a"], "cost_micros": [1_000_000] * 5,
                         "impressions": [10] * 5, "clicks": [1] * 5, "conversions": [1] * 5})
    comp = mod.AdSpendStandardizerComponent(asset_name="spend", google_ads_asset_key="g", aggregate_by_day=False, **PART)
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))
    fn = asset.op.compute_fn.decorated_fn
    seen = {}

    def prof(frame, event, arg):
        if event == "return" and frame.f_code is fn.__code__:
            seen["google"] = frame.f_locals.get("google_ads_df")

    @dg.asset(name="g")
    def g():
        return rows

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([g], instance=instance)
    sys.setprofile(prof)
    try:
        dg.materialize([asset, g], instance=instance, selection=[asset.key], partition_key=DAY, raise_on_error=False)
    finally:
        sys.setprofile(None)
    assert list(seen["google"]["date"]) == ["2026-01-03", "2026-01-03"]


def test_parametric_generator_gives_each_partition_its_own_reproducible_data(monkeypatch):
    mod = _load("assets/source/parametric_data_generator", monkeypatch)
    cols = {"n": {"type": "int", "min": 0, "max": 10**9}}
    comp = mod.ParametricDataGeneratorComponent(asset_name="gen", row_count=5, random_state=7, columns=cols, **PART)
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))

    def day(key):
        r = _run(asset, partition_key=key, resources={"io_manager": dg.InMemoryIOManager()})
        assert r.success, r
        return list(r.output_for_node("gen")["n"])

    assert day("2026-01-03") == day("2026-01-03")       # reproducible
    assert day("2026-01-03") != day("2026-01-04")       # but not the same rows every partition


ROWS = [("a", "2026-01-02T10:00:00Z"), ("b", "2026-01-02T18:00:00Z"), ("c", "2026-01-03T09:00:00Z"),
        ("d", "2026-01-03T20:00:00Z"), ("e", "2026-01-04T01:00:00Z")]


def _materialized_frame(asset, resources, partition_key=None):
    r = dg.materialize([asset], partition_key=partition_key, resources={"io_manager": dg.InMemoryIOManager(), **resources}, raise_on_error=False)
    assert r.success, r
    return r.output_for_node(asset.op.name)


def test_monday_board_items_return_only_the_partitions_items(monkeypatch):
    class Client:
        def execute(self, query, variables):
            items = [{"id": i, "name": i, "created_at": ts, "updated_at": ts, "column_values": []} for i, ts in ROWS]
            return {"boards": [{"items_page": {"items": items, "cursor": None}}]}

    mod = _load("assets/ingestion/monday_board_items_ingestion", monkeypatch)
    comp = mod.MondayBoardItemsIngestionComponent(asset_name="items", resource_key="monday", board_id="1", partition_date_column="created_at", **PART)
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))
    res = {"monday": dg.ResourceDefinition.hardcoded_resource(Client())}
    assert sorted(_materialized_frame(asset, res, DAY)["id"]) == ["c", "d"]
    plain = mod.MondayBoardItemsIngestionComponent(asset_name="items2", resource_key="monday", board_id="1")
    assert len(_materialized_frame(next(a for a in plain.build_defs(None).assets if hasattr(a, "node_def")), res)) == 5


def test_firestore_collection_returns_only_the_partitions_documents(monkeypatch):
    class Doc:
        def __init__(self, i, ts):
            self.id, self._d = i, {"created_at": ts}

        def to_dict(self):
            return dict(self._d)

    class Query:
        def stream(self):
            return [Doc(i, ts) for i, ts in ROWS]

    class Client:
        def collection(self, path):
            return Query()

    class Resource:
        def get_firestore(self):
            return Client()

    fs = types.ModuleType("google.cloud.firestore_v1")
    fs.Query = type("Query", (), {"ASCENDING": "ASC", "DESCENDING": "DESC"})
    bq = types.ModuleType("google.cloud.firestore_v1.base_query")
    bq.FieldFilter = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "google.cloud.firestore_v1", fs)
    monkeypatch.setitem(sys.modules, "google.cloud.firestore_v1.base_query", bq)
    mod = _load("assets/ingestion/firestore_collection_ingestion", monkeypatch)
    comp = mod.FirestoreCollectionIngestionComponent(asset_name="docs", resource_key="fb", collection_path="users", partition_date_column="created_at", **PART)
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))
    res = {"fb": dg.ResourceDefinition.hardcoded_resource(Resource())}
    assert sorted(_materialized_frame(asset, res, DAY)["_id"]) == ["c", "d"]
    plain = mod.FirestoreCollectionIngestionComponent(asset_name="docs2", resource_key="fb", collection_path="users")
    assert len(_materialized_frame(next(a for a in plain.build_defs(None).assets if hasattr(a, "node_def")), res)) == 5


def test_pyspark_pipeline_resolves_partition_tokens_in_sources_sql_sinks_and_variables(monkeypatch):
    mod = _load("assets/transforms/pyspark_pipeline", monkeypatch)
    captured = {}

    def fake_execute(context, spark_config, spark_app_name, steps, sinks, upstream_arg_names, upstreams, **kw):
        captured.update(steps=steps, sinks=sinks, variables=kw["variables"])

    monkeypatch.setattr(mod, "_execute", fake_execute)
    attrs = dict(asset_name="spark", source={"kind": "parquet", "path": "lake/events/dt={partition_date}"},
                 operations=[{"op": "filter", "predicate": "ts >= '{partition_start}' AND ts < '{partition_end}' AND {not_a_token} > 0"}],
                 sink={"kind": "parquet", "path": "lake/out/dt={partition_date}", "mode": "overwrite"},
                 variables={"day": "{partition_key}"})
    comp = mod.PySparkPipelineComponent(**attrs, **PART)
    asset = next(a for a in comp.build_defs(None).assets if hasattr(a, "node_def"))
    assert _run(asset, partition_key=DAY).success
    text = repr(captured)
    assert "lake/events/dt=2026-01-03" in text and "lake/out/dt=2026-01-03" in text
    assert "ts >= '2026-01-03T00:00:00' AND ts < '2026-01-04T00:00:00'" in text and "{not_a_token}" in text  # other braces survive
    assert captured["variables"] == {"day": "2026-01-03"}
    plain = mod.PySparkPipelineComponent(**{**attrs, "asset_name": "spark2"})
    assert _run(next(a for a in plain.build_defs(None).assets if hasattr(a, "node_def"))).success
    assert "{partition_date}" in repr(captured)  # unpartitioned: left as written
