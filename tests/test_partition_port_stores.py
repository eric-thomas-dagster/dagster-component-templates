"""LLM-inference transforms and vector/search stores filter their upstream rows to the partition.

Rather than faking eight external services, each test runs the component's real step and lets it fail naturally at its
first external dependency (a missing library or credential). The failure's stack still holds the step's `upstream` local,
which is exactly what survived the partition filter."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent
COMPONENTS = {
    "assets/ai/langchain_chain_asset": "langchain_chain_asset",
    "assets/ai/litellm_inference_asset": "litellm_inference_asset",
    "assets/ai/ollama_inference_asset": "ollama_inference_asset",
    "assets/analytics/chromadb_asset": "_chromadb_asset",
    "assets/analytics/pinecone_asset": "_pinecone_asset",
    "assets/analytics/wandb_asset": "_wandb_asset",
    "assets/analytics/elasticsearch_asset": "_elasticsearch_asset",
    "assets/analytics/pgvector_asset": "_pgvector_asset",
}
DAY = "2026-01-03"


def _build(folder, extra):
    spec = importlib.util.spec_from_file_location(f"store_{folder.name}", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    cls = getattr(mod, doc["type"].rsplit(".", 1)[-1])
    defs = cls(**{**doc["attributes"], **extra}).build_defs(None)
    return next(a for a in defs.assets if hasattr(a, "node_def")), dict(doc["attributes"])


def _upstream_seen_by_step(exc, step_name):
    """Walk the failure's chained stack frames for the step function and return its `upstream` local."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        tb = exc.__traceback__
        while tb is not None:
            if tb.tb_frame.f_code.co_name == step_name and "upstream" in tb.tb_frame.f_locals:
                return tb.tb_frame.f_locals["upstream"]
            tb = tb.tb_next
        exc = exc.__cause__ or exc.__context__
    return None


def _frame():
    rng = np.random.default_rng(0)
    return pd.DataFrame({"text": [f"row {i}" for i in range(40)], "id": range(40), "v": rng.normal(size=40),
                         "ts": pd.to_datetime(["2026-01-02"] * 20 + ["2026-01-03"] * 20)})


def _run(folder_rel, step, partitioned, monkeypatch):
    for var in ("OPENAI_API_KEY", "PINECONE_API_KEY", "WANDB_API_KEY", "ELASTIC_API_KEY"):
        monkeypatch.delenv(var, raising=False)
    folder = ROOT / folder_rel
    base = dict(yaml.safe_load((folder / "example.yaml").read_text().split("\n---")[0])["attributes"]) if False else None
    extra = {"partition_type": "daily", "partition_start": "2026-01-01", "partition_date_column": "ts"} if partitioned else {}
    if partitioned and folder_rel.endswith(("pgvector_asset", "elasticsearch_asset")):
        extra["if_exists"] = "append"  # replace would wipe the other partitions (refused; see the guard test below)
    asset, attrs = _build(folder, {**extra, "upstream_asset_key": "up_src"})
    frame = _frame()

    @dg.asset(name="up_src")
    def up_src():
        return frame

    instance = dg.DagsterInstance.ephemeral()
    dg.materialize([up_src], instance=instance)
    kwargs = {"partition_key": DAY, "selection": [asset.key]} if partitioned else {"selection": [asset.key]}
    try:
        dg.materialize([asset, up_src], instance=instance, **kwargs)
    except Exception as exc:  # the step is expected to fail at its first external dependency
        return _upstream_seen_by_step(exc, step), asset
    pytest.skip("the step completed without an external dependency; nothing to inspect")


@pytest.mark.parametrize("folder,step", list(COMPONENTS.items()), ids=lambda v: v.rsplit("/", 1)[-1] if "/" in v else v)
def test_partitioned_run_only_sees_its_days_rows(folder, step, monkeypatch):
    seen, asset = _run(folder, step, True, monkeypatch)
    assert isinstance(asset.partitions_def, dg.DailyPartitionsDefinition)
    assert seen is not None, "the step failed before it ever held an upstream frame"
    assert len(seen) == 20 and set(seen["ts"].dt.strftime("%Y-%m-%d")) == {DAY}


@pytest.mark.parametrize("folder,step", list(COMPONENTS.items()), ids=lambda v: v.rsplit("/", 1)[-1] if "/" in v else v)
def test_unpartitioned_run_sees_every_row(folder, step, monkeypatch):
    seen, asset = _run(folder, step, False, monkeypatch)
    assert asset.partitions_def is None and seen is not None and len(seen) == 40


@pytest.mark.parametrize("folder", ["assets/analytics/pgvector_asset", "assets/analytics/elasticsearch_asset"])
def test_replace_mode_is_refused_for_partitioned_assets(folder):
    extra = {"mode": "query"} if folder.endswith("elasticsearch_asset") else {}  # elasticsearch: only query mode writes a table
    with pytest.raises(ValueError, match="would delete the other partitions"):
        _build(ROOT / folder, {**extra, "partition_type": "daily", "partition_start": "2026-01-01", "if_exists": "replace"})
    _build(ROOT / folder, {**extra, "partition_type": "daily", "partition_start": "2026-01-01", "if_exists": "append"})  # allowed


def test_elasticsearch_query_mode_scopes_the_query_to_the_partition(monkeypatch, tmp_path):
    import json
    import sys
    import types
    seen = {}

    class FakeES:
        def __init__(self, *a, **k):
            pass

        def search(self, index, body, size):
            seen["body"] = body
            return {"hits": {"hits": [{"_source": {"id": 1, "name": "a"}}]}}

    es = types.ModuleType("elasticsearch")
    es.Elasticsearch = FakeES
    helpers = types.ModuleType("elasticsearch.helpers")
    helpers.bulk = lambda *a, **k: (0, [])
    es.helpers = helpers
    monkeypatch.setitem(sys.modules, "elasticsearch", es)
    monkeypatch.setitem(sys.modules, "elasticsearch.helpers", helpers)
    monkeypatch.setenv("DEST_DB", f"sqlite:///{tmp_path / 'out.db'}")
    monkeypatch.setenv("ELASTICSEARCH_URL", "http://localhost:9200")
    window = {"range": {"ts": {"gte": "{partition_start}", "lt": "{partition_end}"}}}
    asset, _ = _build(ROOT / "assets/analytics/elasticsearch_asset", {
        "mode": "query", "upstream_asset_key": None, "query": json.dumps({"query": window}), "if_exists": "append", "database_url_env_var": "DEST_DB",
        "table_name": "hits", "partition_type": "daily", "partition_start": "2026-01-01"})
    assert dg.materialize([asset], partition_key=DAY).success
    assert seen["body"]["query"]["range"]["ts"] == {"gte": "2026-01-03T00:00:00", "lt": "2026-01-04T00:00:00"}
