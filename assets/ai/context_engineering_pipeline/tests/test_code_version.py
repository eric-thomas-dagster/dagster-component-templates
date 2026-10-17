"""Code version = the recipe that built the context. It marks the asset stale in Dagster when the recipe
changes and is stamped on every chunk, so outcomes can be compared across recipe versions."""
import hashlib
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_chromadb


@pytest.fixture()
def mod():
    return load_component_module()


def _fake_embedding(model, input, api_key=None):
    resp = MagicMock()
    resp.data = [{"embedding": [float(len(t)), float(int(hashlib.md5(t.encode()).hexdigest(), 16) % 97), 1.0]} for t in input]
    return resp


DOC = "Paragraph one about refund policies.\n\nParagraph two about shipping delays.\n\nParagraph three about app crashes."


def _component(mod, tmp_path, chunk_size=60, model="fake-1", location=None, sync_mode=None, **kw):
    write = {"id": "w", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(location or tmp_path / "chroma"), "collection_name": "kb_test"}
    if sync_mode:
        write["sync_mode"] = sync_mode
    return mod.ContextEngineeringPipelineComponent(
        asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body",
        steps=[{"id": "c", "op": "chunk", "strategy": "recursive", "chunk_size": chunk_size, "chunk_overlap": 0},
               {"id": "e", "op": "embed", "provider": "litellm", "model": model}, write], **kw)


def _code_version(component):
    return list(component.build_defs(None).assets)[0].get_asset_spec(dg.AssetKey("kb")).code_version


def test_code_version_is_a_recipe_fingerprint_that_ignores_where_it_is_written(mod, tmp_path):
    base = _code_version(_component(mod, tmp_path))
    assert base == _code_version(_component(mod, tmp_path))
    assert base == _code_version(_component(mod, tmp_path, location=tmp_path / "elsewhere", sync_mode="upsert"))
    assert base != _code_version(_component(mod, tmp_path, chunk_size=80))  # chunking changed
    assert base != _code_version(_component(mod, tmp_path, model="fake-2"))  # embedding model changed
    assert _code_version(_component(mod, tmp_path, code_version="git-abc123")) == "git-abc123"  # explicit wins


def _run(component, rows, instance):
    asset_def = list(component.build_defs(None).assets)[0]

    @dg.asset(name="raw")
    def raw():
        return pd.DataFrame(rows, columns=["id", "body"])
    return dg.materialize([asset_def, raw], instance=instance)


def _tags(instance):
    tags = instance.get_latest_data_version_record(dg.AssetKey("kb")).asset_materialization.tags
    return {k: tags[k] for k in ("dagster/code_version", "dagster/data_version")}


@requires_chromadb
def test_materialization_and_every_chunk_carry_the_code_version(mod, tmp_path):
    import chromadb
    instance = dg.DagsterInstance.ephemeral()
    comp = _component(mod, tmp_path)
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(comp, [("d1", DOC)], instance)
    cv = _code_version(comp)
    assert _tags(instance)["dagster/code_version"] == cv
    metas = chromadb.PersistentClient(path=str(tmp_path / "chroma")).get_collection("kb_test").get(include=["metadatas"])["metadatas"]
    assert metas and {m["source_code_version"] for m in metas} == {cv}


@requires_chromadb
def test_recipe_change_flips_code_version_and_data_version_without_content_change(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path), [("d1", DOC)], instance)
        t1 = _tags(instance)
        _run(_component(mod, tmp_path), [("d1", DOC)], instance)
        assert _tags(instance) == t1  # no-op rebuild: both versions stable
        _run(_component(mod, tmp_path, model="fake-2"), [("d1", DOC)], instance)
    t2 = _tags(instance)
    assert t2["dagster/code_version"] != t1["dagster/code_version"]
    assert t2["dagster/data_version"] != t1["dagster/data_version"]


@requires_chromadb
def test_outcomes_can_be_compared_across_recipe_versions(mod, tmp_path):
    """The point of recording the recipe: did the new chunking help, judged by what users thought?"""
    path = Path(__file__).parents[4] / "resources/context_receipt_store/component.py"
    spec = importlib.util.spec_from_file_location("receipts_for_code_version", path)
    receipts = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(receipts)
    import chromadb
    store = receipts.ContextReceiptStore(str(tmp_path / "r.sqlite3"))
    instance = dg.DagsterInstance.ephemeral()

    def serve_and_judge(signal, n):
        metas = chromadb.PersistentClient(path=str(tmp_path / "chroma")).get_collection("kb_test").get(include=["metadatas"])
        item = {"item_id": metas["ids"][0], **{k: metas["metadatas"][0][k] for k in
                ("source_asset_key", "source_version", "source_code_version")}}
        for i in range(n):
            rid = store.record_retrieval(context_name="kb", items=[item])
            store.record_outcome(rid, signal=signal, outcome_ref=f"u{i}")

    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, chunk_size=60), [("d1", DOC)], instance)
        v1 = _code_version(_component(mod, tmp_path, chunk_size=60))
        serve_and_judge("thumbs_up", 3)
        _run(_component(mod, tmp_path, chunk_size=200), [("d1", DOC)], instance)  # new recipe
        v2 = _code_version(_component(mod, tmp_path, chunk_size=200))
        serve_and_judge("thumbs_down", 3)
    result = store.outcomes_by_code_version("kb")
    assert result[v1] == {"receipts": 3, "outcomes": 3, "mean_outcome": 1.0}
    assert result[v2] == {"receipts": 3, "outcomes": 3, "mean_outcome": -1.0}
