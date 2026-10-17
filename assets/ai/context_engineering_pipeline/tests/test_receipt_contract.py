"""The contract between this pipeline and the retrieval-receipt loop (resources/context_receipt_store):
the index never keeps stale chunks, every chunk is stamped with provenance a gateway can put straight
on a receipt, and the emitted data version changes only when content or recipe changes. Real chromadb
and real Dagster materializations; only the embedding call is faked."""
import hashlib
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module, requires_chromadb

pytestmark = requires_chromadb

PARAGRAPHS = [
    "First paragraph about billing refunds.",
    "Second paragraph about shipping delays.",
    "Third paragraph about app crashes.",
]
DOC = "\n\n".join(PARAGRAPHS)


@pytest.fixture()
def mod():
    return load_component_module()


def _fake_embedding(model, input, api_key=None):
    resp = MagicMock()
    resp.data = [{"embedding": [float(len(t)), float(int(hashlib.md5(t.encode()).hexdigest(), 16) % 97), 1.0]} for t in input]
    return resp


def _component(mod, tmp_path, sync_mode=None, partition_type=None, **kw):
    write = {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"}
    if sync_mode:
        write["sync_mode"] = sync_mode
    steps = kw.pop("steps", None) or [
        {"id": "chunks", "op": "chunk", "strategy": "recursive", "chunk_size": 60, "chunk_overlap": 0},
        {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-1"},
        write,
    ]
    extra = {"partition_type": partition_type, "partition_start": "2026-10-01"} if partition_type else {}
    return mod.ContextEngineeringPipelineComponent(
        asset_name="kb", upstream_asset_key="raw", id_column="id", text_column="body", steps=steps, **extra, **kw)


def _run(component, rows, instance, partition_key=None, raise_on_error=True):
    asset_def = list(component.build_defs(None).assets)[0]
    df = pd.DataFrame(rows, columns=["id", "body"])

    @dg.asset(name="raw")
    def raw():
        return df

    if partition_key is not None:
        # raw is unpartitioned and kb is partitioned: materialize raw first, then only kb's partition
        dg.materialize([raw], instance=instance)
        return dg.materialize([asset_def, raw], instance=instance, partition_key=partition_key,
                              selection=["kb"], raise_on_error=raise_on_error)
    return dg.materialize([asset_def, raw], instance=instance, raise_on_error=raise_on_error)


def _collection(tmp_path):
    import chromadb
    return chromadb.PersistentClient(path=str(tmp_path / "chroma")).get_collection("kb_test")


def _data_version(instance):
    record = instance.get_latest_data_version_record(dg.AssetKey("kb"))
    return record.asset_materialization.tags["dagster/data_version"]


def test_shrunk_document_leaves_no_stale_chunks_and_other_sources_untouched(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path), [("d1", DOC), ("d2", "Other source text.")], instance)
        assert sorted(_collection(tmp_path).get()["ids"]) == ["d1_0", "d1_1", "d1_2", "d2_0"]
        # d1 shrinks to one paragraph; d2 is absent from this run (e.g. a partition slice)
        _run(_component(mod, tmp_path), [("d1", PARAGRAPHS[0])], instance)
    assert sorted(_collection(tmp_path).get()["ids"]) == ["d1_0", "d2_0"]  # d1_1, d1_2 gone; d2 kept


def test_upsert_mode_keeps_legacy_behaviour_and_replace_all_makes_run_exact(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, sync_mode="upsert"), [("d1", DOC)], instance)
        _run(_component(mod, tmp_path, sync_mode="upsert"), [("d1", PARAGRAPHS[0])], instance)
        assert len(_collection(tmp_path).get()["ids"]) == 3  # stale chunks remain by explicit choice
        _run(_component(mod, tmp_path, sync_mode="replace_all"), [("d9", "Brand new corpus.")], instance)
    assert _collection(tmp_path).get()["ids"] == ["d9_0"]


def test_every_chunk_is_stamped_with_provenance_matching_the_emitted_data_version(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path), [("d1", DOC)], instance)
    got = _collection(tmp_path).get(include=["metadatas", "documents"])
    version = _data_version(instance)
    for cid, meta, doc in zip(got["ids"], got["metadatas"], got["documents"]):
        assert meta["source_asset_key"] == "kb"
        assert meta["source_version"] == version
        assert meta["source_id"] == "d1" and meta["chunk_id"] == cid
        assert meta["content_hash"] == hashlib.sha256(doc.encode()).hexdigest()
        assert meta["embedding_model"] == "fake-1"
        assert "source_partition" not in meta  # unpartitioned asset


def test_stamping_can_be_disabled(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, stamp_provenance=False), [("d1", DOC)], instance)
    meta = _collection(tmp_path).get(include=["metadatas"])["metadatas"][0]
    assert "source_version" not in meta and "source_asset_key" not in meta and meta["source_id"] == "d1"


def test_data_version_is_stable_for_noop_rebuilds_and_changes_with_content_or_recipe(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path), [("d1", DOC)], instance)
        v1 = _data_version(instance)
        _run(_component(mod, tmp_path), [("d1", DOC)], instance)
        assert _data_version(instance) == v1  # Dagster's default would have changed here
        _run(_component(mod, tmp_path), [("d1", DOC.replace("delays", "costs"))], instance)
        v2 = _data_version(instance)
        assert v2 != v1  # content changed
        recipe = _component(mod, tmp_path, steps=[
            {"id": "chunks", "op": "chunk", "strategy": "recursive", "chunk_size": 61, "chunk_overlap": 0},
            {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-1"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"},
        ])
        _run(recipe, [("d1", DOC.replace("delays", "costs"))], instance)
        assert _data_version(instance) != v2  # chunking recipe is part of the version even if output is identical
        model = _component(mod, tmp_path, steps=[
            {"id": "chunks", "op": "chunk", "strategy": "recursive", "chunk_size": 60, "chunk_overlap": 0},
            {"id": "embedded", "op": "embed", "provider": "litellm", "model": "fake-2"},
            {"id": "indexed", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": "kb_test"},
        ])
        _run(model, [("d1", DOC.replace("delays", "costs"))], instance)
        assert _data_version(instance) != v2  # embedding model is part of the recipe


def test_data_version_emission_can_be_turned_off(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, emit_data_version=False), [("d1", DOC)], instance)
        v1 = _data_version(instance)
        _run(_component(mod, tmp_path, emit_data_version=False), [("d1", DOC)], instance)
    assert _data_version(instance) != v1  # falls back to Dagster's per-run default


def test_partitioned_run_stamps_partition_and_replace_all_is_refused(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, partition_type="daily"), [("d1", DOC)], instance, partition_key="2026-10-08")
        meta = _collection(tmp_path).get(include=["metadatas"])["metadatas"][0]
        assert meta["source_partition"] == "2026-10-08"
        result = _run(_component(mod, tmp_path, partition_type="daily", sync_mode="replace_all"),
                      [("d1", DOC)], instance, partition_key="2026-10-08", raise_on_error=False)
    assert not result.success


def test_end_to_end_receipts_see_noop_rebuild_as_fresh_and_content_change_as_stale(mod, tmp_path):
    """The real loop: pipeline -> index -> gateway-style receipt from index metadata -> rebuild -> staleness."""
    spec = importlib.util.spec_from_file_location(
        "receipt_store_for_cep", Path(__file__).parents[4] / "resources/context_receipt_store/component.py")
    receipts_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(receipts_mod)
    store = receipts_mod.ContextReceiptStore(str(tmp_path / "receipts.sqlite3"))
    instance = dg.DagsterInstance.ephemeral()

    def retrieve_and_receipt():
        got = _collection(tmp_path).get(include=["metadatas"])
        items = [{"item_id": cid, "source_asset_key": m["source_asset_key"], "source_version": m["source_version"],
                  **({"source_partition": m["source_partition"]} if "source_partition" in m else {}), "rank": i}
                 for i, (cid, m) in enumerate(zip(got["ids"], got["metadatas"]))]
        return store.record_retrieval(context_name="kb", items=items)

    def stale_items(receipt_id):
        current = receipts_mod._current_data_version(instance, dg.AssetKey("kb"))
        return [i for i in store.get_receipt(receipt_id)["items"] if i["source_version"] != current]

    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path), [("d1", DOC)], instance)
        receipt = retrieve_and_receipt()
        assert stale_items(receipt) == []
        _run(_component(mod, tmp_path), [("d1", DOC)], instance)  # nothing changed
        assert stale_items(receipt) == []  # no false stale
        _run(_component(mod, tmp_path), [("d1", DOC.replace("delays", "costs"))], instance)
        assert len(stale_items(receipt)) == 3  # the knowledge base really changed
