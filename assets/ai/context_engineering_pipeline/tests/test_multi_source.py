"""Building context from several kinds of source (tickets, product docs, product code), alone and sharing
one index. Real chromadb and real Dagster materializations; only the embedding call is faked."""
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


class Ctx:
    class log:
        @staticmethod
        def info(*a, **k): ...
        warning = info


PY_SOURCE = '''import os
from typing import List

LIMIT = 10


@cached
def load(path: str) -> List[str]:
    """Load lines."""
    return open(path).read().splitlines()


class Parser:
    def parse(self, line):
        return line.split(",")

    def clean(self, row):
        return [c.strip() for c in row]


def helper():
    return LIMIT
'''


def _code(mod, text, **step):
    df = pd.DataFrame({"id": ["f1"], "body": [text], "path": [step.pop("path", "x.py")]})
    return mod._do_chunk(df, {"op": "chunk", "strategy": "code", **step}, "id", "body", [], Ctx)


# ── code-aware chunking ─────────────────────────────────────────────────────

def test_python_units_follow_symbols_and_keep_decorators(mod):
    out = _code(mod, PY_SOURCE, chunk_size=150, chunk_overlap=0, language="python")
    by_label = {r.section_path.split(" +")[0]: r.chunk_text for r in out.itertuples()}
    assert by_label["module"].startswith("import os") and "LIMIT = 10" in by_label["module"]
    assert by_label["load"].startswith("@cached\ndef load") and "Load lines." in by_label["load"]
    cls = [r.chunk_text for r in out.itertuples() if "class Parser" in r.chunk_text][0]
    assert "def parse" in cls and "def clean" in cls and "def helper" not in cls


def test_small_units_are_packed_and_oversized_units_are_split(mod):
    packed = _code(mod, PY_SOURCE, chunk_size=4000, chunk_overlap=0, language="python")
    assert len(packed) == 1 and "+" in packed["section_path"].iloc[0]  # everything fits: one chunk, label notes the merge
    big = "def big():\n" + "\n".join(f"    x{i} = {i}" for i in range(80))
    split = _code(mod, big, chunk_size=200, chunk_overlap=0, language="python")
    assert len(split) > 1 and (split["chunk_text"].str.len() <= 200).all()
    assert set(split["section_path"]) == {"big"}


def test_language_from_column_path_or_default_and_fallbacks(mod):
    js = "const a = 1;\n\nexport function one() {\n  return 1;\n}\n\nexport class Two {\n  m() {}\n}\n"
    out = _code(mod, js, chunk_size=40, chunk_overlap=0, path_column="path", path="src/app.ts")
    labels = [r.section_path for r in out.itertuples()]
    assert any("export function one" in l for l in labels) and any("export class Two" in l for l in labels)
    go = "package x\n\nfunc A() {}\n\nfunc B() {}\n"
    assert len(_code(mod, go, chunk_size=15, chunk_overlap=0, language="go")) >= 2
    broken = "def oops(:\n    pass\n\ndef fine():\n    return 1\n"  # not valid Python: falls back to the heuristic
    out = _code(mod, broken, chunk_size=20, chunk_overlap=0, language="python")
    assert any("def fine" in t for t in out["chunk_text"])
    assert len(_code(mod, "just prose with no definitions at all", chunk_size=100, chunk_overlap=0)) == 1


def test_code_strategy_is_valid_and_prefix_is_applied(mod):
    out = _code(mod, "def a():\n    pass", language="python", chunk_id_prefix="code:")
    assert out["chunk_id"].iloc[0] == "code:f1_0"
    with pytest.raises(ValueError, match="strategy"):
        _code(mod, "x", strategy="nope") if False else mod._do_chunk(
            pd.DataFrame({"id": ["1"], "body": ["x"]}), {"strategy": "nope"}, "id", "body", [], Ctx)


# ── several assets, one shared index ────────────────────────────────────────

def _fake_embedding(model, input, api_key=None):
    resp = MagicMock()
    resp.data = [{"embedding": [float(len(t)), float(int(hashlib.md5(t.encode()).hexdigest(), 16) % 97), 1.0]} for t in input]
    return resp


def _component(mod, tmp_path, name, upstream, prefix=None, collection="shared_kb", sync_mode=None, strategy="recursive", **kw):
    chunk = {"id": "c", "op": "chunk", "strategy": strategy, "chunk_size": 60, "chunk_overlap": 0}
    if prefix:
        chunk["chunk_id_prefix"] = prefix
    write = {"id": "w", "op": "write_vector_store", "provider": "chromadb",
             "connection_string": str(tmp_path / "chroma"), "collection_name": collection}
    if sync_mode:
        write["sync_mode"] = sync_mode
    return mod.ContextEngineeringPipelineComponent(
        asset_name=name, upstream_asset_key=upstream, id_column="id", text_column="body",
        steps=[chunk, {"id": "e", "op": "embed", "provider": "litellm", "model": "fake"}, write], **kw)


def _run(component, upstream, rows, instance=None, raise_on_error=True):
    asset_def = list(component.build_defs(None).assets)[0]

    @dg.asset(name=upstream)
    def raw():
        return pd.DataFrame(rows, columns=["id", "body"])
    return dg.materialize([asset_def, raw], instance=instance, raise_on_error=raise_on_error)


def _collection(tmp_path, name="shared_kb"):
    import chromadb
    return chromadb.PersistentClient(path=str(tmp_path / "chroma")).get_collection(name)


@requires_chromadb
def test_overlapping_ids_in_a_shared_collection_fail_loudly_instead_of_overwriting(mod, tmp_path):
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, "tickets", "raw_t"), "raw_t", [("1", "Ticket one about billing.")])
        result = _run(_component(mod, tmp_path, "docs", "raw_d"), "raw_d", [("1", "Docs page one about refunds.")],
                      raise_on_error=False)
    assert not result.success  # used to silently replace the ticket's chunk
    got = _collection(tmp_path).get(include=["metadatas", "documents"])
    assert got["ids"] == ["1_0"] and got["metadatas"][0]["source_asset_key"] == "tickets"


@requires_chromadb
def test_prefixed_assets_coexist_and_syncing_one_never_touches_the_other(mod, tmp_path):
    instance = dg.DagsterInstance.ephemeral()
    long_doc = "Paragraph one about refund policies.\n\nParagraph two about shipping delays.\n\nParagraph three about app crashes."
    with patch("litellm.embedding", side_effect=_fake_embedding):
        _run(_component(mod, tmp_path, "tickets", "raw_t", prefix="tkt:"), "raw_t", [("1", "Ticket one about billing.")], instance)
        _run(_component(mod, tmp_path, "docs", "raw_d", prefix="doc:"), "raw_d", [("1", long_doc)], instance)
        assert sorted(_collection(tmp_path).get()["ids"]) == ["doc:1_0", "doc:1_1", "doc:1_2", "tkt:1_0"]
        # docs shrinks: only docs' stale chunks go; the ticket with the SAME source id is untouched
        _run(_component(mod, tmp_path, "docs", "raw_d", prefix="doc:"), "raw_d", [("1", "Paragraph one about refund policies.")], instance)
        assert sorted(_collection(tmp_path).get()["ids"]) == ["doc:1_0", "tkt:1_0"]
        # replace_all is scoped to the asset too: rebuilding docs from scratch leaves tickets alone
        _run(_component(mod, tmp_path, "docs", "raw_d", prefix="doc:", sync_mode="replace_all"), "raw_d", [("9", "Fresh docs.")], instance)
    assert sorted(_collection(tmp_path).get()["ids"]) == ["doc:9_0", "tkt:1_0"]
    metas = {cid: m for cid, m in zip(*[_collection(tmp_path).get(include=["metadatas"])[k] for k in ("ids", "metadatas")])}
    assert metas["tkt:1_0"]["source_asset_key"] == "tickets" and metas["doc:9_0"]["source_asset_key"] == "docs"


@requires_chromadb
def test_tickets_docs_and_code_as_three_governed_assets(mod, tmp_path):
    """The multi-source recipe: each kind of source is its own asset with its own chunking, collection and
    policy; the gateway discovers all three with context_policies and every chunk names its asset."""
    receipts_path = Path(__file__).parents[4] / "resources/context_receipt_store/component.py"
    spec = importlib.util.spec_from_file_location("receipts_multi", receipts_path)
    receipts = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(receipts)
    sources = {
        "support_tickets": dict(strategy="recursive", policy={"scope": "customer_id", "sensitivity": "confidential"},
                                rows=[("T1", "Customer cannot export reports since Tuesday.")]),
        "product_docs": dict(strategy="markdown", policy={"sensitivity": "public"},
                             rows=[("D1", "# Exports\nUse the Export button.\n## Limits\nMax 10k rows.")]),
        "product_code": dict(strategy="code", policy={"sensitivity": "internal", "access_tags": ["engineering"]},
                             rows=[("export.py", "def export(rows):\n    return rows[:10000]\n")]),
    }
    instance = dg.DagsterInstance.ephemeral()
    defs_assets = []
    with patch("litellm.embedding", side_effect=_fake_embedding):
        for name, cfg in sources.items():
            comp = _component(mod, tmp_path, name, f"raw_{name}", prefix=f"{name}:", collection=name,
                              strategy=cfg["strategy"], context_policy=cfg["policy"])
            _run(comp, f"raw_{name}", cfg["rows"], instance)
            defs_assets.append(comp.build_defs(None))
    merged = dg.Definitions.merge(*defs_assets, dg.Definitions(assets=[
        dg.AssetSpec(f"raw_{n}") for n in sources]))
    policies = receipts.context_policies(merged)
    assert set(policies) == set(sources)
    assert policies["support_tickets"].scope == "customer_id" and policies["product_code"].access_tags == ("engineering",)
    for name in sources:
        meta = _collection(tmp_path, name).get(include=["metadatas"])["metadatas"][0]
        assert meta["source_asset_key"] == name and meta["source_version"]
    versions = {n: instance.get_latest_data_version_record(dg.AssetKey(n)).asset_materialization.tags["dagster/data_version"]
                for n in sources}
    assert len(set(versions.values())) == 3  # independent versions: rebuilding one never makes the others look stale
