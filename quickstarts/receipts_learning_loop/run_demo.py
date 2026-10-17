#!/usr/bin/env python3
"""The receipts learning loop, end to end, with the REAL components. Offline: DuckDB + local Chroma + SQLite receipts.

    python3 quickstarts/receipts_learning_loop/run_demo.py

Only two things are faked (see fake_embeddings.py): the embedding model (a deterministic hashing stand-in) and the LLM
answer text. Every other line below -- chunking, dedupe, the Chroma writes, receipts, rerank, quarantine, shadow -- is the
component code from assets/ai/ and resources/context_receipt_store/.

`run_cycle()` is what tests/test_demo.py calls. Env DEMO_DISABLE_FEEDBACK=1 is a mutation switch for that test: outcomes
are never recorded, so the loop has nothing to learn from (the test must then fail).
"""
import csv
import importlib.util
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Callable, Dict, List, Optional

import dagster as dg
import duckdb
import pandas as pd
import yaml
from dagster_duckdb import DuckDBResource

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
import fake_embeddings  # noqa: E402

OUTDATED = "billing_refund_2021_outdated"
STORE_KEY = "context_receipts"


# --------------------------------------------------------------------------- loading components and their YAML
def load_module(rel: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_yaml(name: str, workdir: Path) -> dict:
    text = (HERE / "defs" / name).read_text().replace("${WORKDIR}", str(workdir))
    return yaml.safe_load(text)["attributes"]


def read_csv(name: str) -> List[dict]:
    with open(HERE / "data" / name, newline="") as f:
        return list(csv.DictReader(f))


# --------------------------------------------------------------------------- the pieces of the loop
def build_index(cep, workdir: Path, yaml_name: str, duckdb_path: str, store, use_receipts: bool = True) -> dict:
    """Materialize a context_engineering_pipeline asset. Returns its materialization metadata."""
    attrs = load_yaml(yaml_name, workdir)
    if not use_receipts:
        attrs.pop("receipts", None)
    comp = cep.ContextEngineeringPipelineComponent(**attrs)
    resources = {"duckdb": DuckDBResource(database=duckdb_path), STORE_KEY: store}
    with fake_embeddings.installed():
        res = dg.materialize(list(comp.build_defs(None).assets), resources=resources, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    ev = next(e for e in res.get_asset_materialization_events() if e.materialization.asset_key == dg.AssetKey("support_kb"))
    return dict(ev.materialization.metadata)


def chroma_ids(workdir: Path, directory: str, collection: str) -> Dict[str, dict]:
    import chromadb
    got = chromadb.PersistentClient(path=str(workdir / directory)).get_collection(collection).get()
    return dict(zip(got["ids"], got["metadatas"]))


def ask(rag, workdir: Path, store, questions: List[str], yaml_name: str = "answers.yaml"):
    """Run rag_pipeline over the questions. Returns [(question, receipt_id, served chunk ids in rank order)]."""
    comp = rag.RAGPipelineComponent(**load_yaml(yaml_name, workdir))

    @dg.asset(name="questions")
    def questions_asset():
        return pd.DataFrame({"query": questions})

    defs = comp.build_defs(None)
    resources = {"io_manager": dg.InMemoryIOManager(), STORE_KEY: dg.ResourceDefinition.hardcoded_resource(store)}
    with fake_embeddings.installed(rag):
        res = dg.materialize([questions_asset, *defs.assets], resources=resources)
        assert res.success
    out = res.output_for_node("answers")
    served = []
    for q, rid in zip(out["query"], out["receipt_id"]):
        items = sorted(store.get_receipt(rid)["items"], key=lambda i: i.get("rank") or 0)
        served.append((q, rid, [i["item_id"] for i in items]))
    return served


def user_rating(served_ids: List[str], gold: str) -> str:
    """A simulated person: thumbs_up only if the right chunk came first AND the outdated policy was not shown at all."""
    ok = bool(served_ids) and served_ids[0] == f"{gold}_0" and f"{OUTDATED}_0" not in served_ids
    return "thumbs_up" if ok else "thumbs_down"


def apply_feedback_csv(store, by_question: Dict[str, str], path: Path) -> int:
    """Fallback feedback ingestion (used when assets/ai/receipt_outcome_ingestion is not available): CSV -> store.record_outcome."""
    n = 0
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            rid = by_question.get(row["question"])
            if rid:
                store.record_outcome(rid, signal="thumbs_up" if row["rating"] == "up" else "thumbs_down", outcome_ref=f"csv:{row['user']}")
                n += 1
    return n


# --------------------------------------------------------------------------- the whole cycle
def run_cycle(workdir: Optional[str] = None, say: Callable[[str], None] = lambda s: None) -> dict:
    disable = os.environ.get("DEMO_DISABLE_FEEDBACK") == "1"
    workdir = Path(workdir or tempfile.mkdtemp(prefix="receipts_demo_"))
    workdir.mkdir(parents=True, exist_ok=True)
    cep = load_module("assets/ai/context_engineering_pipeline/component.py", "demo_cep")
    rag = load_module("assets/ai/rag_pipeline/component.py", "demo_rag")
    store_mod = load_module("resources/context_receipt_store/component.py", "demo_store")
    store = store_mod.ContextReceiptStore(load_yaml("context_receipt_store.yaml", workdir)["database_path"])
    qs = {r["question"]: r for r in read_csv("questions.csv")}
    train = [q for q, r in qs.items() if r["round"] == "train"]
    shadow_qs = [q for q, r in qs.items() if r["round"] == "shadow"]
    R: dict = {"workdir": str(workdir), "feedback_disabled": disable}

    # 1) the corpus lives in DuckDB; the knowledge base is built from it
    db = str(workdir / "warehouse.duckdb")
    con = duckdb.connect(db)
    con.execute(f"CREATE TABLE support_docs AS SELECT * FROM read_csv_auto('{HERE / 'data' / 'support_docs.csv'}')")
    n_docs = con.execute("SELECT count(*) FROM support_docs").fetchone()[0]
    con.close()
    md1 = build_index(cep, workdir, "support_kb.yaml", db, store)
    ids1 = chroma_ids(workdir, "chroma_live", "support_kb")
    R.update(n_docs=n_docs, ids_before=sorted(ids1), build1_summary=md1["receipts_feedback"].value if "receipts_feedback" in md1 else None)
    say(f"STEP 1  build: {n_docs} support docs in DuckDB -> chunk -> dedupe -> embed -> Chroma = {len(ids1)} chunks indexed "
        f"({n_docs - len(ids1)} duplicate dropped). Every chunk is stamped with source_code_version={ids1[next(iter(ids1))].get('source_code_version')}.")

    # 2) answer the questions; rag_pipeline records a receipt per answer (what was retrieved, from which recipe)
    before = ask(rag, workdir, store, train)
    R["before"] = before
    say("STEP 2  ask (receipts record what each answer was built from):")
    for q, rid, served in before:
        say(f"          {q!r:70} -> {served}")

    # 3) outcomes. Two users via the store API directly, three more from data/feedback.csv
    by_q = {q: rid for q, rid, _ in before}
    n_direct = 0
    if not disable:
        for q, rid, served in before:
            for user in ("direct_u1", "direct_u2"):
                store.record_outcome(rid, signal=user_rating(served, qs[q]["gold_doc"]), outcome_ref=user)
                n_direct += 1
        ingest_note = _ingest_feedback(store, by_q, workdir)
        n_csv = ingest_note["rows"]
    else:
        n_csv, ingest_note = 0, {"via": "disabled (mutation switch)", "rows": 0}
    R.update(outcomes_direct=n_direct, outcomes_csv=n_csv, ingest_via=ingest_note["via"])
    downs = [(q, s) for q, _, s in before if user_rating(s, qs[q]["gold_doc"]) == "thumbs_down"]
    say(f"STEP 3  outcomes: {n_direct} recorded via the store API, {n_csv} from feedback.csv via {ingest_note['via']}. "
        f"{len(downs)} of {len(before)} questions were served a wrong first answer (thumbs_down).")
    served_bad = {q for q, _, s in before if OUTDATED + "_0" in s}
    R["questions_served_outdated"] = sorted(served_bad)

    # 4) rebuild: the build reads the receipts and acts on them
    md2 = build_index(cep, workdir, "support_kb.yaml", db, store, use_receipts=not disable)
    ids2 = chroma_ids(workdir, "chroma_live", "support_kb")
    fb = md2["receipts_feedback"].value if "receipts_feedback" in md2 else {}
    R.update(ids_after=sorted(ids2), build2_summary=fb, metas_after=ids2)
    gone = sorted(set(ids1) - set(ids2))
    say(f"STEP 4  rebuild: quarantined (removed from the index): {gone or 'nothing'}")
    if fb:
        say(f"          receipts_feedback: receipts_in_window={fb['receipts_in_window']} chunks_rated={fb['chunks_rated']} "
            f"quarantined_ids={fb['quarantined_ids']}")
        say(f"          rewrite_candidates (worst first): {[(c['chunk_id'], round(c['mean_outcome'], 2)) for c in fb['rewrite_candidates']]}")
        say(f"          prune: judged={fb.get('prune_judged')} pruned={fb.get('pruned')} (needs {load_yaml('support_kb.yaml', workdir)['receipts']['prune']['never_retrieved_after_receipts']} receipts before dead weight is judged)")
    stamped = {i: m for i, m in ids2.items() if "feedback_retrievals" in m}
    best = sorted(stamped.items(), key=lambda kv: -float(kv[1].get("feedback_mean_outcome") or 0))[:3]
    say(f"          feedback_* now stamped on {len(stamped)}/{len(ids2)} chunks, e.g. "
        f"{[(i, 'retrievals=%s' % m['feedback_retrievals'], 'mean=%.2f' % float(m['feedback_mean_outcome'])) for i, m in best]}")

    # 5) re-ask the same questions
    after = ask(rag, workdir, store, train)
    R["after"] = after
    say("STEP 5  re-ask, what changed:")
    for (q, _, s0), (_, _, s1) in zip(before, after):
        gold = f"{qs[q]['gold_doc']}_0"
        r0 = s0.index(gold) + 1 if gold in s0 else None
        r1 = s1.index(gold) + 1 if gold in s1 else None
        mark = "CHANGED" if s0 != s1 else "same"
        say(f"          [{mark:7}] {q!r:70} gold rank {r0} -> {r1}   top-3 {s1}")

    # 6) shadow: candidate recipe (title+body) on half the traffic, judged by outcomes per recipe
    # The comparison gets its own receipts window (a fresh store) so the training-phase outcomes above do not count for recipe-7.
    # In production you bound it with receipts.shadow.window_days instead.
    store = store_mod.ContextReceiptStore(str(workdir / "receipts_shadow.sqlite3"))
    build_index(cep, workdir, "support_kb_candidate.yaml", db, store, use_receipts=False)
    served_sh = ask(rag, workdir, store, shadow_qs, "answers_shadow.yaml")
    for q, rid, s in served_sh:
        store.record_outcome(rid, signal=user_rating(s, qs[q]["gold_doc"]), outcome_ref=f"shadow:{q}")
    by_cv = store.outcomes_by_code_version("support_kb")
    check = _shadow_check(rag, workdir, store)
    R.update(shadow_by_code_version=by_cv, shadow_check=check)
    purposes = [store.get_receipt(rid)["purpose"] for _, rid, _ in served_sh]
    R["shadow_purposes"] = purposes
    R["shadow_detail"] = [(q, p, s, user_rating(s, qs[q]["gold_doc"])) for (q, _, s), p in zip(served_sh, purposes)]
    say(f"STEP 6  shadow: {sum(1 for p in purposes if p)} of {len(purposes)} questions served from candidate recipe-8, the rest live recipe-7.")
    for q, p, s, rating in R["shadow_detail"]:
        say(f"          {'candidate' if p else 'live     '} {rating:11} {q!r:62} top1={s[0]}")
    for cv in ("recipe-7", "recipe-8"):
        e = by_cv.get(cv, {})
        say(f"          {cv}: outcomes={e.get('outcomes')} mean_outcome={e.get('mean_outcome')}")
    say(f"          shadow_vs_live check -> verdict={check['verdict']} passed={check['passed']} (WARN-severity; WARNs only if live is better)")
    return R


def _ingest_feedback(store, by_question: Dict[str, str], workdir: Path) -> dict:
    """data/feedback.csv -> outcomes. Uses assets/ai/receipt_outcome_ingestion when that component is present (a real file-source
    ingestion asset, idempotent); otherwise falls back to a plain store.record_outcome loop."""
    if not (ROOT / "assets/ai/receipt_outcome_ingestion/component.py").exists():
        return {"via": "store.record_outcome (CSV loop in run_demo.py; receipt_outcome_ingestion not present)",
                "rows": apply_feedback_csv(store, by_question, HERE / "data" / "feedback.csv")}
    rows = [{**r, "receipt_id": by_question[r["question"]]} for r in read_csv("feedback.csv") if r["question"] in by_question]
    joined = workdir / "feedback_with_receipts.csv"
    pd.DataFrame(rows).to_csv(joined, index=False)
    roi = load_module("assets/ai/receipt_outcome_ingestion/component.py", "demo_roi")
    comp = roi.ReceiptOutcomeIngestionComponent(**load_yaml("receipt_outcome_ingestion.yaml", workdir))
    res = dg.materialize(list(comp.build_defs(None).assets), resources={STORE_KEY: store}, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    md = next(e for e in res.get_asset_materialization_events()).materialization.metadata
    return {"via": "assets/ai/receipt_outcome_ingestion", "rows": len(rows), "metadata": {k: v.value for k, v in md.items() if k in ("recorded", "rows_read", "skipped")}}


def _shadow_check(rag, workdir: Path, store) -> dict:
    """Evaluate the real `shadow_vs_live` asset check against the receipts recorded so far (no new answers)."""
    comp = rag.RAGPipelineComponent(**load_yaml("answers_shadow.yaml", workdir))
    defs = comp.build_defs(None)
    check_def = next(c for c in defs.asset_checks if any(k.name == 'shadow_vs_live' for k in c.check_keys))
    from dagster import build_op_context
    res = check_def(build_op_context(resources={STORE_KEY: store}))
    res = next(iter(res)) if not hasattr(res, 'metadata') else res
    md = {k: v.value for k, v in res.metadata.items()}
    return {"passed": bool(res.passed), "severity": str(res.severity), **md}


if __name__ == "__main__":
    out = run_cycle(say=print)
    print(f"\n(work dir: {out['workdir']})")
