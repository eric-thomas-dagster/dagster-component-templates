"""End-to-end regression test: runs the demo's whole cycle with the real components and asserts the loop actually learned.

Offline (DuckDB + local Chroma + SQLite). Only the embedding model (fake_embeddings.py) and LLM text are stand-ins.
Run:  python3 -m pytest quickstarts/receipts_learning_loop/tests -q
"""
import sys
from pathlib import Path

import pytest

pytest.importorskip("chromadb")
pytest.importorskip("duckdb")
pytest.importorskip("dagster_duckdb")

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import run_demo  # noqa: E402

BAD = f"{run_demo.OUTDATED}_0"
GOOD = "billing_refund_current_0"
REFUND_QS = ["Why was I charged twice for my subscription?", "I was charged twice so how do I get my money back?",
             "Is a double charge on my subscription refundable?"]


def assert_learned(R: dict) -> None:
    """Everything the loop is supposed to have done. Raises AssertionError when it did not."""
    served_before = {c for _, _, s in R["before"] for c in s}
    assert BAD in served_before, "precondition: the outdated chunk is being served before the rebuild"
    # 4) bad chunk quarantined (removed from the index) and reported
    assert BAD in R["ids_before"] and BAD not in R["ids_after"], "outdated chunk not quarantined by the rebuild"
    fb = R["build2_summary"]
    assert fb["quarantined_ids"] == [BAD]
    assert BAD in [c["chunk_id"] for c in fb["rewrite_candidates"]], "rewrite_candidates should name the bad chunk"
    assert set(R["ids_before"]) - set(R["ids_after"]) == {BAD}, "nothing but the bad chunk was touched"
    # feedback_* stamped on the surviving chunks
    meta = R["metas_after"][GOOD]
    assert meta["feedback_retrievals"] >= 5 and meta["feedback_rated"] >= 5 and "feedback_mean_outcome" in meta
    # 5) re-ask: the bad chunk is gone from every answer, and a good chunk's rank improved or stayed
    for (q, _, before), (_, _, after) in zip(R["before"], R["after"]):
        assert BAD not in after, f"{q!r} still served the quarantined chunk"
    gold = {r["question"]: r["gold_doc"] for r in run_demo.read_csv("questions.csv")}
    for (q, _, before), (_, _, after) in zip(R["before"], R["after"]):
        if gold[q] == "billing_refund_current" and GOOD in before:      # questions the good chunk is the right answer to
            assert GOOD in after and after.index(GOOD) <= before.index(GOOD), f"good chunk lost rank for {q!r}"
    for q in REFUND_QS:
        before = next(s for qq, _, s in R["before"] if qq == q)
        after = next(s for qq, _, s in R["after"] if qq == q)
        assert before[0] == BAD and after[0] == GOOD, f"{q!r}: ranking should have changed from the outdated page to the current one"


@pytest.fixture(scope="module")
def cycle(tmp_path_factory):
    return run_demo.run_cycle(str(tmp_path_factory.mktemp("demo")))


def test_corpus_built_once_with_duplicate_dropped(cycle):
    assert cycle["n_docs"] == 30
    assert len(cycle["ids_before"]) == 29


def test_the_loop_learned(cycle):
    assert_learned(cycle)


def test_outcomes_came_from_both_paths(cycle):
    assert cycle["outcomes_direct"] == 2 * len(cycle["before"]) and cycle["outcomes_csv"] == 3 * len(cycle["before"])


def test_receipt_per_answered_question_with_right_code_versions(cycle):
    from run_demo import load_module
    store = load_module("resources/context_receipt_store/component.py", "t_store").ContextReceiptStore(
        str(Path(cycle["workdir"]) / "receipts.sqlite3"))
    for phase in ("before", "after"):
        rids = [rid for _, rid, _ in cycle[phase]]
        assert len(rids) == len(set(rids)) == 14
        for (_, rid, served) in cycle[phase]:
            r = store.get_receipt(rid)
            assert [i["item_id"] for i in sorted(r["items"], key=lambda i: i.get("rank") or 0)] == served
            assert {i["source_code_version"] for i in r["items"]} == {"recipe-7"}
            assert {i["source_asset_key"] for i in r["items"]} == {"support_kb"}


def test_shadow_verdict_is_computed_from_both_recipes(cycle):
    chk = cycle["shadow_check"]
    assert chk["verdict"] in {"candidate_better", "live_better", "inconclusive"}, chk      # not insufficient_evidence
    assert chk["n_live"] + chk["n_candidate"] == len(cycle["shadow_purposes"]) == 20
    assert chk["n_candidate"] == sum(1 for p in cycle["shadow_purposes"] if p) > 0
    assert set(cycle["shadow_by_code_version"]) == {"recipe-7", "recipe-8"}
    assert {s[0] for _, p, s, _ in cycle["shadow_detail"] if not p}.isdisjoint({BAD})        # live no longer serves the bad page


def test_mutation_without_feedback_the_loop_does_not_learn(tmp_path, monkeypatch):
    """Mutation check, kept as a permanent test: with outcomes switched off the same assertions must FAIL."""
    monkeypatch.setenv("DEMO_DISABLE_FEEDBACK", "1")
    R = run_demo.run_cycle(str(tmp_path))
    assert BAD in R["ids_after"], "with no outcomes the outdated chunk must still be indexed"
    with pytest.raises(AssertionError):
        assert_learned(R)
