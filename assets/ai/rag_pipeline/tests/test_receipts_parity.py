"""The inlined receipt-rerank block must behave exactly like the store's real `rerank` (loaded by path)."""
import importlib.util
import pathlib
import random

import pytest

from .conftest import load_component_module

STORE_PY = pathlib.Path(__file__).resolve().parents[4] / "resources" / "context_receipt_store" / "component.py"


@pytest.fixture(scope="module")
def store_mod():
    spec = importlib.util.spec_from_file_location("context_receipt_store_for_parity", STORE_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _case(rnd: random.Random):
    n = rnd.randint(0, 12)
    candidates = [{"item_id": f"c{i}", "score": rnd.choice([None, rnd.random(), rnd.random() * 2 - 1]), "text": f"t{i}"} for i in range(n)]
    scores = {f"c{i}": {"prior": rnd.choice([None, 0.0, rnd.uniform(-1, 1)])} for i in range(n) if rnd.random() < 0.7}
    top_k = rnd.choice([None, 0, 1, 3, n, n + 2])
    return candidates, scores, top_k, rnd.choice([0.0, 0.2, 5.0]), rnd.choice([0.0, 0.1, 0.5, 1.0])


def test_inline_rerank_matches_the_stores_rerank_on_seeded_inputs(store_mod):
    inline = load_component_module().receipt_rerank
    seeds = random.Random(1234)
    explored = 0
    for _ in range(300):
        candidates, scores, top_k, weight, explore = _case(seeds)
        seed = seeds.randint(0, 10**9)
        kw = dict(top_k=top_k, prior_weight=weight, explore_fraction=explore)
        want = store_mod.rerank(candidates, scores, rng=random.Random(seed), **kw)
        got = inline(candidates, scores, rng=random.Random(seed), **kw)
        assert got == want
        explored += any(c["selection"] == "explore" for c in got)
    assert explored > 10  # the exploration branch was really exercised


def test_inline_rerank_rejects_bad_explore_fraction_like_the_store(store_mod):
    inline = load_component_module().receipt_rerank
    for fn in (inline, store_mod.rerank):
        with pytest.raises(ValueError):
            fn([], {}, explore_fraction=1.5)
