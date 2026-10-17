"""Deterministic, offline STAND-IN for an embedding model. NOT a real model.

The components call `litellm.embedding(...)` (context_engineering_pipeline, at build time) and
`rag_pipeline._embed_query(...)` (at question time). This module replaces exactly those two seams with a
bag-of-words hashing embedding: lowercase, drop stopwords, crude plural stemming, hash each word into one of
DIM buckets, L2-normalise. Texts that share words land close together (retrieval is "semantic-ish": lexical
overlap, no synonyms), and it is fully deterministic so the demo and its test are reproducible.

Same seams the committed component tests patch (see assets/ai/context_engineering_pipeline/tests/test_closed_loop.py).

To use a REAL embedding model instead, delete the `with fake_embeddings.installed(...)` blocks in run_demo.py
and set a real model in defs/support_kb.yaml, e.g. `model: text-embedding-3-small` (needs OPENAI_API_KEY) --
that is the one-line change. Everything else (receipts, rerank, quarantine, shadow) is unchanged.
"""
import contextlib
import hashlib
import re
from unittest.mock import MagicMock, patch

DIM = 512
_STOP = set("a an the is are was were be to of in on for and or it its my me i we you your our can do does how what why when where "
            "with by at as that this if so no not from they them there their will would should".split())


def _stem(w: str) -> str:
    return w[:-1] if len(w) > 3 and w.endswith("s") and not w.endswith("ss") else w


def vec(text: str) -> list:
    v = [0.0] * DIM
    for w in re.findall(r"[a-z]+", text.lower()):
        if w in _STOP:
            continue
        v[int(hashlib.md5(_stem(w).encode()).hexdigest(), 16) % DIM] += 1.0
    n = sum(x * x for x in v) ** 0.5 or 1.0
    return [x / n for x in v]


def _fake_litellm_embedding(model, input, api_key=None, **kw):
    resp = MagicMock()
    resp.data = [{"embedding": vec(t)} for t in input]
    return resp


def _fake_completion(**kw):
    resp = MagicMock()
    resp.choices = [MagicMock(message=MagicMock(content="(stand-in answer: no LLM is called in this demo)", tool_calls=None))]
    resp.usage = MagicMock(total_tokens=0, model_dump=lambda: {"total_tokens": 0})
    return resp


@contextlib.contextmanager
def installed(rag_module=None):
    """Patch the embedding seams (and, if rag_module is given, its query embedder plus the LLM completion call)."""
    with contextlib.ExitStack() as st:
        st.enter_context(patch("litellm.embedding", side_effect=_fake_litellm_embedding))
        if rag_module is not None:
            st.enter_context(patch.object(rag_module, "_embed_query", lambda cfg, q: vec(q)))
            st.enter_context(patch("litellm.completion", side_effect=_fake_completion))
            st.enter_context(patch("litellm.completion_cost", return_value=0.0))
        yield
