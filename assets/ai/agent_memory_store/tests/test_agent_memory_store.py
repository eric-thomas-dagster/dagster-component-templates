"""agent_memory_store: real end-to-end tests of the demo-mode seam, grouping
logic, infer/metadata handling, the validate_search smoke check, and the
real-mode `_build_memory_client` seam.

demo_mode (the default) exercises the real asset-building/execution code
with the component's own internal `_DemoMemoryClient` -- no Mem0/LLM/embedder
calls. Real mode is exercised by monkeypatching `_build_memory_client` (this
component's documented seam). `dg.materialize()` is real throughout.
"""
import json

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

mod = load_component_module()
AgentMemoryStoreComponent = mod.AgentMemoryStoreComponent


def _component(**kw):
    attrs = dict(asset_name="mem", upstream_asset_key="chat_turns")
    attrs.update(kw)
    return AgentMemoryStoreComponent(**attrs)


def _materialize(comp, df, **kwargs):
    @dg.asset(name="chat_turns")
    def chat_turns():
        return df

    defs = comp.build_defs(None)
    return dg.materialize([chat_turns, *defs.assets], **kwargs)


def test_demo_mode_groups_by_user_and_writes_one_memory_per_group(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(demo_store_path=str(demo_path))
    df = pd.DataFrame({
        "user_id": ["alex", "alex", "sam"],
        "role": ["user", "assistant", "user"],
        "content": ["I love basketball", "Great!", "I prefer tennis"],
    })
    result = _materialize(comp, df, raise_on_error=True)
    assert result.success
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["groups_written"].value == 2
    assert md["memories_written"].value == 2
    memories = json.loads(demo_path.read_text())
    assert {m["user_id"] for m in memories} == {"alex", "sam"}
    assert memories[0]["memory"] == "I love basketball"  # infer=True picks the last user message


def test_infer_false_stores_raw_messages(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(demo_store_path=str(demo_path), infer=False)
    df = pd.DataFrame({"user_id": ["alex"], "role": ["user"], "content": ["secret"]})
    _materialize(comp, df, raise_on_error=True)
    memories = json.loads(demo_path.read_text())
    assert memories[0]["memory"] == json.dumps([{"role": "user", "content": "secret"}])


def test_run_id_column_scopes_grouping(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(demo_store_path=str(demo_path), run_id_column="session")
    df = pd.DataFrame({
        "user_id": ["alex", "alex"],
        "session": ["s1", "s2"],
        "role": ["user", "user"],
        "content": ["first session", "second session"],
    })
    result = _materialize(comp, df, raise_on_error=True)
    md = result.get_asset_materialization_events()[-1].materialization.metadata
    assert md["groups_written"].value == 2  # same user, different run_id -- two groups, not one
    memories = json.loads(demo_path.read_text())
    assert {m["run_id"] for m in memories} == {"s1", "s2"}


def test_metadata_column_parsed_and_attached(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(demo_store_path=str(demo_path), metadata_column="meta")
    df = pd.DataFrame({"user_id": ["alex"], "role": ["user"], "content": ["hi"], "meta": ['{"source": "ticket"}']})
    _materialize(comp, df, raise_on_error=True)
    memories = json.loads(demo_path.read_text())
    assert memories[0]["metadata"] == {"source": "ticket"}


def test_validate_search_passes_when_memories_exist(tmp_path):
    demo_path = tmp_path / "demo.json"
    comp = _component(demo_store_path=str(demo_path), validate_search=True, validate_search_query="what do they like")
    df = pd.DataFrame({"user_id": ["alex"], "role": ["user"], "content": ["I love hiking"]})
    result = _materialize(comp, df, raise_on_error=True)
    evals = result.get_asset_check_evaluations()
    assert len(evals) == 1 and evals[0].check_name == "memory_search_smoke_check" and evals[0].passed


def test_validate_search_fails_when_nothing_comes_back():
    class EmptySearchClient:
        def add(self, messages, **kw):
            return {"results": [{"id": "x"}]}

        def search(self, query, filters=None):
            return {"results": []}

    mod._build_memory_client = lambda config: EmptySearchClient()
    comp = _component(demo_mode=False, validate_search=True, validate_search_query="anything")
    df = pd.DataFrame({"user_id": ["alex"], "role": ["user"], "content": ["hi"]})
    result = _materialize(comp, df, raise_on_error=True)  # WARN severity -- doesn't block
    assert result.success
    evals = result.get_asset_check_evaluations()
    assert not evals[0].passed


def test_validate_search_requires_query():
    with pytest.raises(Exception, match="validate_search_query is required"):
        _component(validate_search=True)


def test_real_mode_seam_receives_vector_store_llm_embedder_config():
    captured = {}

    class FakeClient:
        def add(self, messages, **kw):
            return {"results": []}

    mod._build_memory_client = lambda config: (captured.update(config=config), FakeClient())[1]
    comp = _component(
        demo_mode=False,
        vector_store={"provider": "qdrant", "config": {"host": "localhost"}},
        llm={"provider": "openai", "config": {"model": "gpt-5-mini"}},
        embedder={"provider": "openai", "config": {"model": "text-embedding-3-small"}},
    )
    df = pd.DataFrame({"user_id": ["alex"], "role": ["user"], "content": ["hi"]})
    _materialize(comp, df, raise_on_error=True)
    assert captured["config"]["vector_store"]["provider"] == "qdrant"
    assert captured["config"]["llm"]["provider"] == "openai"
    assert captured["config"]["embedder"]["provider"] == "openai"


def test_real_mode_with_no_config_passes_none():
    captured = {}

    class FakeClient:
        def add(self, messages, **kw):
            return {"results": []}

    mod._build_memory_client = lambda config: (captured.update(config=config), FakeClient())[1]
    comp = _component(demo_mode=False)
    df = pd.DataFrame({"user_id": ["alex"], "role": ["user"], "content": ["hi"]})
    _materialize(comp, df, raise_on_error=True)
    assert captured["config"] is None


def test_agent_id_and_run_id_passed_to_add_only_when_configured():
    captured = []

    class FakeClient:
        def add(self, messages, **kw):
            captured.append(kw)
            return {"results": []}

    mod._build_memory_client = lambda config: FakeClient()
    comp = _component(demo_mode=False)  # no agent_id_column/run_id_column set
    df = pd.DataFrame({"user_id": ["alex"], "role": ["user"], "content": ["hi"]})
    _materialize(comp, df, raise_on_error=True)
    assert "agent_id" not in captured[0]
    assert "run_id" not in captured[0]


def test_upstream_asset_key_accepts_list_shape():
    # Regression (tests/test_upstream_keys.py, repo-wide): Designer may write
    # upstream_asset_keys: [...] even for a single-upstream component.
    comp = AgentMemoryStoreComponent(asset_name="mem", upstream_asset_keys=["chat_turns"])
    assert comp.upstream_asset_key == "chat_turns"


def test_upstream_asset_key_rejects_multiple():
    with pytest.raises(Exception, match="takes exactly one upstream asset"):
        AgentMemoryStoreComponent(asset_name="mem", upstream_asset_key=["a", "b"])
