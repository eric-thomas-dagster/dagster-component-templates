"""per_step_ops=True: only the FINAL step's asset should carry the `dagster-community/knowledge-base`
marker (it's the one terminal, queryable index a gateway actually searches) -- every step shares the
rest of `all_tags`, plus `dagster-community/knowledge-base-name` (= asset_name) and
`dagster-community/knowledge-base-stage` (= that step's own id), so a UI can group every step of one
recipe together even though only the last one is itself searchable.

Before this fix, `tags_by_output_name[output_name] = all_tags` gave EVERY per-step asset the same
`all_tags` dict verbatim, including the knowledge-base marker meant for the final index only.
"""
import pytest

from .conftest import load_component_module

KB_TAG = "dagster-community/knowledge-base"
KB_NAME_TAG = "dagster-community/knowledge-base-name"
KB_STAGE_TAG = "dagster-community/knowledge-base-stage"


@pytest.fixture()
def mod():
    return load_component_module()


def _tags_by_asset(defs):
    return {k.to_user_string(): dict(a.tags_by_key[k]) for a in defs.assets for k in a.keys}


def _comp(mod, **kw):
    base = dict(asset_name="kb", id_column="id", text_column="body", upstream_asset_key="raw", per_step_ops=True)
    base.update(kw)
    return mod.ContextEngineeringPipelineComponent(**base)


def test_only_the_final_step_gets_the_knowledge_base_marker(mod):
    steps = [
        {"id": "chunks", "op": "chunk", "chunk_size": 200},
        {"id": "embedded", "op": "embed", "provider": "sentence_transformers"},
        {"id": "indexed", "op": "write_vector_store", "provider": "chromadb", "collection_name": "c"},
    ]
    tags = _tags_by_asset(_comp(mod, steps=steps).build_defs(None))
    assert tags["kb_chunks"].get(KB_TAG) is None
    assert tags["kb_embedded"].get(KB_TAG) is None
    assert tags["kb"][KB_TAG] == "true"  # final step keeps the plain asset_name key


def test_every_step_names_its_knowledge_base_and_stage(mod):
    steps = [
        {"id": "chunks", "op": "chunk", "chunk_size": 200},
        {"id": "embedded", "op": "embed", "provider": "sentence_transformers"},
        {"id": "indexed", "op": "write_vector_store", "provider": "chromadb", "collection_name": "c"},
    ]
    tags = _tags_by_asset(_comp(mod, steps=steps).build_defs(None))
    assert tags["kb_chunks"][KB_NAME_TAG] == "kb"
    assert tags["kb_chunks"][KB_STAGE_TAG] == "chunks"
    assert tags["kb_embedded"][KB_NAME_TAG] == "kb"
    assert tags["kb_embedded"][KB_STAGE_TAG] == "embedded"
    # The final step also gets the naming tags, not just the boolean marker -- so a UI can group
    # every stage of one recipe (including the terminal index) by the same knowledge-base-name tag.
    assert tags["kb"][KB_NAME_TAG] == "kb"
    assert tags["kb"][KB_STAGE_TAG] == "indexed"


def test_no_write_vector_store_step_means_no_knowledge_base_tags_at_all(mod):
    steps = [
        {"id": "chunks", "op": "chunk", "chunk_size": 200},
        {"id": "embedded", "op": "embed", "provider": "sentence_transformers"},
    ]
    tags = _tags_by_asset(_comp(mod, steps=steps).build_defs(None))
    for t in tags.values():
        assert KB_TAG not in t
        assert KB_NAME_TAG not in t
        assert KB_STAGE_TAG not in t


def test_sql_per_step_ops_never_has_write_vector_store_so_no_knowledge_base_tags(mod):
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body",
        source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT id, body FROM ds.src"},
        execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb", code_version="v1",
        per_step_ops=True,
        steps=[{"id": "chunks", "op": "chunk", "chunk_size": 200}, {"id": "embedded", "op": "embed"}],
    )
    tags = _tags_by_asset(comp.build_defs(None))
    for t in tags.values():
        assert KB_TAG not in t
        assert KB_NAME_TAG not in t
        assert KB_STAGE_TAG not in t
