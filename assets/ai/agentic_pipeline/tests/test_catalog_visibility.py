"""Definition-level tags + metadata so a pipeline no one has run yet is still
recognizable in the catalog -- 0.10.141 only tagged dagster/kind/<op>, and the
richer <step>__op / pipeline_version fields only exist on materializations, so a
never-run pipeline's step assets couldn't be told apart from any other asset."""
import dagster as dg


def _specs(component):
    asset_def = component.build_defs(None).assets[0]
    return asset_def.specs_by_key


def test_agentic_pipeline_tag_names_the_asset_name_prefix(mod):
    component = mod.AgenticPipelineComponent(
        asset_name_prefix="research_bot",
        source={"kind": "literal", "text": "hello"},
        steps=[{"id": "answer", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"}],
        outputs={"assets": ["answer"]},
    )
    specs = _specs(component)
    key = dg.AssetKey.from_user_string("research_bot_answer")
    assert specs[key].tags["dagster-community/agentic-pipeline"] == "research_bot"


def test_definition_metadata_identifies_pipeline_step_op_reads_and_partition_type(mod):
    component = mod.AgenticPipelineComponent(
        asset_name_prefix="research_bot",
        source={"kind": "literal", "text": "hello {partition_key}"},
        steps=[
            {"id": "first", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"},
            {"id": "second", "op": "llm_call", "source": "first", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"},
        ],
        outputs={"assets": ["first", "second"]},
        partition_type="daily", partition_start="2026-01-01",
    )
    specs = _specs(component)

    first = specs[dg.AssetKey.from_user_string("research_bot_first")].metadata
    assert first["pipeline"].value == "research_bot"
    assert first["step"].value == "first"
    assert first["op"].value == "llm_call"
    assert first["reads"].value == "source"  # no explicit source: -> the reserved initial-source id
    assert first["partition_type"].value == "daily"

    second = specs[dg.AssetKey.from_user_string("research_bot_second")].metadata
    assert second["step"].value == "second"
    assert second["reads"].value == "first"


def test_reads_metadata_joins_multiple_refs_for_a_multi_input_step(mod):
    component = mod.AgenticPipelineComponent(
        asset_name_prefix="research_bot",
        source={"kind": "literal", "text": "hello"},
        steps=[
            {"id": "a", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"},
            {"id": "b", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"},
            {
                "id": "merged", "op": "synthesize", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY",
                "inputs": {"x": {"from": "a"}, "y": {"from": "b"}},
            },
        ],
        outputs={"assets": ["a", "b", "merged"]},
    )
    specs = _specs(component)
    reads = specs[dg.AssetKey.from_user_string("research_bot_merged")].metadata["reads"].value
    assert set(reads.split(",")) == {"a", "b"}


def test_partition_type_metadata_reports_multi_for_partition_dimensions(mod):
    component = mod.AgenticPipelineComponent(
        asset_name_prefix="research_bot",
        source={"kind": "literal", "text": "hello {partition_key}"},
        steps=[{"id": "answer", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"}],
        outputs={"assets": ["answer"]},
        partition_dimensions=[
            {"name": "date", "type": "daily", "start": "2026-01-01"},
            {"name": "region", "type": "static", "values": ["us", "eu"]},
        ],
    )
    specs = _specs(component)
    assert specs[dg.AssetKey.from_user_string("research_bot_answer")].metadata["partition_type"].value == "multi"


def test_partition_type_metadata_reports_none_when_unpartitioned(mod):
    component = mod.AgenticPipelineComponent(
        asset_name_prefix="research_bot",
        source={"kind": "literal", "text": "hello"},
        steps=[{"id": "answer", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY"}],
        outputs={"assets": ["answer"]},
    )
    specs = _specs(component)
    assert specs[dg.AssetKey.from_user_string("research_bot_answer")].metadata["partition_type"].value == "none"
