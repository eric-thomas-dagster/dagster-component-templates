"""Shared test helpers for AgenticPipelineAuthoringToolsComponent."""
import importlib.util
import pathlib

import pytest

HERE = pathlib.Path(__file__).resolve().parent


def load_component_module():
    path = HERE.parent / "component.py"
    spec = importlib.util.spec_from_file_location("agentic_pipeline_authoring_tools_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def tools(mod):
    return mod.AgenticPipelineAuthoringTools()


GOOD_PIPELINE_YAML = """
asset_name_prefix: demo
source: {kind: literal, text: hello}
steps:
  - id: draft
    op: llm_call
    model: gpt-4o-mini
    api_key_env_var: OPENAI_API_KEY
    prompt_template: 'Draft: {text}'
outputs: {assets: [draft]}
"""
