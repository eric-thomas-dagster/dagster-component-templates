"""End-to-end test against a real skill: a trimmed-down copy of Anthropic's own
dagster-expert plugin skill (SKILL.md + 2 of its reference docs), committed as a fixture
so this runs the same in CI as locally -- no network, no mocking, real front matter
parsing and real file I/O."""
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "dagster-expert"


def test_describe_reads_real_front_matter(mod):
    component = mod.SkillResourceComponent(resource_key="dagster_expert_skill", local_path=str(FIXTURE_DIR))
    resource = component.build_defs(None).resources["dagster_expert_skill"]
    info = resource.describe()
    assert info["key"] == "dagster_expert_skill"
    assert info["name"] == "dagster-expert"
    assert "dg CLI" in info["description"]


def test_load_returns_real_body_and_bundled_reference_files(mod):
    component = mod.SkillResourceComponent(resource_key="dagster_expert_skill", local_path=str(FIXTURE_DIR))
    resource = component.build_defs(None).resources["dagster_expert_skill"]
    loaded = resource.load()
    assert "## Core Dagster Concepts" in loaded["content"]
    assert set(loaded["files"]) == {
        "references/asset-selection.md",
        "references/env-vars.md",
    }


def test_read_file_returns_a_real_reference_doc(mod):
    component = mod.SkillResourceComponent(resource_key="dagster_expert_skill", local_path=str(FIXTURE_DIR))
    resource = component.build_defs(None).resources["dagster_expert_skill"]
    content = resource.read_file("references/env-vars.md")
    assert len(content) > 0
    assert content == (FIXTURE_DIR / "references" / "env-vars.md").read_text()
