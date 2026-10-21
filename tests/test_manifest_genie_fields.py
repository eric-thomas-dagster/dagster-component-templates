"""Shapes of the optional Genie planner fields in manifest.json (see MANIFEST_CONVENTIONS.md)."""
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = json.loads((ROOT / "manifest.json").read_text())["components"]
BY_ID = {c["id"]: c for c in MANIFEST}

BUILDERS = {"agents_pipelines", "rag"}
CHECKS = {"model_provider", "per_row_misuse", "context_pipeline"}
WITH_FIELDS = [c for c in MANIFEST if {"builders", "genie_checks", "requires_resources"} & set(c)]


def test_the_agent_and_knowledge_base_components_declare_their_fields():
    # The designer has these as built-in defaults too; declaring them here is what lets a new component join without a designer release.
    assert {"agentic_pipeline", "context_engineering_pipeline", "rag_pipeline"} <= {c["id"] for c in WITH_FIELDS}


@pytest.mark.parametrize("c", WITH_FIELDS, ids=lambda c: c["id"])
def test_builders_and_checks_use_known_values(c):
    if "builders" in c:
        assert isinstance(c["builders"], list) and set(c["builders"]) <= BUILDERS, c["builders"]
    if "genie_checks" in c:
        assert isinstance(c["genie_checks"], list) and set(c["genie_checks"]) <= CHECKS, c["genie_checks"]


@pytest.mark.parametrize("c", [c for c in WITH_FIELDS if "requires_resources" in c], ids=lambda c: c["id"])
def test_required_resources_point_at_a_real_resource_and_a_real_config_path(c):
    schema = json.loads((ROOT / c["path"] / "schema.json").read_text())
    for r in c["requires_resources"]:
        assert set(r) >= {"component_id", "config_path"}, r
        target = BY_ID.get(r["component_id"])
        assert target is not None, f"{r['component_id']} is not in the manifest"
        assert target.get("category") == "resource", f"{r['component_id']} is not a resource component"
        head = r["config_path"].split(".")[0]
        assert head in schema["attributes"], f"{c['id']} has no `{head}` attribute for config_path {r['config_path']}"


def test_pipeline_check_flags_only_go_on_components_with_steps():
    for c in WITH_FIELDS:
        if "genie_checks" in c and c["genie_checks"]:
            schema = json.loads((ROOT / c["path"] / "schema.json").read_text())
            assert "steps" in schema["attributes"], f"{c['id']} declares genie_checks but has no `steps`"
