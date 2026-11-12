"""Loads component.py by file path so tests don't depend on package install."""
import importlib.util
from pathlib import Path

import pytest


def load_component_module():
    path = Path(__file__).resolve().parent.parent / "component.py"
    spec = importlib.util.spec_from_file_location("_skill_resource_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def mod(monkeypatch, tmp_path_factory):
    """Fresh module + fresh cache root per test -- the git-checkout cache is keyed by
    (git_url, git_ref) under a shared system temp dir, which would otherwise let one
    test's fake clone leak into another's (or across separate pytest runs)."""
    module = load_component_module()
    monkeypatch.setattr(module, "_CACHE_ROOT", tmp_path_factory.mktemp("skill_cache"))
    return module


SAMPLE_SKILL_MD = """---
name: pdf-report
description: Generate a formatted PDF report from a DataFrame.
---

## Instructions

1. Load the DataFrame.
2. Render it as a styled table.
3. Export to PDF.
"""
