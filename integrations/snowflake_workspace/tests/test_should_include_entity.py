"""Real, committed tests for SnowflakeWorkspaceComponent._should_include_entity --
this component had NO test suite at all before this one. Scope: just the
filtering logic itself (pure, no Snowflake connection needed), which is the
single chokepoint every entity type's discovery goes through (24 call sites
across pipes/tasks/streams/stages/tables/alerts/etc.).

Added alongside include_names (explicit allow-list) and
filter_by_comment_pattern (matches a pipe's real SHOW PIPES COMMENT field) --
real, deterministic alternatives to filter_by_name_pattern for teams whose
real naming convention isn't known yet, or who classify objects by comment/
tag rather than name. Not a "learned from history" heuristic -- this stays
fully deterministic, same as every other selector in this catalog.
"""
import importlib.util
import pathlib

import pytest


def _load_component_module():
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "snowflake_workspace_component", here / "component.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mod = _load_component_module()
SnowflakeWorkspaceComponent = mod.SnowflakeWorkspaceComponent


def _component(**overrides):
    from dagster_snowflake import SnowflakeResource

    attrs = dict(
        workspace=SnowflakeResource(
            account="fake-account", user="fake-user", password="fake-password",
            warehouse="fake-wh", database="FAKE_DB", schema="FAKE_SCHEMA",
        ),
    )
    attrs.update(overrides)
    return SnowflakeWorkspaceComponent(**attrs)


# --- pre-existing behavior, unaffected by the new fields (regression) -----

def test_no_filters_includes_everything():
    comp = _component()
    assert comp._should_include_entity("ANY_PIPE") is True


def test_name_pattern_still_requires_a_match_when_its_the_only_criterion():
    comp = _component(filter_by_name_pattern="^SITE_DETAILS_PIPE$")
    assert comp._should_include_entity("SITE_DETAILS_PIPE") is True
    assert comp._should_include_entity("OTHER_PIPE") is False


def test_exclude_pattern_still_vetoes():
    comp = _component(filter_by_name_pattern=".*", exclude_name_pattern="^TEST_")
    assert comp._should_include_entity("TEST_PIPE") is False
    assert comp._should_include_entity("REAL_PIPE") is True


# --- new: include_names (explicit allow-list) ------------------------------

def test_include_names_bypasses_name_pattern_entirely():
    comp = _component(include_names=["BSEG_PIPE", "KONV_PIPE"])
    assert comp._should_include_entity("BSEG_PIPE") is True
    assert comp._should_include_entity("RANDOM_OTHER_PIPE") is False


def test_include_names_is_case_insensitive():
    comp = _component(include_names=["bseg_pipe"])
    assert comp._should_include_entity("BSEG_PIPE") is True


def test_exclude_pattern_vetoes_even_an_explicit_include_names_entry():
    comp = _component(include_names=["BSEG_PIPE"], exclude_name_pattern="^BSEG")
    assert comp._should_include_entity("BSEG_PIPE") is False


def test_include_names_and_name_pattern_union_together():
    """Both set -- an entity matching EITHER one is included (same OR
    semantics as the selection DSLs used elsewhere in this catalog)."""
    comp = _component(include_names=["BSEG_PIPE"], filter_by_name_pattern="^KONV")
    assert comp._should_include_entity("BSEG_PIPE") is True
    assert comp._should_include_entity("KONV_PIPE") is True
    assert comp._should_include_entity("RANDOM_PIPE") is False


# --- new: filter_by_comment_pattern -----------------------------------------

def test_comment_pattern_matches_the_real_show_pipes_comment_field():
    comp = _component(filter_by_comment_pattern="cadence:hourly")
    assert comp._should_include_entity("ANY_PIPE", comment="cadence:hourly") is True
    assert comp._should_include_entity("ANY_PIPE", comment="cadence:daily") is False


def test_comment_pattern_with_no_comment_passed_does_not_include():
    comp = _component(filter_by_comment_pattern="cadence:hourly")
    assert comp._should_include_entity("ANY_PIPE") is False


def test_comment_pattern_unions_with_name_pattern():
    comp = _component(
        filter_by_name_pattern="^ZOTC_SETTLE_PIPE$",
        filter_by_comment_pattern="cadence:hourly",
    )
    # Matches by name alone, no comment needed.
    assert comp._should_include_entity("ZOTC_SETTLE_PIPE") is True
    # Matches by comment alone, name doesn't match the pattern.
    assert comp._should_include_entity("SOME_OTHER_PIPE", comment="cadence:hourly") is True
    # Matches neither.
    assert comp._should_include_entity("SOME_OTHER_PIPE", comment="cadence:daily") is False
