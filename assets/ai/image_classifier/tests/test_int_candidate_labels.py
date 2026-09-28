"""Regression test for ImageClassifierComponent accepting int-valued
candidate_labels -- same root cause as text_classifier/zero_shot_classifier:
dagster-components' Jinja2 NativeTemplate coercion turns a purely-numeric-
looking label back into an int regardless of YAML quoting, and the field
used to be `Optional[List[str]]`, rejecting that at defs-load time.
"""
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_candidate_labels_field_accepts_int_values(mod):
    component = mod.ImageClassifierComponent(
        asset_name="classified_out",
        upstream_asset_key="raw",
        image_column="local_path",
        candidate_labels=[2024, 2025],
    )
    assert component.candidate_labels == [2024, 2025]


def test_build_defs_succeeds_with_int_candidate_labels(mod):
    # The normalization line (`[str(c) for c in self.candidate_labels]`)
    # runs inside the asset closure at materialize time, not at build_defs
    # time -- this just confirms the type change doesn't break component
    # construction/defs building, which used to be impossible at all with
    # the old `List[str]` type once the framework's Jinja coercion kicked in.
    component = mod.ImageClassifierComponent(
        asset_name="classified_out",
        upstream_asset_key="raw",
        image_column="local_path",
        candidate_labels=[2024, 2025],
    )
    defs = component.build_defs(load_context=None)
    assert list(defs.assets)
