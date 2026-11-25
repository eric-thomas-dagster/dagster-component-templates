"""Committed tests for XmlParser's `streamsets_tree_column` (full-document parse
matching a real StreamSets XML Parser's output convention: attr|name for
attributes, value for element text, children grouped by tag into LISTS of
these same dicts -- even a single child becomes a one-item list).

Real XML fixtures, routed through the real asset graph via dg.materialize(),
nothing mocked.
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

XML_WITH_ATTR_TEXT_AND_CHILDREN = (
    '<order id="42" status="paid">'
    '<customer>Jane Doe</customer>'
    '<items>'
    '<item sku="A1">Widget</item>'
    '<item sku="A2">Gadget</item>'
    '</items>'
    '</order>'
)
XML_WITH_SINGLE_CHILD = '<root><only>solo</only></root>'


@pytest.fixture()
def mod():
    return load_component_module()


def _build_asset(mod, **kwargs):
    comp = mod.XmlParser(
        asset_name="parsed",
        xml_column="xml_data",
        upstream_asset_key="src",
        **kwargs,
    )
    defs = comp.build_defs(load_context=None)
    return next(iter(defs.assets))


def test_attributes_text_and_repeated_children(mod):
    df = pd.DataFrame({"xml_data": [XML_WITH_ATTR_TEXT_AND_CHILDREN]})

    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return df

    parsed = _build_asset(mod, streamsets_tree_column="tree")
    result = dg.materialize([src, parsed], resources={"io_manager": dg.InMemoryIOManager()})
    out = result.output_for_node("parsed")
    tree = out.loc[0, "tree"]

    assert tree["attr|id"] == "42"
    assert tree["attr|status"] == "paid"
    assert tree["customer"] == [{"value": "Jane Doe"}]
    assert tree["items"][0]["item"] == [
        {"attr|sku": "A1", "value": "Widget"},
        {"attr|sku": "A2", "value": "Gadget"},
    ]


def test_single_child_is_still_a_one_item_list(mod):
    df = pd.DataFrame({"xml_data": [XML_WITH_SINGLE_CHILD]})

    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return df

    parsed = _build_asset(mod, streamsets_tree_column="tree")
    result = dg.materialize([src, parsed], resources={"io_manager": dg.InMemoryIOManager()})
    out = result.output_for_node("parsed")
    tree = out.loc[0, "tree"]

    assert tree["only"] == [{"value": "solo"}]


def test_tree_and_xpath_expressions_can_combine(mod):
    df = pd.DataFrame({"xml_data": [XML_WITH_ATTR_TEXT_AND_CHILDREN]})

    @dg.asset(name="src")
    def src() -> pd.DataFrame:
        return df

    parsed = _build_asset(
        mod,
        xpath_expressions={"customer_name": "./customer"},
        streamsets_tree_column="tree",
    )
    result = dg.materialize([src, parsed], resources={"io_manager": dg.InMemoryIOManager()})
    out = result.output_for_node("parsed")

    assert out.loc[0, "customer_name"] == "Jane Doe"
    assert out.loc[0, "tree"]["attr|id"] == "42"


def test_requires_xpath_expressions_or_tree_column(mod):
    with pytest.raises(ValueError, match="xpath_expressions.*streamsets_tree_column"):
        mod.XmlParser(
            asset_name="parsed",
            xml_column="xml_data",
            upstream_asset_key="src",
        ).build_defs(load_context=None)
