"""Regression tests for TextClassifierComponent accepting int-valued
categories.

Real-world trigger: dagster-components runs every string attribute through
Jinja2's NativeTemplate (to support {{ }} templating), which coerces any
purely-numeric-looking value back to its native Python type regardless of
how it was quoted in the source YAML -- e.g. `categories: ['2024', '2025']`
loads as `[2024, 2025]` (real ints), not the strings the YAML wrote. The
field used to be `List[str]`, so Pydantic rejected this at defs-load time
before the asset ever got a chance to run.
"""
from unittest.mock import MagicMock, patch

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def test_categories_field_accepts_int_values(mod):
    component = mod.TextClassifierComponent(
        asset_name="classified_out",
        upstream_asset_key="raw",
        input_column="body",
        categories=[2024, 2025],
        provider="openai",
        model="gpt-4",
    )
    assert component.categories == [2024, 2025]


def test_int_categories_survive_prompt_building_and_classification(mod):
    df = pd.DataFrame({"body": ["playoff schedule for next season"]})
    component = mod.TextClassifierComponent(
        asset_name="classified_out",
        upstream_asset_key="raw",
        input_column="body",
        categories=[2024, 2025],
        provider="openai",
        model="gpt-4",
    )
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]

    @dg.asset(name="raw")
    def raw():
        return df

    fake_message = MagicMock()
    fake_message.content = '{"category": "2024", "confidence": 0.9}'
    fake_response = MagicMock()
    fake_response.choices = [MagicMock(message=fake_message)]
    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = fake_response

    with patch("openai.OpenAI", return_value=fake_client):
        result = dg.materialize([asset_def, raw])

    assert result.success
    # ', '.join(categories) would raise TypeError on a mixed/int list before
    # this fix -- reaching a successful materialize proves it didn't.
    prompt_sent = fake_client.chat.completions.create.call_args.kwargs["messages"][0]["content"]
    assert "2024" in prompt_sent and "2025" in prompt_sent
    df_out = result.output_for_node("classified_out")
    assert df_out["category"].tolist() == ["2024"]
