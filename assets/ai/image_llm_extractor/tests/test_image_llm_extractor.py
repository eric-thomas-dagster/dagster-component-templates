"""Committed regression tests for ImageLlmExtractorComponent's retry +
output-validation fix.

Before this fix, the LLM call had no retry at all, and a non-dict JSON
response (or any parsing surprise) was only caught by a blanket
except Exception -- no num_retries forwarded to litellm, no isinstance
check distinguishing "the model returned valid-but-wrong-shaped JSON"
from "the whole call failed". Mirrors the same fix already applied to
structured_document_extractor and the 13 legacy extractor components.
"""
import base64
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def fake_litellm(monkeypatch):
    calls = []
    state = {"response_text": '{"product_name": "Widget"}'}

    def fake_completion(model, messages, api_key=None, **kwargs):
        calls.append({"model": model, "messages": messages, "kwargs": kwargs})
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=types.SimpleNamespace(content=state["response_text"]))]
        )

    fake_module = types.ModuleType("litellm")
    fake_module.completion = fake_completion
    monkeypatch.setitem(sys.modules, "litellm", fake_module)
    return calls, state


def _materialize_one_row(component, file_path):
    defs = component.build_defs(load_context=None)
    asset_def = list(defs.assets)[0]
    upstream_df = pd.DataFrame({"image_path": [file_path]})

    @dg.asset(name="images_in")
    def images_in():
        return upstream_df

    return dg.materialize([asset_def, images_in])


def test_real_image_reaches_prompt_as_exact_vision_block(mod, fake_litellm, tmp_path):
    from PIL import Image

    calls, _ = fake_litellm
    img_path = tmp_path / "product.png"
    Image.new("RGB", (16, 16), color=(0, 255, 0)).save(img_path)

    component = mod.ImageLlmExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="images_in",
        image_column="image_path",
        extraction_fields={"product_name": "name of product shown"},
    )
    result = _materialize_one_row(component, str(img_path))
    assert result.success

    content_blocks = calls[0]["messages"][0]["content"]
    image_blocks = [c for c in content_blocks if c.get("type") == "image_url"]
    assert len(image_blocks) == 1
    data_url = image_blocks[0]["image_url"]["url"]
    b64_payload = data_url.split(",", 1)[1]
    assert base64.b64decode(b64_payload) == img_path.read_bytes()


def test_llm_max_retries_forwarded_to_litellm(mod, fake_litellm, tmp_path):
    from PIL import Image

    calls, _ = fake_litellm
    img_path = tmp_path / "product.png"
    Image.new("RGB", (16, 16)).save(img_path)

    component = mod.ImageLlmExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="images_in",
        image_column="image_path",
        extraction_fields={"product_name": "name of product shown"},
        llm_max_retries=5,
    )
    result = _materialize_one_row(component, str(img_path))
    assert result.success
    assert calls[0]["kwargs"]["num_retries"] == 5


def test_non_dict_llm_response_rejected_not_crashed(mod, fake_litellm, tmp_path):
    from PIL import Image

    _, state = fake_litellm
    state["response_text"] = "[1, 2, 3]"
    img_path = tmp_path / "product.png"
    Image.new("RGB", (16, 16)).save(img_path)

    component = mod.ImageLlmExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="images_in",
        image_column="image_path",
        extraction_fields={"product_name": "name of product shown"},
    )
    result = _materialize_one_row(component, str(img_path))
    assert result.success

    df = result.output_for_node("extracted")
    assert df.loc[0, "product_name"] is None
    meta = result.get_asset_materialization_events()[-1].materialization.metadata
    assert meta["extraction_failures"].value == 1


def test_missing_field_in_llm_response_filled_with_none(mod, fake_litellm, tmp_path):
    from PIL import Image

    _, state = fake_litellm
    state["response_text"] = '{"product_name": "Widget"}'  # omits "price"
    img_path = tmp_path / "product.png"
    Image.new("RGB", (16, 16)).save(img_path)

    component = mod.ImageLlmExtractorComponent(
        asset_name="extracted",
        upstream_asset_key="images_in",
        image_column="image_path",
        extraction_fields={"product_name": "name of product shown", "price": "price if visible"},
    )
    result = _materialize_one_row(component, str(img_path))
    assert result.success

    df = result.output_for_node("extracted")
    assert df.loc[0, "product_name"] == "Widget"
    assert df.loc[0, "price"] is None
