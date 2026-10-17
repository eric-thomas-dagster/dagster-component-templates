"""<think> stripping + opt-in include_reasoning for image_captioner.

Only the model call is faked (a stand-in `litellm` module returning `<think>...</think>answer`); the REAL component is
built and materialized with dg.materialize() against a real upstream DataFrame.
"""
import base64
import inspect
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

THINK = "weighing the options"
REPLY = "a red square"


_PNG = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=='


def _upstream(tmp_path):
    for n in ("a.png", "b.png"):
        (tmp_path / n).write_bytes(base64.b64decode(_PNG))
    return pd.DataFrame({"img": [str(tmp_path / "a.png"), str(tmp_path / "b.png")]})


def _fake_litellm(reply):
    calls = []

    def completion(**kw):
        calls.append(kw)
        msg = types.SimpleNamespace(content=reply, tool_calls=None)
        return types.SimpleNamespace(
            choices=[types.SimpleNamespace(message=msg)],
            usage=types.SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        )

    mod = types.ModuleType("litellm")
    mod.completion = completion
    mod.completion_cost = lambda **kw: 0.0
    return mod, calls


def _run(monkeypatch, tmp_path, reply, **extra):
    fake, calls = _fake_litellm(reply)
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(
        c for c in vars(mod).values()
        if inspect.isclass(c) and issubclass(c, dg.Component)
        and c is not dg.Component and c.__module__ == mod.__name__
    )
    component = cls(asset_name="out_asset", upstream_asset_key='src', image_path_column='img', **extra)
    frame = _upstream(tmp_path)

    @dg.asset(name="src")
    def src():
        return frame

    asset_def = component.build_defs(None).assets[0]
    res = dg.materialize([src, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.output_for_node("out_asset"), calls


def _clean(out):
    assert out["caption"].tolist() == ["a red square"] * len(out)


def test_think_block_is_stripped_before_parsing(monkeypatch, tmp_path):
    out, _ = _run(monkeypatch, tmp_path, f"<think>{THINK}</think>" + REPLY)
    _clean(out)
    assert "caption_reasoning" not in out.columns   # default output is unchanged


def test_plain_reply_output_is_identical_with_or_without_thinking(monkeypatch, tmp_path):
    plain, _ = _run(monkeypatch, tmp_path, REPLY)
    thinking, _ = _run(monkeypatch, tmp_path, f"<think>{THINK}</think>" + REPLY)
    _clean(plain)
    pd.testing.assert_frame_equal(plain, thinking)


def test_include_reasoning_exposes_the_thinking(monkeypatch, tmp_path):
    out, _ = _run(monkeypatch, tmp_path, f"<think>{THINK}</think>" + REPLY, include_reasoning=True)
    _clean(out)
    assert out["caption_reasoning"].tolist() == [THINK] * len(out)


def test_include_reasoning_with_plain_reply_is_null(monkeypatch, tmp_path):
    out, _ = _run(monkeypatch, tmp_path, REPLY, include_reasoning=True)
    _clean(out)
    assert out["caption_reasoning"].isna().all()
