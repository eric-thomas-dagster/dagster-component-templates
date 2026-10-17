"""<think> stripping + opt-in include_reasoning for litellm_structured_output.

Only the model call is faked (a stand-in `litellm` module returning `<think>...</think>answer`); the REAL component is
built and materialized with dg.materialize() against a real upstream DataFrame.
"""
import inspect
import sys
import types

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

THINK = "weighing the options"
REPLY = '{"name": "Ada", "age": 36}'


def _upstream(tmp_path):
    return pd.DataFrame({"text": ["Ada is 36", "Ada, 36"]})


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
    component = cls(asset_name="out_asset", upstream_asset_key='src', text_column='text', schema_definition={'name': {'type': 'string'}, 'age': {'type': 'integer'}}, on_error='raise', **extra)
    frame = _upstream(tmp_path)

    @dg.asset(name="src")
    def src():
        return frame

    asset_def = component.build_defs(None).assets[0]
    res = dg.materialize([src, asset_def], resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    return res.output_for_node("out_asset"), calls


def _clean(out):
    assert out["name"].tolist() == ["Ada"] * len(out)
    assert out["age"].tolist() == [36] * len(out)


def test_think_block_is_stripped_before_parsing(monkeypatch, tmp_path):
    out, _ = _run(monkeypatch, tmp_path, f"<think>{THINK}</think>" + REPLY)
    _clean(out)
    assert "_reasoning" not in out.columns   # default output is unchanged


def test_plain_reply_output_is_identical_with_or_without_thinking(monkeypatch, tmp_path):
    plain, _ = _run(monkeypatch, tmp_path, REPLY)
    thinking, _ = _run(monkeypatch, tmp_path, f"<think>{THINK}</think>" + REPLY)
    _clean(plain)
    pd.testing.assert_frame_equal(plain, thinking)


def test_include_reasoning_exposes_the_thinking(monkeypatch, tmp_path):
    out, _ = _run(monkeypatch, tmp_path, f"<think>{THINK}</think>" + REPLY, include_reasoning=True)
    _clean(out)
    assert out["_reasoning"].tolist() == [THINK] * len(out)


def test_include_reasoning_with_plain_reply_is_null(monkeypatch, tmp_path):
    out, _ = _run(monkeypatch, tmp_path, REPLY, include_reasoning=True)
    _clean(out)
    assert out["_reasoning"].isna().all()


def test_row_count_and_schema_metadata_are_emitted_when_no_rows_are_dropped(monkeypatch, tmp_path):
    """Regression: the column-schema/row-count/preview metadata block was nested inside `if rows_to_drop:`, so a clean run
    (every row parsed, nothing dropped -- the normal case) emitted no metadata at all."""
    fake, _ = _fake_litellm(REPLY)
    monkeypatch.setitem(sys.modules, "litellm", fake)
    mod = load_component_module()
    cls = next(c for c in vars(mod).values() if inspect.isclass(c) and issubclass(c, dg.Component)
               and c is not dg.Component and c.__module__ == mod.__name__)
    component = cls(asset_name="out_asset", upstream_asset_key="src", text_column="text",
                    schema_definition={"name": {"type": "string"}, "age": {"type": "integer"}})

    @dg.asset(name="src")
    def src():
        return _upstream(tmp_path)

    res = dg.materialize([src, component.build_defs(None).assets[0]], resources={"io_manager": dg.InMemoryIOManager()})
    assert res.success
    md = next(e.materialization.metadata for e in res.get_asset_materialization_events()
              if e.materialization.asset_key.to_user_string() == "out_asset")
    assert md["dagster/row_count"].value == 2          # no rows were dropped
    assert "dagster/column_schema" in md
