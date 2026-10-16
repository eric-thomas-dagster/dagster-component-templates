"""Committed tests for this component's backfill_policy / partition-range support.

The REAL component asset is materialized with dg.materialize() against a real
(real-data) upstream asset and dg.InMemoryIOManager(), using the run tags a
real single_run backfill sets (dagster/asset_partition_range_start/_end).
Only the paid/network model SDKs are replaced (fake `litellm` / `openai` /
`anthropic` modules in sys.modules).

single_run loops over context.partition_keys, running the unchanged per-key
logic once per key and concatenating the results. A thin pandas subclass
records every boolean-mask selection made on the upstream frame so we can
assert the per-key partition filter REALLY ran once per key in the range.
"""
import inspect
import sys
import types

import dagster as dg
import numpy as np
import pandas as pd
import pytest

from .conftest import load_component_module

EXTRA = {'text_column': 'text'}

SEEN = []


class _SpyFrame(pd.DataFrame):
    @property
    def _constructor(self):
        return _SpyFrame

    def __getitem__(self, key):
        out = super().__getitem__(key)
        if isinstance(key, (np.ndarray, pd.Series)) and key.dtype == bool:
            SEEN.append(out)
        return out


_TXT = [f"great product number {i}, works well" for i in range(1, 7)]


def _rows():
    return _SpyFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "event_date": [
            "2024-01-01", "2024-01-02", "2024-01-02",
            "2024-01-03", "2024-01-04", "2024-01-05",
        ],
        "seg": ["a", "b", "a", "b", "a", "b"],
        # many aliases so whichever input column a component defaults to exists
        **{c: _TXT for c in (
            "text", "content", "question", "response", "llm_response", "query",
            "document", "ticket_text", "description", "body", "message", "prompt",
            "input", "title", "sentence", "comment", "review", "doc_text",
            "image_path", "image_url", "file_path", "image", "text_a", "text_b",
        )},
    })


def _fake_completion(*args, **kwargs):
    content = (
        '{"label": "a", "a": "x", "result": "ok", "score": 0.5, "category": "a", '
        '"mappings": [{"target_col": "a", "source_col": "text", "transformation": "direct", "expression": ""}]}'
    )
    msg = types.SimpleNamespace(content=content, tool_calls=None, role="assistant")
    return types.SimpleNamespace(
        choices=[types.SimpleNamespace(message=msg, finish_reason="stop")],
        usage=types.SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        model="fake",
    )


def _mod(monkeypatch, name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    monkeypatch.setitem(sys.modules, name, m)
    return m


def _extra_fakes(monkeypatch):
    pass


@pytest.fixture(autouse=True)
def _fake_model_sdks(monkeypatch):
    lit = types.ModuleType("litellm")
    lit.completion = _fake_completion
    lit.cost_per_token = lambda **kw: (0.0, 0.0)
    lit.completion_cost = lambda *a, **k: 0.0
    monkeypatch.setitem(sys.modules, "litellm", lit)

    class _Completions:
        create = staticmethod(_fake_completion)

    class _OpenAI:
        def __init__(self, *a, **k):
            self.chat = types.SimpleNamespace(completions=_Completions())

    oa = types.ModuleType("openai")
    oa.OpenAI = _OpenAI
    oa.AzureOpenAI = _OpenAI
    monkeypatch.setitem(sys.modules, "openai", oa)
    _extra_fakes(monkeypatch)
    SEEN.clear()
    yield
    SEEN.clear()


@pytest.fixture()
def mod():
    return load_component_module()


def _make(mod, **kw):
    cls = next(
        c for c in vars(mod).values()
        if inspect.isclass(c) and issubclass(c, dg.Component)
        and c is not dg.Component and c.__module__ == mod.__name__
    )
    return cls(asset_name="out_asset", upstream_asset_key="src", **EXTRA, **kw)


def _run(component, **kw):
    @dg.asset(name="src")
    def src():
        return _rows()

    asset_def = component.build_defs(None).assets[0]
    return asset_def, dg.materialize(
        [src, asset_def],
        resources={"io_manager": dg.InMemoryIOManager()},
        raise_on_error=False,
        **kw,
    )


def _filtered_ids():
    assert SEEN, "the component's partition filter never ran on the upstream frame"
    return sorted(i for s in SEEN for i in s["id"].tolist())


DAILY = dict(partition_type="daily", partition_start="2024-01-01")


def test_backfill_policy_wiring(mod):
    assert _make(mod, **DAILY).build_defs(None).assets[0].backfill_policy is None
    single = _make(mod, backfill_policy="single_run", **DAILY)
    assert single.build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.single_run()
    multi = _make(mod, backfill_policy="multi_run",
                  backfill_policy_max_partitions_per_run=3, **DAILY)
    assert multi.build_defs(None).assets[0].backfill_policy == dg.BackfillPolicy.multi_run(3)


def test_backfill_policy_ignored_when_unpartitioned(mod):
    comp = _make(mod, backfill_policy="single_run")
    assert comp.build_defs(None).assets[0].backfill_policy is None


def test_single_partition_tick_keeps_only_that_days_rows(mod):
    comp = _make(mod, partition_date_column="event_date", **DAILY)
    _, result = _run(comp, partition_key="2024-01-02")
    assert result.success
    assert _filtered_ids() == [2, 3]
    out = result.output_for_node("out_asset")
    # a single-key run is unchanged: no partition_key label column is added
    assert "partition_key" not in out.columns
    assert out["frequency"].sum() == 10  # 2 rows x 5 non-stopword tokens


def test_single_run_backfill_range_loops_over_every_key_and_concatenates(mod):
    """Before this change a range run crashed on context.partition_key (a range
    has no single key). Now each of the 3 keys is filtered and processed once
    and the results are concatenated into one frame."""
    comp = _make(mod, partition_date_column="event_date",
                 backfill_policy="single_run", **DAILY)
    asset_def, result = _run(comp, tags={
        "dagster/asset_partition_range_start": "2024-01-02",
        "dagster/asset_partition_range_end": "2024-01-04",
    })
    assert asset_def.backfill_policy == dg.BackfillPolicy.single_run()
    assert result.success
    # one mask per key (01-02, 01-03, 01-04); 01-01 and 01-05 rows excluded
    assert [sorted(s["id"].tolist()) for s in SEEN] == [[2, 3], [4], [5]]
    out = result.output_for_node("out_asset")
    # aggregate output: one word-frequency table per key, labelled by key
    assert sorted(out["partition_key"].unique()) == ["2024-01-02", "2024-01-03", "2024-01-04"]


def test_static_partition_range_filters_on_static_column(mod):
    comp = _make(mod, partition_type="static", partition_values="a,b,c",
                 partition_static_column="seg", backfill_policy="single_run")
    _, result = _run(comp, tags={
        "dagster/asset_partition_range_start": "a",
        "dagster/asset_partition_range_end": "b",
    })
    assert result.success
    assert [sorted(s["id"].tolist()) for s in SEEN] == [[1, 3, 5], [2, 4, 6]]
    SEEN.clear()
    _, result = _run(comp, partition_key="a")
    assert result.success
    assert _filtered_ids() == [1, 3, 5]
