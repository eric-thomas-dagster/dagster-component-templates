"""classify op mode='custom': bring-your-own-classifier, same 'module.path:function_name' convention as
langgraph_agent's graph_fn. Real, nothing mocked -- the fixtures in _fixtures.py are plain Python, no
network/model calls, imported through the exact dotted path the component's own importlib.import_module
call resolves (see _fixtures.py's own docstring for why that matters)."""
import importlib

import pandas as pd
import pytest

from .conftest import load_component_module
from .test_context_engineering_pipeline import _materialize, tickets_df  # noqa: F401  (tickets_df is a fixture)

_fixtures = importlib.import_module("assets.ai.context_engineering_pipeline.tests._fixtures")

LABELS = ["billing", "technical", "shipping"]


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def _reset_fixture_state():
    _fixtures.reset()
    yield
    _fixtures.reset()


class Ctx:
    class log:
        @staticmethod
        def info(*a, **k): ...
        @staticmethod
        def warning(*a, **k): ...


def _df(bodies):
    return pd.DataFrame({"chunk_text": bodies})


def test_custom_classifier_tags_each_chunk_and_is_called_once_per_row(mod):
    out = mod._do_classify(
        _df(["I need a billing refund", "the app crashes on launch", "there was a shipping delay"]),
        {"op": "classify", "mode": "custom", "candidate_labels": LABELS,
         "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:keyword_classifier"},
        Ctx,
    )
    assert list(out["category"]) == ["billing", None, "shipping"]   # "crashes" matches no configured label
    assert len(_fixtures.CALLS) == 3


def test_custom_classifier_requires_fn(mod):
    with pytest.raises(ValueError, match="requires `fn`"):
        mod._do_classify(_df(["x"]), {"op": "classify", "mode": "custom", "candidate_labels": LABELS}, Ctx)


def test_custom_classifier_rejects_malformed_path(mod):
    with pytest.raises(ValueError, match="module.path:function_name"):
        mod._do_classify(
            _df(["x"]), {"op": "classify", "mode": "custom", "candidate_labels": LABELS, "fn": "not_a_valid_path"}, Ctx,
        )


def test_custom_classifier_fn_exception_drops_to_none(mod):
    out = mod._do_classify(
        _df(["x", "y"]),
        {"op": "classify", "mode": "custom", "candidate_labels": LABELS,
         "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:raises"},
        Ctx,
    )
    assert list(out["category"]) == [None, None]


def test_custom_classifier_label_not_in_candidate_labels_drops_to_none(mod):
    out = mod._do_classify(
        _df(["x"]),
        {"op": "classify", "mode": "custom", "candidate_labels": LABELS,
         "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:returns_unknown_label"},
        Ctx,
    )
    assert out["category"].iloc[0] is None


def test_execution_mode_sql_rejects_classify_mode_custom(mod):
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body",
        execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb",
        source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT * FROM t"},
        steps=[
            {"id": "c", "op": "chunk"},
            {"id": "k", "op": "classify", "mode": "custom", "candidate_labels": LABELS,
             "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:keyword_classifier"},
            {"id": "e", "op": "embed", "model": "p.d.m"},
        ],
    )
    with pytest.raises(ValueError, match="mode='custom', not supported with execution_mode='sql'"):
        component.build_defs(context=None)


def test_full_pipeline_with_a_custom_classifier(mod, tickets_df):  # noqa: F811
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="support_kb", upstream_asset_key="raw_tickets", id_column="ticket_id", text_column="body",
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 200},
            {"id": "classified", "op": "classify", "mode": "custom", "candidate_labels": LABELS,
             "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:keyword_classifier"},
        ],
    )
    result = _materialize(component, tickets_df)
    assert result.success
    df_out = result.output_for_node("support_kb")
    assert len(df_out) == 3
    assert set(df_out["category"]) <= set(LABELS) | {None}
    assert len(_fixtures.CALLS) == 3
