"""custom op: bring-your-own-Python-callable text transform, same 'module.path:function_name'
convention as classify mode='custom' (see test_classify_custom.py, used as the structural template
here). Real, nothing mocked -- the fixtures in _fixtures.py are plain Python, no network/model
calls, imported through the exact dotted path the component's own importlib.import_module call
resolves.

The real motivating integration test (test_real_pii_redaction_via_log_redaction_module) calls the
repo's own log_redaction.pii_log_redaction:redact_text for real -- no mock -- since its default
regex-only mode (enable_presidio=False) has no external dependency."""
import importlib

import pandas as pd
import pytest

from .conftest import load_component_module
from .test_context_engineering_pipeline import _materialize, tickets_df  # noqa: F401  (tickets_df is a fixture)

_fixtures = importlib.import_module("assets.ai.context_engineering_pipeline.tests._fixtures")


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


def _df(mod, bodies):
    """A tiny DataFrame shaped like chunk op output: chunk_text + a real content_hash, so the
    redact-then-rehash path (output_column == text_column) has something real to verify."""
    return pd.DataFrame({
        "chunk_id": [f"c{i}" for i in range(len(bodies))],
        "chunk_text": bodies,
        "content_hash": [mod._sha256(b) for b in bodies],
    })


def test_custom_op_transforms_text_and_updates_content_hash(mod):
    df = _df(mod, ["hello world", "another chunk"])
    original_hashes = list(df["content_hash"])
    out = mod._do_custom(
        df,
        {"op": "custom", "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:uppercase"},
        Ctx,
    )
    assert list(out["chunk_text"]) == ["HELLO WORLD", "ANOTHER CHUNK"]
    assert list(out["content_hash"]) != original_hashes
    assert list(out["content_hash"]) == [mod._sha256(t) for t in out["chunk_text"]]
    assert out.attrs["stats"] == {"transformed": 2, "failed": 0}
    assert len(_fixtures.CALLS) == 2


def test_custom_op_requires_fn(mod):
    df = _df(mod, ["x"])
    with pytest.raises(ValueError, match="requires `fn`"):
        mod._do_custom(df, {"op": "custom"}, Ctx)


def test_custom_op_rejects_malformed_fn_path(mod):
    df = _df(mod, ["x"])
    with pytest.raises(ValueError, match="module.path:function_name"):
        mod._do_custom(df, {"op": "custom", "fn": "not_a_valid_path"}, Ctx)


def test_custom_op_on_error_raise_default_propagates(mod):
    df = _df(mod, ["x", "y"])
    with pytest.raises(RuntimeError, match="boom"):
        mod._do_custom(
            df,
            {"op": "custom", "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:raises_for_custom"},
            Ctx,
        )


def test_custom_op_on_error_skip_keeps_original_text(mod):
    df = _df(mod, ["x", "y"])
    original_hashes = list(df["content_hash"])
    out = mod._do_custom(
        df,
        {"op": "custom", "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:raises_for_custom",
         "on_error": "skip"},
        Ctx,
    )
    assert list(out["chunk_text"]) == ["x", "y"]  # unchanged
    assert list(out["content_hash"]) == original_hashes  # rehash of unchanged text == unchanged hash
    assert out.attrs["stats"] == {"transformed": 0, "failed": 2}


def test_custom_op_rejects_unknown_on_error(mod):
    df = _df(mod, ["x"])
    with pytest.raises(ValueError, match="on_error must be"):
        mod._do_custom(
            df,
            {"op": "custom", "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:uppercase",
             "on_error": "bogus"},
            Ctx,
        )


def test_custom_op_output_column_writes_derived_column_leaves_original_untouched(mod):
    df = _df(mod, ["hello world"])
    original_text = list(df["chunk_text"])
    original_hash = list(df["content_hash"])
    out = mod._do_custom(
        df,
        {"op": "custom", "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:uppercase",
         "output_column": "chunk_text_upper"},
        Ctx,
    )
    assert list(out["chunk_text"]) == original_text          # untouched
    assert list(out["content_hash"]) == original_hash          # not rehashed
    assert list(out["chunk_text_upper"]) == ["HELLO WORLD"]    # new column holds the transform


def test_custom_op_passes_params_as_kwargs(mod):
    """params: dict knob is passed through as **params to fn."""
    df = _df(mod, ["abc"])
    out = mod._do_custom(
        df,
        {"op": "custom", "fn": "log_redaction.pii_log_redaction:redact_text",
         "params": {"mask_format": "<<{entity}>>"}},
        Ctx,
    )
    # no PII in "abc", so this just proves params reached redact_text without raising (a bad kwarg
    # name would TypeError inside fn and surface via on_error='raise', the default).
    assert out["chunk_text"].iloc[0] == "abc"


def test_execution_mode_sql_rejects_custom_op(mod):
    """custom is deliberately NOT in sql_capable_ops -- this proves the existing python_only
    rejection mechanism (shared by every python-mode-only op, e.g. write_vector_store) already
    covers it correctly, with no new SQL-mode-specific code required."""
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body",
        execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb",
        source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT * FROM t"},
        steps=[
            {"id": "c", "op": "chunk"},
            {"id": "custom_step", "op": "custom",
             "fn": "assets.ai.context_engineering_pipeline.tests._fixtures:uppercase"},
            {"id": "e", "op": "embed", "model": "p.d.m"},
        ],
    )
    with pytest.raises(ValueError, match="python-mode-only op"):
        component.build_defs(context=None)


def test_real_pii_redaction_via_log_redaction_module(mod, tickets_df):  # noqa: F811
    """The real motivating use case: a custom step wired to the repo's own
    log_redaction.pii_log_redaction:redact_text, for NER-grade... well, here just its default
    regex-only mode (no presidio/spacy dependency needed) -- real, non-mocked call end to end
    through the full pipeline (chunk -> custom)."""
    component = mod.ContextEngineeringPipelineComponent(
        asset_name="redacted_kb", upstream_asset_key="raw_tickets", id_column="ticket_id", text_column="body",
        steps=[
            {"id": "chunks", "op": "chunk", "chunk_size": 500},
            {"id": "redacted", "op": "custom", "fn": "log_redaction.pii_log_redaction:redact_text"},
        ],
    )
    # tickets_df has no email in its bodies -- add one row that does, so the real redaction has
    # something real to catch end to end.
    df = tickets_df.copy()
    df.loc[len(df)] = {"ticket_id": "T4", "customer_id": "C4", "body": "Please reach me at eric@example.com about this."}
    result = _materialize(component, df, upstream_name="raw_tickets")
    assert result.success
    df_out = result.output_for_node("redacted_kb")
    redacted_row = df_out[df_out["chunk_text"].str.contains("reach me", na=False)]
    assert len(redacted_row) == 1
    text = redacted_row["chunk_text"].iloc[0]
    assert "eric@example.com" not in text
    assert "[EMAIL_ADDRESS]" in text


def test_unit_level_real_redact_text_call(mod):
    """Direct unit-level confirmation (no Dagster materialize overhead) of the real redact_text
    masking format, independent of the end-to-end test above."""
    df = _df(mod, ["contact me at eric.thomas@dagsterlabs.com please", "no pii here"])
    out = mod._do_custom(
        df,
        {"op": "custom", "fn": "log_redaction.pii_log_redaction:redact_text"},
        Ctx,
    )
    assert out["chunk_text"].iloc[0] == "contact me at [EMAIL_ADDRESS] please"
    assert out["chunk_text"].iloc[1] == "no pii here"
    assert out.attrs["stats"] == {"transformed": 2, "failed": 0}
