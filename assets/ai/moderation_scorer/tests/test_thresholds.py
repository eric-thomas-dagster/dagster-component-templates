"""ModerationScorerComponent: default thresholds reproduce the historical
hardcoded behavior (flagged > 0.5, needs_review > 0.3); custom thresholds change decisions."""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module

TEXTS = {
    "clean": "this is a perfectly fine long message",  # risk 0.0
    "one_kw": "spam",                                   # 0.3 + 0.1 (short) = 0.4
    "three_kw": "spam scam fraud",                      # 0.9
    "two_kw": "spam scam message here is",              # 0.6
}


@pytest.fixture
def mod():
    return load_component_module()


def _run(mod, **kwargs):
    @dg.asset
    def user_content():
        return pd.DataFrame({"id": list(TEXTS), "content_text": list(TEXTS.values())})

    comp = mod.ModerationScorerComponent(asset_name="scores", upstream_asset_key="user_content", **kwargs)
    result = dg.materialize([user_content, *comp.build_defs(context=None).assets])
    assert result.success
    df = result.output_for_node("scores")
    return dict(zip(df["id"], df["moderation_decision"])), df


def test_defaults_match_historical_hardcoded_behavior(mod):
    decisions, df = _run(mod)
    assert decisions == {"clean": "approved", "one_kw": "needs_review", "three_kw": "flagged", "two_kw": "flagged"}
    assert "moderation_decision" in df.columns and "decision" not in df.columns
    assert mod.ModerationScorerComponent.model_fields["flag_threshold"].default == 0.5
    assert mod.ModerationScorerComponent.model_fields["review_threshold"].default == 0.3


def test_stricter_flag_threshold_downgrades_flags(mod):
    decisions, _ = _run(mod, flag_threshold=0.7)
    assert decisions["two_kw"] == "needs_review"  # 0.6 no longer > 0.7
    assert decisions["three_kw"] == "flagged"     # 0.9 still > 0.7


def test_custom_thresholds_change_decisions(mod):
    decisions, _ = _run(mod, flag_threshold=0.95, review_threshold=0.5)
    assert decisions == {"clean": "approved", "one_kw": "approved", "three_kw": "needs_review", "two_kw": "needs_review"}


@pytest.mark.parametrize("kw", [
    {"review_threshold": 0.8, "flag_threshold": 0.5},
    {"flag_threshold": 1.5},
    {"review_threshold": -0.1},
])
def test_invalid_thresholds_rejected(mod, kw):
    comp = mod.ModerationScorerComponent(asset_name="s", upstream_asset_key="u", **kw)
    with pytest.raises(ValueError, match="thresholds must satisfy"):
        comp.build_defs(context=None)
