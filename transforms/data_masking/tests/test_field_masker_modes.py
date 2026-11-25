"""Tests for DataMaskingComponent's StreamSets-Field-Masker-parity `method='mask'`
modes: fixed_length, variable_length, custom, regex. Ported directly from
StreamSets' own open-source FieldMaskProcessor.java (verified by reading
https://github.com/kiritbasu/datacollector/blob/9a475fd6ad125f31b318d9b1f9ddfa58cb224478/
basic-lib/src/main/java/com/streamsets/pipeline/stage/processor/fieldmask/FieldMaskProcessor.java
directly, not inferred) -- test values below are worked through that exact
algorithm by hand, not just "it runs."
"""
import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


def _materialize_and_get_df(mod, rules, upstream_df, asset_name="out_asset"):
    comp = mod.DataMaskingComponent(
        asset_name=asset_name,
        upstream_asset_key="upstream_data",
        rules=rules,
    )
    defs = comp.build_defs(context=None)

    @dg.asset(key=dg.AssetKey("upstream_data"))
    def upstream_data() -> pd.DataFrame:
        return upstream_df

    result = dg.materialize([upstream_data, *defs.assets])
    assert result.success
    return result.asset_value(dg.AssetKey(asset_name))


def test_fixed_length_is_always_the_same_ten_x_regardless_of_input(mod):
    df = pd.DataFrame({"ssn": ["123-45-6789", "1", ""]})
    out = _materialize_and_get_df(
        mod, [{"column": "ssn", "method": "mask", "mask_mode": "fixed_length"}], df
    )
    assert list(out["ssn"]) == ["xxxxxxxxxx"] * 3


def test_variable_length_is_one_x_per_input_character(mod):
    df = pd.DataFrame({"ssn": ["123-45-6789", "ab", ""]})
    out = _materialize_and_get_df(
        mod, [{"column": "ssn", "method": "mask", "mask_mode": "variable_length"}], df
    )
    assert list(out["ssn"]) == ["x" * 11, "xx", ""]


def test_custom_mask_shows_hash_chars_and_preserves_existing_formatting(mod):
    """Pattern 'xxx-xx-####' against an already-formatted SSN: the dashes in
    the input line up with the literal dashes in the pattern, so they're
    consumed (not doubled); '#' positions show through."""
    df = pd.DataFrame({"ssn": ["123-45-6789"]})
    out = _materialize_and_get_df(
        mod,
        [{"column": "ssn", "method": "mask", "mask_mode": "custom", "mask": "xxx-xx-####"}],
        df,
    )
    assert out["ssn"].iloc[0] == "xxx-xx-6789"


def test_custom_mask_inserts_formatting_into_unformatted_input(mod):
    """Same pattern against an UNFORMATTED 9-digit SSN: the literal '-' in
    the pattern doesn't match the input char at that position, so it's
    inserted without consuming an input character -- this is what lets the
    same mask pattern both format and mask raw data."""
    df = pd.DataFrame({"ssn": ["123456789"]})
    out = _materialize_and_get_df(
        mod,
        [{"column": "ssn", "method": "mask", "mask_mode": "custom", "mask": "xxx-xx-####"}],
        df,
    )
    assert out["ssn"].iloc[0] == "xxx-xx-6789"


def test_custom_mask_requires_mask_pattern(mod):
    df = pd.DataFrame({"a": ["x"]})
    with pytest.raises(ValueError, match="'mask'"):
        _materialize_and_get_df(mod, [{"column": "a", "method": "mask", "mask_mode": "custom"}], df)


def test_regex_mask_shows_only_the_listed_group_at_its_original_position(mod):
    """'(.*)([0-9]{4})' showing group 2 on a 10-char value: group 2 matches
    the last 4 chars at positions [6,10) -- result is 6 x's then those exact
    4 characters, not a separate concatenation."""
    df = pd.DataFrame({"card": ["1234567890"]})
    out = _materialize_and_get_df(
        mod,
        [{
            "column": "card", "method": "mask", "mask_mode": "regex",
            "regex": r"(.*)([0-9]{4})", "groups_to_show": [2],
        }],
        df,
    )
    assert out["card"].iloc[0] == "xxxxxx7890"


def test_regex_mask_nonmatching_value_is_left_completely_unmasked(mod):
    """StreamSets: a value that doesn't match the (full-string) regex is
    returned AS-IS, not masked and not an error."""
    df = pd.DataFrame({"card": ["not-a-card-number"]})
    out = _materialize_and_get_df(
        mod,
        [{
            "column": "card", "method": "mask", "mask_mode": "regex",
            "regex": r"(.*)([0-9]{4})", "groups_to_show": [2],
        }],
        df,
    )
    assert out["card"].iloc[0] == "not-a-card-number"


def test_regex_mask_requires_full_string_match_not_partial(mod):
    """Java's Matcher.matches() (-> Python re.fullmatch) requires the WHOLE
    value to match -- a regex that only matches a substring must still fail
    to match the full value and leave it unmasked."""
    df = pd.DataFrame({"val": ["prefix-1234-suffix"]})
    out = _materialize_and_get_df(
        mod,
        [{
            "column": "val", "method": "mask", "mask_mode": "regex",
            "regex": r"[0-9]{4}", "groups_to_show": [1],
        }],
        df,
    )
    # "[0-9]{4}" has no capture group 1 to show even if it matched, but more
    # importantly it can't fullmatch "prefix-1234-suffix" at all -> unchanged.
    assert out["val"].iloc[0] == "prefix-1234-suffix"


def test_regex_mask_multiple_groups_shown(mod):
    df = pd.DataFrame({"val": ["AAA-1234-BBB"]})
    out = _materialize_and_get_df(
        mod,
        [{
            "column": "val", "method": "mask", "mask_mode": "regex",
            "regex": r"([A-Z]+)-([0-9]+)-([A-Z]+)", "groups_to_show": [1, 3],
        }],
        df,
    )
    assert out["val"].iloc[0] == "AAAxxxxxxBBB"


def test_mask_requires_mask_mode(mod):
    df = pd.DataFrame({"a": ["x"]})
    with pytest.raises(ValueError, match="mask_mode"):
        _materialize_and_get_df(mod, [{"column": "a", "method": "mask"}], df)


def test_unknown_mask_mode_raises(mod):
    df = pd.DataFrame({"a": ["x"]})
    with pytest.raises(ValueError, match="unknown mask_mode"):
        _materialize_and_get_df(mod, [{"column": "a", "method": "mask", "mask_mode": "nope"}], df)
