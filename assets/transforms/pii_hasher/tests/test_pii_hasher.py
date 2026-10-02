"""Committed tests for PiiHasherComponent.

No external API calls are involved (pure in-memory transform), so these
exercise the real hashing/normalization logic directly -- no mocking.
Covers: both phone normalization modes, both email normalization modes,
hash correctness against SHA-256 test vectors computed independently via
`hashlib` directly (not by re-calling the component's own wrapper), drop-
vs-keep-original-column behavior, generic/passthrough mode, empty
DataFrame handling, and the two "unsupported config" ValueErrors.
"""
import hashlib

import dagster as dg
import pandas as pd
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(mod, df: pd.DataFrame, **attrs):
    @dg.asset(name="raw")
    def raw():
        return df

    component = mod.PiiHasherComponent(
        asset_name="out", upstream_asset_key="raw", **attrs,
    )
    asset_def = list(component.build_defs(None).assets)[0]
    result = dg.materialize([asset_def, raw])
    assert result.success
    return result.output_for_node("out")


def _independent_sha256(value: str) -> str:
    """Ground truth computed directly via hashlib -- independent of
    component.py's `_sha256_hex` wrapper."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Pure-function unit tests (no Dagster materialize involved)
# ---------------------------------------------------------------------------

def test_hash_correctness_against_known_sha256_vector(mod):
    # Known test vector computed independently via hashlib, not copied from
    # any online table -- verifies the component's hex digest matches the
    # real SHA-256 algorithm, not a stand-in / truncated / re-encoded value.
    raw = "test@example.com"
    expected = _independent_sha256("test@example.com")  # normalized form equals raw here (already lowercase, no spaces)
    assert mod._sha256_hex(raw) == expected
    assert len(expected) == 64  # SHA-256 hex digest is always 64 hex chars
    assert all(c in "0123456789abcdef" for c in expected)


def test_hash_identifier_email_matches_independent_vector(mod):
    expected = _independent_sha256("jane.doe@example.com")
    assert mod._hash_identifier(" Jane.Doe@Example.com ", "email", False, "e164_with_plus") == expected


def test_normalize_email_default_trim_lowercase(mod):
    assert mod._normalize_email("  Jane.Doe@Example.com  ", False) == "jane.doe@example.com"
    # internal whitespace is NOT removed in default mode
    assert mod._normalize_email("jane doe@example.com", False) == "jane doe@example.com"


def test_normalize_email_strip_all_whitespace_linkedin_rule(mod):
    # LinkedIn's broader rule: lowercase + remove ALL whitespace, not just
    # leading/trailing.
    assert mod._normalize_email(" Jane Doe@Example.com ", True) == "janedoe@example.com"


def test_normalize_phone_e164_with_plus(mod):
    # Google Ads / TikTok / X convention: leading '+' retained, digits only.
    assert mod._normalize_phone("+1 (415) 555-2671", "e164_with_plus") == "+14155552671"
    # No leading '+' supplied -- one is still prefixed.
    assert mod._normalize_phone("14155552671", "e164_with_plus") == "+14155552671"


def test_normalize_phone_digits_only_no_plus(mod):
    # Meta / Pinterest convention: no '+', digits only.
    assert mod._normalize_phone("+1 (415) 555-2671", "digits_only_no_plus") == "14155552671"
    assert mod._normalize_phone("415.555.2671", "digits_only_no_plus") == "4155552671"


def test_normalize_generic_trim_only_no_case_change(mod):
    assert mod._normalize_generic("  Loyalty-ID-007  ") == "Loyalty-ID-007"


def test_hash_identifier_null_and_nan_return_none(mod):
    assert mod._hash_identifier(None, "email", False, "e164_with_plus") is None
    assert mod._hash_identifier(float("nan"), "email", False, "e164_with_plus") is None
    assert mod._hash_identifier("   ", "generic", False, "e164_with_plus") is None


def test_hash_identifier_unsupported_type_raises(mod):
    with pytest.raises(ValueError):
        mod._hash_identifier("value", "ssn", False, "e164_with_plus")


# ---------------------------------------------------------------------------
# Full-component materialize tests
# ---------------------------------------------------------------------------

def test_email_and_phone_hashing_e164_mode_with_known_vectors(mod):
    df = pd.DataFrame({
        "email": [" Jane.Doe@Example.com ", "BOB@TEST.COM"],
        "phone": ["+1 (415) 555-2671", "14155552672"],
        "name": ["Jane", "Bob"],
    })
    out = _materialize(
        mod, df,
        column_identifiers={"email": "email", "phone": "phone"},
        phone_mode="e164_with_plus",
    )
    assert out["email_hashed"].tolist() == [
        _independent_sha256("jane.doe@example.com"),
        _independent_sha256("bob@test.com"),
    ]
    assert out["phone_hashed"].tolist() == [
        _independent_sha256("+14155552671"),
        _independent_sha256("+14155552672"),
    ]
    # Passthrough column untouched.
    assert out["name"].tolist() == ["Jane", "Bob"]
    # Default drop_original_columns=True: plaintext columns gone.
    assert "email" not in out.columns
    assert "phone" not in out.columns


def test_phone_digits_only_no_plus_mode(mod):
    df = pd.DataFrame({"phone": ["+1 (415) 555-2671"]})
    out = _materialize(
        mod, df,
        column_identifiers={"phone": "phone"},
        phone_mode="digits_only_no_plus",
    )
    assert out["phone_hashed"].iloc[0] == _independent_sha256("14155552671")


def test_email_strip_all_whitespace_mode(mod):
    df = pd.DataFrame({"email": [" Jane Doe@Example.com "]})
    out = _materialize(
        mod, df,
        column_identifiers={"email": "email"},
        email_strip_all_whitespace=True,
    )
    assert out["email_hashed"].iloc[0] == _independent_sha256("janedoe@example.com")


def test_generic_passthrough_identifier_mode(mod):
    df = pd.DataFrame({"loyalty_id": ["  ABC-123  "]})
    out = _materialize(
        mod, df,
        column_identifiers={"loyalty_id": "generic"},
    )
    assert out["loyalty_id_hashed"].iloc[0] == _independent_sha256("ABC-123")


def test_drop_original_columns_true_removes_plaintext(mod):
    df = pd.DataFrame({"email": ["a@b.com"], "other": [1]})
    out = _materialize(mod, df, column_identifiers={"email": "email"})
    assert "email" not in out.columns
    assert "email_hashed" in out.columns
    assert "other" in out.columns


def test_drop_original_columns_false_keeps_plaintext(mod):
    df = pd.DataFrame({"email": ["a@b.com"], "other": [1]})
    out = _materialize(
        mod, df,
        column_identifiers={"email": "email"},
        drop_original_columns=False,
    )
    assert "email" in out.columns
    assert out["email"].iloc[0] == "a@b.com"
    assert "email_hashed" in out.columns
    assert out["email_hashed"].iloc[0] == _independent_sha256("a@b.com")


def test_custom_hashed_column_suffix(mod):
    df = pd.DataFrame({"email": ["a@b.com"]})
    out = _materialize(
        mod, df,
        column_identifiers={"email": "email"},
        hashed_column_suffix="_sha256",
    )
    assert "email_sha256" in out.columns
    assert "email_hashed" not in out.columns


def test_null_and_nan_values_skipped_not_hashed(mod):
    df = pd.DataFrame({"email": ["a@b.com", None, float("nan"), ""]})
    out = _materialize(mod, df, column_identifiers={"email": "email"})
    assert out["email_hashed"].iloc[0] == _independent_sha256("a@b.com")
    assert pd.isna(out["email_hashed"].iloc[1])
    assert pd.isna(out["email_hashed"].iloc[2])
    assert pd.isna(out["email_hashed"].iloc[3])


def test_empty_dataframe_with_matching_columns_does_not_crash(mod):
    df = pd.DataFrame({"email": pd.Series([], dtype="object"), "phone": pd.Series([], dtype="object")})
    out = _materialize(mod, df, column_identifiers={"email": "email", "phone": "phone"})
    assert len(out) == 0
    assert "email_hashed" in out.columns
    assert "phone_hashed" in out.columns


def test_empty_dataframe_missing_columns_warns_but_does_not_crash(mod):
    df = pd.DataFrame({"unrelated": pd.Series([], dtype="object")})
    out = _materialize(mod, df, column_identifiers={"email": "email"})
    assert len(out) == 0
    # Column never existed upstream, so no hashed column is created either.
    assert "email_hashed" not in out.columns
    assert "unrelated" in out.columns


def test_missing_column_on_nonempty_dataframe_warns_but_does_not_crash(mod):
    df = pd.DataFrame({"name": ["Jane"]})
    out = _materialize(mod, df, column_identifiers={"email": "email"})
    assert len(out) == 1
    assert "email_hashed" not in out.columns
    assert out["name"].iloc[0] == "Jane"


def test_unsupported_identifier_type_raises_value_error(mod):
    component = mod.PiiHasherComponent(
        asset_name="out",
        upstream_asset_key="raw",
        column_identifiers={"ssn": "social_security_number"},
    )
    with pytest.raises(ValueError, match="unsupported identifier type"):
        component.build_defs(None)


def test_unsupported_phone_mode_raises_value_error(mod):
    component = mod.PiiHasherComponent(
        asset_name="out",
        upstream_asset_key="raw",
        column_identifiers={"phone": "phone"},
        phone_mode="not_a_real_mode",
    )
    with pytest.raises(ValueError, match="phone_mode"):
        component.build_defs(None)


def test_multiple_identifiers_and_column_lineage_default(mod):
    df = pd.DataFrame({
        "email": ["a@b.com"],
        "phone": ["+14155552671"],
        "loyalty_id": ["XYZ"],
    })
    component = mod.PiiHasherComponent(
        asset_name="out",
        upstream_asset_key="raw",
        column_identifiers={"email": "email", "phone": "phone", "loyalty_id": "generic"},
    )
    asset_def = list(component.build_defs(None).assets)[0]

    @dg.asset(name="raw")
    def raw():
        return df

    result = dg.materialize([asset_def, raw])
    assert result.success
    out = result.output_for_node("out")
    assert set(out.columns) == {"email_hashed", "phone_hashed", "loyalty_id_hashed"}
