"""`produces` in manifest.json uses Dagster's kinds plus the documented manifest-only ones (tools/produces_vocabulary.py)."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

from produces_vocabulary import (  # noqa: E402
    ALLOWED_KINDS,
    LEGACY_ALIASES,
    MANIFEST_ONLY_KINDS,
    NATIVE_KINDS,
    to_native_produces,
)

MANIFEST = json.loads((ROOT / "manifest.json").read_text())["components"]


def _kinds(entry):
    produces = entry.get("produces")
    return produces if isinstance(produces, list) else ([produces] if produces else [])


def test_every_manifest_value_is_in_the_vocabulary():
    bad = {c["id"]: _kinds(c) for c in MANIFEST if set(_kinds(c)) - ALLOWED_KINDS}
    assert not bad, f"unknown `produces` values (see MANIFEST_CONVENTIONS.md): {bad}"


def test_legacy_spellings_are_gone():
    stale = {c["id"]: _kinds(c) for c in MANIFEST if set(_kinds(c)) & set(LEGACY_ALIASES)}
    assert not stale, f"use {LEGACY_ALIASES} instead: {stale}"


def test_native_and_manifest_only_kinds_do_not_overlap():
    assert not NATIVE_KINDS & MANIFEST_ONLY_KINDS


def test_native_kinds_match_dagster_when_available():
    # Dagster's own list, when this environment has a version that declares it.
    try:
        from dagster_shared.serdes.objects.package_entry import ComponentProducesKind
    except ImportError:
        pytest.skip("this Dagster version doesn't declare ComponentProducesKind")
    assert NATIVE_KINDS == {kind.value for kind in ComponentProducesKind}


@pytest.mark.parametrize("produces,expected", [
    (["asset"], ["asset"]),
    (["multi_asset"], ["asset"]),
    (["multi_asset", "asset"], ["asset"]),
    (["asset", "asset_check"], ["asset", "asset_check"]),
    (["job", "schedule"], ["job", "schedule"]),
    (["io_manager"], []),
    (["other", "sensor"], ["sensor"]),
    ([], []),
    (None, []),
])
def test_to_native_produces(produces, expected):
    assert to_native_produces(produces) == expected


def test_to_native_produces_only_ever_returns_native_kinds():
    every = {k for c in MANIFEST for k in _kinds(c)}
    assert set(to_native_produces(sorted(every))) <= NATIVE_KINDS
