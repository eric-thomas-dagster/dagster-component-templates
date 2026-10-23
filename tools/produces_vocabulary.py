"""The allowed values of a manifest entry's `produces`, and how they relate to Dagster's own.

Dagster declares what a component creates with `ComponentTypeSpec(produces=[...])`, validated against
`ComponentProducesKind`. The manifest uses those same names, plus a few of its own that the catalog and
Designer find useful. One place defines the vocabulary so the inference tool, the tests and any future
generator agree.
"""

# Dagster's own kinds (dagster_shared.serdes.objects.package_entry.ComponentProducesKind).
NATIVE_KINDS = frozenset({"asset", "asset_check", "schedule", "sensor", "job", "resource"})

# Manifest-only. `multi_asset` refines `asset` (many assets from one component; see `emitted_assets`).
MANIFEST_ONLY_KINDS = frozenset({"multi_asset", "io_manager", "partitions_def", "other"})

ALLOWED_KINDS = NATIVE_KINDS | MANIFEST_ONLY_KINDS

# Older spellings that were used by hand before the vocabulary was written down.
LEGACY_ALIASES = {"check": "asset_check", "external_asset": "asset"}


def to_native_produces(produces):
    """The Dagster-native kinds for a manifest `produces` list: `multi_asset` becomes `asset`, manifest-only kinds are dropped."""
    out = []
    for kind in produces or []:
        native = "asset" if kind == "multi_asset" else kind
        if native in NATIVE_KINDS and native not in out:
            out.append(native)
    return out
