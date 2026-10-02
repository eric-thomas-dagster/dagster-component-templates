#!/usr/bin/env python3
"""Backfill missing `description` fields in schema.json's `attributes` from
the real `Field(description=...)` values already in each component's
component.py.

Root cause: Dagster Designer's UI reads schema.json for attribute
descriptions, not component.py. 692 of 952 components with attributes
(73%, repo-wide) are missing at least one -- concentrated in ~20 shared
boilerplate fields (asset_name, group_name, owners, tags, kinds, deps,
retry_policy_*, partition_*, resource_key, include_preview_metadata, ...)
that get copy-pasted into nearly every component. The descriptions exist
in every one of these components' Python source; schema.json was just
never kept in sync with it.

This script is additive-only and narrowly scoped:
  - Only fills a description that is currently missing or empty.
  - Only touches attributes that already exist in schema.json (never adds
    a new attribute key).
  - Never overwrites a description that's already present, even if the
    Python source's wording differs.
  - Leaves every other schema.json key (type, default, required, label,
    ui:widget, x-dagster-io, category, icon, tags, vertical, ...) untouched.

Reuses `parse_fields()` from regen_readme_fields.py -- the same AST-based
Field() extraction already proven correct for the README Fields tables --
rather than re-deriving extraction logic.

Usage:
    python3 tools/backfill_schema_descriptions.py              # report only
    python3 tools/backfill_schema_descriptions.py --fix         # apply
    python3 tools/backfill_schema_descriptions.py --fix --only PATH [PATH ...]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))
from regen_readme_fields import parse_fields  # noqa: E402

EXCLUDE_TOP = {
    ".git", "node_modules", "dagster_community_components", ".claude",
    "docs", "tests", "tools", "cli", ".pytest_cache", "__pycache__",
}

# Tier-2 fallback: canonical wording for genuinely generic boilerplate fields,
# pulled from this repo's own most common existing usage (hundreds of
# components already use this exact wording) -- used only when a component's
# OWN Python source has no description for the field at all (Tier 1 always
# wins when present). Deliberately excludes component-specific fields like
# batch_size, whose real meaning varies too much per component to fake.
CANONICAL_DESCRIPTIONS = {
    "asset_name": "Output Dagster asset name.",
    "group_name": "Dagster asset group name.",
    "description": "Asset description.",
    "deps": "Lineage-only upstream asset keys (no data passed at runtime).",
    "owners": "Asset owners.",
    "tags": "Catalog tags.",
    "asset_tags": "Additional key-value tags applied to the asset in the Dagster catalog.",
    "kinds": "Asset kinds for the Dagster catalog.",
    "retry_policy_max_retries": "Max retries on asset failure. Defines a RetryPolicy.",
    "retry_policy_delay_seconds": "Seconds between retries.",
    "retry_policy_backoff": "Backoff strategy: 'linear' or 'exponential'.",
    "freshness_max_lag_minutes": "Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    "freshness_cron": "Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    "partition_type": "Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned.",
    "partition_start": "Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    "partition_values": "Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    "partition_dimensions": "Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    "dynamic_partition_name": "Name for DynamicPartitionsDefinition (when partition_type='dynamic').",
    "partition_static_dim": "Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    "partition_static_column": "Column used to filter the upstream DataFrame to the current static partition value.",
    "partition_date_column": "Column used to filter the upstream DataFrame to the current date partition key.",
    "include_preview_metadata": "Include a preview of the output data in metadata (sample rows) for builder UIs.",
    "preview_rows": "Rows to include in the preview metadata when `include_preview_metadata` is True.",
    "resource_key": "Resource key for the paired resource component.",
}


def _infer_resource_key_description(attrs: dict) -> str | None:
    """For a `resource_key` field with a `default` like 'braze', try to find
    the matching resources/braze_resource/component.py and name its real
    ResourceComponent class, matching this repo's own established wording
    ("Resource key registered by XResourceComponent.") rather than falling
    back to the generic constant."""
    default = (attrs.get("resource_key") or {}).get("default")
    if not isinstance(default, str) or not default:
        return None
    for candidate in [f"resources/{default}_resource", f"resources/{default}"]:
        comp = REPO_ROOT / candidate / "component.py"
        if not comp.exists():
            continue
        src = comp.read_text()
        m = re.search(r"class\s+(\w+ResourceComponent)\b", src)
        if m:
            return f"Resource key registered by {m.group(1)}."
    return None


def find_schema_dirs() -> list[Path]:
    dirs = []
    for p in REPO_ROOT.rglob("schema.json"):
        rel = p.relative_to(REPO_ROOT)
        if rel.parts[0] in EXCLUDE_TOP:
            continue
        if "tests" in rel.parts or "__pycache__" in rel.parts:
            continue
        if not (p.parent / "component.py").exists():
            continue
        dirs.append(p.parent)
    return sorted(dirs)


def _scan_balanced_brace(text: str, open_brace_pos: int) -> int | None:
    """Given the position of an opening '{', return the position of its
    matching '}' via a proper string-aware scanner (not a regex)."""
    depth = 0
    in_string = False
    escape = False
    i = open_brace_pos
    while i < len(text):
        c = text[i]
        if in_string:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    return None


def _find_attributes_block_span(text: str) -> tuple[int, int] | None:
    """Find the span of the top-level `"attributes": { ... }` block, so key
    lookups can be confined to it. Some schema.json files (legacy, duplicate
    representation) also have a `"properties"` block with the SAME key names
    already described -- without this, a naive whole-file search would match
    the wrong occurrence and silently no-op."""
    m = re.search(r'"attributes"\s*:\s*\{', text)
    if not m:
        return None
    start = m.end() - 1
    end = _scan_balanced_brace(text, start)
    if end is None:
        return None
    return start, end


def _find_attr_span(text: str, key: str, search_start: int, search_end: int) -> tuple[int, int] | None:
    """Find the character span [start_brace, end_brace] (inclusive) of the
    `{ ... }` value belonging to a `"key": { ... }` entry, searched only
    within text[search_start:search_end] (confined to the attributes block)."""
    m = re.search(r'"' + re.escape(key) + r'"\s*:\s*\{', text[search_start:search_end])
    if not m:
        return None
    start = search_start + m.end() - 1  # position of the opening '{'
    end = _scan_balanced_brace(text, start)
    if end is None:
        return None
    return start, end


def _splice_description(text: str, key: str, description: str) -> str | None:
    """Insert `"description": "..."` into an existing `"key": { ... }`
    attribute block, touching only that block's characters -- the rest of
    the file (formatting, key order, unrelated attributes) is untouched
    byte-for-byte. Returns None if the span can't be found/parsed."""
    attrs_block = _find_attributes_block_span(text)
    if attrs_block is None:
        return None
    span = _find_attr_span(text, key, attrs_block[0], attrs_block[1])
    if span is None:
        return None
    start, end = span
    block = text[start : end + 1]
    try:
        parsed = json.loads(block)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, dict) or parsed.get("description"):
        return None  # already has one, or isn't a plain dict -- don't touch

    desc_json = json.dumps(description, ensure_ascii=False)

    if "\n" not in block:
        # Single-line attribute, e.g. {"label": "Kinds", "type": "array"}
        # -- insert right after the opening brace. Strip any whitespace the
        # original had right after '{' so we don't produce a double space.
        rest = block[1:].lstrip(" ")
        new_block = block[:1] + f'"description": {desc_json}, ' + rest
    else:
        # Multi-line -- insert as the first key, matching the indentation
        # of the next real line inside the block.
        first_nl = block.index("\n")
        rest = block[first_nl + 1 :]
        next_line_indent = re.match(r"[ \t]*", rest).group(0)
        new_block = (
            block[: first_nl + 1]
            + f'{next_line_indent}"description": {desc_json},\n'
            + rest
        )

    return text[:start] + new_block + text[end + 1 :]


def process_dir(d: Path, apply: bool) -> tuple[int, list[str]]:
    """Returns (count_backfilled, list of 'attr: description' changes)."""
    schema_path = d / "schema.json"
    component_path = d / "component.py"

    raw_text = schema_path.read_text()
    schema = json.loads(raw_text)
    attrs = schema.get("attributes")
    if not isinstance(attrs, dict) or not attrs:
        return 0, []

    fields = parse_fields(component_path)
    field_descriptions = {f["name"]: f["description"] for f in fields if f["description"]}

    changes = []
    working_text = raw_text
    for key, attr in attrs.items():
        if not isinstance(attr, dict):
            continue
        existing = attr.get("description")
        if existing:
            continue
        # Tier 1: the component's own Python source has a real description
        # (schema.json just drifted out of sync with it).
        new_desc = field_descriptions.get(key)
        tier = 1
        if not new_desc:
            # Tier 2: genuinely never authored anywhere for this component --
            # fall back to this repo's own canonical wording for well-known
            # boilerplate fields only (never invented for component-specific
            # fields like batch_size).
            if key == "resource_key":
                new_desc = _infer_resource_key_description(attrs) or CANONICAL_DESCRIPTIONS.get(key)
            else:
                new_desc = CANONICAL_DESCRIPTIONS.get(key)
            tier = 2
        if not new_desc:
            continue

        if apply:
            spliced = _splice_description(working_text, key, new_desc)
            if spliced is None:
                continue  # couldn't locate/parse this attribute's span -- skip, don't guess
            working_text = spliced

        changes.append(f"{key} (tier {tier}): {new_desc!r}")

    if apply and changes:
        # Confirm the fully-spliced file is still valid JSON before writing.
        json.loads(working_text)
        schema_path.write_text(working_text)

    return len(changes), changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fix", action="store_true", help="Apply changes (default: report only).")
    parser.add_argument("--only", nargs="*", help="Restrict to these directories (relative to repo root).")
    parser.add_argument("--verbose", action="store_true", help="Print every individual change, not just counts.")
    args = parser.parse_args()

    if args.only:
        dirs = [REPO_ROOT / p for p in args.only]
    else:
        dirs = find_schema_dirs()

    total_files_changed = 0
    total_descriptions = 0
    for d in dirs:
        try:
            count, changes = process_dir(d, apply=args.fix)
        except Exception as e:
            print(f"ERROR in {d.relative_to(REPO_ROOT)}: {e}")
            continue
        if count:
            total_files_changed += 1
            total_descriptions += count
            rel = d.relative_to(REPO_ROOT)
            print(f"{rel}: {count} description(s) {'backfilled' if args.fix else 'would be backfilled'}")
            if args.verbose:
                for c in changes:
                    print(f"    {c}")

    print()
    mode = "Backfilled" if args.fix else "Would backfill"
    print(f"{mode} {total_descriptions} descriptions across {total_files_changed} files "
          f"(of {len(dirs)} component directories scanned).")
    if not args.fix:
        print("Run with --fix to apply.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
