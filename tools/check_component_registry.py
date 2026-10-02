#!/usr/bin/env python3
"""Verify every `component.py`'s `*Component` class is registered in
`dagster_community_components/__init__.py`'s `_CLASS_PATHS` dict.

This is the actual discovery mechanism `dg list components` and the Dagster
UI's Components tab use (manifest.json is a separate, unrelated catalog) --
a class missing here is invisible to both despite being a real, tested
component. This bug has recurred twice in this repo (see git log for
`registry:` commits); this script exists so a third time is a CI failure,
not a silent gap discovered by hand weeks later.

Usage:
    python3 tools/check_component_registry.py          # report only, exit 1 if gaps found
    python3 tools/check_component_registry.py --fix     # also append missing entries

--fix is purely additive: it only ever inserts new "ClassName": "path" lines
into the existing alphabetically-sorted block. It never removes, reorders
beyond re-sorting, or touches any other line in the file (decorator
exports, bare aliases like `XmlParser`, constants, etc. are left alone).
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INIT_PATH = REPO_ROOT / "dagster_community_components" / "__init__.py"

# Directories that are never component source (registry itself, tests, tooling, docs).
EXCLUDE_TOP = {
    ".git", "node_modules", "dagster_community_components", ".claude",
    "docs", "tests", "tools", "cli", ".pytest_cache", "__pycache__",
}

CLASS_DEF_RE = re.compile(r"^class\s+(\w+Component)\(", re.MULTILINE)
ENTRY_RE = re.compile(r'"(?P<key>\w+)":\s*"(?P<val>[^"]+)",?')


def find_component_files() -> list[Path]:
    files = []
    for p in REPO_ROOT.rglob("component.py"):
        rel = p.relative_to(REPO_ROOT)
        if rel.parts[0] in EXCLUDE_TOP:
            continue
        if "tests" in rel.parts or "__pycache__" in rel.parts:
            continue
        files.append(rel)
    return sorted(files)


def existing_registry_paths(init_src: str) -> set[str]:
    return {m.group("val") for m in ENTRY_RE.finditer(init_src)}


def find_gaps() -> list[tuple[str, str]]:
    """Returns list of (class_name, relative_path) not yet in the registry."""
    init_src = INIT_PATH.read_text()
    registered_paths = existing_registry_paths(init_src)

    gaps = []
    for rel in find_component_files():
        relpath = str(rel).replace("\\", "/")
        if relpath in registered_paths:
            continue
        src = (REPO_ROOT / rel).read_text()
        for m in CLASS_DEF_RE.finditer(src):
            gaps.append((m.group(1), relpath))
    return gaps


def apply_fix(gaps: list[tuple[str, str]]) -> None:
    src = INIT_PATH.read_text()
    start_marker = "_CLASS_PATHS: dict[str, str] = {"
    end_marker = "\n}"
    start_idx = src.index(start_marker) + len(start_marker)
    end_idx = src.index(end_marker, start_idx)
    body = src[start_idx:end_idx]

    existing_entries: dict[str, str] = {}
    for m in ENTRY_RE.finditer(body):
        existing_entries[m.group("key")] = m.group("val")

    for cls, path in gaps:
        existing_entries[cls] = path

    first_entry_match = ENTRY_RE.search(body)
    leading = body[: first_entry_match.start()] if first_entry_match else ""
    # Collapse to just the real comment lines -- drops any blank/whitespace-only
    # noise so repeated --fix runs are idempotent regardless of prior drift.
    comment_lines = [l for l in leading.split("\n") if l.strip().startswith("#")]

    sorted_keys = sorted(existing_entries.keys())
    new_lines = [f'    "{k}": "{existing_entries[k]}",' for k in sorted_keys]
    new_body = (
        "\n"
        + ("\n".join(comment_lines) + "\n\n" if comment_lines else "")
        + "\n".join(new_lines)
        + "\n"
    )

    new_src = src[: src.index(start_marker) + len(start_marker)] + new_body + src[end_idx:]
    INIT_PATH.write_text(new_src)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fix", action="store_true", help="Append missing entries (additive only).")
    args = parser.parse_args()

    gaps = find_gaps()
    if not gaps:
        print(f"OK -- every *Component class across {len(find_component_files())} "
              f"component.py files is registered.")
        return 0

    print(f"Found {len(gaps)} *Component class(es) missing from "
          f"dagster_community_components/__init__.py:")
    for cls, path in sorted(gaps):
        print(f"  {cls:50s} {path}")

    if args.fix:
        apply_fix(gaps)
        print(f"\n--fix: appended {len(gaps)} entries to {INIT_PATH}.")
        return 0

    print("\nRun with --fix to append these entries (additive only -- "
          "nothing else in the file is touched).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
