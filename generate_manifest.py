#!/usr/bin/env python3
"""Sync manifest.json against the real component tree.

This is a MERGE, not a regenerate-from-scratch: manifest.json carries a lot
of hand-curated, non-derivable metadata per entry (icon, validation,
agent_hints, keywords, produces, consumes, component_type -- see any
existing entry) that nothing on disk can reconstruct. A prior version of
this script did a naive one-level-deep `assets/*` + `sensors/*` scan and
wrote a brand new manifest from scratch, which (a) missed every component
because the real layout is `assets/<category>/<name>/` plus a dozen other
top-level dirs (resources/, integrations/, sensors/, ...), and (b) would
have thrown away all the hand-curated fields for anything it did find.

Discovery mirrors tools/check_component_registry.py's walk exactly (same
EXCLUDE_TOP, same rglob("component.py") + sibling schema.json check) so the
two tools can never silently disagree on what counts as a component.

For each discovered component dir:
  - id already in manifest.json -> update only the fields this script can
    legitimately re-derive from source (name/category/description/tags from
    schema.json, dependencies.pip from requirements.txt, the *_url fields).
    Every other existing key on that entry (icon, validation, agent_hints,
    keywords, produces, consumes, component_type, dependencies.brew, etc.)
    is left byte-for-byte alone.
  - id not yet in manifest.json -> insert a new entry with just the
    derivable fields above. It will be missing icon/agent_hints/etc. (every
    existing entry has icon + agent_hints, no entry is considered "done"
    without them) -- printed as a loud warning so it gets manually enriched,
    the same way every component built this session had those fields added
    by hand rather than by this script.

Stale manifest entries (id on disk no longer exists) are reported, not
deleted -- same conservative, additive-first philosophy as
check_component_registry.py --fix.

Usage:
    python3 generate_manifest.py           # sync + write manifest.json
    python3 generate_manifest.py --check   # report only, exit 1 if out of sync
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
MANIFEST_PATH = REPO_ROOT / "manifest.json"
BASE_URL = "https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main"

# Must match tools/check_component_registry.py's EXCLUDE_TOP exactly -- the
# two scripts' notion of "what is a component" must never diverge.
EXCLUDE_TOP = {
    ".git", "node_modules", "dagster_community_components", ".claude",
    "docs", "tests", "tools", "cli", ".pytest_cache", "__pycache__",
}

DERIVABLE_KEYS = {
    "name", "category", "description", "tags", "path",
    "readme_url", "component_url", "schema_url", "example_url", "requirements_url",
}


def find_component_dirs() -> list[Path]:
    """Every directory (anywhere in the repo) holding both component.py and
    schema.json, excluding the same paths check_component_registry.py does."""
    dirs = []
    for p in REPO_ROOT.rglob("component.py"):
        rel = p.relative_to(REPO_ROOT)
        if rel.parts[0] in EXCLUDE_TOP:
            continue
        if "tests" in rel.parts or "__pycache__" in rel.parts:
            continue
        if (p.parent / "schema.json").exists():
            dirs.append(p.parent)
    return sorted(dirs, key=lambda d: d.name)


def build_derivable_fields(component_dir: Path) -> dict:
    rel = component_dir.relative_to(REPO_ROOT)
    component_id = component_dir.name
    rel_str = str(rel).replace("\\", "/")

    with open(component_dir / "schema.json") as f:
        schema = json.load(f)

    entry = {
        "id": component_id,
        "name": schema.get("name", component_id),
        "category": schema.get("category", "other"),
        "description": schema.get("description", ""),
        "path": rel_str,
        "tags": schema.get("tags", []),
        "readme_url": f"{BASE_URL}/{rel_str}/README.md",
        "component_url": f"{BASE_URL}/{rel_str}/component.py",
        "schema_url": f"{BASE_URL}/{rel_str}/schema.json",
        "example_url": f"{BASE_URL}/{rel_str}/example.yaml",
    }

    requirements_file = component_dir / "requirements.txt"
    pip_deps: list[str] = []
    if requirements_file.exists():
        entry["requirements_url"] = f"{BASE_URL}/{rel_str}/requirements.txt"
        with open(requirements_file) as f:
            pip_deps = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    entry["_pip_deps"] = pip_deps  # consumed by caller, not written as-is
    return entry


def sync(check_only: bool) -> int:
    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    existing_by_id = {c["id"]: c for c in manifest["components"]}
    on_disk_dirs = find_component_dirs()
    on_disk_ids = {d.name for d in on_disk_dirs}

    new_ids = []
    updated_ids = []
    merged_components = []

    for component_dir in on_disk_dirs:
        derived = build_derivable_fields(component_dir)
        pip_deps = derived.pop("_pip_deps")
        component_id = derived["id"]

        if component_id in existing_by_id:
            entry = dict(existing_by_id[component_id])  # preserve all extra keys
            changed = any(entry.get(k) != v for k, v in derived.items())
            entry.update(derived)
            deps = dict(entry.get("dependencies") or {})
            if deps.get("pip") != pip_deps:
                changed = True
            deps["pip"] = pip_deps
            entry["dependencies"] = deps
            if changed:
                updated_ids.append(component_id)
        else:
            entry = dict(derived)
            entry["version"] = "1.0.0"
            entry["author"] = "Dagster Community"
            entry["dependencies"] = {"pip": pip_deps}
            new_ids.append(component_id)

        merged_components.append(entry)

    stale_ids = sorted(set(existing_by_id) - on_disk_ids)

    merged_components.sort(key=lambda c: c["id"])
    manifest["components"] = merged_components
    manifest["total"] = len(merged_components)

    if check_only:
        print(f"On disk: {len(on_disk_ids)} components. Manifest: {len(existing_by_id)}.")
        if new_ids:
            print(f"Missing from manifest.json ({len(new_ids)}): {new_ids}")
        if stale_ids:
            print(f"Stale manifest.json entries, no longer on disk ({len(stale_ids)}): {stale_ids}")
        if updated_ids:
            print(f"Out of date (derivable fields changed) ({len(updated_ids)}): {updated_ids}")
        if not (new_ids or stale_ids or updated_ids):
            print("manifest.json is in sync.")
            return 0
        return 1

    manifest["last_updated"] = date.today().isoformat()
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"Synced manifest.json: {len(merged_components)} components.")
    if new_ids:
        print(f"  + {len(new_ids)} new (missing icon/agent_hints/validation -- enrich by hand): {new_ids}")
    if updated_ids:
        print(f"  ~ {len(updated_ids)} updated (name/category/description/tags/deps/urls refreshed): {updated_ids}")
    if stale_ids:
        print(f"  ! {len(stale_ids)} stale entries kept as-is, not deleted (no longer on disk): {stale_ids}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Report only, exit 1 if out of sync.")
    args = parser.parse_args()
    return sync(check_only=args.check)


if __name__ == "__main__":
    sys.exit(main())
