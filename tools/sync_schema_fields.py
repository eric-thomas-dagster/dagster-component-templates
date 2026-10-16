#!/usr/bin/env python3
"""Add fields declared in component.py but missing from schema.json `attributes`.

The Designer form is built from schema.json, so a field that only exists in
component.py can never be set from the UI (and the Designer's AI can only guess
at it). Additive-only:
  - never touches an existing attribute, and never removes one;
  - inserts text in place (no json round-trip -- 234 schemas don't round-trip);
  - for fields shared by many components (retry_policy_*, freshness_*, partition_*...)
    copies the majority entry already used elsewhere, so wording/widgets match;
  - otherwise derives the entry from the Python Field() (primitive / list / dict /
    Literal types only; anything fancier is skipped and reported).

Usage:
    python3 tools/sync_schema_fields.py            # report
    python3 tools/sync_schema_fields.py --fix      # apply
    python3 tools/sync_schema_fields.py --skip-names defs_state translation
"""
from __future__ import annotations

import argparse
import ast
import collections
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from regen_readme_fields import parse_fields  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = (".venv", "site-packages", "dist")
# Framework-object fields that don't belong on a form.
NEVER = {"defs_state", "translation", "asset_overrides", "wraps", "workspace", "polling_sensor", "on_materialize"}
CANONICAL_MIN = 10  # field must already appear in this many schemas to be "shared"
# Only genuinely boilerplate fields are copied from the majority entry; anything else
# (e.g. `model`, whose default differs per component) is derived from its own Field().
SHARED = {
    "freshness_max_lag_minutes", "freshness_cron", "retry_policy_max_retries",
    "retry_policy_delay_seconds", "retry_policy_backoff", "partition_type", "partition_values",
    "partition_start", "dynamic_partition_name", "partition_dimensions", "emit_materialization",
    "owners", "kinds", "asset_tags", "deps",
}

_PRIMS = {"str": "string", "int": "integer", "float": "number", "bool": "boolean"}


def _schemas():
    for p in sorted(ROOT.glob("**/schema.json")):
        if any(x in p.relative_to(ROOT).parts for x in SKIP_DIRS):
            continue
        yield p


def canonical_entries() -> dict[str, dict]:
    seen: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for p in _schemas():
        for k, v in (json.loads(p.read_text()).get("attributes") or {}).items():
            if isinstance(v, dict):
                seen[k][json.dumps(v, sort_keys=True)] += 1
    return {
        k: json.loads(c.most_common(1)[0][0])
        for k, c in seen.items()
        if sum(c.values()) >= CANONICAL_MIN and k in SHARED
    }


def component_field_names(cp: Path) -> set[str]:
    """Fields declared directly on the Component class (not nested models)."""
    out: set[str] = set()
    for n in ast.parse(cp.read_text()).body:
        if isinstance(n, ast.ClassDef) and any("Component" in ast.unparse(b) for b in n.bases):
            out |= {x.target.id for x in n.body if isinstance(x, ast.AnnAssign) and isinstance(x.target, ast.Name)}
    return out


def derive_entry(f: dict) -> dict | None:
    t = re.sub(r"\s+", "", f["type_str"] or "")
    optional = t.startswith("Optional[")
    if optional:
        t = t[len("Optional["):-1]
    entry: dict = {}
    if t in _PRIMS:
        entry["type"] = _PRIMS[t]
    elif t.startswith("Literal["):
        try:
            vals = list(ast.literal_eval("[" + t[len("Literal["):-1] + "]"))
        except Exception:
            return None
        entry.update(type="string", enum=vals)
    elif t in ("List[str]", "list[str]"):
        entry.update(type="array", items={"type": "string"})
    elif t in ("Union[str,List[str]]",):
        entry["type"] = "string"
    elif t.startswith(("Dict[", "dict[")) or t == "dict":
        entry["type"] = "object"
    else:
        return None
    entry["label"] = f["name"].replace("_", " ").title()
    entry["description"] = f["description"] or entry["label"]
    entry["required"] = bool(f["required"]) and not optional
    if f["default_str"] not in (None, ""):
        try:
            entry["default"] = ast.literal_eval(f["default_str"])
        except Exception:
            pass
    elif optional or not f["required"]:
        entry["default"] = None
    return entry


def _attr_span(text: str) -> tuple[int, int] | None:
    """Offsets of '{' and its matching '}' for the top-level "attributes" object."""
    m = re.search(r'"attributes"\s*:\s*\{', text)
    if not m:
        return None
    start = m.end() - 1
    depth, i, in_str = 0, start, False
    while i < len(text):
        c = text[i]
        if in_str:
            if c == "\\":
                i += 1
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return start, i
        i += 1
    return None


def insert_entries(text: str, entries: dict[str, dict]) -> str:
    span = _attr_span(text)
    assert span, "no attributes object"
    start, end = span
    body = text[start + 1:end]
    km = re.search(r"\n([ \t]+)\"", body)
    key_indent = km.group(1) if km else "    "
    unit = "\t" if "\t" in key_indent else " " * (len(key_indent) // 2 if len(key_indent) >= 4 else len(key_indent))
    closing_indent = text[text.rfind("\n", 0, end) + 1:end]
    chunks = []
    for name, entry in entries.items():
        raw = json.dumps(entry, indent=len(unit) if unit != "\t" else 2, ensure_ascii=False)
        raw = raw.replace("\n", "\n" + key_indent)
        chunks.append(f'{key_indent}{json.dumps(name)}: {raw}')
    stripped = body.rstrip()
    sep = "," if stripped.strip() else ""
    return text[:start + 1] + stripped + sep + "\n" + ",\n".join(chunks) + "\n" + closing_indent + text[end:]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true")
    ap.add_argument("--skip-names", nargs="*", default=[])
    args = ap.parse_args()
    canon = canonical_entries()
    skip = NEVER | set(args.skip_names)
    changed, skipped = 0, collections.Counter()
    added = collections.Counter()
    for sp in _schemas():
        cp = sp.parent / "component.py"
        if not cp.exists():
            continue
        data = json.loads(sp.read_text())
        have = set(data.get("attributes") or {})
        if not have:
            continue
        new: dict[str, dict] = {}
        own = component_field_names(cp)
        for f in parse_fields(cp):
            n = f["name"]
            if n not in own:
                continue
            if n in have or n in skip:
                continue
            entry = canon.get(n) or derive_entry(f)
            if entry is None:
                skipped[n] += 1
                continue
            new[n] = entry
        if not new:
            continue
        out = insert_entries(sp.read_text(), new)
        json.loads(out)  # must still be valid
        added.update(new.keys())
        changed += 1
        if args.fix:
            sp.write_text(out)
    print(f"{'Fixed' if args.fix else 'Would fix'} {changed} schema.json files; fields added:")
    for k, v in added.most_common(25):
        print(f"  {v:4d} {k}")
    if skipped:
        print("skipped (unsupported type):", dict(skipped.most_common(15)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
