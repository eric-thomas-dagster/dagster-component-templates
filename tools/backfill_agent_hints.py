#!/usr/bin/env python3
"""Backfill agent_hints on manifest entries that lack them.

Only touches unhinted entries (`agent_hints` absent or falsy). Preserves any
existing hint blocks unchanged. Emits a minimum-viable hint set per category
so LLM planners can pick components without scraping every README.

Fields emitted:
  input_type   / output_type — from category template (dataflow categories only)
  outputs                    — short one-line description derived from the
                               manifest description (first sentence, capped)
  requires_pip               — read from the component's requirements.txt

Usage:
  python3 tools/backfill_agent_hints.py                 # write manifest
  python3 tools/backfill_agent_hints.py --dry-run       # preview N samples
  python3 tools/backfill_agent_hints.py --sample 20     # preview 20 rows
"""
import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "manifest.json"

# Per-category input/output type templates. Categories not listed here get
# no input_type/output_type — hints still include `outputs` + `requires_pip`.
DATAFLOW_TEMPLATES = {
    "ai":             {"input_type": "pd.DataFrame", "output_type": "pd.DataFrame"},
    "analytics":      {"input_type": "pd.DataFrame", "output_type": "pd.DataFrame"},
    "transformation": {"input_type": "pd.DataFrame", "output_type": "pd.DataFrame"},
    "ingestion":      {                              "output_type": "pd.DataFrame"},
    "source":         {                              "output_type": "pd.DataFrame"},
    "sink":           {"input_type": "pd.DataFrame", "output_type": "None"},
    "check":          {"input_type": "pd.DataFrame", "output_type": "AssetCheckResult"},
    "external":       {                              "output_type": "AssetSpec"},
    "observation":    {                              "output_type": "AssetObservation"},
    "io_manager":     {"input_type": "Any",          "output_type": "Any"},
    "data_warehouse": {"input_type": "pd.DataFrame", "output_type": "pd.DataFrame"},
    "dbt":            {                              "output_type": "AssetSpec"},
}


def read_requirements(component_dir: Path) -> list[str]:
    reqs_file = component_dir / "requirements.txt"
    if not reqs_file.exists():
        return []
    out = []
    for line in reqs_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def first_sentence(text: str, max_chars: int = 200) -> str:
    if not text:
        return ""
    # Sentence break: end of first "." not followed by another letter (crude).
    m = re.match(r"^(.+?[.!?])(?:\s|$)", text.strip())
    s = m.group(1) if m else text.strip()
    if len(s) > max_chars:
        s = s[:max_chars].rsplit(" ", 1)[0] + "…"
    return s


def build_hints(entry: dict) -> dict:
    hints: dict = {}
    category = entry.get("category", "")
    hints.update(DATAFLOW_TEMPLATES.get(category, {}))

    outputs_text = first_sentence(entry.get("description", ""))
    if outputs_text:
        hints["outputs"] = outputs_text

    reqs = read_requirements(ROOT / entry["path"])
    if reqs:
        hints["requires_pip"] = reqs

    return hints


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sample", type=int, default=0,
                    help="Show N sample rows before writing (0 = just apply).")
    args = ap.parse_args()

    m = json.loads(MANIFEST.read_text())
    comps = m["components"]

    unhinted = [c for c in comps if not c.get("agent_hints")]
    updated = 0
    samples = []
    for c in unhinted:
        hints = build_hints(c)
        if not hints:
            continue
        if args.sample and len(samples) < args.sample:
            samples.append({"id": c["id"], "category": c.get("category"), "hints": hints})
        c["agent_hints"] = hints
        updated += 1

    if samples:
        for s in samples:
            print(f'--- {s["id"]}  ({s["category"]}) ---')
            print(json.dumps(s["hints"], indent=2))
            print()

    print(f"unhinted before: {len(unhinted)}")
    print(f"backfilled:      {updated}")
    print(f"skipped (empty hints): {len(unhinted) - updated}")

    if args.dry_run:
        print("(dry-run — manifest not written)")
        return

    MANIFEST.write_text(json.dumps(m, indent=2))
    remaining = sum(1 for c in comps if not c.get("agent_hints"))
    print(f"unhinted after write: {remaining}")


if __name__ == "__main__":
    main()
