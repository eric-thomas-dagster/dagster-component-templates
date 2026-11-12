#!/usr/bin/env python3
"""Regenerate assets/ai/agentic_pipeline/ops_schema.json from the real, live `_STEP_MODELS` Pydantic models in
assets/ai/agentic_pipeline/component.py -- one real JSON Schema per op (fields, types, defaults, descriptions,
required-ness), not a hand-maintained summary. This is the file a UI (or anything else that can't spin up the
authoring MCP server) reads to build a per-op form; `describe_op` on the authoring server returns the identical
schema live, generated the same way, so the two can never drift from each other -- only from the real models, which
`tests/test_step_schema_drift.py` (model vs. README) and this script's own drift test (file vs. live models) both
guard against.

Usage:
    python3 tools/gen_agentic_pipeline_ops_schema.py          # check only, exit 1 if the file is stale
    python3 tools/gen_agentic_pipeline_ops_schema.py --fix    # regenerate the file in place
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
COMPONENT_PY = REPO_ROOT / "assets" / "ai" / "agentic_pipeline" / "component.py"
OUT_PATH = REPO_ROOT / "assets" / "ai" / "agentic_pipeline" / "ops_schema.json"


def _load_component_module():
    spec = importlib.util.spec_from_file_location("agentic_pipeline_for_ops_schema_gen", COMPONENT_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_ops_schema(mod) -> dict:
    out = {}
    for op in sorted(mod._STEP_MODELS):
        model = mod._STEP_MODELS[op]
        summary = (model.__doc__ or "").strip().splitlines()[0] if model.__doc__ else ""
        out[op] = {"summary": summary, "schema": model.model_json_schema()}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fix", action="store_true")
    args = ap.parse_args()

    mod = _load_component_module()
    fresh = build_ops_schema(mod)
    fresh_text = json.dumps(fresh, indent=2, sort_keys=True) + "\n"

    current_text = OUT_PATH.read_text() if OUT_PATH.exists() else None
    if current_text == fresh_text:
        print(f"{OUT_PATH} is up to date ({len(fresh)} ops).")
        return 0

    if args.fix:
        OUT_PATH.write_text(fresh_text)
        print(f"Wrote {OUT_PATH} ({len(fresh)} ops).")
        return 0

    print(f"{OUT_PATH} is STALE vs. the live _STEP_MODELS -- run with --fix to regenerate.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
