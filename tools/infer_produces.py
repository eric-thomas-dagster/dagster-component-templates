#!/usr/bin/env python3
"""Infer the `produces` field for every component in manifest.json.

AST-based inspection of each component's `component.py`. No code execution
(safe, no optional-SDK setup needed, works even for components whose
runtime deps aren't installed).

Signals, in order of strength:

  1. Explicit decorator use inside build_defs / definitions:
       @dg.asset / @asset          → asset
       @dg.multi_asset             → multi_asset
       @dg.asset_check / @asset_check → asset_check
       @dg.sensor / @sensor        → sensor
       @dg.schedule / @schedule    → schedule
       @dg.job / @job              → job
  2. Definitions() kwargs — the return-shape from build_defs:
       assets=[...]         → asset (or multi_asset if list has multi-key items)
       asset_checks=[...]   → asset_check
       jobs=[...]           → job
       schedules=[...]      → schedule
       sensors=[...]        → sensor
       resources={...}      → resource
  3. Base class inference:
       StateBackedComponent           → multi_asset + stateful=true
       DbtProjectComponent (subclass) → multi_asset + asset_check
  4. Manifest `path` prefix (fallback):
       sensors/**       → sensor
       io_managers/**   → io_manager
       resources/**     → resource
       jobs/**          → job
       schedules/**     → schedule
       observations/**  → sensor (observation sensors)
       asset_checks/**  → asset_check
       external_assets/** → asset  (external declarations)
  5. Class-name hint: name ends in "Sensor" → sensor, "Resource" → resource,
     "IOManager" → io_manager, "Workspace" → multi_asset, etc.

Confidence:
  1.00 — signal (1) or (2) present, no conflict
  0.80 — signal (3) or (2) present
  0.60 — signal (4) only
  0.40 — signal (5) only
  0.20 — nothing conclusive; leaves produces empty for manual review

Output:
  infer_produces_report.json — per-component inference:
    { "id": ..., "path": ..., "produces": [...], "consumes": [...],
      "stateful": bool | null, "confidence": float, "evidence": [...] }

Usage:
  python tools/infer_produces.py                    # infer all
  python tools/infer_produces.py --ids foo,bar      # subset
  python tools/infer_produces.py --apply            # write inferred values back into manifest.json
  python tools/infer_produces.py --min-confidence 0.7 --apply   # only apply high-confidence

Exit code 0 on success.
"""
from __future__ import annotations

import argparse
import ast
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
MANIFEST = REPO_ROOT / "manifest.json"
REPORT_PATH = REPO_ROOT / "infer_produces_report.json"


# ─── Enum ──────────────────────────────────────────────────────────────────
PRIMITIVES = {
    "asset", "multi_asset", "asset_check",
    "job", "schedule", "sensor",
    "resource", "io_manager", "partitions_def", "other",
}


# ─── Signal extraction ─────────────────────────────────────────────────────
def _decorator_name(dec: ast.expr) -> Optional[str]:
    """Extract the trailing identifier from a decorator expr.

    Returns e.g. 'asset' for @dg.asset, @asset, @dagster.asset, @dg.asset(...).
    """
    node = dec
    if isinstance(node, ast.Call):
        node = node.func
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


_DECORATOR_TO_PRIMITIVE = {
    "asset": "asset",
    "assets": "asset",
    "multi_asset": "multi_asset",
    "graph_multi_asset": "multi_asset",
    "asset_check": "asset_check",
    "multi_asset_check": "asset_check",
    "sensor": "sensor",
    "asset_sensor": "sensor",
    "multi_asset_sensor": "sensor",
    "run_status_sensor": "sensor",
    "run_failure_sensor": "sensor",
    "schedule": "schedule",
    "job": "job",
    "op": None,   # ops alone don't count — must be in a job
    "resource": "resource",
    "io_manager": "io_manager",
    "static_partitioned_config": "partitions_def",
    "dynamic_partitioned_config": "partitions_def",
    "dbt_assets": "multi_asset",       # official dagster-dbt
    "sling_assets": "multi_asset",
    "airbyte_assets": "multi_asset",
    "fivetran_assets": "multi_asset",
    "observable_source_asset": "asset",
    "external_assets_from_specs": "asset",
}


_DEFS_KWARG_TO_PRIMITIVE = {
    "assets": "asset",           # will upgrade to multi_asset by inspection
    "asset_checks": "asset_check",
    "jobs": "job",
    "schedules": "schedule",
    "sensors": "sensor",
    "resources": "resource",
}


# ─── AST visitor ───────────────────────────────────────────────────────────
class ComponentInspector(ast.NodeVisitor):
    def __init__(self):
        self.primitives: set[str] = set()
        self.evidence: list[str] = []
        self.base_classes: set[str] = set()
        self.has_definitions_call: bool = False
        self.definitions_kwargs: set[str] = set()
        self.uses_state_config: bool = False   # signal for stateful

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for base in node.bases:
            name = _decorator_name(base) or ""
            if name:
                self.base_classes.add(name)
        # Recurse into class body — decorators inside build_defs / helpers
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        for dec in node.decorator_list:
            name = _decorator_name(dec)
            if name is None:
                continue
            prim = _DECORATOR_TO_PRIMITIVE.get(name)
            if prim:
                self.primitives.add(prim)
                self.evidence.append(f"decorator @{name} → {prim}")
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        # Same decorator scan as sync functions.
        for dec in node.decorator_list:
            name = _decorator_name(dec)
            if name is None:
                continue
            prim = _DECORATOR_TO_PRIMITIVE.get(name)
            if prim:
                self.primitives.add(prim)
                self.evidence.append(f"decorator @{name} → {prim}")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # Dig into Definitions(...) construction — look at kwargs
        callee = node.func
        callee_name = ""
        if isinstance(callee, ast.Attribute):
            callee_name = callee.attr
        elif isinstance(callee, ast.Name):
            callee_name = callee.id

        if callee_name == "Definitions":
            self.has_definitions_call = True
            for kw in node.keywords:
                if kw.arg in _DEFS_KWARG_TO_PRIMITIVE:
                    prim = _DEFS_KWARG_TO_PRIMITIVE[kw.arg]
                    self.primitives.add(prim)
                    self.evidence.append(f"Definitions({kw.arg}=...) → {prim}")
                    self.definitions_kwargs.add(kw.arg)

        # AssetSpec construction — likely part of multi_asset emission
        if callee_name == "AssetSpec":
            # weak signal — leave, but useful cross-check
            self.evidence.append("AssetSpec(...) constructed")

        # multi_asset factory calls (not decorators)
        if callee_name in ("multi_asset", "graph_multi_asset"):
            self.primitives.add("multi_asset")
            self.evidence.append(f"{callee_name}(...) call → multi_asset")

        # asset factory calls
        if callee_name == "asset":
            self.primitives.add("asset")
            self.evidence.append("asset(...) factory call → asset")

        # observable_source_asset
        if callee_name in ("observable_source_asset", "external_asset_from_spec", "external_assets_from_specs"):
            self.primitives.add("asset")
            self.evidence.append(f"{callee_name}(...) → asset")

        # define_asset_job → job
        if callee_name in ("define_asset_job", "job"):
            self.primitives.add("job")
            self.evidence.append(f"{callee_name}(...) → job")

        # ScheduleDefinition / RunScheduleDefinition
        if callee_name in ("ScheduleDefinition", "RunScheduleDefinition"):
            self.primitives.add("schedule")
            self.evidence.append(f"{callee_name}(...) → schedule")

        # SensorDefinition
        if callee_name == "SensorDefinition":
            self.primitives.add("sensor")
            self.evidence.append("SensorDefinition(...) → sensor")

        # signals for stateful
        if callee_name in ("StateBackedDefsStateConfig", "ResolvedDefsStateConfig"):
            self.uses_state_config = True

        self.generic_visit(node)


# ─── Inference orchestration ───────────────────────────────────────────────
def _path_prefix_signal(path: str) -> list[str]:
    """Weak fallback signal — the manifest `path` prefix."""
    prefix = (path.split("/", 1) + [""])[0]
    mapping = {
        "sensors": ["sensor"],
        "io_managers": ["io_manager"],
        "resources": ["resource"],
        "jobs": ["job"],
        "schedules": ["schedule"],
        "observations": ["sensor"],   # observation-emitting sensors
        "asset_checks": ["asset_check"],
        "external_assets": ["asset"],
        "sinks": ["asset"],           # sinks are assets that side-effect
        "compute_log_managers": ["other"],
    }
    return mapping.get(prefix, [])


def _classname_hint(name: str) -> list[str]:
    """Weak fallback signal — the class Name suffix."""
    name = name or ""
    if name.endswith(("Sensor", "SensorComponent")):
        return ["sensor"]
    if name.endswith(("Resource", "ResourceComponent")):
        return ["resource"]
    if name.endswith(("IOManager", "IOManagerComponent", "IoManager", "IoManagerComponent")):
        return ["io_manager"]
    if name.endswith(("Workspace", "WorkspaceComponent")):
        return ["multi_asset"]
    if "AssetCheck" in name or name.endswith(("Check", "CheckComponent")):
        return ["asset_check"]
    if name.endswith(("Schedule", "ScheduleComponent")):
        return ["schedule"]
    if name.endswith(("Job", "JobComponent")):
        return ["job"]
    return []


def _base_class_signal(base_classes: set[str]) -> tuple[list[str], bool]:
    """(primitives, stateful) from base-class hints."""
    if "StateBackedComponent" in base_classes:
        return ["multi_asset"], True
    # Match DbtProjectComponent AND aliased/private variants (_DbtProjectComponent).
    if any("DbtProject" in b for b in base_classes):
        return ["multi_asset", "asset_check"], False
    return [], False


# Hardcoded overrides for components whose build_defs body is a call to an
# external factory function (dagster_airflow / dagster_airlift). AST can't
# infer what those factories return without executing them.
_KNOWN_OVERRIDES: dict[str, dict] = {
    "airflow_dag_proxy": {
        "produces": ["multi_asset"],
        "evidence": "override: dagster_airflow.make_dagster_definitions_from_airflow_dag returns multi-asset Definitions",
    },
    "airlift_dag_assets": {
        "produces": ["multi_asset"],
        "evidence": "override: dagster_airlift.build_defs_from_airflow_instance returns multi-asset Definitions",
    },
}


def infer_for_entry(entry: dict) -> dict:
    """Return a result dict for a single manifest entry."""
    id_ = entry.get("id", "")
    path = entry.get("path", "")
    name = entry.get("name", "")

    result: dict[str, Any] = {
        "id": id_,
        "path": path,
        "name": name,
        "produces": [],
        "stateful": None,
        "confidence": 0.0,
        "evidence": [],
    }

    if id_ in _KNOWN_OVERRIDES:
        ov = _KNOWN_OVERRIDES[id_]
        result["produces"] = ov["produces"]
        result["confidence"] = 1.0
        result["evidence"].append(ov["evidence"])
        return result

    component_py = REPO_ROOT / path / "component.py"
    if not component_py.exists():
        result["evidence"].append(f"MISSING: {component_py.relative_to(REPO_ROOT)}")
        # Fall back to path + name only
        signal = _path_prefix_signal(path) + _classname_hint(name)
        if signal:
            result["produces"] = sorted(set(signal))
            result["confidence"] = 0.4
            result["evidence"].append(f"fallback (no component.py): path/name → {signal}")
        return result

    try:
        source = component_py.read_text()
        tree = ast.parse(source)
    except (OSError, SyntaxError) as e:
        result["evidence"].append(f"PARSE_ERROR: {type(e).__name__}: {e}")
        return result

    ins = ComponentInspector()
    ins.visit(tree)

    # Base-class inference
    base_prims, base_stateful = _base_class_signal(ins.base_classes)
    if base_prims:
        ins.primitives.update(base_prims)
        ins.evidence.append(f"base class → {base_prims}")

    # StateBackedComponent → stateful=true
    if base_stateful:
        result["stateful"] = True
    elif ins.uses_state_config:
        result["stateful"] = True

    # Name-based fanout hint — a *Workspace or *Ingestion class that reads as
    # a for-loop of @asset is conceptually multi_asset (fanout from one YAML).
    name_signal = _classname_hint(name)
    if "multi_asset" in name_signal:
        ins.primitives.add("multi_asset")
        ins.evidence.append(f"classname suffix → multi_asset (fanout shape)")

    if ins.primitives:
        # Strong signal — decorator or Definitions call
        # Normalization: multi_asset supersedes asset (fanout implies asset shape).
        if "multi_asset" in ins.primitives:
            ins.primitives.discard("asset")
        result["produces"] = sorted(ins.primitives)
        result["confidence"] = 1.0 if ins.has_definitions_call or any(
            e.startswith("decorator ") for e in ins.evidence
        ) else 0.8
        result["evidence"].extend(ins.evidence)
        return result

    # Weak signal — path prefix
    path_signal = _path_prefix_signal(path)
    if path_signal:
        result["produces"] = sorted(set(path_signal))
        result["confidence"] = 0.6
        result["evidence"].append(f"path prefix → {path_signal}")
        return result

    # Weakest signal — classname suffix
    name_signal = _classname_hint(name)
    if name_signal:
        result["produces"] = sorted(set(name_signal))
        result["confidence"] = 0.4
        result["evidence"].append(f"classname → {name_signal}")
        return result

    result["confidence"] = 0.2
    result["evidence"].append("no signals found")
    return result


# ─── Bulk driver ───────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Infer `produces` for every manifest entry")
    parser.add_argument("--ids", type=str, default=None, help="Comma-separated subset")
    parser.add_argument("--apply", action="store_true",
                        help="Write inferred produces/stateful back into manifest.json")
    parser.add_argument("--min-confidence", type=float, default=0.7,
                        help="Minimum confidence for --apply (default 0.7)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite existing produces field (default: skip if set)")
    args = parser.parse_args()

    manifest = json.loads(MANIFEST.read_text())
    components = manifest.get("components", [])
    if args.ids:
        ids = set(args.ids.split(","))
        components = [c for c in components if c.get("id") in ids]

    results: list[dict] = []
    for entry in components:
        results.append(infer_for_entry(entry))

    # Report
    REPORT_PATH.write_text(json.dumps(results, indent=2, sort_keys=False))
    print(f"Wrote {REPORT_PATH.name} ({len(results)} entries)")

    # Confidence histogram
    hist = Counter()
    for r in results:
        hist[f"{r['confidence']:.2f}"] += 1
    print("\nConfidence distribution:")
    for score, count in sorted(hist.items()):
        print(f"  {score}: {count}")

    # Produces histogram
    prod_hist = Counter()
    for r in results:
        for p in r["produces"]:
            prod_hist[p] += 1
    print("\nProduces primitive counts:")
    for prim, count in sorted(prod_hist.items(), key=lambda x: -x[1]):
        print(f"  {prim}: {count}")

    # Low-confidence surface
    low = [r for r in results if r["confidence"] < args.min_confidence]
    print(f"\nLow-confidence (< {args.min_confidence}): {len(low)}")
    for r in low[:20]:
        print(f"  [{r['confidence']:.2f}] {r['id']:<40} → {r['produces']!r}  ({r['path']})")

    if args.apply:
        by_id = {c.get("id"): c for c in manifest["components"]}
        applied = 0
        skipped = 0
        for r in results:
            if r["confidence"] < args.min_confidence:
                skipped += 1
                continue
            c = by_id.get(r["id"])
            if c is None:
                continue
            if not args.overwrite and "produces" in c:
                skipped += 1
                continue
            if r["produces"]:
                c["produces"] = r["produces"]
                applied += 1
            if r["stateful"] is not None:
                c["stateful"] = r["stateful"]
        MANIFEST.write_text(json.dumps(manifest, indent=2))
        print(f"\nApplied to manifest.json — {applied} updated, {skipped} skipped (below threshold or already set)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
