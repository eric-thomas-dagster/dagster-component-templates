"""Fail if any function decorated with @dg.sensor / @sensor yields
AssetMaterialization directly.

Sensor evaluations must return SensorResult / RunRequest / SkipReason.
`yield AssetMaterialization(...)` inside a sensor body is silently
dropped by Dagster (or errors, version-dependent). Nasty failure mode
because the sensor still appears healthy in the UI — it just never
records anything.

Wraps that observation as a CI check. Exits non-zero when hits are
found; prints file:line for each so devs can fix quickly.

Usage:
    python tools/lint_sensor_pattern.py                   # scan whole repo
    python tools/lint_sensor_pattern.py integrations/     # scan a subtree
"""
from __future__ import annotations

import ast
import pathlib
import sys


def _is_sensor_decorator(dec: ast.expr) -> bool:
    """Match @sensor / @dg.sensor / @dagster.sensor (with or without call)."""
    # `@sensor` or `@dg.sensor` — dec is Name / Attribute
    # `@sensor(...)` or `@dg.sensor(...)` — dec is Call, unwrap .func
    target = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(target, ast.Name):
        return target.id == "sensor"
    if isinstance(target, ast.Attribute):
        return target.attr == "sensor"
    return False


def _is_asset_materialization(node: ast.expr) -> bool:
    """Match AssetMaterialization / dg.AssetMaterialization / dagster.AssetMaterialization."""
    if isinstance(node, ast.Name):
        return node.id == "AssetMaterialization"
    if isinstance(node, ast.Attribute):
        return node.attr == "AssetMaterialization"
    return False


def _yields_asset_materialization(fn: ast.FunctionDef) -> list[int]:
    """Return line numbers of `yield [dg.]AssetMaterialization(...)` inside fn."""
    hits: list[int] = []
    for node in ast.walk(fn):
        if isinstance(node, ast.Yield) and isinstance(node.value, ast.Call):
            if _is_asset_materialization(node.value.func):
                hits.append(node.lineno)
    return hits


def _scan_file(path: pathlib.Path) -> list[tuple[pathlib.Path, str, int]]:
    """Return (path, sensor_name, line) tuples for each offending yield."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []
    hits: list[tuple[pathlib.Path, str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and any(
            _is_sensor_decorator(d) for d in node.decorator_list
        ):
            for line in _yields_asset_materialization(node):
                hits.append((path, node.name, line))
    return hits


def main(argv: list[str]) -> int:
    roots = [pathlib.Path(p) for p in (argv[1:] or ["."])]
    all_hits: list[tuple[pathlib.Path, str, int]] = []
    for root in roots:
        if root.is_file() and root.suffix == ".py":
            all_hits.extend(_scan_file(root))
            continue
        for path in root.rglob("*.py"):
            if any(part in {".venv", "venv", "__pycache__", "node_modules"} for part in path.parts):
                continue
            all_hits.extend(_scan_file(path))

    if not all_hits:
        print("lint_sensor_pattern: OK — no sensors yield AssetMaterialization directly.")
        return 0

    print(f"lint_sensor_pattern: FAIL — found {len(all_hits)} offending yield(s):", file=sys.stderr)
    for path, sensor_name, line in all_hits:
        print(f"  {path}:{line}  sensor `{sensor_name}` yields AssetMaterialization", file=sys.stderr)
    print(
        "\nSensor evaluations must return `SensorResult(asset_events=[...])`\n"
        "instead of yielding AssetMaterialization directly. See\n"
        "integrations/azure_data_factory/component.py for the pattern.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
