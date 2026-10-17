"""Settings a component declares must not be silent no-ops.

- Every @asset built in build_defs applies `retry_policy` when the component declares retry_policy_* settings.
- Every `retry_policy=` / `freshness_policy=` variable handed to an @asset is assigned in a scope that reaches it
  (a policy built inside one `if` branch but referenced from another raises UnboundLocalError only when that branch runs).
- transforms/router applies freshness, retry and partitions to its branches in both modes (it used to drop all three in
  exclusive mode).
"""
import ast
import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
KW = ("retry_policy", "freshness_policy")


def _component_files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts"):
            continue
        yield p


def _is_asset(d):
    f = getattr(d, "func", None)
    return isinstance(d, ast.Call) and (
        (isinstance(f, ast.Name) and f.id == "asset")
        or (isinstance(f, ast.Attribute) and f.attr == "asset" and getattr(f.value, "id", "") in ("dg", "dagster"))
    )


def _build_defs_asset_decorators(tree):
    for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
        for fn in (n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "build_defs"):
            for inner in ast.walk(fn):
                if isinstance(inner, ast.FunctionDef):
                    for d in inner.decorator_list:
                        if _is_asset(d):
                            yield d


def test_components_that_declare_retry_settings_apply_them_to_every_asset():
    unapplied = []
    for p in _component_files():
        text = p.read_text()
        if "retry_policy_max_retries" not in text:
            continue
        for call in _build_defs_asset_decorators(ast.parse(text)):
            if not any(k.arg in ("retry_policy", None) for k in call.keywords):
                unapplied.append(f"{p.relative_to(ROOT)}:{call.lineno}")
    assert not unapplied, unapplied[:15]


def _unreachable_policy_variables(text):
    tree = ast.parse(text)
    parent = {c: n for n in ast.walk(tree) for c in ast.iter_child_nodes(n)}
    problems = []
    for fn in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)):
        for d in fn.decorator_list:
            if not _is_asset(d):
                continue
            for k in d.keywords:
                if k.arg not in KW or not isinstance(k.value, ast.Name):
                    continue
                name = k.value.id
                chain, n = [], fn
                while n in parent:
                    chain.append(n)
                    n = parent[n]
                reaches = False
                for a in ast.walk(tree):
                    if not isinstance(a, (ast.Assign, ast.AnnAssign)) or a.lineno >= fn.lineno:
                        continue
                    targets = a.targets if isinstance(a, ast.Assign) else [a.target]
                    if not any(isinstance(x, ast.Name) and x.id == name for x in targets):
                        continue
                    siblings = parent[a]
                    ancestor = next((c for c in chain if parent.get(c) is siblings), None)
                    if ancestor is not None and any(
                        a in getattr(siblings, f, []) and ancestor in getattr(siblings, f, [])
                        for f in ("body", "orelse", "finalbody")
                    ):
                        reaches = True
                        break
                if not reaches:
                    params = {x.arg for f in chain if isinstance(f, ast.FunctionDef) for x in f.args.args + f.args.kwonlyargs}
                    module_level = {t.id for s in tree.body if isinstance(s, ast.Assign) for t in s.targets if isinstance(t, ast.Name)}
                    if name not in params and name not in module_level:
                        problems.append(f"line {d.lineno}: {k.arg}={name}")
    return problems


def test_policy_variables_passed_to_assets_are_assigned_in_a_scope_that_reaches_them():
    bad = {}
    for p in _component_files():
        problems = _unreachable_policy_variables(p.read_text())
        if problems:
            bad[str(p.relative_to(ROOT))] = problems
    assert not bad, bad


def test_scope_check_catches_a_policy_built_in_the_other_branch():
    code = (
        "class C:\n"
        "    def build_defs(self, context):\n"
        "        if self.x:\n"
        "            retry = make()\n"
        "            @asset(retry_policy=retry)\n"
        "            def a(): ...\n"
        "        else:\n"
        "            @asset(retry_policy=retry)\n"
        "            def b(): ...\n"
    )
    assert _unreachable_policy_variables(code) == ["line 8: retry_policy=retry"]
    ok = code.replace("        if self.x:\n            retry = make()\n", "        retry = make()\n        if self.x:\n")
    assert _unreachable_policy_variables(ok) == []


@pytest.mark.parametrize("exclusive", [True, False])
def test_router_applies_freshness_retry_and_partitions_in_both_modes(exclusive):
    import dagster as dg
    folder = ROOT / "transforms/router"
    doc = next(d for d in yaml.safe_load_all((folder / "example.yaml").read_text()) if d and "attributes" in d)
    spec = importlib.util.spec_from_file_location("router_for_settings", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    component = getattr(mod, doc["type"].rsplit(".", 1)[-1])(**{
        **doc["attributes"], "exclusive": exclusive, "freshness_max_lag_minutes": 60,
        "retry_policy_max_retries": 3, "partition_type": "daily", "partition_start": "2026-01-01"})
    assets = list(component.build_defs(None).assets)
    assert len(assets) >= 2
    for a in assets:
        assert a.get_asset_spec(next(iter(a.keys))).freshness_policy is not None
        assert a.node_def.retry_policy is not None and a.node_def.retry_policy.max_retries == 3
        assert isinstance(a.partitions_def, dg.DailyPartitionsDefinition)
