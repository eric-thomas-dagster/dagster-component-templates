"""No component.py may reference a name that is never bound (a latent NameError).

Static, scope-aware (stdlib `symtable`): flags any name a function/class body reads as an implicit global
that the module never defines, imports, or assigns. This catches the bug class that hid in rarely-run code
paths -- e.g. `self` used inside a module-level helper, `upstream_asset_key` in a lineage block, an
`_event_cls` assigned in a sibling closure, a missing `MetadataValue` import -- none of which the import-time
tests ever executed.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Reviewed, intentionally allowed:
#  - the three `... if 'upstream_asset_key' in dir() else None` lines are guarded and never evaluate the name
#  - aws_glue's bare `Field` is in the legacy fallback class used only when StateBackedComponent is missing
#    (dagster < 1.8; this package requires >= 1.10)
ALLOWED = {
    ("assets/ai/synthetic_data_generator/component.py", "upstream_asset_key"),
    ("assets/analytics/ad_spend_standardizer/component.py", "upstream_asset_key"),
    ("assets/analytics/time_series_generator/component.py", "upstream_asset_key"),
    ("integrations/aws_glue/component.py", "Field"),
}

import ast
import builtins
import symtable
BUILTINS = set(dir(builtins)) | {"__file__", "__name__", "__doc__", "__path__", "__builtins__", "__class__"}
def module_bound(t):
    b = set()
    def visit(n):
        if isinstance(n, (ast.Import, ast.ImportFrom)):
            for a in n.names: b.add((a.asname or a.name).split(".")[0])
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            b.add(n.name); return  # do not descend into function/class bodies
        elif isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)): b.add(n.id)
        elif isinstance(n, ast.ExceptHandler) and n.name: b.add(n.name)
        elif isinstance(n, ast.Global): b.update(n.names)
        elif isinstance(n, ast.arg): return
        for c in ast.iter_child_nodes(n): visit(c)
    for n in t.body: visit(n)
    return b
def scan(p):
    src = open(p).read()
    t = ast.parse(src)
    mb = module_bound(t)
    top = symtable.symtable(src, p, "exec")
    out = []
    def rec(st, path):
        for s in st.get_symbols():
            if st.get_type() != "module" and s.is_global() and not s.is_declared_global() and s.is_referenced():
                n = s.get_name()
                if n not in mb and n not in BUILTINS: out.append((path + st.get_name(), n))
            elif st.get_type() == "module" and s.is_referenced() and not (s.is_assigned() or s.is_imported() or s.is_namespace()):
                n = s.get_name()
                if n not in mb and n not in BUILTINS: out.append(("<module>", n))
        for c in st.get_children(): rec(c, path + st.get_name() + ".")
    rec(top, "")
    return out


def test_no_undefined_names_in_any_component():
    bad = []
    for p in sorted(ROOT.glob("**/component.py")):
        rel = p.relative_to(ROOT)
        if ".venv" in rel.parts or "site-packages" in rel.parts or rel.parts[0] == "dist":
            continue
        try:
            found = scan(str(p))
        except SyntaxError:
            continue
        for scope, name in sorted(set(found)):
            if (str(rel), name) not in ALLOWED:
                bad.append(f"{rel}: {name!r} in {scope}")
    assert not bad, "undefined names (latent NameError):\n  " + "\n  ".join(bad)
