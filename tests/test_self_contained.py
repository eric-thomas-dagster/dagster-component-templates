"""Every component.py is fully self-contained: no helper/sibling modules, no imports of other
components. The CLI copies only component.py (+ schema.json, README.md, ...), so anything else a
component needs at runtime would be missing in a user's project."""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPO_PKGS = ("dagster_community_components", "dagster_component_templates")
# The one deliberate exception: invoke_component resolves YAML `type: dagster_community_components.X`
# strings BY NAME at runtime, so the installed package is the thing being addressed, not a helper.
ALLOWED = {"assets/ai/agentic_pipeline/component.py"}


def _components():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] == "dist":
            continue
        yield p


def test_no_component_imports_repo_code():
    bad = []
    for p in _components():
        if str(p.relative_to(ROOT)) in ALLOWED:
            continue
        for n in ast.walk(ast.parse(p.read_text())):
            if isinstance(n, ast.ImportFrom) and (n.level > 0 or (n.module or "").split(".")[0] in REPO_PKGS):
                bad.append(f"{p.relative_to(ROOT)}:{n.lineno} from {'.' * n.level}{n.module or ''}")
            elif isinstance(n, ast.Import) and any(a.name.split(".")[0] in REPO_PKGS for a in n.names):
                bad.append(f"{p.relative_to(ROOT)}:{n.lineno} import {ast.unparse(n)}")
    assert not bad, "components must not import repo code (inline it instead):\n  " + "\n  ".join(bad)


def test_no_sibling_python_modules():
    """A component dir holds component.py + __init__.py (+ tests/ and examples/); nothing else."""
    bad = []
    for p in _components():
        for f in p.parent.glob("*.py"):
            if f.name not in ("component.py", "__init__.py"):
                bad.append(str(f.relative_to(ROOT)))
    assert not bad, "unvendorable sibling modules:\n  " + "\n  ".join(bad)
