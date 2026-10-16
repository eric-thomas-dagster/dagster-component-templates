"""Every class the package registry (`_CLASS_PATHS` in
dagster_community_components/__init__.py) advertises must actually be defined
in the source file it points at.

A mismatch ships silently (the registry is hand-maintained, not generated at
build time) and then breaks anything that loads the WHOLE library --
`dg dev`, `dg list components`, `dg check defs` -- with
`AttributeError: Class 'X' not found in <path> -- registry/source mismatch`.
That happened in 0.10.109/0.10.110 after a class was renamed in its
component.py but not in the registry.
"""
import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
INIT = ROOT / "dagster_community_components" / "__init__.py"


def _registry() -> dict[str, str]:
    src = INIT.read_text()
    m = re.search(r"_CLASS_PATHS[^=]*=\s*(\{.*?\n\})", src, re.S)
    assert m, "couldn't find _CLASS_PATHS in __init__.py"
    return ast.literal_eval(m.group(1))


def _source_file(path: str) -> Path:
    f = path.split(":")[0]
    if not f.endswith(".py"):
        f = f.replace("dagster_community_components.", "").replace(".", "/") + ".py"
    return ROOT / f


def test_registry_is_not_empty():
    assert len(_registry()) > 1000


def test_every_registered_class_exists_in_its_source_file():
    problems = []
    for name, path in _registry().items():
        f = _source_file(path)
        if not f.exists():
            problems.append(f"{name}: source file missing ({f.relative_to(ROOT)})")
            continue
        text = f.read_text()
        if not re.search(rf"^\s*class {re.escape(name)}\b|^\s*{re.escape(name)}\s*=", text, re.M):
            problems.append(f"{name}: not defined in {f.relative_to(ROOT)}")
    assert not problems, "registry/source mismatch:\n  " + "\n  ".join(problems)


# --- resilience: one bad registry entry must not break the whole library ---

import warnings  # noqa: E402

import dagster_community_components as pkg  # noqa: E402

_REAL_FILE = "resources/composer_airflow_auth_backend/component.py"


@pytest.mark.parametrize(
    "path",
    [_REAL_FILE, "resources/does_not_exist/component.py"],
    ids=["class-not-in-file", "file-missing"],
)
def test_bad_registry_entry_degrades_instead_of_raising(monkeypatch, path):
    monkeypatch.setitem(pkg._CLASS_PATHS, "BogusComponent", path)
    monkeypatch.delitem(pkg._loaded, "BogusComponent", raising=False)
    monkeypatch.delitem(pkg._stubs, "BogusComponent", raising=False)

    stub = getattr(pkg, "BogusComponent")
    # What `dg dev` / `dg list components` do for every entry: must not raise.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        stub.get_model_cls()
        stub.get_spec()
    assert any("BogusComponent" in str(w.message) for w in caught)

    # Only actually USING the component fails, with the real reason.
    with pytest.raises(ImportError, match="BogusComponent.*unavailable"):
        stub()
