"""`output_columns` accepts a real list, a comma-separated string, or (the common LLM
mistake) a string holding a Python list's own repr instead of a real YAML/JSON list.

Regression: a model wrote `output_columns: "['local_part', 'domain']"` for an email-split
TextToColumns asset. The field is `Optional[List[Union[str, int]]]`, and nothing stripped
the stray `[`, `'`, `]` characters before the comma-split, so the asset ended up with two
columns literally named `"['local_part'"` and `"'domain']"` -- no error, just garbage column
names. Designer now avoids writing the string form, but the component should accept either
shape, in the same spirit as the upstream-asset-key normalization (tests/test_upstream_keys.py).

Each component.py is self-contained (no helper files; the CLI copies only that file), so the
normalization block is inlined in every component that declares `output_columns`.
`test_inlined_blocks_are_identical` guards against drift between the copies.
"""
import ast
import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN output-columns normalization", "# END output-columns normalization"
REF = ROOT / "assets/transforms/text_to_columns/component.py"


def _declares_field(p: Path) -> bool:
    return any(
        isinstance(s, ast.AnnAssign) and getattr(s.target, "id", None) == "output_columns"
        for n in ast.parse(p.read_text()).body if isinstance(n, ast.ClassDef)
        for s in n.body
    )


def _component_files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] == "dist":
            continue
        if _declares_field(p):
            yield p


FILES = list(_component_files())


def _load(rel: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _block(p: Path) -> str:
    t = p.read_text()
    return t[t.index(_BEGIN): t.index(_END) + len(_END)]


def _fn():
    return _load("assets/transforms/text_to_columns/component.py", "_oc_text_to_columns").normalize_output_columns


def test_covers_the_repo():
    # Currently text_to_columns + regex_parser; guards against a third copy being
    # added without the normalization block (or one of these two losing it).
    assert len(FILES) >= 2


def test_inlined_blocks_are_identical():
    ref = _block(REF)
    drift = [str(p.relative_to(ROOT)) for p in FILES if _block(p) != ref]
    assert not drift, "output-columns block differs from the text_to_columns copy:\n  " + "\n  ".join(drift)


@pytest.mark.parametrize("raw,want", [
    (None, None),
    (["a", "b"], ["a", "b"]),
    ("a,b", ["a", "b"]),
    ("a, b", ["a", "b"]),
    (["local_part", "domain"], ["local_part", "domain"]),
    # the actual regression: a stringified Python list repr instead of a real list
    ("['local_part', 'domain']", ["local_part", "domain"]),
    ("[\"local_part\", \"domain\"]", ["local_part", "domain"]),
    # numeric labels pass through untouched
    ([0, 1], [0, 1]),
    (["a", 1, "b"], ["a", 1, "b"]),
    # stray whitespace/empties
    ([" a ", "", "b"], ["a", "b"]),
    ("", []),
])
def test_normalize_output_columns(raw, want):
    assert _fn()(raw) == want


def test_defs_yaml_with_string_output_columns_loads(tmp_path, monkeypatch):
    from dagster.components.testing import create_defs_folder_sandbox

    _load("assets/transforms/text_to_columns/component.py", "_oc_text_to_columns")
    (tmp_path / "oc_shim.py").write_text(
        "import sys\nTextToColumns = sys.modules['_oc_text_to_columns'].TextToColumns\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    attrs = {
        "asset_name": "out",
        "upstream_asset_key": "src",
        "column": "email",
        "separator": "@",
        # the regression shape: a string holding a Python list repr
        "output_columns": "['local_part', 'domain']",
    }
    with create_defs_folder_sandbox() as sb:
        d = sb.defs_folder_path / "f"
        d.mkdir()
        (d / "defs.yaml").write_text(yaml.safe_dump({"type": "oc_shim.TextToColumns", "attributes": attrs}))
        with sb.load_component_and_build_defs(defs_path=d) as (c, _defs):
            assert c.output_columns == ["local_part", "domain"]
