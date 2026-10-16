"""kinds/owners/deps accept a str, partition_values accepts a list, asset_tags/tags accept
non-str scalars -- the shapes people and LLMs naturally write.

Like the upstream-key helper (tests/test_upstream_keys.py), the normalization block is
inlined in every component (no helper files), so a drift test compares the copies.
"""
import ast
import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN field-shape normalization", "# END field-shape normalization"
REF = ROOT / "assets/transforms/filter/component.py"


def _block(p: Path) -> str:
    t = p.read_text()
    return t[t.index(_BEGIN): t.index(_END) + len(_END)]


def _files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] == "dist":
            continue
        if _BEGIN in p.read_text():
            yield p


FILES = list(_files())


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _fn():
    return _load("assets/transforms/filter/component.py", "_fs_filter").coerce_field_shapes


def test_covers_the_repo():
    assert len(FILES) >= 900


def test_inlined_blocks_are_identical():
    ref = _block(REF)
    drift = [str(p.relative_to(ROOT)) for p in FILES if _block(p) != ref]
    assert not drift, "field-shape block differs from the filter copy:\n  " + "\n  ".join(drift)


def test_every_declared_field_has_a_validator_and_widened_type():
    bad = []
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] == "dist":
            continue
        for cls in (n for n in ast.parse(p.read_text()).body if isinstance(n, ast.ClassDef)):
            if not any("Component" in ast.unparse(b) for b in cls.bases):
                continue
            ann = {s.target.id: ast.unparse(s.annotation) for s in cls.body
                   if isinstance(s, ast.AnnAssign) and isinstance(s.target, ast.Name)}
            needs = (
                {k for k in ("kinds", "owners", "deps") if "ist[" in ann.get(k, "")}
                | {k for k in ("asset_tags", "tags") if "ict" in ann.get(k, "")}
                | ({"partition_values"} if ann.get("partition_values") == "Optional[str]" else set())
            )
            if needs and not any(isinstance(s, ast.FunctionDef) and s.name == "_coerce_field_shapes" for s in cls.body):
                bad.append(f"{p.relative_to(ROOT)}: {cls.name} {sorted(needs)}")
    assert not bad, "\n".join(bad)


@pytest.mark.parametrize("raw,want", [
    ("snowflake", ["snowflake"]),
    ("a, b", ["a", "b"]),
    ([" a ", "", "b"], ["a", "b"]),
    (["a"], ["a"]),
    ([], []),
])
def test_list_fields(raw, want):
    assert _fn()({"kinds": raw}, lists=("kinds",))["kinds"] == want


def test_list_fields_none_and_absent_untouched():
    f = _fn()
    assert f({"kinds": None}, lists=("kinds",)) == {"kinds": None}
    assert f({"x": 1}, lists=("kinds",)) == {"x": 1}


def test_csv_field():
    f = _fn()
    assert f({"partition_values": ["a", " b "]}, csv=("partition_values",))["partition_values"] == "a,b"
    assert f({"partition_values": "a,b"}, csv=("partition_values",))["partition_values"] == "a,b"
    assert f({"partition_values": None}, csv=("partition_values",))["partition_values"] is None


def test_tag_fields():
    f = _fn()
    assert f({"asset_tags": {"n": 1, "b": True, "f": False, "s": "x", "gone": None}}, tags=("asset_tags",))["asset_tags"] == {
        "n": "1", "b": "true", "f": "false", "s": "x"}
    assert f({"asset_tags": ["team=a", "tier: gold"]}, tags=("asset_tags",))["asset_tags"] == {"team": "a", "tier": "gold"}


def test_non_dict_input_passes_through():
    assert _fn()("not a dict", lists=("kinds",)) == "not a dict"


@pytest.mark.parametrize("p", FILES, ids=lambda p: str(p.parent.relative_to(ROOT)))
def test_every_component_coerces(p):
    rel = str(p.relative_to(ROOT))
    try:
        mod = _load(rel, "_fs_" + rel.replace("/", "_").replace(".", "_"))
    except Exception as e:  # optional third-party deps not installed
        pytest.skip(f"cannot import {rel}: {type(e).__name__}")
    classes = [c for c in vars(mod).values() if isinstance(c, type) and "_coerce_field_shapes" in vars(c)]
    assert classes
    for cls in classes:
        out = cls._coerce_field_shapes({"kinds": "a, b", "owners": "x", "deps": "k",
                                        "partition_values": ["p", "q"], "asset_tags": {"n": 1}, "tags": {"n": 1}})
        for k in ("kinds", "owners", "deps"):
            if k in cls.model_fields and "ist[" in str(cls.model_fields[k].annotation):
                assert isinstance(out[k], list)


def test_defs_yaml_with_loose_shapes_loads(tmp_path, monkeypatch):
    from dagster.components.testing import create_defs_folder_sandbox

    _load("assets/transforms/filter/component.py", "_fs_filter")
    (tmp_path / "fs_shim.py").write_text("import sys\nFilterComponent = sys.modules['_fs_filter'].FilterComponent\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    attrs = {
        "asset_name": "out", "condition": "a > 1", "upstream_asset_key": "x",
        "kinds": "snowflake", "owners": "team:a", "deps": "other",
        "partition_type": "static", "partition_values": ["a", "b"],
        "asset_tags": {"n": 1, "ok": True},
    }
    with create_defs_folder_sandbox() as sb:
        d = sb.defs_folder_path / "f"
        d.mkdir()
        (d / "defs.yaml").write_text(yaml.safe_dump({"type": "fs_shim.FilterComponent", "attributes": attrs}))
        with sb.load_component_and_build_defs(defs_path=d) as (c, _defs):
            assert c.kinds == ["snowflake"] and c.owners == ["team:a"] and c.deps == ["other"]
            assert c.partition_values == "a,b"
            assert c.asset_tags == {"n": "1", "ok": "true"}
