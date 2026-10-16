"""`upstream_asset_key` / `upstream_asset_keys` accept str, list, or None.

Regression: Designer wrote `upstream_asset_keys: ['generated_orders']`, it was
mapped onto the `str` field `upstream_asset_key`, pydantic rejected the list,
and the whole project failed to load.

Each component.py is self-contained (no helper files; the CLI copies only that
file), so the normalization block is inlined in every component.
`test_inlined_blocks_are_identical` guards against drift between the copies, and
the helper's behaviour is tested through the `filter` component's copy.
"""
import ast
import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
_BEGIN = "# BEGIN upstream-asset-key"
_END = "# END upstream-asset-key normalization"
FIELDS = {"upstream_asset_key", "upstream_asset_keys"}

def _declares_fields(p: Path) -> bool:
    """True if a top-level class in `p` declares one of the upstream fields (AST, not grep:
    several components merely mention `upstream_asset_key:` in prompts and docs)."""
    return any(
        isinstance(s, ast.AnnAssign) and getattr(s.target, "id", None) in FIELDS
        for n in ast.parse(p.read_text()).body if isinstance(n, ast.ClassDef)
        for s in n.body
    )


def _component_files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] == "dist":
            continue
        if _declares_fields(p):
            yield p


FILES = list(_component_files())


def _load(rel: str, name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _target_classes(mod):
    return [
        c for c in vars(mod).values()
        if isinstance(c, type) and "_coerce_upstream_asset_keys" in vars(c)
    ]


def _block(p: Path) -> str:
    t = p.read_text()
    return t[t.index(_BEGIN): t.index(_END) + len(_END)]


class _Canon:
    """The inlined helper, as it appears in the filter component."""

    @staticmethod
    def normalize_upstream_keys(*a):
        return _load("assets/transforms/filter/component.py", "_uk_filter").normalize_upstream_keys(*a)


canon = _Canon


# ── the inlined helper ────────────────────────────────────────────────────

@pytest.mark.parametrize("single,plural,expected", [
    ("a", None, ["a"]),
    (["a"], None, ["a"]),
    (["a", "b"], None, ["a", "b"]),
    (("a", "b"), None, ["a", "b"]),
    ("a", ["b", "c"], ["a", "b", "c"]),      # both spellings at once
    (" a ", [" a", "b "], ["a", "b"]),       # strip + dedupe, order kept
    ("", None, []),                          # empty string
    (["", "  "], "", []),
    (None, None, []),
])
def test_normalize(single, plural, expected):
    assert canon.normalize_upstream_keys(single, plural) == expected


@pytest.mark.parametrize("bad", [5, [1], {"a": 1}])
def test_normalize_rejects_non_strings(bad):
    with pytest.raises(ValueError):
        canon.normalize_upstream_keys(bad)


# ── repo-wide static guards ─────────────────────────────────────────────────

def test_every_component_is_covered():
    assert len(FILES) >= 470


def test_inlined_blocks_are_identical():
    ref = _block(ROOT / "assets/transforms/filter/component.py")
    drift = [str(p.relative_to(ROOT)) for p in FILES if _block(p) != ref]
    assert not drift, "inlined upstream helper differs from the filter copy:\n  " + "\n  ".join(drift)


def test_every_declared_field_is_union_typed_with_validator():
    bad = []
    for p in FILES:
        for cls in (n for n in ast.parse(p.read_text()).body if isinstance(n, ast.ClassDef)):
            fields = [
                s for s in cls.body
                if isinstance(s, ast.AnnAssign) and getattr(s.target, "id", None) in FIELDS
            ]
            if not fields:
                continue
            if not any(isinstance(s, ast.FunctionDef) and s.name == "_coerce_upstream_asset_keys" for s in cls.body):
                bad.append(f"{p.relative_to(ROOT)}: {cls.name} has no validator")
            for f in fields:
                if "Union[str, List[str]]" not in ast.unparse(f.annotation):
                    bad.append(f"{p.relative_to(ROOT)}: {f.target.id} is {ast.unparse(f.annotation)}")
    assert not bad, "\n".join(bad)


# ── repo-wide dynamic check: every importable component coerces correctly ──

@pytest.mark.parametrize("p", FILES, ids=lambda p: str(p.parent.relative_to(ROOT)))
def test_every_component_coerces(p):
    rel = str(p.relative_to(ROOT))
    try:
        mod = _load(rel, "_uk_" + rel.replace("/", "_").replace(".", "_"))
    except Exception as e:  # optional third-party deps not installed
        pytest.skip(f"cannot import {rel}: {type(e).__name__}")
    classes = _target_classes(mod)
    assert classes, f"{rel} declares the fields but has no coercing class"
    for cls in classes:
        coerce = cls._coerce_upstream_asset_keys
        single = "upstream_asset_keys" not in cls.model_fields
        key = "upstream_asset_key" if single else "upstream_asset_keys"
        for raw, want in ((" x ", ["x"]), (["x"], ["x"]), ([" x", "x", ""], ["x"])):
            assert coerce({key: raw})[key] == (want[0] if single else want)
        # the other spelling merges onto the declared field
        other = "upstream_asset_keys" if single else "upstream_asset_key"
        assert coerce({other: ["x"]})[key] == ("x" if single else ["x"])
        if single:
            with pytest.raises(
                ValueError,
                match=rf"{cls.__name__} takes exactly one upstream asset, got \['x', 'y'\]",
            ):
                coerce({"upstream_asset_key": ["x", "y"]})
        else:
            merged = coerce({"upstream_asset_key": "x", "upstream_asset_keys": ["y"]})
            assert merged["upstream_asset_keys"] == ["x", "y"]


# ── real model construction, one component per family ──────────────────────

def _filter():
    return _load("assets/transforms/filter/component.py", "_uk_filter").FilterComponent


def _mk_filter(**kw):
    return _filter()(asset_name="o", condition="a > 1", **kw)


@pytest.mark.parametrize("value,expected", [
    ("x", "x"), (["x"], "x"), (" x ", "x"), ([" x "], "x"),
    ("", None), ([], None), ([""], None), (None, None),
])
def test_single_family_shapes(value, expected):
    assert _mk_filter(upstream_asset_key=value).upstream_asset_key == expected


def test_single_family_both_spellings():
    assert _mk_filter(upstream_asset_key="x", upstream_asset_keys=["x"]).upstream_asset_key == "x"
    with pytest.raises(ValueError, match="takes exactly one upstream asset"):
        _mk_filter(upstream_asset_key="x", upstream_asset_keys=["y"])


def test_single_family_multi_item_is_an_error_not_first_wins():
    with pytest.raises(ValueError, match=r"FilterComponent takes exactly one upstream asset, got \['x', 'y'\]"):
        _mk_filter(upstream_asset_key=["x", "y"])


def test_multi_family_shapes():
    U = _load("assets/transforms/dataframe_union/component.py", "_uk_union").DataframeUnion
    for v, want in (("a", ["a"]), (["a"], ["a"]), (["a", "b"], ["a", "b"])):
        assert U(asset_name="o", upstream_asset_keys=v).upstream_asset_keys == want
    merged = U(asset_name="o", upstream_asset_key="a", upstream_asset_keys=["b", "a"])
    assert merged.upstream_asset_keys == ["a", "b"]


def test_both_fields_family():
    W = _load("assets/sinks/duckdb_table_writer/component.py", "_uk_duckdb").DuckDBTableWriterComponent
    base = dict(asset_name="o", database_path=":memory:", table="t")
    for kw, want in (
        ({"upstream_asset_key": "a"}, ["a"]),
        ({"upstream_asset_key": ["a", "b"]}, ["a", "b"]),
        ({"upstream_asset_keys": "a"}, ["a"]),
        ({"upstream_asset_key": "a", "upstream_asset_keys": ["b"]}, ["a", "b"]),
        ({"upstream_asset_key": ""}, None),
        ({}, None),
    ):
        c = W(**base, **kw)
        assert c.upstream_asset_keys == want
        assert not c.upstream_asset_key


# ── a real defs.yaml carrying a list in the single-string field loads ──────

def test_defs_yaml_with_list_in_singular_field_loads(tmp_path, monkeypatch):
    from dagster.components.testing import create_defs_folder_sandbox

    _filter()  # registers sys.modules["_uk_filter"]
    (tmp_path / "uk_shim.py").write_text(
        "import sys\nFilterComponent = sys.modules['_uk_filter'].FilterComponent\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    def write(d, value):
        (d / "defs.yaml").write_text(yaml.safe_dump({
            "type": "uk_shim.FilterComponent",
            "attributes": {"asset_name": "out", "condition": "a > 1", "upstream_asset_key": value},
        }))

    with create_defs_folder_sandbox() as sb:
        d = sb.defs_folder_path / "f"
        d.mkdir()
        for value in (["x"], "x"):
            write(d, value)
            with sb.load_component_and_build_defs(defs_path=d) as (component, _defs):
                assert component.upstream_asset_key == "x"

        write(d, ["x", "y"])
        with pytest.raises(Exception) as exc:
            with sb.load_component_and_build_defs(defs_path=d):
                pass
        chain, e = [], exc.value
        while e is not None:
            chain.append(str(e))
            e = e.__cause__ or e.__context__
        assert any("takes exactly one upstream asset, got ['x', 'y']" in m for m in chain)
