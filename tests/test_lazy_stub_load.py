"""Regression test: loading a component through its dagster_community_components
lazy stub (the path every `dg scaffold`/UI-created `defs.yaml` actually uses --
`type: dagster_community_components.<Name>`) must resolve the component's real
attributes, not build it with none.

`Component.load`'s default impl branches on `issubclass(cls, Resolvable)`. Since
the stub never inherits from the real class's MRO (by design -- see
`_LazyComponentMeta`'s docstring), that check is always False against a stub,
so the default fell through to `cls()` with zero arguments instead of
`cls.resolve_from_model(attributes)`. Every real, UI-authored component built
this way silently dropped all of its configured attributes and failed with
"Field required" on any required field. Fixed by delegating `load` itself in
`_make_stub`'s namespace, like `get_model_cls`/`get_schema`/`get_spec` already
were.

Note: in this installed dagster version, `dg`'s own YamlDecl loader happens to
call `component_type.get_model_cls()` before `component_type.load(...)`, and
the first call incidentally warms this package's `_loaded` cache -- so by the
time `.load()` runs, the SECOND `component_type` lookup already resolves to
the real class instead of the stub, self-healing the bug for that one call
path. That's an accident of call order, not something to rely on: a caller
that invokes `.load()` directly on a freshly-registry-provided class (exactly
what "stub.load runs on the stub" in the bug report describes, and plausible
for any component-UI backend that doesn't call `get_model_cls()` itself first)
hits it every time. This test captures a real `(attributes, context)` pair
from a genuine sandbox run, then replays them against a BRAND NEW, never-
before-touched stub for the same component -- isolating exactly that case.
"""
import importlib.util
import sys
import types
from pathlib import Path

import yaml


def test_stub_load_resolves_real_attributes_on_first_use(monkeypatch):
    import dagster_community_components as dcc
    from dagster.components.testing import create_defs_folder_sandbox

    name = "TextToColumns"
    root = Path(dcc.__file__).resolve().parent.parent
    real_component_path = root / "assets/transforms/text_to_columns/component.py"

    # `_load_real_class`'s existence check looks for component files physically
    # under `dagster_community_components/` -- true in an installed wheel
    # (hatchling's force-include copies `assets/` etc. in there), but this
    # repo's own editable dev install resolves `dagster_community_components.
    # assets.*` via a different mechanism, with the files still living at the
    # repo root. Point the check at the repo root so it matches what a real
    # installed environment sees, and pre-register the module chain so the
    # subsequent `importlib.import_module` call (which would otherwise fail in
    # dev mode) finds it already loaded -- neither of these is the thing under
    # test; they just route around a dev-checkout-only import quirk.
    monkeypatch.setattr(dcc, "_PACKAGE_ROOT", root)
    mod_name = "dagster_community_components.assets.transforms.text_to_columns.component"
    for dotted in (
        "dagster_community_components.assets",
        "dagster_community_components.assets.transforms",
        "dagster_community_components.assets.transforms.text_to_columns",
    ):
        if dotted not in sys.modules:
            pkg = types.ModuleType(dotted)
            pkg.__path__ = []
            monkeypatch.setitem(sys.modules, dotted, pkg)
    if mod_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(mod_name, real_component_path)
        mod = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, mod_name, mod)
        spec.loader.exec_module(mod)
    real_cls = sys.modules[mod_name].TextToColumns

    captured = {}
    orig_load = real_cls.__dict__.get("load") or real_cls.load.__func__
    def _spy_load(cls, attributes, context):
        captured["attributes"] = attributes
        captured["context"] = context
        return orig_load.__func__(cls, attributes, context) if hasattr(orig_load, "__func__") else orig_load(cls, attributes, context)
    monkeypatch.setattr(real_cls, "load", classmethod(_spy_load))

    dcc._loaded.pop(name, None)
    dcc._stubs.pop(name, None)
    attrs = {
        "asset_name": "out",
        "upstream_asset_key": "src",
        "column": "email",
        "separator": "@",
        "output_columns": ["local_part", "domain"],
    }
    with create_defs_folder_sandbox() as sb:
        d = sb.defs_folder_path / "f"
        d.mkdir()
        (d / "defs.yaml").write_text(
            yaml.safe_dump({"type": f"dagster_community_components.{name}", "attributes": attrs})
        )
        with sb.load_component_and_build_defs(defs_path=d):
            pass

    assert "attributes" in captured, "the real class's .load() was never reached -- can't replay"

    # Now the actual regression case: a BRAND NEW stub for this same component,
    # never previously resolved, `.load()`'d directly with a real
    # (attributes, context) pair -- mirrors a caller that only calls `.load()`.
    dcc._loaded.pop(name, None)
    dcc._stubs.pop(name, None)
    fresh_stub = dcc._make_stub(name)
    assert type(fresh_stub).__name__ in ("_LazyComponentComposedMeta", "_LazyComponentMeta")

    result = fresh_stub.load(captured["attributes"], captured["context"])
    assert result.asset_name == "out"
    assert result.upstream_asset_key == "src"
    assert result.column == "email"
    assert result.separator == "@"
    assert result.output_columns == ["local_part", "domain"]
