"""Regression test: subclassing a component through its dagster_community_components lazy
stub (`class My(dcc.ShellCommandAssetComponent): ...`) must not crash.

`_LazyComponentMeta._resolve_real`/`_make_stub`'s `_delegate`/`_load` all used to derive which
real component a stub represents from `cls.__name__` -- correct for the stub itself (`cls.__name__
== "ShellCommandAssetComponent"`), but WRONG the instant anyone subclasses it: `type(My)` is still
the same composed metaclass (inherited), yet `My.__name__` is `"My"`, which isn't a `_CLASS_PATHS`
key. Every metaclass-mediated operation on `My` -- instantiation, `get_model_cls()`, `get_schema()`,
`load()` -- raised `AttributeError: Unknown component: 'My'`. Fixed by stamping the real registry
name onto the stub itself as a plain class attribute (`__dcc_stub_for__`, found by ordinary MRO
attribute lookup on any subclass, never touching metaclass `__getattr__`), read instead of
`cls.__name__` at all three call sites.

Known, deliberate limitation this fix does NOT attempt to solve (tested explicitly below, not
left as an undocumented surprise): `__call__`'s `cls._resolve_real()(*args, **kwargs)` always
constructs an instance of the REAL class, never of the subclass itself -- so `My`'s own added
methods/fields are never combined with the real class's behavior when going through the lazy
stub. Real customization (overriding `build_defs`, adding fields, ...) needs the real submodule
import path instead (`from dagster_community_components.assets.infrastructure.shell_command_asset.
component import ShellCommandAssetComponent`), which has no stub involved at all.

Uses the exact same dev-checkout-only workaround as test_lazy_stub_load.py: this repo's editable
install resolves `dagster_community_components.assets.*` differently than an installed wheel
(hatchling's force-include copies `assets/` etc. under `dagster_community_components/` only at
build time) -- `_PACKAGE_ROOT` is pointed at the repo root and the module chain pre-registered in
sys.modules so `_load_real_class`'s own `importlib.import_module` call finds it. Neither of these
is the thing under test; they just route around that dev-checkout-only import quirk.
"""
import importlib.util
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture()
def dcc(monkeypatch):
    import dagster_community_components as dcc_mod

    root = Path(dcc_mod.__file__).resolve().parent.parent
    monkeypatch.setattr(dcc_mod, "_PACKAGE_ROOT", root)

    name = "ShellCommandAssetComponent"
    real_path = root / "assets/infrastructure/shell_command_asset/component.py"
    mod_name = "dagster_community_components.assets.infrastructure.shell_command_asset.component"
    for dotted in (
        "dagster_community_components.assets",
        "dagster_community_components.assets.infrastructure",
        "dagster_community_components.assets.infrastructure.shell_command_asset",
    ):
        if dotted not in sys.modules:
            pkg = types.ModuleType(dotted)
            pkg.__path__ = []
            monkeypatch.setitem(sys.modules, dotted, pkg)
    if mod_name not in sys.modules:
        spec = importlib.util.spec_from_file_location(mod_name, real_path)
        mod = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, mod_name, mod)
        spec.loader.exec_module(mod)

    dcc_mod._loaded.pop(name, None)
    dcc_mod._stubs.pop(name, None)
    yield dcc_mod
    dcc_mod._loaded.pop(name, None)
    dcc_mod._stubs.pop(name, None)


def test_stub_itself_still_instantiates_normally(dcc):
    """Regression safety: the fix must not change behavior for the common, unsubclassed case."""
    inst = dcc.ShellCommandAssetComponent(asset_name="x", command="echo hi")
    assert inst.asset_name == "x"
    assert inst.command == "echo hi"


def test_subclass_get_model_cls_does_not_crash(dcc):
    class My(dcc.ShellCommandAssetComponent):
        pass

    assert My.get_model_cls() is not None


def test_subclass_get_schema_does_not_crash(dcc):
    # get_schema() returns None for this particular real component too (confirmed directly
    # against the real, un-stubbed class) -- the point here is only that it doesn't crash.
    class My(dcc.ShellCommandAssetComponent):
        pass

    My.get_schema()


def test_subclass_instantiation_does_not_crash(dcc):
    class My(dcc.ShellCommandAssetComponent):
        pass

    inst = My(asset_name="x", command="echo hi")
    assert inst.asset_name == "x"
    assert inst.command == "echo hi"


def test_subclass_load_does_not_crash(dcc):
    """Same technique as test_lazy_stub_load.py: there's no simple standalone way to construct
    a real ComponentLoadContext (`.load()`'s own first line calls `context.with_rendering_scope(
    ...)`, which only a genuine one supports) -- so load the real component once through a real
    sandbox, SPY on its own `.load()` to capture the real `(attributes, context)` it was called
    with, then replay that same context against `My.load(...)` directly. That replay is the
    actual regression case: a subclass stub's own `.load()`, not the un-subclassed stub's."""
    import yaml
    from dagster.components.testing import create_defs_folder_sandbox

    class My(dcc.ShellCommandAssetComponent):
        pass

    real_cls = dcc._load_real_class("ShellCommandAssetComponent")
    captured = {}
    orig_load = real_cls.__dict__.get("load") or real_cls.load.__func__

    def _spy_load(cls, attributes, context):
        captured["attributes"] = attributes
        captured["context"] = context
        return orig_load.__func__(cls, attributes, context) if hasattr(orig_load, "__func__") else orig_load(cls, attributes, context)

    import pytest as _pytest
    with _pytest.MonkeyPatch.context() as mp:
        mp.setattr(real_cls, "load", classmethod(_spy_load))
        with create_defs_folder_sandbox() as sb:
            d = sb.defs_folder_path / "f"
            d.mkdir()
            (d / "defs.yaml").write_text(
                yaml.safe_dump({
                    "type": "dagster_community_components.ShellCommandAssetComponent",
                    "attributes": {"asset_name": "x", "command": "echo hi"},
                })
            )
            with sb.load_component_and_build_defs(defs_path=d):
                pass

    assert "context" in captured, "the real class's .load() was never reached -- can't replay"
    result = My.load(captured["attributes"], captured["context"])
    assert result.asset_name == "x"
    assert result.command == "echo hi"


def test_known_limitation_subclass_added_methods_do_not_survive_through_the_stub(dcc):
    """Documented, deliberate scope cut, not an oversight: __call__ always constructs an
    instance of the REAL class, so a subclass's own added method is never reachable this way.
    If this assertion ever starts failing, the fix's scope grew -- update this test's docstring
    (and the module docstring above) to describe the new, stronger guarantee, don't just delete it."""
    class My(dcc.ShellCommandAssetComponent):
        def extra_method(self):
            return "custom"

    inst = My(asset_name="x", command="echo hi")
    assert type(inst).__name__ == "ShellCommandAssetComponent"
    assert not hasattr(inst, "extra_method")
