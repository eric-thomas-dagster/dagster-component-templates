"""Test helpers for AWSCDKAssetComponent: loads the real component.py by path (no installed package needed)."""
import importlib.util
import pathlib
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("aws_cdk_asset_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeStdout:
    def readline(self):
        return ""


class FakePopen:
    """Minimal subprocess.Popen stand-in: records the command, yields no stdout lines, exits clean."""
    last_cmd = None

    def __init__(self, cmd, **kwargs):
        FakePopen.last_cmd = cmd
        self.stdout = FakeStdout()
        self.returncode = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def communicate(self):
        return ("", "")

    def wait(self):
        return self.returncode
