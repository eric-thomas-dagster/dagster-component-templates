import importlib.util
from pathlib import Path

_COMPONENT = Path(__file__).resolve().parent.parent / "component.py"


def load_component_module():
    """Load ../component.py by file path (component dirs are not importable packages)."""
    spec = importlib.util.spec_from_file_location("redshift_io_manager_component", _COMPONENT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
