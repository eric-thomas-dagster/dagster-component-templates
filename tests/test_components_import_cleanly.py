"""Every component module imports without a NameError, SyntaxError or similar.

Missing optional libraries are fine (they are imported lazily or skipped); an undefined name at class-definition time
(for example a type annotation that was never imported) is not."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
FILES = sorted(ROOT.glob("assets/*/*/component.py"))


def test_every_component_module_defines_its_names():
    broken = []
    for p in FILES:
        spec = importlib.util.spec_from_file_location("imp_" + p.parent.name, p)
        mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except (ImportError, EnvironmentError):
            continue  # an optional dependency is not installed here
        except Exception as exc:  # NameError, SyntaxError, AttributeError, TypeError at import time
            broken.append((str(p.parent.relative_to(ROOT)), f"{type(exc).__name__}: {exc}"[:120]))
    assert broken == [], broken
