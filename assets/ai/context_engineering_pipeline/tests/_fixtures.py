"""Real, importable fixtures for classify mode='custom' (bring-your-own-classifier). Imported via its
absolute dotted path (`assets.ai.context_engineering_pipeline.tests._fixtures`), the same path the
component's own `_resolve_classify_fn` resolves through `importlib.import_module` -- not `from . import
_fixtures`, which would land on a second, separate module object with its own independent CALLS list."""
from typing import List, Optional

CALLS: List[str] = []


def reset() -> None:
    CALLS.clear()


def keyword_classifier(text: str, candidate_labels: List[str]) -> Optional[str]:
    """Trivial deterministic rule: the first candidate label whose text appears in the chunk, else None."""
    CALLS.append(text)
    lowered = text.lower()
    for label in candidate_labels:
        if label.lower() in lowered:
            return label
    return None


def raises(text: str, candidate_labels: List[str]) -> str:
    raise RuntimeError("boom")


def returns_unknown_label(text: str, candidate_labels: List[str]) -> str:
    return "not_a_real_label"


def uppercase(text: str) -> str:
    """Trivial custom-op transform: used by test_custom_op.py the same way keyword_classifier is used
    by test_classify_custom.py -- a real importable function the component's _resolve_custom_fn can
    resolve via 'module.path:function_name'."""
    CALLS.append(text)
    return text.upper()


def raises_for_custom(text: str) -> str:
    raise RuntimeError("boom")
