"""`_azure_storage_options`/`_require_env` builds fsspec storage_options for an abfs://.../
abfss://.../az://... URI only when explicit Azure credential env vars are configured for it (ambient
fsspec credential discovery applies otherwise). Inlined per component (no shared helper module --
this repo's self-contained-component convention) and guarded against drift the same way
test_preview_safe_metadata_drift.py guards its own inlined block.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_BEGIN = "# BEGIN azure storage options"
_END = "# END azure storage options"


def _component_files():
    for p in sorted(ROOT.glob("**/component.py")):
        parts = p.relative_to(ROOT).parts
        if ".venv" in parts or "site-packages" in parts or parts[0] in ("dist", "quickstarts"):
            continue
        yield p


def _block(text: str) -> str:
    return text[text.index(_BEGIN): text.index(_END) + len(_END)]


def test_inlined_blocks_are_identical():
    blocks = {}
    for p in _component_files():
        text = p.read_text()
        if _BEGIN in text:
            blocks.setdefault(_block(text), []).append(str(p.relative_to(ROOT)))
    assert len(blocks) == 1, {k[:60]: v for k, v in blocks.items()}
    assert sum(len(v) for v in blocks.values()) >= 2
