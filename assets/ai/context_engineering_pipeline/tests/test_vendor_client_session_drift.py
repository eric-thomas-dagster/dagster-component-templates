"""_vendor_client_session is inlined per component (components are self-contained); the copies must not drift."""
from pathlib import Path

AI = Path(__file__).resolve().parents[2]
FILES = [AI / "context_engineering_pipeline" / "component.py", AI / "context_receipt_bigquery_sync" / "component.py",
         AI / "receipt_outcome_ingestion" / "component.py"]
BEGIN, END = "# --- _vendor_client_session: begin", "# --- _vendor_client_session: end ---"


def _block(path):
    s = path.read_text()
    assert s.count(BEGIN) == 1 and s.count(END) == 1, path
    return s[s.index(BEGIN):s.index(END) + len(END)]


def test_the_inlined_copies_are_identical():
    blocks = {p: _block(p) for p in FILES}
    first = blocks[FILES[0]]
    assert all(b == first for b in blocks.values()), [p.parent.name for p, b in blocks.items() if b != first]
