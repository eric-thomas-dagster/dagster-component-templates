"""The receipt-table contract is inlined (no shared module) into every component that touches the tables; the copies must stay identical."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FILES = [
    "resources/context_receipt_store/component.py",
    "assets/ai/context_receipt_bigquery_sync/component.py",
    "assets/ai/context_engineering_pipeline/component.py",
]


def block(rel):
    text = (ROOT / rel).read_text()
    m = re.search(r"# BEGIN receipt-warehouse-ddl.*?# END receipt-warehouse-ddl", text, re.S)
    return m.group(0) if m else None


def test_inlined_receipt_warehouse_blocks_are_identical():
    blocks = {rel: block(rel) for rel in FILES if (ROOT / rel).exists()}
    present = {k: v for k, v in blocks.items() if v is not None}
    assert len(present) >= 2, f"expected the block in several components, found {list(present)}"
    reference = present["resources/context_receipt_store/component.py"]
    for rel, b in present.items():
        assert b == reference, f"{rel} drifted from the canonical receipt-warehouse block"
