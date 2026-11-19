"""The receipt-message-links table (links a receipt_id to the Slack message that carried the
answer built from it) is inlined (components are self-contained -- no shared module) into
every component that creates or reads it. This keeps the copies byte-identical so a fix lands
everywhere."""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent / "assets"
COMPONENTS = [
    "sinks/slack_reply",
    "ai/slack_chat_tools",
]
BEGIN = "# BEGIN receipt-message-links"
END = "# END receipt-message-links"


def _block(rel):
    src = (ROOT / rel / "component.py").read_text()
    assert BEGIN in src and END in src, f"{rel}: receipt-message-links block missing"
    return src[src.index(BEGIN): src.index(END) + len(END)]


def test_receipt_message_links_block_copies_are_identical():
    blocks = {rel: _block(rel) for rel in COMPONENTS}
    ref_rel = COMPONENTS[0]
    for rel, blk in blocks.items():
        assert blk == blocks[ref_rel], f"{rel} drifted from {ref_rel}'s receipt-message-links block"
