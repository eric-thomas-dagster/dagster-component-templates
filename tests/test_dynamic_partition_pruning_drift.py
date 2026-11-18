"""The dynamic-partition pruning helper is inlined (components are self-contained -- no
shared module) into every sensor that registers dynamic partitions and supports pruning them.
This keeps the copies byte-identical so a fix lands everywhere."""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
COMPONENTS = [
    "sensors/s3_monitor",
    "sensors/filesystem_monitor",
]
BEGIN = "# BEGIN dynamic-partition pruning"
END = "# END dynamic-partition pruning"


def _block(rel):
    src = (ROOT / rel / "component.py").read_text()
    assert BEGIN in src and END in src, f"{rel}: dynamic-partition pruning block missing"
    return src[src.index(BEGIN): src.index(END) + len(END)]


def test_dynamic_partition_pruning_helper_copies_are_identical():
    blocks = {rel: _block(rel) for rel in COMPONENTS}
    ref_rel = COMPONENTS[0]
    for rel, blk in blocks.items():
        assert blk == blocks[ref_rel], f"{rel} drifted from {ref_rel}'s pruning block"
