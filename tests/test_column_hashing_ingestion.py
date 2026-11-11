"""`hash_columns` pseudonymizes PII columns (salted SHA-256) via dlt's own documented
`DltResource.add_map()` pattern (https://dlthub.com/docs/general-usage/customising-pipelines/
pseudonymizing_columns), inlined per dlt-based ingestion component rather than shared
through a helper module (this repo's "inlined helper, drift-tested" convention).

`dlt_load_collection` is excluded: it predates this convention and implements the same
feature with its own bespoke logic across its three source engines (sql_database,
rest_api, filesystem) -- there is nothing here for it to drift against.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BEGIN, END = "# BEGIN column-hashing helper", "# END column-hashing helper"


def _users():
    return [p for p in sorted(ROOT.glob("assets/ingestion/*/component.py")) if BEGIN in p.read_text()]


def test_helper_copies_are_byte_identical():
    blocks = {}
    for p in _users():
        s = p.read_text()
        blocks.setdefault(re.search(rf"{BEGIN}.*?{END}", s, re.S).group(0), []).append(p)
    assert len(blocks) == 1, {str(k)[:80]: [p.parent.name for p in v] for k, v in blocks.items()}
    assert len(_users()) == 160


def test_helper_is_never_nested_inside_another_inlined_helper():
    """The hash-columns helper must sit as its own top-level block -- not sandwiched inside
    another inlined helper's BEGIN/END span (a real bug: it landed inside the partition
    row-filter helper in 41 components during the initial rollout)."""
    other_helpers = [
        ("# BEGIN partition row-filter helper", "# END partition row-filter helper"),
        ("# BEGIN column-lineage helper", "# END column-lineage helper"),
    ]
    for p in _users():
        s = p.read_text()
        bh = s.find(BEGIN)
        eh = s.find(END)
        for other_begin, other_end in other_helpers:
            ob = s.find(other_begin)
            oe = s.find(other_end)
            if ob == -1 or oe == -1:
                continue
            assert not (ob < bh < oe), f"{p}: column-hashing helper nested inside {other_begin!r}"
            assert not (bh < ob < eh), f"{p}: {other_begin!r} nested inside column-hashing helper"


def test_every_component_with_the_helper_declares_and_wires_the_three_fields():
    for p in _users():
        s = p.read_text()
        assert re.search(r"hash_columns\s*:\s*Optional\[List\[str\]\]\s*=\s*Field\(", s), p
        assert re.search(r"hash_salt\s*:\s*Optional\[str\]\s*=\s*Field\(", s), p
        assert re.search(r"hash_salt_env_var\s*:\s*Optional\[str\]\s*=\s*Field\(", s), p
        assert "hash_columns = self.hash_columns" in s, p
        assert "hash_salt = self.hash_salt" in s, p
        assert "hash_salt_env_var = self.hash_salt_env_var" in s, p
        assert "_apply_hash_columns(" in s, p


def test_every_apply_hash_columns_call_on_a_resources_accessor_is_guarded():
    """`_apply_hash_columns(..., hash_columns, ...)` has its own internal `if not
    hash_columns: return` guard, but a call site passing `source.resources.values()`
    (or `.resources`) evaluates that attribute access eagerly, before the function body
    runs -- which crashes against a test fake (or a dlt source shape) that doesn't carry
    a `.resources` attribute, even when hash_columns is unset. Every such call site must
    also be wrapped in `if hash_columns:` at the call site itself. (A call passing a
    plain list, e.g. `selected_resources`, needs no such guard -- list construction can't
    raise AttributeError.)"""
    for p in _users():
        lines = p.read_text().split("\n")
        for i, line in enumerate(lines):
            if "_apply_hash_columns(" not in line or "def _apply_hash_columns" in line:
                continue
            if ".resources.values()" not in line and not re.search(r"\.resources\)", line):
                continue
            guarded = False
            for j in range(i - 1, max(-1, i - 3), -1):
                if j < 0:
                    break
                if "if hash_columns:" in lines[j]:
                    guarded = True
                    break
                if lines[j].strip() == "":
                    continue
                break
            assert guarded, f"{p}:{i+1}: unguarded _apply_hash_columns call site: {line.strip()!r}"


def test_apply_hash_columns_pseudonymizes_and_validates_salt():
    import hashlib

    def resolve_hash_salt(hash_salt, hash_salt_env_var):
        if hash_salt:
            return hash_salt
        if hash_salt_env_var:
            import os

            val = os.environ.get(hash_salt_env_var)
            if val is None:
                raise ValueError(f"hash_salt_env_var={hash_salt_env_var!r} unset")
            return val
        return None

    def make_hash_columns_map(columns, salt):
        def _hash_map(doc):
            for col in columns:
                value = doc.get(col)
                if value is not None:
                    doc[col] = hashlib.sha256((str(value) + salt).encode()).hexdigest()
            return doc

        return _hash_map

    salt = "pepper"
    hash_map = make_hash_columns_map(["email"], resolve_hash_salt(salt, None))
    row = {"email": "a@b.com", "name": "A"}
    hashed = hash_map(dict(row))
    assert hashed["email"] == hashlib.sha256(("a@b.com" + salt).encode()).hexdigest()
    assert hashed["name"] == "A"
    # Missing column is skipped, not an error.
    assert hash_map({"name": "A"}) == {"name": "A"}
