"""on_schema_change/exclude_columns/max_rows_per_resource, inlined per dlt-based
ingestion component (this repo's "inlined helper, drift-tested" convention) alongside
hash_columns, which they share a helper block style with.

`dlt_load_collection` is excluded: it predates this convention and implements all
three with its own bespoke, per-resource-dict logic across its three source engines
(sql_database, rest_api, filesystem) -- there is nothing here for it to drift against.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BEGIN, END = "# BEGIN dlt-advanced-options helper", "# END dlt-advanced-options helper"


def _users():
    return [p for p in sorted(ROOT.glob("assets/ingestion/*/component.py")) if BEGIN in p.read_text()]


def test_helper_copies_are_byte_identical():
    blocks = {}
    for p in _users():
        s = p.read_text()
        blocks.setdefault(re.search(rf"{BEGIN}.*?{END}", s, re.S).group(0), []).append(p)
    assert len(blocks) == 1, {str(k)[:80]: [p.parent.name for p in v] for k, v in blocks.items()}


def test_helper_is_never_nested_inside_another_inlined_helper():
    """Must sit as its own top-level block -- not sandwiched inside another inlined
    helper's BEGIN/END span. A real bug during the hash_columns rollout: the
    column-hashing helper landed inside the partition row-filter helper in 41
    components (see test_column_hashing_ingestion.py); this guards the same failure
    mode for this helper too."""
    other_helpers = [
        ("# BEGIN partition row-filter helper", "# END partition row-filter helper"),
        ("# BEGIN column-lineage helper", "# END column-lineage helper"),
        ("# BEGIN column-hashing helper", "# END column-hashing helper"),
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
            assert not (ob < bh < oe), f"{p}: dlt-advanced-options helper nested inside {other_begin!r}"
            assert not (bh < ob < eh), f"{p}: {other_begin!r} nested inside dlt-advanced-options helper"


def test_every_component_with_the_helper_declares_and_wires_the_three_fields():
    for p in _users():
        s = p.read_text()
        assert re.search(r"on_schema_change\s*:\s*Literal\[", s), p
        assert re.search(r"exclude_columns\s*:\s*Optional\[List\[str\]\]\s*=\s*Field\(", s), p
        assert re.search(r"max_rows_per_resource\s*:\s*Optional\[int\]\s*=\s*Field\(", s), p
        assert "on_schema_change = self.on_schema_change" in s, p
        assert "exclude_columns = self.exclude_columns" in s, p
        assert "max_rows_per_resource = self.max_rows_per_resource" in s, p
        assert "_resolve_schema_contract(" in s, p
        assert "_apply_exclude_columns(" in s, p
        assert "_apply_max_rows_per_resource(" in s, p


def test_every_apply_call_on_a_resources_accessor_is_guarded():
    """Same eager-evaluation hazard as hash_columns (see
    test_column_hashing_ingestion.py): a call site passing `.resources.values()`/
    `.resources[...]` evaluates that attribute/subscript access before the called
    function's own internal guard runs, which crashes against a test fake lacking it
    -- even when the field is unset. Every such call site for _apply_exclude_columns/
    _apply_max_rows_per_resource must also be wrapped in its own `if <field>:` at the
    call site. (A call passing a plain list, e.g. `selected_resources`, needs no such
    guard -- list construction can't raise AttributeError.) `on_schema_change`'s own
    apply_hints loop is exempt -- it is itself the guard (`if on_schema_change !=
    "allow_all":`), skipping resource access entirely for the default, by far the
    most common, case."""
    targets = ("_apply_exclude_columns(", "_apply_max_rows_per_resource(")
    guard_fields = {"_apply_exclude_columns(": "exclude_columns", "_apply_max_rows_per_resource(": "max_rows_per_resource"}
    for p in _users():
        lines = p.read_text().split("\n")
        for i, line in enumerate(lines):
            target = next((t for t in targets if t in line and f"def {t}" not in line), None)
            if target is None:
                continue
            if ".resources.values()" not in line and not re.search(r"\.resources\[", line):
                continue
            field = guard_fields[target]
            guarded = False
            for j in range(i - 1, max(-1, i - 3), -1):
                if j < 0:
                    break
                if f"if {field}:" in lines[j]:
                    guarded = True
                    break
                if lines[j].strip() == "":
                    continue
                break
            assert guarded, f"{p}:{i+1}: unguarded {target.rstrip('(')} call site: {line.strip()!r}"


def test_resolve_schema_contract_and_make_exclude_columns_map_are_correct():
    import hashlib
    import importlib.util

    import pytest

    p = _users()[0]
    spec = importlib.util.spec_from_file_location("dlt_advanced_options_check", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    assert mod._resolve_schema_contract("allow_all") == {"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
    assert mod._resolve_schema_contract("allow_new_columns") == {"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
    assert mod._resolve_schema_contract("block_all") == {"tables": "evolve", "columns": "freeze", "data_type": "freeze"}
    with pytest.raises(ValueError):
        mod._resolve_schema_contract("bogus")

    exclude_map = mod._make_exclude_columns_map(["ssn", "missing"])
    assert exclude_map({"id": 1, "ssn": "x", "name": "kept"}) == {"id": 1, "name": "kept"}

    hash_map = mod._make_hash_columns_map(["email"], "s")
    email_exclude_map = mod._make_exclude_columns_map(["email"])
    assert hash_map(email_exclude_map({"email": "a@b.com"})) == {}
