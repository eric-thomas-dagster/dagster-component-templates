"""execution_mode='sql': filter / dedupe / sensitivity (tag|drop) / stamp.

Executed here the same way as the rest of execution_mode='sql' (sqlglot -> DuckDB through
bq_fake.py, real dg.materialize): all four are plain SQL (LENGTH/REGEXP_CONTAINS/REGEXP_REPLACE/
QUALIFY ROW_NUMBER/CONCAT), so this is real execution, not a stub. NOT supported and asserted to
raise instead of silently doing the wrong thing: filter's exclude_patterns / sensitivity's fixed
detector regexes that need lookahead/lookbehind (phone) or Luhn validation (credit_card) BigQuery's
RE2 engine can't express or we haven't ported, and sensitivity's action='redact' (would need
content_hash recomputed mid-statement)."""
import re

import dagster as dg
import pytest
import sqlglot

from .bq_fake import FakeBigQueryResource
from .test_sql_incremental import READ, Harness


@pytest.fixture()
def mod(request):
    from .conftest import load_component_module
    return load_component_module()


@pytest.fixture(params=list(READ))
def h(request, mod):
    harness = Harness(mod, request.param)
    yield harness
    harness.wh.close()


def _run_build(harness, steps, **kw):
    harness.steps = steps
    md = harness.run(**kw)
    return md, harness.table_rows()


# ── filter ──────────────────────────────────────────────────────────────────────────────────────

FILTER_DOCS = {"short": "hi", "ok": "this is a normal length sentence about billing questions",
               "symbols": "####!!!!$$$$%%%%^^^^&&&&****((((", "other": "a decent sentence about shipping times"}
FILTER_STEPS = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
                {"id": "f", "op": "filter", "min_chars": 10, "max_chars": 100, "min_alnum_ratio": 0.5},
                {"id": "e", "op": "embed", "model": "p.d.m1"}]


def test_filter_min_max_chars_and_alnum_ratio(h):
    h.set_docs(FILTER_DOCS)
    md, rows = _run_build(h, FILTER_STEPS)
    kept = {r["chunk_text"] for r in rows}
    assert kept == {"this is a normal length sentence about billing questions", "a decent sentence about shipping times"}


def test_filter_exclude_patterns_drops_matching_chunks(h):
    h.set_docs({"a": "please contact support for a refund", "b": "this document is CONFIDENTIAL internal only"})
    steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
              {"id": "f", "op": "filter", "exclude_patterns": [r"(?i)confidential"]},
              {"id": "e", "op": "embed", "model": "p.d.m1"}]
    md, rows = _run_build(h, steps)
    assert {r["chunk_text"] for r in rows} == {"please contact support for a refund"}


def test_filter_exclude_patterns_rejects_lookahead(h, mod):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "f", "op": "filter", "exclude_patterns": [r"foo(?=bar)"]},
               {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="lookahead/lookbehind/backreferences"):
        h.run()


def test_filter_exclude_patterns_rejects_lookbehind(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "f", "op": "filter", "exclude_patterns": [r"(?<=foo)bar"]},
               {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="lookahead/lookbehind/backreferences"):
        h.run()


def test_filter_exclude_patterns_rejects_backreference(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "f", "op": "filter", "exclude_patterns": [r"(foo)\1"]},
               {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="lookahead/lookbehind/backreferences"):
        h.run()


# ── dedupe ──────────────────────────────────────────────────────────────────────────────────────

def test_dedupe_exact_keeps_first_of_byte_identical_chunks(h):
    h.set_docs({"d1": "same text", "d2": "same text", "d3": "different text entirely"})
    steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
              {"id": "dd", "op": "dedupe", "mode": "exact"},
              {"id": "e", "op": "embed", "model": "p.d.m1"}]
    md, rows = _run_build(h, steps)
    assert len(rows) == 2
    assert {r["chunk_text"] for r in rows} == {"same text", "different text entirely"}
    kept_ids = {r["id"] for r in rows if r["chunk_text"] == "same text"}
    assert kept_ids == {"d1"}  # first (by chunk_id) wins


def test_dedupe_normalized_ignores_case_punctuation_whitespace(h):
    h.set_docs({"d1": "Same  Text!!", "d2": "same text", "d3": "different"})
    steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
              {"id": "dd", "op": "dedupe", "mode": "normalized"},
              {"id": "e", "op": "embed", "model": "p.d.m1"}]
    md, rows = _run_build(h, steps)
    assert len(rows) == 2


def test_dedupe_within_scopes_per_group(h):
    h.con.execute("DROP TABLE IF EXISTS ds.src")
    h.con.execute("CREATE TABLE ds.src(id VARCHAR, body VARCHAR, tenant VARCHAR)")
    h.con.execute("INSERT INTO ds.src VALUES ('d1', 'same text', 'A'), ('d2', 'same text', 'B')")
    steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
              {"id": "dd", "op": "dedupe", "mode": "exact", "within": ["tenant"]},
              {"id": "e", "op": "embed", "model": "p.d.m1"}]
    comp = h.component(steps=steps, metadata_columns=["tenant"],
                        source={"kind": "warehouse_query", "resource_key": "wh", "sql": "SELECT id, body, tenant FROM ds.src"})
    asset_def = list(comp.build_defs(None).assets)[0]
    res = dg.materialize([asset_def], resources={"wh": h.resource}, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    assert len(h.table_rows()) == 2  # same text kept once per tenant, not deduped across tenants


def test_dedupe_rejects_unknown_within_column(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "dd", "op": "dedupe", "within": ["nope"]}, {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="within column 'nope' not in upstream columns"):
        h.run()


# ── sensitivity ─────────────────────────────────────────────────────────────────────────────────

def test_sensitivity_tag_flags_pii_and_lists_types(h):
    h.set_docs({"a": "contact me at jane@example.com for details", "b": "no sensitive info here at all"})
    steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
              {"id": "s", "op": "sensitivity", "detectors": ["email"]},
              {"id": "e", "op": "embed", "model": "p.d.m1"}]
    md, rows = _run_build(h, steps)
    by_id = {r["id"]: r for r in rows}
    assert by_id["a"]["sensitivity"] == "pii" and by_id["a"]["pii_types"] == "email"
    assert by_id["b"]["sensitivity"] == "none" and by_id["b"]["pii_types"] == ""


def test_sensitivity_drop_removes_pii_rows(h):
    h.set_docs({"a": "my ssn is 123-45-6789 for verification", "b": "no sensitive info here at all"})
    steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
              {"id": "s", "op": "sensitivity", "detectors": ["ssn"], "action": "drop"},
              {"id": "e", "op": "embed", "model": "p.d.m1"}]
    md, rows = _run_build(h, steps)
    assert len(rows) == 1 and rows[0]["id"] == "b"


def test_sensitivity_rejects_phone_detector(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "s", "op": "sensitivity", "detectors": ["phone"]}, {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="phone.*not supported"):
        h.run()


def test_sensitivity_rejects_credit_card_detector(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "s", "op": "sensitivity", "detectors": ["credit_card"]}, {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="credit_card.*not supported"):
        h.run()


def test_sensitivity_rejects_redact_action(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "s", "op": "sensitivity", "action": "redact"}, {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="action='redact' is not supported"):
        h.run()


# ── stamp ───────────────────────────────────────────────────────────────────────────────────────

def test_stamp_columns_template_and_ttl_days(h):
    h.con.execute("DROP TABLE IF EXISTS ds.src")
    h.con.execute("CREATE TABLE ds.src(id VARCHAR, body VARCHAR, region VARCHAR)")
    h.con.execute("INSERT INTO ds.src VALUES ('d1', 'hello world this is a test document', 'us-east')")
    steps = [{"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
              {"id": "st", "op": "stamp", "columns": {"scope": "support-{region}"}, "ttl_days": 30},
              {"id": "e", "op": "embed", "model": "p.d.m1"}]
    comp = h.component(steps=steps, metadata_columns=["region"],
                        source={"kind": "warehouse_query", "resource_key": "wh", "sql": "SELECT id, body, region FROM ds.src"})
    asset_def = list(comp.build_defs(None).assets)[0]
    res = dg.materialize([asset_def], resources={"wh": h.resource}, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    rows = h.table_rows()
    assert rows[0]["scope"] == "support-us-east"
    assert rows[0]["valid_until"] is not None


def test_stamp_rejects_format_spec_template(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "st", "op": "stamp", "columns": {"x": "{id:.2f}"}}, {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="must be a plain literal or simple"):
        h.run()


def test_stamp_rejects_unknown_placeholder_column(h):
    h.steps = [{"id": "c", "op": "chunk"}, {"id": "st", "op": "stamp", "columns": {"x": "{nope}"}}, {"id": "e", "op": "embed", "model": "p.d.m1"}]
    with pytest.raises(ValueError, match="not in upstream columns"):
        h.run()


# ── combined + per_step_ops rejection + parse-only across all 3 dialects ──────────────────────────

def test_filter_dedupe_sensitivity_stamp_combined_with_classify_and_embed(h):
    h.set_docs({"d1": "I was charged twice for my subscription, please refund the duplicate charge.",
                "d2": "I was charged twice for my subscription, please refund the duplicate charge.",  # exact dup of d1
                "d3": "x"})  # too short, filtered
    steps = [
        {"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
        {"id": "f", "op": "filter", "min_chars": 5},
        {"id": "dd", "op": "dedupe", "mode": "exact"},
        {"id": "s", "op": "sensitivity", "detectors": ["email"]},
        {"id": "st", "op": "stamp", "columns": {"scope": "support"}},
        {"id": "k", "op": "classify", "candidate_labels": ["billing", "other"]},
        {"id": "e", "op": "embed", "model": "p.d.m1"},
    ]
    md, rows = _run_build(h, steps)
    assert len(rows) == 1  # d3 filtered out, d2 deduped against d1
    assert rows[0]["scope"] == "support" and rows[0]["sensitivity"] == "none" and rows[0]["category"] in {"billing", "other"}
    assert "embedding" in rows[0]


def test_per_step_ops_sql_rejects_the_new_ops(mod):
    comp = mod.ContextEngineeringPipelineComponent(
        asset_name="kb", id_column="id", text_column="body", execution_mode="sql", sql_dialect="bigquery",
        output_table="ds.kb", per_step_ops=True,
        source={"kind": "warehouse_query", "resource_key": "bq", "sql": "SELECT * FROM t"},
        steps=[{"id": "c", "op": "chunk"}, {"id": "f", "op": "filter", "min_chars": 5}],
    )
    with pytest.raises(ValueError, match="aren't available as their own per-step SQL tables"):
        comp.build_defs(context=None)


@pytest.mark.parametrize("dialect", list(READ))
def test_new_ops_parse_in_every_dialect(mod, dialect):
    steps = [
        {"id": "c", "op": "chunk", "chunk_size": 200, "chunk_overlap": 0, "chunk_id_prefix": "kb:"},
        {"id": "f", "op": "filter", "min_chars": 5, "max_chars": 500, "min_alnum_ratio": 0.3, "exclude_patterns": [r"(?i)boilerplate"]},
        {"id": "dd", "op": "dedupe", "mode": "normalized"},
        {"id": "s", "op": "sensitivity", "detectors": ["email", "ssn", "ipv4"]},
        {"id": "st", "op": "stamp", "columns": {"scope": "support-{cust}"}, "ttl_days": 30},
        {"id": "k", "op": "classify", "candidate_labels": ["billing", "other"]},
        {"id": "e", "op": "embed", "model": "p.d.m1" if dialect != "bigquery" else "p.d.m"},
    ]
    sql = mod._build_sql_mode_query(
        steps, {"sql": "SELECT id, body, cust FROM ds.src"}, dialect, "ds.kb", "id", "body", ["cust"],
        asset_name="kb", code_version="v1", incremental=True,
    )
    sqlglot.parse_one(sql, read=READ[dialect])
