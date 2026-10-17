"""receipts: maintenance in execution_mode='sql' on BigQuery, Snowflake (snowflake_cortex) and Databricks: `rewrite_candidates` and `prune`.

EXECUTED (not just parsed): the statements run in DuckDB through bq_fake.py / sql_fake.py (sqlglot parses them in their own dialect first)
over tables loaded from a real ContextReceiptStore, and the results are compared with the store's own fold_maintenance_report (loaded
via importlib, never imported by the component). This is NOT BigQuery / Snowflake / Databricks: type-coercion corners of TIMESTAMP
comparisons can still differ there. The warehouse AI functions are not involved (chunk-only recipe).

Scenario (see test_sql_mode.build_scenario_store): kb has chunks kb:d1_0..2, kb:d2_0 and several kb:d3_* (never retrieved). Receipts in the
30-day window for kb: 4 with the current recipe v1 (r1-r4), 6 across all recipes (+ r5 'v0', r6 no recipe)."""
import re
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import dagster as dg
import pytest

from .bq_fake import FakeBigQueryResource
from .conftest import load_component_module
from .test_sql_mode import FEEDBACK_STEPS, _seed_matching_source, build_scenario_store, dump_store_to_warehouse, make_client, rows as bq_rows
from .test_sql_mode_dialects import DEFAULT_STYLE, DIALECTS, STYLES, sc  # noqa: F401  (sc: the snowflake / databricks fixture)
from .test_sql_mode import scenario  # noqa: F401  (the BigQuery fixture)

NEVER_RETRIEVED = ("kb:d1_3", "kb:d3_")        # chunks no receipt ever retrieved (d1 has a 4th chunk nobody got)
ALL_DIALECTS = ["bigquery", "snowflake_cortex", "databricks"]
SRC = {"sql": "SELECT id, body FROM ds.src"}


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(params=ALL_DIALECTS)
def dialect(request):
    return request.param


@pytest.fixture()
def wh(dialect, request, mod):
    if dialect == "bigquery":
        s = request.getfixturevalue("scenario")
        _seed_matching_source(s.client)
        return SimpleNamespace(dialect=dialect, dataset="proj.rcpt", store=s.store, run=lambda q: s.client.query(q),
                               rows=lambda q: bq_rows(s.client, q), raw=lambda q: s.client.con.execute(q),
                               resource=lambda: FakeBigQueryResource(s.client))
    s = request.getfixturevalue("sc")
    return SimpleNamespace(dialect=dialect, dataset=s.dataset, store=s.store, run=s.wh.run, rows=s.wh.rows, raw=lambda q: s.wh.con.execute(q),
                           resource=lambda: STYLES[DEFAULT_STYLE[dialect]](s.wh))


def rcfg(mod, wh, **kw):
    return mod._validate_receipts({"dataset": wh.dataset, **kw}, [], "sql", wh.dialect, {"resource_key": "wh"})


def q(mod, wh, rc, **extra):
    kw = dict(asset_name="kb", code_version="v1", stamp_provenance=True, receipts=rc, since=mod._receipts_since(rc))
    return mod._build_sql_mode_query(FEEDBACK_STEPS, SRC, wh.dialect, "ds.kb", "id", "body", [], **{**kw, **extra})


NOW = datetime.now(timezone.utc)
PRUNE0 = {"never_retrieved_after_receipts": 4, "min_age_days": 0}      # 4 = receipts of recipe v1 in the window


def build_table(mod, wh, rc, **extra):
    wh.run(q(mod, wh, rc, now=NOW.isoformat(), **extra))
    return {r["chunk_id"]: r for r in wh.rows("SELECT * FROM ds.kb ORDER BY chunk_id")}


# ── rewrite candidates == the store's maintenance report ─────────────────────────────────────────────────

@pytest.mark.parametrize("scope", [True, False])
def test_rewrite_candidates_equal_the_stores_maintenance_report(mod, wh, scope):
    rc = rcfg(mod, wh, min_retrievals=1, min_outcomes=1, rewrite_below=-0.2, scope_to_recipe=scope)
    got = wh.rows(q(mod, wh, rc, preview="rewrite"))
    theirs = wh.store.maintenance_report("kb", since=mod._receipts_since(rc), code_version="v1" if scope else None, min_retrievals=1,
                                         min_outcomes=1, bad_below=-0.2)["rewrite_candidates"]
    key = lambda cid, r, m, c, mo: (cid, r, m, mo, c)  # noqa: E731
    assert [(r["chunk_id"], r["feedback_retrievals"], r["feedback_rated"], r["feedback_cited"], pytest.approx(r["feedback_mean_outcome"]))
            for r in got] == [(f"{t['item_id']}", t["retrievals"], t["rated"], t["cited"], pytest.approx(t["mean_outcome"])) for t in theirs]
    assert len(got) >= 3 and [r["source_id"] for r in got] == [r["chunk_id"].split(":")[1].split("_")[0] for r in got]   # source id: d1 / d2
    assert [r["feedback_mean_outcome"] for r in got] == sorted(r["feedback_mean_outcome"] for r in got)             # worst first


def test_rewrite_thresholds_are_applied_and_quarantine_independent(mod, wh):
    rc = rcfg(mod, wh, min_retrievals=3, min_outcomes=3, rewrite_below=-0.2, quarantine={"mode": "exclude", "mean_outcome_below": -0.2})
    ids = [r["chunk_id"] for r in wh.rows(q(mod, wh, rc, preview="rewrite"))]
    assert ids == ["kb:d1_0"]                                    # only d1_0 has >= 3 retrievals and ratings (r1, r2, r4); mean < -0.2
    assert wh.rows(q(mod, wh, rcfg(mod, wh, min_retrievals=3, min_outcomes=3, rewrite_below=-0.99), preview="rewrite")) == []
    built = build_table(mod, wh, rc)                             # the exclude quarantine drops it from the table, not from the report
    assert "kb:d1_0" not in built


# ── prune ────────────────────────────────────────────────────────────────────────────────────────────────

def test_prune_flag_equals_the_stores_dead_weight_and_stamps_first_seen(mod, wh):
    rc = rcfg(mod, wh, scope_to_recipe=False, prune={**PRUNE0, "never_retrieved_after_receipts": 6, "mode": "flag"})   # unscoped receipts = 6
    built = build_table(mod, wh, rc)
    flagged = sorted(c for c, r in built.items() if r["feedback_dead_weight"])
    theirs = wh.store.maintenance_report("kb", since=mod._receipts_since(rc), known_item_ids=list(built), dead_after_receipts=6)["dead_weight"]
    assert flagged == theirs and len(flagged) >= 2 and all(c.startswith(NEVER_RETRIEVED) for c in flagged)
    assert all(r["feedback_first_seen_at"] is not None for r in built.values())
    counts = wh.rows(q(mod, wh, rc, now=NOW.isoformat(), preview="counts"))[0]
    values = list(counts.values())
    assert (int(values[0]), int(values[1])) == (len(flagged), 6)


def test_prune_exclude_drops_the_dead_rows_and_changes_the_content_version(mod, wh):
    flag = build_table(mod, wh, rcfg(mod, wh, prune={**PRUNE0, "mode": "flag"}))
    dead = {c for c, r in flag.items() if r["feedback_dead_weight"]}
    assert dead and all(c.startswith(NEVER_RETRIEVED) for c in dead)
    ex = build_table(mod, wh, rcfg(mod, wh, prune={**PRUNE0, "mode": "exclude"}))
    assert set(ex) == set(flag) - dead and "feedback_dead_weight" not in next(iter(ex.values()))
    assert {r["source_version"] for r in ex.values()} != {r["source_version"] for r in flag.values()}


def dead_after(mod, wh, rc, sql_mutation=None, **extra):
    sql = q(mod, wh, rc, now=NOW.isoformat(), **extra)
    if sql_mutation:
        new = sql_mutation(sql)
        assert new != sql, "mutation did not apply"
        sql = new
    wh.run(sql)
    return sorted(c for c, in ((r["chunk_id"],) for r in wh.rows("SELECT chunk_id, feedback_dead_weight FROM ds.kb") if r["feedback_dead_weight"]))


def test_footgun_not_enough_receipts_flags_nothing_and_the_guard_is_mutation_checked(mod, wh):
    rc = rcfg(mod, wh, prune={**PRUNE0, "never_retrieved_after_receipts": 5, "mode": "flag"})       # 4 receipts of v1 < 5
    assert dead_after(mod, wh, rc) == []
    mutate = lambda sql: sql.replace("(SELECT n FROM window_receipts) >= 5", "(SELECT n FROM window_receipts) >= 0")  # noqa: E731
    assert dead_after(mod, wh, rc, mutate) != []


def test_footgun_a_recipe_change_is_not_mistaken_for_no_traffic_and_the_scope_is_mutation_checked(mod, wh):
    rc = rcfg(mod, wh, prune={**PRUNE0, "never_retrieved_after_receipts": 5, "mode": "flag"})       # 4 receipts of v1, 6 of all recipes
    assert dead_after(mod, wh, rc) == []

    def mutate(sql):                                              # drop the recipe filter from the window_receipts CTE only
        head, tail = sql.split("window_receipts AS (", 1)
        return head + "window_receipts AS (" + re.sub(r" AND COALESCE\(i\.source_code_version, 'unknown'\) = 'v1'", "", tail, count=1)
    assert dead_after(mod, wh, rc, mutate) != []                  # 6 receipts >= 5: would have judged absence under the NEW recipe


def test_footgun_new_chunks_are_protected_for_min_age_and_the_guard_is_mutation_checked(mod, wh):
    rc = rcfg(mod, wh, prune={"never_retrieved_after_receipts": 4, "min_age_days": 30, "mode": "flag"})
    assert dead_after(mod, wh, rc) == []                          # nothing has existed for 30 days: all new
    mutate = lambda sql: re.sub(r" AND feedback_first_seen_at <= (TIMESTAMP '[^']*'|CAST\('[^']*' AS TIMESTAMP_TZ\))", "", sql, count=1)  # noqa: E731
    assert dead_after(mod, wh, rc, mutate) != []
    counts = wh.rows(q(mod, wh, rc, now=NOW.isoformat(), preview="counts"))[0]
    assert int(list(counts.values())[0]) == 0                    # counts preview agrees: protected, not dead


def test_first_seen_is_carried_over_from_the_previous_table_so_old_chunks_age_and_new_ones_do_not(mod, wh):
    rc = rcfg(mod, wh, prune={"never_retrieved_after_receipts": 4, "min_age_days": 30, "mode": "flag"})
    t0 = NOW - timedelta(days=60)
    wh.run(q(mod, wh, rc, now=t0.isoformat()))                    # first build, 60 days ago: no prior table
    first = {r["chunk_id"]: r["feedback_first_seen_at"] for r in wh.rows("SELECT chunk_id, feedback_first_seen_at FROM ds.kb")}
    assert len(set(first.values())) == 1
    wh.raw("INSERT INTO ds.src VALUES ('d4', 'brand new doc')")      # a new source row appears since
    kw = dict(now=NOW.isoformat(), prior_first_seen=True)
    wh.run(q(mod, wh, rc, **kw))
    now_rows = {r["chunk_id"]: r for r in wh.rows("SELECT * FROM ds.kb")}
    assert all(now_rows[c]["feedback_first_seen_at"] == first[c] for c in first)                # carried over, not re-stamped
    dead = {c for c, r in now_rows.items() if r["feedback_dead_weight"]}
    assert dead and all(c.startswith(NEVER_RETRIEVED) for c in dead)                                      # old + never retrieved
    new = [c for c in now_rows if c.startswith("kb:d4_")]
    assert new and not any(now_rows[c]["feedback_dead_weight"] for c in new)                       # new + never retrieved: protected
    wh.run(q(mod, wh, rc, now=NOW.isoformat(), prior_first_seen=False))                           # mutation: forget the prior table
    assert not any(r["feedback_dead_weight"] for r in wh.rows("SELECT * FROM ds.kb"))


# ── real Dagster materialization: summary metadata + the prior-table probe ───────────────────────────────

def component(mod, wh, **receipts):
    return mod.ContextEngineeringPipelineComponent(
        asset_name="kb", source={"kind": "warehouse_query", "resource_key": "wh", "sql": SRC["sql"]}, id_column="id", text_column="body",
        metadata_columns=[], execution_mode="sql", sql_dialect=wh.dialect, output_table="ds.kb", code_version="v1", steps=FEEDBACK_STEPS,
        receipts={"dataset": wh.dataset, "min_retrievals": 1, "min_outcomes": 1, "rewrite_below": -0.2, **receipts})


def materialize(mod, wh, **receipts):
    asset_def = list(component(mod, wh, **receipts).build_defs(None).assets)[0]
    res = dg.materialize([asset_def], resources={"wh": wh.resource()}, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    return res.get_asset_materialization_events()[0].materialization.metadata


@pytest.mark.parametrize("mode", ["flag", "exclude"])
def test_materialize_reports_rewrite_candidates_and_pruning_and_keeps_first_seen_across_runs(mod, wh, mode):
    md = materialize(mod, wh, prune={**PRUNE0, "mode": mode})
    s = md["receipts_feedback"].value
    assert s["rewrite_candidates"] and all(set(c) == {"chunk_id", "source_id", "retrievals", "rated", "mean_outcome", "cited"} for c in s["rewrite_candidates"])
    assert {c["source_id"] for c in s["rewrite_candidates"]} <= {"d1", "d2"}
    n_dead = md["receipts_pruned"].value
    assert n_dead >= 2 and s["pruned"] == n_dead and s["prune_mode"] == mode and s["prune_judged"] is True and s["prune_receipts_in_window"] == 4
    left = wh.rows("SELECT chunk_id, feedback_first_seen_at FROM ds.kb")
    assert any(r["chunk_id"].startswith(NEVER_RETRIEVED) for r in left) == (mode == "flag")      # exclude: dead rows are gone from the table
    seen = {r["chunk_id"]: r["feedback_first_seen_at"] for r in left}
    materialize(mod, wh, prune={**PRUNE0, "mode": mode})          # second run: the table exists with the column -> prior first_seen is read back
    again = {r["chunk_id"]: r["feedback_first_seen_at"] for r in wh.rows("SELECT chunk_id, feedback_first_seen_at FROM ds.kb")}
    assert {c: again[c] for c in seen if c in again} == seen and len(again) >= len(seen) - 0   # survivors keep their first_seen_at (later `now` would differ)


def test_materialize_without_prune_still_reports_rewrite_candidates(mod, wh):
    md = materialize(mod, wh)
    s = md["receipts_feedback"].value
    assert s["rewrite_candidates"] and "pruned" not in s and "receipts_pruned" not in md
    assert wh.rows("SELECT * FROM ds.kb")[0].get("feedback_first_seen_at") is None


def test_sql_mode_config_accepts_prune_and_rejects_bad_values(mod, wh):
    assert rcfg(mod, wh, prune={})["prune"]["never_retrieved_after_receipts"] == 200
    with pytest.raises(ValueError, match="flag' or 'exclude"):
        rcfg(mod, wh, prune={"mode": "drop"})
    assert mod._SQL_RESERVED_COLUMNS >= {"feedback_dead_weight", "feedback_first_seen_at"}
