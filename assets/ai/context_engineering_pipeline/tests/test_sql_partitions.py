"""Partitioned execution_mode='sql' (and the python-mode warehouse_query source): partition placeholders, no cross-partition clobbering, backfill policy.

Executed here (sqlglot -> DuckDB through the fakes, real dg.materialize, all three dialects): partition A then B leaves both, re-running A replaces
only A's rows, placeholders are escaped, multi-dimension keys bind per axis, a single_run backfill range runs. The partition swap itself is
BigQuery's BEGIN TRANSACTION script / Snowflake's BEGIN..COMMIT / Delta's INSERT ... REPLACE WHERE: the fakes run the first two as written
(transaction semantics are DuckDB's) and emulate REPLACE WHERE as DELETE + INSERT (atomicity not modelled). The real engines are not run."""
from types import SimpleNamespace

import dagster as dg
import pytest
from dagster import MultiPartitionKey

from .conftest import load_component_module
from .test_sql_incremental import READ, Harness, h as _unused  # noqa: F401  (Harness, READ)

SRC_SQL = "SELECT id, body FROM ds.src WHERE part = {partition_key}"


@pytest.fixture()
def mod():
    return load_component_module()


class PHarness(Harness):
    PARTS = "A,B"

    def set_docs(self, docs):
        self.docs = docs

    def set_src(self, rows):
        """rows: (id, body, part[, ts])"""
        self.con.execute("DROP TABLE IF EXISTS ds.src")
        self.con.execute("CREATE TABLE ds.src(id VARCHAR, body VARCHAR, part VARCHAR, ts VARCHAR)")
        for r in rows:
            r = tuple(r) + (None,) * (4 - len(r))
            self.con.execute("INSERT INTO ds.src VALUES (?, ?, ?, ?)", list(r))

    def run(self, partition_key=None, sql=SRC_SQL, partition_values="A,B", **kw):
        kw.setdefault("source", {"kind": "warehouse_query", "resource_key": "wh", "sql": sql})
        if "partition_dimensions" not in kw and "partition_type" not in kw:
            kw.update(partition_type="static", partition_values=partition_values)
        self.ai.reset()
        asset_def = list(self.component(**kw).build_defs(None).assets)[0]
        instance = dg.DagsterInstance.ephemeral()
        res = dg.materialize([asset_def], resources={"wh": self.resource}, partition_key=partition_key, instance=instance)
        assert res.success
        self.last = (res, instance)
        self.event = res.get_asset_materialization_events()[0].materialization
        return self.event.metadata

    def by_partition(self):
        out = {}
        for r in self.table_rows():
            out.setdefault(r["source_partition"], []).append(r["chunk_text"])
        return {k: sorted(v) for k, v in out.items()}


@pytest.fixture(params=list(READ))
def p(request, mod):
    harness = PHarness(mod, request.param)
    harness.set_src([("a1", "alpha one", "A"), ("a2", "alpha two", "A"), ("b1", "beta one", "B"), ("b2", "alpha two", "B")])
    yield harness
    harness.wh.close()


# ── no cross-partition clobbering ───────────────────────────────────────────────────────────────────────

def test_partition_a_then_b_leaves_both(p):
    p.run("A")
    assert p.by_partition() == {"A": ["alpha one", "alpha two"]}
    p.run("B")
    assert p.by_partition() == {"A": ["alpha one", "alpha two"], "B": ["alpha two", "beta one"]}
    # the staging table is gone and nothing else was left behind
    assert not [r for r in p.con.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%stage%'").fetchall()]


def test_rerun_of_a_replaces_only_a_without_duplicates_and_reuses_embeddings(p):
    p.run("A")
    p.run("B")
    p.set_src([("a1", "alpha one", "A"), ("a2", "alpha TWO", "A"), ("b1", "beta one", "B"), ("b2", "alpha two", "B")])
    md = p.run("A")
    assert p.by_partition() == {"A": ["alpha TWO", "alpha one"], "B": ["alpha two", "beta one"]}      # A replaced, B untouched, no duplicates
    assert [t for _, t in p.ai.embed] == ["alpha TWO"]                                                   # 'alpha one' reused from A's own prior rows
    assert md["dagster/row_count"].value == 2 and md["new_embeddings"].value == 1 and md["reused_embeddings"].value == 1


def test_embeddings_are_reused_across_partitions(p):
    p.run("A")
    p.run("B")                                    # B's 'alpha two' is also in A: only 'beta one' is new
    assert [t for _, t in p.ai.embed] == ["beta one"]


def test_data_version_and_row_count_describe_only_this_partition(p):
    p.run("A")
    va, row_a = p.event.metadata["context_version"].value, p.event.metadata["dagster/row_count"].value
    p.run("B")
    vb = p.event.metadata["context_version"].value
    assert row_a == 2 and p.event.metadata["dagster/row_count"].value == 2 and va != vb
    assert p.event.tags["dagster/data_version"] == vb
    p.run("A")
    assert p.event.metadata["context_version"].value == va                # unchanged partition A: same version, whatever B holds


def test_source_partition_is_written_even_without_stamp_provenance(p):
    p.run("A", stamp_provenance=False)
    assert {r["source_partition"] for r in p.table_rows()} == {"A"}
    assert "source_version" not in p.table_rows()[0]


def test_unpartitioned_run_still_replaces_the_whole_table(p):
    p.run("A")
    p.set_src([("a1", "alpha one", "A")])
    p.ai.reset()
    asset_def = list(p.component(source={"kind": "warehouse_query", "resource_key": "wh", "sql": "SELECT id, body FROM ds.src"}).build_defs(None).assets)[0]
    assert dg.materialize([asset_def], resources={"wh": p.resource}).success
    assert [r["chunk_text"] for r in p.table_rows()] == ["alpha one"]


def test_mutation_create_or_replace_for_every_partition_is_caught(p, monkeypatch):
    """With the old CREATE OR REPLACE behaviour partition B wipes A: the assertion test_partition_a_then_b_leaves_both makes must fail."""
    real_build, real_cols = p.mod._build_sql_mode_query, p.mod._sql_table_columns
    monkeypatch.setattr(p.mod, "_sql_table_columns", lambda *a, **k: [])
    monkeypatch.setattr(p.mod, "_build_sql_mode_query", lambda *a, **k: real_build(*a, **k).replace("CREATE TABLE ", "CREATE OR REPLACE TABLE ", 1))
    p.run("A")
    p.run("B")
    assert "A" not in p.by_partition()            # A's rows are gone: the unmutated test would fail here


def test_existing_table_without_source_partition_is_refused_not_clobbered(p):
    p.con.execute(f"CREATE TABLE {p.duck}(chunk_id VARCHAR, content_hash VARCHAR, chunk_text VARCHAR)")
    p.con.execute(f"INSERT INTO {p.duck} VALUES ('x', 'y', 'precious')")
    with pytest.raises(Exception, match="source_partition"):
        p.run("A")
    assert p.con.execute(f"SELECT chunk_text FROM {p.duck}").fetchall() == [("precious",)]


def test_a_failed_ai_step_leaves_the_other_partitions_and_the_old_rows_alone(p):
    p.run("A")
    p.run("B")
    before = p.table_rows()
    p.set_src([("a1", "alpha one", "A"), ("a2", "brand new text", "A"), ("b1", "beta one", "B")])

    def boom(model, text):
        raise RuntimeError("AI quota exceeded")

    for name in ("ai_embed", "ai_query"):
        p.con.remove_function(name)
        p.con.create_function(name, boom, ["VARCHAR", "VARCHAR"], "DOUBLE[]")
    with pytest.raises(Exception):
        p.run("A")
    assert p.table_rows() == before                                   # old A rows intact, B intact
    assert not p.con.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%stage%'").fetchall()   # stage cleaned up


# ── placeholders ────────────────────────────────────────────────────────────────────────────────────────

def test_partition_key_with_a_quote_is_escaped(p):
    p.set_src([("o1", "irish text", "O'Brien"), ("o2", "other", "A")])
    p.run("O'Brien", partition_values="A,O'Brien")
    assert p.by_partition() == {"O'Brien": ["irish text"]}
    assert "'O'Brien'" not in p.last[0].get_asset_materialization_events()[0].materialization.metadata["generated_sql"].value.replace("O\\'Brien", "")


def test_time_window_placeholders(p):
    p.set_src([("t1", "in window", "x", "2026-01-02T10:00:00+00:00"), ("t2", "next day", "x", "2026-01-03T10:00:00+00:00"),
               ("t0", "day before", "x", "2026-01-01T23:59:59+00:00")])
    sql = "SELECT id, body FROM ds.src WHERE ts >= {partition_start} AND ts < {partition_end}"
    p.run("2026-01-02", sql=sql, partition_type="daily", partition_start="2026-01-01")
    assert p.by_partition() == {"2026-01-02": ["in window"]}
    p.run("2026-01-03", sql=sql, partition_type="daily", partition_start="2026-01-01")
    assert p.by_partition() == {"2026-01-02": ["in window"], "2026-01-03": ["next day"]}


def test_multi_dimension_partition_key_binds_per_axis_and_backfill_range_is_not_needed(p):
    p.set_src([("u1", "us text", "us", "2026-01-02"), ("e1", "eu text", "eu", "2026-01-02"), ("u2", "us later", "us", "2026-01-03")])
    dims = [{"name": "date", "type": "daily", "start": "2026-01-01"}, {"name": "region", "type": "static", "values": ["us", "eu"]}]
    sql = "SELECT id, body FROM ds.src WHERE ts = {partition_key.date} AND part = {partition_key.region}"
    p.run(MultiPartitionKey({"date": "2026-01-02", "region": "us"}), sql=sql, partition_dimensions=dims)
    p.run(MultiPartitionKey({"date": "2026-01-02", "region": "eu"}), sql=sql, partition_dimensions=dims)
    assert p.by_partition() == {"2026-01-02|us": ["us text"], "2026-01-02|eu": ["eu text"]}


def test_multi_dimension_with_time_axis_gets_partition_start_end(p):
    p.set_src([("u1", "us text", "us", "2026-01-02T05:00:00+00:00"), ("u2", "us later", "us", "2026-01-03T05:00:00+00:00")])
    dims = [{"name": "date", "type": "daily", "start": "2026-01-01"}, {"name": "region", "type": "static", "values": ["us", "eu"]}]
    sql = "SELECT id, body FROM ds.src WHERE ts >= {partition_start} AND ts < {partition_end} AND part = {partition_key.region}"
    p.run(MultiPartitionKey({"date": "2026-01-02", "region": "us"}), sql=sql, partition_dimensions=dims)
    assert p.by_partition() == {"2026-01-02|us": ["us text"]}


def test_partitioned_source_without_a_placeholder_warns(p):
    p.run("A", sql="SELECT id, body, part FROM ds.src")
    assert any("reads the FULL source" in m for m in p.logs())


def _ctx(**kw):
    base = dict(has_partition_key=False, has_partition_key_range=False, log=SimpleNamespace(warning=lambda m: None),
                assets_def=SimpleNamespace(partitions_def=None))
    base.update(kw)
    return SimpleNamespace(**base)


def test_placeholder_errors(mod):
    quote = mod._sql_str
    with pytest.raises(ValueError, match="no partition key"):
        mod._bind_partition_placeholders("SELECT {partition_key}", _ctx(), quote)
    keyed = _ctx(has_partition_key=True, has_partition_key_range=True, partition_key="A")
    with pytest.raises(ValueError, match=r"placeholder \{nope\} is not available"):
        mod._bind_partition_placeholders("SELECT {nope}", keyed, quote)
    with pytest.raises(ValueError, match="remove the quotes"):
        mod._bind_partition_placeholders("SELECT '{partition_key}'", keyed, quote)
    with pytest.raises(ValueError, match=r"partition_start\} is not available"):
        mod._bind_partition_placeholders("SELECT {partition_start}", keyed, quote)     # static partitions have no time window
    assert mod._bind_partition_placeholders("SELECT {partition_key}, '{{literal}}'", keyed, quote) == "SELECT 'A', '{literal}'"
    assert mod._bind_partition_placeholders("SELECT 1 -- {{x}}", _ctx(), quote) == "SELECT 1 -- {x}"
    assert mod._bind_partition_placeholders("SELECT {partition_key}", _ctx(has_partition_key=True, has_partition_key_range=True, partition_key="it's"), quote) \
        == "SELECT 'it\\'s'"


def test_build_time_placeholder_validation(mod):
    base = dict(asset_name="kb", id_column="id", text_column="body", execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb",
                steps=[{"id": "c", "op": "chunk"}])
    src = lambda sql: {"kind": "warehouse_query", "resource_key": "wh", "sql": sql}  # noqa: E731
    C = mod.ContextEngineeringPipelineComponent
    with pytest.raises(ValueError, match="no partitioning is configured"):
        C(source=src("SELECT {partition_key}"), **base).build_defs(None)
    with pytest.raises(ValueError, match="unknown placeholder"):
        C(source=src("SELECT {oops}"), partition_type="static", partition_values="a,b", **base).build_defs(None)
    with pytest.raises(ValueError, match="needs a time-window partition"):
        C(source=src("SELECT {partition_start}"), partition_type="static", partition_values="a,b", **base).build_defs(None)
    with pytest.raises(ValueError, match=r"partition_key\.region|unknown placeholder"):
        C(source=src("SELECT {partition_key.region}"), partition_type="daily", partition_start="2026-01-01", **base).build_defs(None)


# ── multi-axis partitions and backfill policy ───────────────────────────────────────────────────────────

def test_partition_dimensions_shapes(mod):
    f = mod._build_partitions_def
    dims = [{"name": "d", "type": "daily", "start": "2026-01-01"}, {"name": "r", "type": "static", "values": "x, y"}]
    assert type(f(None, None, None, None, dims)).__name__ == "MultiPartitionsDefinition"
    assert type(f(None, None, None, None, dims[:1])).__name__ == "DailyPartitionsDefinition"
    assert f(None, None, None, None, None) is None and f("static", None, "a,b", None).get_partition_keys() == ["a", "b"]
    with pytest.raises(ValueError, match="not both"):
        f("daily", "2026-01-01", None, None, dims)
    with pytest.raises(ValueError, match="requires 'start'"):
        f(None, None, None, None, [{"name": "d", "type": "daily"}])
    with pytest.raises(ValueError, match="unique `name`"):
        f(None, None, None, None, [dims[0], dims[0]])


def test_backfill_policy_validation_and_wiring(mod):
    base = dict(asset_name="kb", id_column="id", text_column="body", execution_mode="sql", sql_dialect="bigquery", output_table="ds.kb",
                steps=[{"id": "c", "op": "chunk"}], partition_type="daily", partition_start="2026-01-01")
    src = lambda sql: {"kind": "warehouse_query", "resource_key": "wh", "sql": sql}  # noqa: E731
    C = mod.ContextEngineeringPipelineComponent
    asset = lambda c: list(c.build_defs(None).assets)[0]  # noqa: E731
    with pytest.raises(ValueError, match="only safe when `source.sql` selects the whole backfill range"):
        C(source=src("SELECT * FROM t WHERE d = {partition_key}"), backfill_policy="single_run", **base).build_defs(None)
    with pytest.raises(ValueError, match="only safe"):
        C(source=src("SELECT * FROM t WHERE d >= {partition_start}"), backfill_policy="single_run", **base).build_defs(None)
    ok = asset(C(source=src("SELECT * FROM t WHERE ts >= {partition_start} AND ts < {partition_end}"), backfill_policy="single_run", **base))
    assert ok.backfill_policy.policy_type.value == "SINGLE_RUN"
    multi = asset(C(source=src("SELECT * FROM t WHERE d = {partition_key}"), backfill_policy="multi_run", backfill_policy_max_partitions_per_run=7, **base))
    assert multi.backfill_policy.max_partitions_per_run == 7
    dims = {k: v for k, v in base.items() if not k.startswith("partition_")}
    dims["partition_dimensions"] = [{"name": "d", "type": "daily", "start": "2026-01-01"}, {"name": "r", "type": "static", "values": ["a"]}]
    with pytest.raises(ValueError, match="multi-dimension"):
        C(source=src("SELECT {partition_start}, {partition_end}"), backfill_policy="single_run", **dims).build_defs(None)
    # python mode: single_run is allowed (the source is filtered in memory / by the placeholders you choose)
    py = {**base, "execution_mode": "python", "sql_dialect": None, "output_table": None}
    assert asset(C(source=src("SELECT 1"), backfill_policy="single_run", **py)).backfill_policy.policy_type.value == "SINGLE_RUN"
    # unpartitioned: ignored
    none = {k: v for k, v in base.items() if not k.startswith("partition_")}
    assert asset(C(source=src("SELECT 1"), backfill_policy="single_run", **none)).backfill_policy is None


def test_single_run_backfill_range_runs_once_over_the_whole_range(p):
    from dagster._core.definitions.asset_checks.asset_check_spec import AssetCheckSpec  # noqa: F401
    p.set_src([("t1", "day one", "x", "2026-01-02T10:00:00+00:00"), ("t2", "day two", "x", "2026-01-03T10:00:00+00:00"),
               ("t3", "day four", "x", "2026-01-05T10:00:00+00:00")])
    sql = "SELECT id, body FROM ds.src WHERE ts >= {partition_start} AND ts < {partition_end}"
    kw = dict(sql=sql, partition_type="daily", partition_start="2026-01-01", backfill_policy="single_run")
    p.ai.reset()
    asset_def = list(p.component(source={"kind": "warehouse_query", "resource_key": "wh", "sql": sql}, partition_type="daily",
                                 partition_start="2026-01-01", backfill_policy="single_run").build_defs(None).assets)[0]
    tags = {"dagster/asset_partition_range_start": "2026-01-02", "dagster/asset_partition_range_end": "2026-01-03"}
    res = dg.materialize([asset_def], resources={"wh": p.resource}, tags=tags, instance=dg.DagsterInstance.ephemeral())
    assert res.success
    assert p.by_partition() == {"2026-01-02..2026-01-03": ["day one", "day two"]}
    # re-running the same range replaces it (no duplicates)
    assert dg.materialize([asset_def], resources={"wh": p.resource}, tags=tags, instance=dg.DagsterInstance.ephemeral()).success
    assert p.by_partition() == {"2026-01-02..2026-01-03": ["day one", "day two"]}
    # a later single-day run replaces its own key (and any range label it is part of is NOT touched: documented limit of single_run)
    assert dg.materialize([asset_def], resources={"wh": p.resource}, partition_key="2026-01-05", instance=dg.DagsterInstance.ephemeral()).success
    assert p.by_partition()["2026-01-05"] == ["day four"]


# ── python mode: the warehouse_query source honours the same placeholders ──────────────────────────────────

def test_python_mode_source_binds_partition_placeholders(mod):
    import duckdb
    con = duckdb.connect()
    con.execute("CREATE TABLE src(id VARCHAR, body VARCHAR, part VARCHAR)")
    con.execute("INSERT INTO src VALUES ('1', 'irish', 'O''Brien'), ('2', 'other', 'A')")

    class Res:
        def get_connection(self):
            import contextlib

            @contextlib.contextmanager
            def cm():
                yield con
            return cm()

    ctx = _ctx(has_partition_key=True, has_partition_key_range=True, partition_key="O'Brien", resources=SimpleNamespace(wh=Res()))
    df = mod._ingest_warehouse_query({"resource_key": "wh", "sql": "SELECT id, body FROM src WHERE part = {partition_key}"}, ctx)
    assert df["body"].tolist() == ["irish"]
    with pytest.raises(ValueError, match="no partition key"):
        mod._ingest_warehouse_query({"resource_key": "wh", "sql": "SELECT * FROM src WHERE part = {partition_key}"},
                                    _ctx(resources=SimpleNamespace(wh=Res())))
    con.close()


@pytest.mark.parametrize("dialect", list(READ))
def test_partition_statements_parse_in_their_dialect(mod, dialect):
    import sqlglot
    steps = [{"id": "c", "op": "chunk"}, {"id": "k", "op": "classify", "candidate_labels": ["a", "b"]}, {"id": "e", "op": "embed", "model": "p.d.m"}]
    src = {"sql": "SELECT id, body FROM ds.src"}
    stage = mod._sql_stage_table("proj.ds.kb", "2026-01-01")
    for write, kw in (("create_table", {}), ("stage", {"stage_table": stage})):
        sql = mod._build_sql_mode_query(steps, src, dialect, "proj.ds.kb", "id", "body", [], asset_name="kb", code_version="v1", partition="it's",
                                        incremental=True, reuse={"embed": True, "classify": True}, write=write, **kw)
        sqlglot.parse_one(sql, read=READ[dialect])
        assert sql.startswith("CREATE TABLE " if write == "create_table" else "CREATE OR REPLACE TABLE ")
    for stmt in mod._sql_swap_partition_script(dialect, "proj.ds.kb", stage, ["id", "source_partition"], "a..b", ["a", "b"]):
        sqlglot.parse(stmt, read=READ[dialect])


def test_partitioned_example_yaml_builds_and_its_sql_parses(mod):
    from pathlib import Path
    import sqlglot
    import yaml
    attrs = yaml.safe_load((Path(mod.__file__).parent / "example_bigquery_sql_partitioned.yaml").read_text())["attributes"]
    asset_def = list(mod.ContextEngineeringPipelineComponent(**attrs).build_defs(None).assets)[0]
    assert asset_def.backfill_policy.max_partitions_per_run == 4 and type(asset_def.partitions_def).__name__ == "MultiPartitionsDefinition"
    bound = mod._bind_partition_placeholders(attrs["source"]["sql"], _ctx(
        has_partition_key=True, has_partition_key_range=True, partition_key=MultiPartitionKey({"date": "2026-01-02", "region": "us"}),
        assets_def=SimpleNamespace(partitions_def=asset_def.partitions_def)), mod._sql_str)
    sqlglot.parse_one(bound, read="bigquery")
    assert "'us'" in bound and "2026-01-02T00:00:00+00:00" in bound and "2026-01-03T00:00:00+00:00" in bound
    assert [s.name for s in asset_def.check_specs] == ["retrieval_quality"]       # the example's golden-set gate
    sqlglot.parse_one(mod._build_retrieval_eval_query(attrs["retrieval_eval"], attrs["steps"], "bigquery", attrs["output_table"], attrs["id_column"],
                                                      asset_name=attrs["asset_name"], scoped=True), read="bigquery")
