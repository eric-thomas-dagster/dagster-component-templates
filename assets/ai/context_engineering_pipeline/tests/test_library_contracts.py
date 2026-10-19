"""The pipeline's warehouse-resource helpers against the REAL google-cloud-bigquery client class and the REAL dagster-gcp resource.

``_ingest_warehouse_query`` (python mode) and ``_sql_session`` (sql mode) are driven with a ``google.cloud.bigquery.Client`` subclass whose
``query()`` binds its arguments against the real method signature and whose configs are the real classes (see real_bq.py); the
statement then runs on DuckDB. The bug class this guards: dagster-gcp's ``BigQueryResource.get_client()`` is a @contextmanager, and a helper
called ``.query`` on the un-entered context manager. Skips when google-cloud-bigquery is not installed. Proves nothing about real BigQuery."""
from types import SimpleNamespace

import pytest


pytest.importorskip("google.cloud.bigquery")
from .real_bq import ContractClient, REAL_BIGQUERY, install_real_bigquery  # noqa: E402

from .conftest import load_component_module  # noqa: E402


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture(autouse=True)
def real_library(monkeypatch):
    install_real_bigquery(monkeypatch)


@pytest.fixture()
def backend():
    c = ContractClient(project="proj")
    c._backend.con.execute("CREATE SCHEMA ds")
    c._backend.con.execute("CREATE TABLE ds.t(id VARCHAR, body VARCHAR)")
    c._backend.con.execute("INSERT INTO ds.t VALUES ('a', 'x'), ('b', 'it''s')")
    return c


class CtxMgrResource:
    """Shaped like dagster-gcp's BigQueryResource: get_client() is a @contextmanager."""

    def __init__(self, client):
        import contextlib

        @contextlib.contextmanager
        def get_client():
            yield client

        self.get_client = get_client


def ctx(resource):
    return SimpleNamespace(resources=SimpleNamespace(wh=resource), has_partition_key=False, has_partition_key_range=False, log=SimpleNamespace(warning=lambda *a, **k: None))


SRC = {"resource_key": "wh", "sql": "SELECT id, body FROM ds.t ORDER BY id"}


def test_ingest_through_a_context_manager_resource(mod, backend):
    df = mod._ingest_warehouse_query(SRC, ctx(CtxMgrResource(backend)))
    assert list(df["id"]) == ["a", "b"] and list(df["body"]) == ["x", "it's"]
    assert backend.sql() == ["SELECT id, body FROM ds.t ORDER BY id"]


def test_ingest_through_the_real_dagster_gcp_resource(mod, backend, monkeypatch):
    pytest.importorskip("dagster_gcp")
    import dagster_gcp

    made = []
    monkeypatch.setattr(REAL_BIGQUERY, "Client", lambda **kw: made.append(kw) or backend)
    df = mod._ingest_warehouse_query(SRC, ctx(dagster_gcp.BigQueryResource(project="proj", location="EU")))
    assert len(df) == 2 and made == [{"project": "proj", "location": "EU"}]


def test_ingest_with_a_bare_real_client_that_is_itself_a_context_manager(mod, backend):
    # the real bigquery.Client has __enter__ AND query: it must be used as is, not entered (exit would close the transport)
    assert hasattr(backend, "__enter__") and hasattr(backend, "query")
    res = SimpleNamespace(get_client=lambda: backend)
    assert len(mod._ingest_warehouse_query(SRC, ctx(res))) == 2
    assert backend.closed == 0


def test_sql_session_through_the_real_client_surface(mod, backend):
    with mod._sql_session(CtxMgrResource(backend), "wh") as session:
        session.execute("CREATE TABLE ds.u AS SELECT * FROM ds.t")
        rows = session.fetch("SELECT id, body FROM ds.u ORDER BY id")
    assert [tuple(r.values()) for r in rows] == [("a", "x"), ("b", "it's")]
    assert [c[0] for c in backend.calls] == ["query", "query"]


def test_sql_session_with_the_real_dagster_gcp_get_client(mod, backend, monkeypatch):
    pytest.importorskip("dagster_gcp")
    import dagster_gcp

    monkeypatch.setattr(REAL_BIGQUERY, "Client", lambda **kw: backend)
    with mod._sql_session(dagster_gcp.BigQueryResource(project="proj"), "wh") as session:
        assert len(session.fetch("SELECT id FROM ds.t")) == 2


def test_query_configs_are_validated_by_the_harness(backend):
    # guards the harness: an undeclared @param and a wrong keyword are both rejected like the real client would
    with pytest.raises(AssertionError):
        backend.query("SELECT @missing")
    with pytest.raises(TypeError):
        backend.query("SELECT 1", config=None)


# -- required_resource_keys: what Dagster really hands an asset for dagster-gcp's BigQueryResource is a bare GENERATOR ----------------------

def _run_asset_with_real_gcp_resource(monkeypatch, backend, body):
    import dagster as dg
    import dagster_gcp

    made = []
    monkeypatch.setattr(REAL_BIGQUERY, "Client", lambda **kw: made.append(kw) or backend)

    @dg.asset(required_resource_keys={"wh"})
    def probe(context):
        body(context)

    result = dg.materialize([probe], resources={"wh": dagster_gcp.BigQueryResource(project="proj")}, raise_on_error=False)
    failures = " ".join(str(e.event_specific_data) for e in result.all_events if e.is_failure)
    assert result.success, failures
    return made


def test_ingest_and_sql_session_via_resource_key_with_the_real_dagster_gcp_resource(mod, backend, monkeypatch):
    pytest.importorskip("dagster_gcp")
    seen = {}

    def body(context):
        assert not hasattr(context.resources.wh, "get_client")                  # the generator, not the resource: the bug this guards
        seen["df"] = mod._ingest_warehouse_query(SRC, context)
        for _ in range(2):                                                      # the SAME shared generator serves every call in the step
            with mod._sql_session(getattr(context.resources, "wh"), "wh") as session:
                seen.setdefault("n", []).append(len(session.fetch("SELECT id FROM ds.t")))
                session.execute("CREATE TABLE IF NOT EXISTS ds.v AS SELECT * FROM ds.t")

    made = _run_asset_with_real_gcp_resource(monkeypatch, backend, body)
    assert list(seen["df"]["id"]) == ["a", "b"] and seen["n"] == [2, 2]
    assert len(made) == 1                                                       # one client per step, however many calls use it


def test_vendor_client_session_shapes(mod):
    import contextlib

    class C:
        def query(self, sql):
            return sql

    c = C()
    closed = []

    def gen():
        try:
            yield c
        finally:
            closed.append(1)

    with mod._vendor_client_session(SimpleNamespace(get_client=gen)) as got:        # get_client() returning a bare generator
        assert got is c
    assert closed == [1]                                                            # our own generator is closed after use
    g = gen()
    for _ in range(3):                                                              # an injected generator is shared: reusable, closed by its owner
        with mod._vendor_client_session(g) as got:
            assert got is c
    assert closed == [1]
    with mod._vendor_client_session(c) as got:                                      # bare client
        assert got is c

    @contextlib.contextmanager
    def cm():
        yield c

    with mod._vendor_client_session(SimpleNamespace(get_client=cm)) as got:         # @contextmanager get_client
        assert got is c
    with mod._vendor_client_session(SimpleNamespace(get_client=lambda: c)) as got:  # get_client() returning a bare client
        assert got is c
