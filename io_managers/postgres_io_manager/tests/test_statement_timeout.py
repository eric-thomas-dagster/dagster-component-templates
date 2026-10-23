"""`statement_timeout_seconds` on PostgresIOManager (pgbouncer transaction-mode
compatibility -- dagster-io/dagster#33416).

A plain session-level `SET statement_timeout` (or a connection-string
`options=-c statement_timeout=...`) silently stops applying once the
connection goes through a transaction-pooled proxy like pgbouncer, because
different statements in the same client "session" can land on different
backend connections. `SET LOCAL` is scoped to -- and reset at the end of --
the current transaction, so it stays correct regardless of pooling mode.
These tests confirm the IO manager issues exactly that statement, inside the
same transaction as the real query, only when configured.
"""
import pandas as pd
import pytest
import sqlalchemy as sa
import dagster as dg

from .conftest import load_component_module

mod = load_component_module()


class RecordingSqliteManager(mod.PostgresIOManager):
    """Same sqlite substitution as test_partition_ranges.py, plus a SET LOCAL shim.

    Executed statements are recorded into a module-level list, not an instance
    attribute: dg.materialize() resolves/binds a ConfigurableIOManager into its
    own instance at resource-init time (confirmed directly -- mutating `self`
    on the object passed into `resources={...}` is NOT visible afterward on
    that same object), so a per-instance recording list would silently record
    nothing.
    """

    db_dir: str = ""

    def _get_engine(self):
        engine = sa.create_engine(f"sqlite:///{self.db_dir}/main.db")
        public = f"{self.db_dir}/public.db"

        @sa.event.listens_for(engine, "connect")
        def _attach(dbapi_conn, _rec):
            dbapi_conn.execute(f"ATTACH DATABASE '{public}' AS public")

        @sa.event.listens_for(engine, "before_cursor_execute", retval=True)
        def _shim(conn, cursor, statement, parameters, context, executemany):
            RECORDED_STATEMENTS.append(statement)
            if statement.startswith("SET LOCAL"):
                return "SELECT 1", ()
            if statement.startswith("CREATE SCHEMA"):
                return "SELECT 1", ()
            if "information_schema.tables" in statement:
                return (
                    "SELECT 1 FROM public.sqlite_master WHERE type='table' AND 'public' = ? AND name = ?",
                    parameters,
                )
            return statement, parameters

        return engine


RECORDED_STATEMENTS: list = []


@pytest.fixture(autouse=True)
def _reset_recorded_statements():
    RECORDED_STATEMENTS.clear()
    yield


@pytest.fixture
def mgr(tmp_path):
    return RecordingSqliteManager(database="d", user="u", db_dir=str(tmp_path))


def test_no_statement_timeout_configured_issues_no_set_local(tmp_path):
    @dg.asset
    def events() -> pd.DataFrame:
        return pd.DataFrame({"v": [1, 2]})

    m = RecordingSqliteManager(database="d", user="u", db_dir=str(tmp_path))
    result = dg.materialize([events], resources={"io_manager": m})
    assert result.success
    assert not any(s.startswith("SET LOCAL") for s in RECORDED_STATEMENTS)


def test_statement_timeout_seconds_issues_set_local_before_the_write(tmp_path):
    @dg.asset
    def events() -> pd.DataFrame:
        return pd.DataFrame({"v": [1, 2]})

    m = RecordingSqliteManager(database="d", user="u", db_dir=str(tmp_path), statement_timeout_seconds=30)
    result = dg.materialize([events], resources={"io_manager": m})
    assert result.success
    set_local = [s for s in RECORDED_STATEMENTS if s.startswith("SET LOCAL")]
    assert set_local == ["SET LOCAL statement_timeout = 30000"]
    # Issued before the real work in the SAME transaction, not as a separate session-level call.
    assert RECORDED_STATEMENTS.index(set_local[0]) < RECORDED_STATEMENTS.index(
        next(s for s in RECORDED_STATEMENTS if "CREATE SCHEMA" in s)
    )


def test_statement_timeout_seconds_applies_to_load_input_too(tmp_path):
    @dg.asset
    def upstream() -> pd.DataFrame:
        return pd.DataFrame({"v": [1, 2]})

    @dg.asset
    def downstream(upstream: pd.DataFrame) -> pd.DataFrame:
        return upstream

    m = RecordingSqliteManager(database="d", user="u", db_dir=str(tmp_path), statement_timeout_seconds=5)
    result = dg.materialize([upstream, downstream], resources={"io_manager": m})
    assert result.success
    assert RECORDED_STATEMENTS.count("SET LOCAL statement_timeout = 5000") >= 2  # handle_output + load_input


def test_component_threads_statement_timeout_seconds_through():
    comp = mod.PostgresIOManagerComponent(
        database="d", user="u", password_env_var="PGPASS", statement_timeout_seconds=15,
    )
    io_manager = comp.build_defs(None).resources["io_manager"]
    assert io_manager.statement_timeout_seconds == 15
