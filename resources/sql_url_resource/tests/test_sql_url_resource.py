"""Real SQLite file via a sqlite:/// URL -- no mocking of the SQLAlchemy engine,
matching sql_to_database_asset's own test convention."""
import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def db_url(tmp_path):
    path = tmp_path / "test.db"
    return f"sqlite:///{path}"


def _seed(resource, mod):
    resource.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)"
    )
    resource.execute(
        "INSERT INTO users (id, name, active) VALUES (:id, :name, :active)",
        {"id": 1, "name": "Ada", "active": True},
    )
    resource.execute(
        "INSERT INTO users (id, name, active) VALUES (:id, :name, :active)",
        {"id": 2, "name": "Bea", "active": False},
    )


def test_execute_select_returns_rows_as_dicts(mod, db_url):
    resource = mod.SQLUrlResource(connection_url=db_url)
    _seed(resource, mod)

    rows = resource.execute("SELECT id, name FROM users ORDER BY id")

    assert rows == [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bea"}]


def test_execute_parameterized_query(mod, db_url):
    resource = mod.SQLUrlResource(connection_url=db_url)
    _seed(resource, mod)

    rows = resource.execute("SELECT name FROM users WHERE active = :active", {"active": True})

    assert rows == [{"name": "Ada"}]


def test_execute_ddl_commits_and_returns_empty_list(mod, db_url):
    resource = mod.SQLUrlResource(connection_url=db_url)

    result = resource.execute("CREATE TABLE t (id INTEGER)")
    assert result == []

    # A second, separate resource instance (fresh connection) sees the committed DDL --
    # proves commit() actually happened, not just that the first connection saw its own write.
    resource2 = mod.SQLUrlResource(connection_url=db_url)
    resource2.execute("INSERT INTO t (id) VALUES (:id)", {"id": 7})
    rows = resource2.execute("SELECT id FROM t")
    assert rows == [{"id": 7}]


def test_get_engine_escape_hatch_is_a_real_sqlalchemy_engine(mod, db_url):
    from sqlalchemy.engine import Engine

    resource = mod.SQLUrlResource(connection_url=db_url)
    engine = resource.get_engine()

    assert isinstance(engine, Engine)
    with engine.connect() as conn:
        from sqlalchemy import text
        assert conn.execute(text("SELECT 1")).scalar() == 1


def test_component_requires_one_of_url_or_env_var(mod):
    component = mod.SQLUrlResourceComponent()
    with pytest.raises(ValueError, match="connection_url"):
        component.build_defs(context=None)


def test_component_literal_connection_url(mod, db_url):
    component = mod.SQLUrlResourceComponent(connection_url=db_url)
    defs = component.build_defs(context=None)
    resource = defs.resources["sql_url_resource"]

    assert isinstance(resource, mod.SQLUrlResource)
    assert resource.connection_url == db_url


def test_component_custom_resource_key(mod, db_url):
    component = mod.SQLUrlResourceComponent(resource_key="my_db", connection_url=db_url)
    defs = component.build_defs(context=None)

    assert "my_db" in defs.resources
    assert "sql_url_resource" not in defs.resources


def test_component_env_var_connection_url_resolves_in_a_real_run(mod, db_url, monkeypatch):
    """EnvVar-wrapped fields only resolve to real values once bound into an actual
    Dagster run, so this drives the resource through a materialized asset rather than
    inspecting the resource object directly (same convention as http_resource's tests)."""
    monkeypatch.setenv("TEST_DATABASE_URL", db_url)
    component = mod.SQLUrlResourceComponent(connection_url_env_var="TEST_DATABASE_URL")
    defs = component.build_defs(context=None)
    resource = defs.resources["sql_url_resource"]

    box = {}

    @dg.asset
    def _probe(sql_url_resource: mod.SQLUrlResource):
        box["rows"] = sql_url_resource.execute("SELECT 1 AS one")
        return dg.MaterializeResult()

    result = dg.materialize([_probe], resources={"sql_url_resource": resource})

    assert result.success
    assert box["rows"] == [{"one": 1}]


def test_execute_literal_colon_in_string_with_no_params_does_not_raise(mod, db_url):
    """A `:name`-shaped substring inside a string literal, with no params supplied,
    must not be mistaken for an unfilled bind parameter (regression -- previously
    always went through text(), which parses `:ab` as a placeholder regardless)."""
    resource = mod.SQLUrlResource(connection_url=db_url)

    rows = resource.execute("SELECT ':ab' AS x")

    assert rows == [{"x": ":ab"}]


def test_execute_real_named_params_still_work_after_the_fix(mod, db_url):
    resource = mod.SQLUrlResource(connection_url=db_url)
    _seed(resource, mod)

    rows = resource.execute("SELECT name FROM users WHERE id = :id", {"id": 2})

    assert rows == [{"name": "Bea"}]


def test_execute_literal_colon_alongside_real_params_is_a_known_limitation(mod, db_url):
    """When params ARE supplied, text() parsing is unavoidable (that's how the real
    substitution happens), so an unrelated literal colon in the same query string can
    still collide with the `:name` syntax. This is a documented SQLAlchemy/DBAPI
    limitation, not something this resource silently papers over -- assert it still
    raises rather than silently returning a wrong result."""
    from sqlalchemy.exc import StatementError

    resource = mod.SQLUrlResource(connection_url=db_url)
    _seed(resource, mod)

    with pytest.raises(StatementError):
        resource.execute("SELECT ':unbound', name FROM users WHERE id = :id", {"id": 1})
