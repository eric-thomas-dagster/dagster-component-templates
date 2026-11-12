"""resource_key tests for ClickhouseSqlQueryComponent.

The default (unset) path exercises the real HTTP+FORMAT JSON request path (shared with
test_clickhouse_sql_query.py's own local HTTP server). The resource_key path is faked at the
single external boundary: a resource object exposing `.get_client()`, whose fake client records
the query passed to `query_df()` and returns a canned DataFrame -- matching the real
clickhouse-connect `Client.query_df()` surface used by the shared `clickhouse_resource` component.
"""
import pandas as pd
import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class FakeClickHouseClient:
    """Same surface as clickhouse_connect's Client, as used by this component: query_df()."""

    def __init__(self, df):
        self._df = df
        self.queries = []

    def query_df(self, query):
        self.queries.append(query)
        return self._df


class FakeClickHouseResource:
    """Same surface as the shared ClickHouseResource: get_client()."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client


def _comp(**kw):
    args = dict(asset_name="daily_counts", host_url="http://unused:8123", query="SELECT day, cnt FROM counts")
    args.update(kw)
    return mod.ClickhouseSqlQueryComponent(**args)


def test_resource_key_default_is_none():
    comp = _comp()
    assert comp.resource_key is None


def test_required_resource_keys_includes_key_when_set():
    comp = _comp(resource_key="ch_shared")
    asset = list(comp.build_defs(None).assets)[0]
    assert asset.required_resource_keys - {"io_manager"} == {"ch_shared"}


def test_required_resource_keys_empty_when_unset():
    comp = _comp()
    asset = list(comp.build_defs(None).assets)[0]
    assert asset.required_resource_keys - {"io_manager"} == set()


def test_resource_path_calls_query_df_on_the_shared_client():
    df = pd.DataFrame({"day": ["2024-01-01", "2024-01-02"], "cnt": [10, 20]})
    client = FakeClickHouseClient(df)
    comp = _comp(resource_key="ch_shared", query="SELECT day, cnt FROM counts;")
    asset = list(comp.build_defs(None).assets)[0]
    result = dg.materialize(
        [asset],
        resources={"io_manager": dg.InMemoryIOManager(), "ch_shared": FakeClickHouseResource(client)},
    )
    assert result.success
    # query_df called directly -- no FORMAT JSON appended, trailing ';' stripped.
    assert client.queries == ["SELECT day, cnt FROM counts"]
    meta = next(
        e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION"
    ).event_specific_data.materialization.metadata
    assert meta["dagster/row_count"].value == 2


def test_resource_path_substitutes_partition_tokens():
    df = pd.DataFrame({"day": ["2024-01-02"], "cnt": [5]})
    client = FakeClickHouseClient(df)
    comp = _comp(
        resource_key="ch_shared",
        query="SELECT * FROM counts WHERE day = '{partition_key}'",
        partition_type="daily", partition_start="2024-01-01",
    )
    asset = list(comp.build_defs(None).assets)[0]
    result = dg.materialize(
        [asset], partition_key="2024-01-02",
        resources={"io_manager": dg.InMemoryIOManager(), "ch_shared": FakeClickHouseResource(client)},
    )
    assert result.success
    assert "2024-01-02" in client.queries[0]


def test_unregistered_resource_key_fails_loudly():
    comp = _comp(resource_key="missing_key")
    asset = list(comp.build_defs(None).assets)[0]
    with pytest.raises(Exception, match="missing_key"):
        dg.materialize([asset], resources={"io_manager": dg.InMemoryIOManager()})
