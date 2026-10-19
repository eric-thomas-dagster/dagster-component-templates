"""python-mode `source: warehouse_query` against a BigQuery resource.

dagster-gcp's BigQueryResource.get_client() is a @contextmanager (checked against the real library): the helper must enter it with
`with`, not call .query on the context manager. A bare google.cloud.bigquery.Client (which has .query) must keep working."""
from types import SimpleNamespace

import pytest

from .bq_fake import FakeBigQueryClient, FakeBigQueryResource
from .conftest import load_component_module


class _BareClientResource:
    """A resource whose get_client() returns the client itself (not a context manager)."""

    def __init__(self, client):
        self._client = client

    def get_client(self):
        return self._client


def _context(resource):
    return SimpleNamespace(resources=SimpleNamespace(bq=resource), has_partition_key=False, partition_key=None)


@pytest.fixture()
def client():
    c = FakeBigQueryClient("proj")
    c.create_dataset("proj.ds")
    c.query("CREATE TABLE ds.docs (id INT64, body STRING)")
    c.query("INSERT INTO ds.docs VALUES (1, 'alpha'), (2, 'beta')")
    return c


@pytest.mark.parametrize("make", [FakeBigQueryResource, _BareClientResource], ids=["dagster-gcp-style context manager", "bare client"])
def test_warehouse_query_reads_through_a_bigquery_resource(client, make):
    mod = load_component_module()
    df = mod._ingest_warehouse_query({"resource_key": "bq", "sql": "SELECT id, body FROM ds.docs ORDER BY id"}, _context(make(client)))
    assert df["id"].tolist() == [1, 2] and df["body"].tolist() == ["alpha", "beta"]


def test_unknown_client_shape_still_gets_the_clear_error():
    mod = load_component_module()

    class Odd:
        def get_client(self):
            return object()

    with pytest.raises(ValueError, match="get_client"):
        mod._ingest_warehouse_query({"resource_key": "bq", "sql": "SELECT 1"}, _context(Odd()))
