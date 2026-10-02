"""Tests for MondayBoardItemsIngestionComponent + MondayResource.

Mocks ONLY the external HTTP call (`requests.post` to monday.com's GraphQL
endpoint). Everything else -- GraphQL JSON->DataFrame parsing, column_values
flattening, items_page -> next_items_page cursor-walking, partitions_def
construction, and asset execution / preview metadata -- runs for real.
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from dagster import DailyPartitionsDefinition, StaticPartitionsDefinition, materialize

from .conftest import load_component_module, load_resource_module

component_mod = load_component_module()
resource_mod = load_resource_module()

MondayBoardItemsIngestionComponent = component_mod.MondayBoardItemsIngestionComponent
_build_partitions_def = component_mod._build_partitions_def
_flatten_item = component_mod._flatten_item
_column_values_selection = component_mod._column_values_selection

MondayResource = resource_mod.MondayResource
MondayGraphQLError = resource_mod.MondayGraphQLError


def _mock_response(json_body, status_code=200):
    resp = MagicMock()
    resp.json.return_value = json_body
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def monday_resource(monkeypatch):
    monkeypatch.setenv("MONDAY_TOKEN", "secret-token")
    return MondayResource(api_token_env_var="MONDAY_TOKEN")


# --- 1. MondayResource.execute ------------------------------------------------

def test_execute_returns_data_on_success(monday_resource):
    body = {"data": {"boards": [{"items_page": {"cursor": None, "items": []}}]}}
    with patch("requests.post", return_value=_mock_response(body)) as mock_post:
        result = monday_resource.execute("query { boards(ids: [1]) { id } }")
    assert mock_post.called
    assert result == body["data"]


def test_execute_sends_raw_token_no_bearer_prefix_and_api_version(monday_resource):
    body = {"data": {}}
    with patch("requests.post", return_value=_mock_response(body)) as mock_post:
        monday_resource.execute("query { x }", {"a": 1})
    _, kwargs = mock_post.call_args
    assert kwargs["headers"]["Authorization"] == "secret-token"
    assert "Bearer" not in kwargs["headers"]["Authorization"]
    assert "API-Version" in kwargs["headers"]
    assert kwargs["json"] == {"query": "query { x }", "variables": {"a": 1}}


def test_execute_raises_monday_graphql_error_on_errors_array(monday_resource):
    body = {"errors": [{"message": "Column not found"}], "data": None}
    with patch("requests.post", return_value=_mock_response(body)):
        with pytest.raises(MondayGraphQLError, match="Column not found"):
            monday_resource.execute("query { boards(ids: [999999]) { id } }")


def test_execute_missing_token_env_var_raises(monkeypatch):
    monkeypatch.delenv("MISSING_MONDAY_TOKEN", raising=False)
    resource = MondayResource(api_token_env_var="MISSING_MONDAY_TOKEN")
    with pytest.raises(RuntimeError, match="MISSING_MONDAY_TOKEN"):
        resource.execute("query { x }")


def test_execute_defaults_variables_to_empty_dict(monday_resource):
    body = {"data": {"ok": True}}
    with patch("requests.post", return_value=_mock_response(body)) as mock_post:
        monday_resource.execute("query { x }")
    assert mock_post.call_args.kwargs["json"]["variables"] == {}


# --- 2. Query-building helpers -------------------------------------------------

def test_column_values_selection_without_filter():
    sel = _column_values_selection(None)
    assert sel == "column_values { id text value type }"


def test_column_values_selection_with_filter():
    sel = _column_values_selection(["status", "date4"])
    assert sel == 'column_values(ids: ["status", "date4"]) { id text value type }'


# --- 3. Item flattening ---------------------------------------------------------

def test_flatten_item_basic_fields():
    item = {
        "id": "123",
        "name": "Task A",
        "state": "active",
        "created_at": "2026-06-01T00:00:00Z",
        "updated_at": "2026-06-02T00:00:00Z",
        "group": {"id": "topics", "title": "To Do"},
        "column_values": [
            {"id": "status", "text": "Working on it", "value": '{"index":1}', "type": "color"},
            {"id": "date4", "text": "2026-07-01", "value": '{"date":"2026-07-01"}', "type": "date"},
        ],
    }
    row = _flatten_item(item)
    assert row["id"] == "123"
    assert row["name"] == "Task A"
    assert row["group_id"] == "topics"
    assert row["group_title"] == "To Do"
    assert row["column_status"] == "Working on it"
    assert row["column_status_raw"] == '{"index":1}'
    assert row["column_date4"] == "2026-07-01"


def test_flatten_item_handles_missing_group_and_columns():
    row = _flatten_item({"id": "1", "name": "Bare item"})
    assert row["group_id"] is None
    assert row["group_title"] is None


# --- 4. Partitions helper -----------------------------------------------------

def test_build_partitions_def_none_when_unset():
    assert _build_partitions_def(None, None, None, None, None) is None


def test_build_partitions_def_daily():
    pdef = _build_partitions_def("daily", "2026-01-01", None, None, None)
    assert isinstance(pdef, DailyPartitionsDefinition)


def test_build_partitions_def_static():
    pdef = _build_partitions_def("static", None, "us,eu", None, None)
    assert isinstance(pdef, StaticPartitionsDefinition)
    assert set(pdef.get_partition_keys()) == {"us", "eu"}


# --- 5. Full asset execution (fake client, no real HTTP) ---------------------

class _FakeMondayClient:
    """Stands in for the resource at `context.resources.<resource_name>` --
    the asset calls `client.execute(query, variables)` directly, so this
    fake just needs that one method, dispatching on which query shape it
    receives (an initial `boards(...)` query vs. a `next_items_page` query)."""

    def __init__(self, pages):
        self._pages = list(pages)
        self.calls = []

    def execute(self, query, variables=None):
        self.calls.append((query, variables))
        return self._pages.pop(0)


def _item(item_id, name="Item"):
    return {
        "id": item_id,
        "name": name,
        "state": "active",
        "created_at": "2026-06-01T00:00:00Z",
        "updated_at": "2026-06-01T00:00:00Z",
        "group": {"id": "g1", "title": "Group 1"},
        "column_values": [{"id": "status", "text": "Done", "value": "{}", "type": "color"}],
    }


def test_asset_execution_walks_cursor_across_items_page_and_next_items_page():
    component = MondayBoardItemsIngestionComponent(
        asset_name="monday_board_items",
        board_id="999",
        page_size=2,
        limit=100,
    )
    defs = component.build_defs(context=MagicMock())
    asset_def = list(defs.assets)[0]

    fake_client = _FakeMondayClient(
        pages=[
            {"boards": [{"items_page": {"cursor": "CURSOR1", "items": [_item("1"), _item("2")]}}]},
            {"next_items_page": {"cursor": None, "items": [_item("3")]}},
        ]
    )

    result = materialize([asset_def], resources={"monday_resource": fake_client})
    assert result.success
    metadata = result.get_asset_materialization_events()[0].materialization.metadata
    assert metadata["row_count"].value == 3
    assert "preview" in metadata

    # First call must be the boards(...) query with boardId/limit variables;
    # second call must be the next_items_page query with the returned cursor.
    first_query, first_vars = fake_client.calls[0]
    assert "boards(ids: [$boardId])" in first_query
    assert first_vars == {"boardId": "999", "limit": 2}

    second_query, second_vars = fake_client.calls[1]
    assert "next_items_page" in second_query
    # page_size=2 and remaining=100-2=98, so the next page is still capped at page_size (2).
    assert second_vars == {"cursor": "CURSOR1", "limit": 2}


def test_asset_execution_respects_limit_and_stops_early():
    component = MondayBoardItemsIngestionComponent(
        asset_name="monday_board_items_limited",
        board_id="999",
        page_size=10,
        limit=2,
    )
    defs = component.build_defs(context=MagicMock())
    asset_def = list(defs.assets)[0]

    # Server would hand back a cursor for more, but limit=2 should stop us
    # after the first page without a next_items_page call.
    fake_client = _FakeMondayClient(
        pages=[
            {"boards": [{"items_page": {"cursor": "CURSOR1", "items": [_item("1"), _item("2")]}}]},
        ]
    )
    result = materialize([asset_def], resources={"monday_resource": fake_client})
    assert result.success
    metadata = result.get_asset_materialization_events()[0].materialization.metadata
    assert metadata["row_count"].value == 2
    assert len(fake_client.calls) == 1


def test_asset_execution_empty_board_returns_empty_dataframe():
    component = MondayBoardItemsIngestionComponent(asset_name="monday_board_items_empty", board_id="0")
    defs = component.build_defs(context=MagicMock())
    asset_def = list(defs.assets)[0]

    fake_client = _FakeMondayClient(pages=[{"boards": []}])
    result = materialize([asset_def], resources={"monday_resource": fake_client})
    assert result.success
    metadata = result.get_asset_materialization_events()[0].materialization.metadata
    assert metadata["row_count"].value == 0


def test_asset_execution_stops_when_cursor_present_but_no_items():
    """Defensive: a server bug returning a non-null cursor with an empty
    items list must not infinite-loop."""
    component = MondayBoardItemsIngestionComponent(asset_name="monday_board_items_defensive", board_id="1")
    defs = component.build_defs(context=MagicMock())
    asset_def = list(defs.assets)[0]

    fake_client = _FakeMondayClient(
        pages=[{"boards": [{"items_page": {"cursor": "STALE_CURSOR", "items": []}}]}]
    )
    result = materialize([asset_def], resources={"monday_resource": fake_client})
    assert result.success
    assert len(fake_client.calls) == 1
