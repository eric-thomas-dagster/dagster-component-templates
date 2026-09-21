"""Unit tests for SnowflakeWorkspaceComponent's fallback QUERY_TAG mechanism.

dagster_snowflake.SnowflakeResource already sets a QUERY_TAG session parameter
at connect time, but only when a Dagster run is in scope -- it has no
visibility into a sensor tick or a defs-state discovery pass, neither of
which runs inside a Dagster run. This component's `_create_connection`
covers those two cases with an explicit ALTER SESSION, using the caller-
supplied `fallback_query_tag_context` label. These tests exercise that
mechanism directly against a mocked `snowflake.connector.connect`, without
needing a real Snowflake account.

Run from this directory:

    pytest -q
"""
from __future__ import annotations

import importlib.util
import json
import pathlib
from unittest import mock

_HERE = pathlib.Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "snowflake_workspace_component", _HERE / "component.py"
)
assert _spec is not None and _spec.loader is not None
component = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(component)

SnowflakeWorkspaceComponent = component.SnowflakeWorkspaceComponent

from dagster_snowflake import SnowflakeResource  # noqa: E402


def _make_component() -> SnowflakeWorkspaceComponent:
    return SnowflakeWorkspaceComponent(
        workspace=SnowflakeResource(
            account="foo",
            user="bar",
            password="baz",
            warehouse="WH",
            database="DB",
            schema="PUBLIC",
        ),
    )


def test_create_connection_without_fallback_sets_no_extra_query_tag():
    """Inside an asset body, SnowflakeResource's own run-scoped QUERY_TAG
    already covers the connection -- _create_connection should not issue a
    second, less specific ALTER SESSION on top of it.
    """
    mock_conn = mock.MagicMock()
    with mock.patch("snowflake.connector.connect", return_value=mock_conn):
        c = _make_component()
        c._create_connection()

    mock_conn.cursor.return_value.execute.assert_not_called()


def test_create_connection_with_fallback_sets_query_tag():
    mock_conn = mock.MagicMock()
    with mock.patch("snowflake.connector.connect", return_value=mock_conn):
        c = _make_component()
        c._create_connection(fallback_query_tag_context="observation_sensor")

    mock_conn.cursor.return_value.execute.assert_called_once()
    (sql,), _ = mock_conn.cursor.return_value.execute.call_args
    assert sql.startswith("ALTER SESSION SET QUERY_TAG = '")
    assert sql.endswith("'")
    payload = json.loads(sql[len("ALTER SESSION SET QUERY_TAG = '") : -1])
    assert payload == {
        "app": "dagster",
        "dagster_component": "snowflake_workspace",
        "dagster_context": "observation_sensor",
    }


def test_create_connection_fallback_context_label_is_preserved():
    for label in ("discovery", "observation_sensor", "dt_refresh_sensor"):
        mock_conn = mock.MagicMock()
        with mock.patch("snowflake.connector.connect", return_value=mock_conn):
            c = _make_component()
            c._create_connection(fallback_query_tag_context=label)

        (sql,), _ = mock_conn.cursor.return_value.execute.call_args
        payload = json.loads(sql[len("ALTER SESSION SET QUERY_TAG = '") : -1])
        assert payload["dagster_context"] == label


def test_create_connection_fallback_failure_does_not_raise():
    """A failure setting the fallback tag (e.g. a transient network blip)
    should not take down the discovery pass or sensor tick that's using the
    connection for real work -- it's a best-effort attribution marker, not
    something the caller depends on.
    """
    mock_conn = mock.MagicMock()
    mock_conn.cursor.return_value.execute.side_effect = Exception("boom")
    with mock.patch("snowflake.connector.connect", return_value=mock_conn):
        c = _make_component()
        conn = c._create_connection(fallback_query_tag_context="discovery")

    assert conn is mock_conn
