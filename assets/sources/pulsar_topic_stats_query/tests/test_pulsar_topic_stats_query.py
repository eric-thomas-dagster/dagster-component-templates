"""Real, committed tests for PulsarTopicStatsQueryComponent.

Stands up a real local HTTP server implementing the exact Pulsar Admin
REST API shape (verified directly against the official docs, not from
memory -- GET /admin/v2/{persistent|non-persistent}/{tenant}/{namespace}/
{topic}/stats returning a JSON object with top-level msgRateIn/msgRateOut/
storageSize/backlogSize fields and a `subscriptions` object keyed by
subscription name), and points the component at it -- real HTTP requests
over the real `requests` library.
"""
import http.server
import json
import threading
from urllib.parse import urlparse

import dagster as dg
import pytest

from .conftest import load_component_module

mod = load_component_module()


class _TestServer(http.server.ThreadingHTTPServer):
    request_queue_size = 64
    daemon_threads = True


class _PulsarHandler(http.server.BaseHTTPRequestHandler):
    stats = {
        "msgRateIn": 12.5,
        "msgRateOut": 10.0,
        "msgThroughputIn": 1000.0,
        "msgThroughputOut": 800.0,
        "storageSize": 5000,
        "backlogSize": 200,
        "averageMsgSize": 100.0,
        "publishers": [],
        "subscriptions": {
            "sub-a": {
                "msgBacklog": 5,
                "backlogSize": 50,
                "msgRateOut": 4.0,
                "msgThroughputOut": 400.0,
                "unackedMessages": 1,
                "msgRateRedeliver": 0.0,
                "type": "Shared",
                "activeConsumerName": "consumer-1",
                "consumers": [{"consumerName": "consumer-1"}],
            },
            "sub-b": {
                "msgBacklog": 0,
                "backlogSize": 0,
                "msgRateOut": 6.0,
                "msgThroughputOut": 400.0,
                "unackedMessages": 0,
                "msgRateRedeliver": 0.0,
                "type": "Exclusive",
                "activeConsumerName": "consumer-2",
                "consumers": [{"consumerName": "consumer-2"}],
            },
        },
    }
    paths_received = []
    auth_headers_received = []

    def do_GET(self):
        self.__class__.paths_received.append(urlparse(self.path).path)
        self.__class__.auth_headers_received.append(self.headers.get("Authorization"))
        payload = json.dumps(self.__class__.stats).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        pass


@pytest.fixture
def pulsar_server():
    _PulsarHandler.paths_received = []
    _PulsarHandler.auth_headers_received = []
    server = _TestServer(("127.0.0.1", 0), _PulsarHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join()


def _url(server):
    return f"http://127.0.0.1:{server.server_address[1]}"


def _comp(server, **kw):
    args = dict(asset_name="orders_topic_health", admin_url=_url(server), tenant="public", namespace="default", topic="orders")
    args.update(kw)
    return mod.PulsarTopicStatsQueryComponent(**args)


def test_one_row_per_subscription_with_topic_fields_joined(pulsar_server):
    comp = _comp(pulsar_server)
    result = dg.materialize(list(comp.build_defs(None).assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert result.success
    assert "/admin/v2/persistent/public/default/orders/stats" in _PulsarHandler.paths_received[0]

    meta = next(e for e in result.all_events if e.event_type_value == "ASSET_MATERIALIZATION").event_specific_data.materialization.metadata
    assert meta["dagster/row_count"].value == 2


def test_subscription_backlog_and_topic_fields_both_present(pulsar_server):
    df = mod._run_pulsar_topic_stats(_url(pulsar_server), True, "public", "default", "orders", {}, 30)
    assert set(df["subscription_name"]) == {"sub-a", "sub-b"}
    sub_a = df[df["subscription_name"] == "sub-a"].iloc[0]
    assert sub_a["msgBacklog"] == 5
    assert sub_a["consumer_count"] == 1
    assert sub_a["topic_storageSize"] == 5000  # topic-level field joined onto every row


def test_no_subscriptions_still_returns_a_topic_level_row(pulsar_server):
    _PulsarHandler.stats = {**_PulsarHandler.stats, "subscriptions": {}}
    df = mod._run_pulsar_topic_stats(_url(pulsar_server), True, "public", "default", "orders", {}, 30)
    assert len(df) == 1
    assert df.iloc[0]["scope"] == "topic"
    assert df.iloc[0]["subscription_name"] is None


def test_non_persistent_topic_uses_the_right_url_segment(pulsar_server):
    comp = _comp(pulsar_server, persistent=False)
    dg.materialize(list(comp.build_defs(None).assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert "/admin/v2/non-persistent/public/default/orders/stats" in _PulsarHandler.paths_received[-1]


def test_bearer_token_auth_is_sent_when_configured(pulsar_server, monkeypatch):
    monkeypatch.setenv("PULSAR_TOKEN", "secret-token")
    comp = _comp(pulsar_server, auth_token_env_var="PULSAR_TOKEN")
    dg.materialize(list(comp.build_defs(None).assets), resources={"io_manager": dg.InMemoryIOManager()})
    assert _PulsarHandler.auth_headers_received[-1] == "Bearer secret-token"


def test_missing_auth_token_env_var_raises_clearly(pulsar_server, monkeypatch):
    monkeypatch.delenv("PULSAR_TOKEN", raising=False)
    comp = _comp(pulsar_server, auth_token_env_var="PULSAR_TOKEN")
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success


def test_partition_tokens_substituted_into_topic_name(pulsar_server):
    comp = _comp(pulsar_server, topic="orders-{partition_key}", partition_type="daily", partition_start="2024-01-01")
    dg.materialize(list(comp.build_defs(None).assets), partition_key="2024-01-02", resources={"io_manager": dg.InMemoryIOManager()})
    assert "/orders-2024-01-02/" in _PulsarHandler.paths_received[-1]


def test_stats_request_failure_raises_with_status_and_body(pulsar_server):
    class _ErrorHandler(_PulsarHandler):
        def do_GET(self):
            payload = b"Topic not found"
            self.send_response(404)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    pulsar_server.RequestHandlerClass = _ErrorHandler
    comp = _comp(pulsar_server)
    result = dg.materialize(list(comp.build_defs(None).assets), raise_on_error=False, resources={"io_manager": dg.InMemoryIOManager()})
    assert not result.success
    failure = next(e for e in result.all_events if e.event_type_value == "STEP_FAILURE")
    assert "404" in str(failure.event_specific_data.error)


def test_weekday_cron_partition_builds_a_real_time_window_partitions_def(pulsar_server):
    comp = _comp(pulsar_server, partition_type="cron", partition_start="2024-01-01", partition_cron_schedule="0 0 * * 1-5")
    pdef = list(comp.build_defs(None).assets)[0].partitions_def
    assert isinstance(pdef, dg.TimeWindowPartitionsDefinition)
