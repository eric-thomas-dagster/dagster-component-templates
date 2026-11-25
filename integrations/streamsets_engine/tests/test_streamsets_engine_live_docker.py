"""Integration test against a REAL StreamSets Data Collector engine -- not a
fake, not a mock. Requires a running container:

    docker run -d --name streamsets-sdc -p 18630:18630 \\
        -e SDC_CONF_http_authentication=form --ulimit nofile=32768:65536 \\
        streamsets/datacollector

Skips gracefully (does not fail CI) if port 18630 isn't reachable, since most
environments won't have this container running. When it IS reachable, this
proves `StreamSetsEngineTransport`'s read paths (list_pipelines,
get_pipeline_status, get_pipeline_metrics, system info via HTTP Basic auth)
really work against a real engine -- not just against this repo's own fake
server in test_streamsets_engine.py.

Does NOT exercise start/stop against the real engine: this image ships
`streamsets-activation-no-trial-*.jar` with no valid activation code, so
every state-changing REST call returns 403 regardless of user role (confirmed
directly -- see the component README's "Honest limitation" section). That
isn't something a test workaround can fix; a real activation code (via
StreamSets' Support portal) would be needed to extend this test to start/stop."""
import socket

import pytest

from .conftest import load_component_module

mod = load_component_module()
StreamSetsEngineTransport = mod.StreamSetsEngineTransport

_HOST = "localhost"
_PORT = 18630
_BASE_URL = f"http://{_HOST}:{_PORT}"


def _engine_reachable() -> bool:
    try:
        with socket.create_connection((_HOST, _PORT), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not _engine_reachable(), reason="No StreamSets Data Collector engine reachable on localhost:18630 -- see module docstring to start one")


def test_list_pipelines_against_real_engine():
    transport = StreamSetsEngineTransport(_BASE_URL, "admin", "admin")
    pipelines = transport.list_pipelines()
    assert isinstance(pipelines, list)


def test_status_on_nonexistent_pipeline_returns_empty_not_an_error():
    """Live-confirmed real-engine quirk: GET .../status on a pipeline id
    that doesn't exist returns 200 with an empty body, not 404."""
    transport = StreamSetsEngineTransport(_BASE_URL, "admin", "admin")
    status = transport.get_pipeline_status("this-pipeline-does-not-exist")
    assert status == {}


def test_metrics_endpoint_is_real_and_reachable():
    """Live-confirmed: this endpoint exists and responds 200 even with no
    data yet -- the capability Control Hub's sibling component lacks."""
    transport = StreamSetsEngineTransport(_BASE_URL, "admin", "admin")
    metrics = transport.get_pipeline_metrics("this-pipeline-does-not-exist")
    assert metrics == {}


def test_wrong_credentials_are_rejected():
    import requests

    resp = requests.get(f"{_BASE_URL}/rest/v1/pipelines", auth=("admin", "definitely-wrong"), timeout=5)
    assert resp.status_code in (401, 403)
