"""VantaResourceComponent exposes `scope` and the registered resource sends it on the token request."""
import pytest

from .conftest import load_component_module

SCOPE = "vanta-api.all:read vanta-api.all:write vanta-api.documents:upload"


@pytest.fixture
def mod(monkeypatch):
    monkeypatch.setenv("VANTA_CLIENT_ID", "cid")
    monkeypatch.setenv("VANTA_CLIENT_SECRET", "csecret")
    return load_component_module()


def test_scope_is_carried_onto_registered_resource(mod):
    defs = mod.VantaResourceComponent(scope=SCOPE).build_defs(context=None)
    assert defs.resources["vanta_resource"].scope == SCOPE


def test_scope_defaults_to_empty(mod):
    defs = mod.VantaResourceComponent().build_defs(context=None)
    assert defs.resources["vanta_resource"].scope == ""


def test_scope_is_sent_in_token_request(mod, monkeypatch):
    sent = []

    class Resp:
        status_code = 200
        text = ""
        def json(self):
            return {"access_token": "tok", "expires_in": 3600}

    monkeypatch.setattr(mod.requests, "post", lambda url, data=None, **kw: (sent.append(data), Resp())[1])
    res = mod.VantaResourceComponent(scope=SCOPE).build_defs(context=None).resources["vanta_resource"]
    assert res.get_access_token() == "tok"
    assert sent[0]["scope"] == SCOPE

    sent.clear()
    res2 = mod.VantaResourceComponent().build_defs(context=None).resources["vanta_resource"]
    res2.get_access_token()
    assert "scope" not in sent[0]
