"""Committed regression tests for RampResource.

Monkeypatches `requests.post` (module-level -- used directly for the token
endpoint and the multipart receipt-upload call) and `requests.Session.post`
/ `requests.Session.patch` (used by `get_client()`'s session for the JSON
calls: mileage reimbursement, virtual card creation, physical card update)
-- the one external, paid-API boundary. Token caching, exact request shapes
(paths/bodies/headers) per Ramp's documented Developer API, and the
spend-limit-update-does-not-exist constraint are all exercised for real.
"""
import base64
import time

import pytest

from .conftest import FakeResponse, load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("RAMP_CLIENT_ID", "client-123")
    monkeypatch.setenv("RAMP_CLIENT_SECRET", "secret-456")


@pytest.fixture()
def resource(mod, env):
    return mod.RampResource(
        client_id_env_var="RAMP_CLIENT_ID",
        client_secret_env_var="RAMP_CLIENT_SECRET",
    )


def _patch_token(monkeypatch, token="ramp_business_tok_1", expires_in=864000):
    import requests

    def fake_post(url, headers=None, data=None, timeout=None):
        if url == "https://api.ramp.com/developer/v1/token":
            expected_basic = "Basic " + base64.b64encode(b"client-123:secret-456").decode("ascii")
            assert headers["Authorization"] == expected_basic
            assert data["grant_type"] == "client_credentials"
            assert data["scope"] == "reimbursements:write cards:write"
            return FakeResponse({"access_token": token, "expires_in": expires_in})
        raise AssertionError(f"unexpected POST to {url}")

    monkeypatch.setattr(requests, "post", fake_post)


# --- token acquisition --------------------------------------------------

def test_get_access_token_fetches_and_caches(resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, headers=None, data=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": "tok-1", "expires_in": 864000})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    token2 = resource._get_access_token()

    assert token1 == token2 == "tok-1"
    assert len(calls) == 1


def test_get_access_token_refetches_once_expired(resource, monkeypatch):
    import requests

    calls = []

    def fake_post(url, headers=None, data=None, timeout=None):
        calls.append(url)
        return FakeResponse({"access_token": f"tok-{len(calls)}", "expires_in": 864000})

    monkeypatch.setattr(requests, "post", fake_post)

    token1 = resource._get_access_token()
    cache_key = f"{resource.base_url}:{resource.client_id_env_var}:{resource.scope}"
    resource._token_cache[cache_key]["expires"] = time.time() - 1
    token2 = resource._get_access_token()

    assert token1 == "tok-1"
    assert token2 == "tok-2"
    assert len(calls) == 2


def test_get_access_token_missing_env_vars_raises(mod, monkeypatch):
    monkeypatch.delenv("RAMP_CLIENT_ID", raising=False)
    monkeypatch.delenv("RAMP_CLIENT_SECRET", raising=False)
    resource = mod.RampResource(
        client_id_env_var="RAMP_CLIENT_ID",
        client_secret_env_var="RAMP_CLIENT_SECRET",
    )
    with pytest.raises(RuntimeError, match="Missing Ramp Developer App credentials"):
        resource._get_access_token()


def test_token_request_uses_basic_auth_and_scope(resource, monkeypatch):
    _patch_token(monkeypatch)
    token = resource._get_access_token()
    assert token == "ramp_business_tok_1"


# --- create_mileage_reimbursement ----------------------------------------

def test_create_mileage_reimbursement_request_shape(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    captured = {}

    def fake_session_post(self, url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        captured["auth_header"] = self.headers.get("Authorization")
        return FakeResponse({"id": "reimb_1", "state": "DRAFT"})

    monkeypatch.setattr(requests.Session, "post", fake_session_post)

    result = resource.create_mileage_reimbursement(
        reimbursee_id="usr_1",
        trip_date="2026-01-15",
        distance=42.5,
        start_location="Office",
        end_location="Client site",
        memo="Client visit",
    )

    assert result == {"id": "reimb_1", "state": "DRAFT"}
    assert captured["url"] == "https://api.ramp.com/developer/v1/reimbursements/mileage"
    assert captured["json"] == {
        "reimbursee_id": "usr_1",
        "trip_date": "2026-01-15",
        "distance": 42.5,
        "distance_units": "MILES",
        "start_location": "Office",
        "end_location": "Client site",
        "memo": "Client visit",
    }
    assert captured["auth_header"] == "Bearer ramp_business_tok_1"


def test_create_mileage_reimbursement_rejects_invalid_distance_units(resource, monkeypatch):
    _patch_token(monkeypatch)
    with pytest.raises(ValueError, match="distance_units must be one of"):
        resource.create_mileage_reimbursement(
            reimbursee_id="usr_1",
            trip_date="2026-01-15",
            distance=10,
            distance_units="LIGHT_YEARS",
        )


# --- upload_reimbursement_receipt (multipart) ----------------------------

def test_upload_reimbursement_receipt_multipart_shape(resource, monkeypatch, tmp_path):
    import requests

    receipt = tmp_path / "receipt.png"
    receipt.write_bytes(b"fake-image-bytes")

    captured = {}

    def fake_post(url, headers=None, data=None, files=None, timeout=None):
        if url == "https://api.ramp.com/developer/v1/token":
            return FakeResponse({"access_token": "ramp_business_tok_1", "expires_in": 864000})
        captured["url"] = url
        captured["headers"] = headers
        captured["data"] = data
        captured["files"] = files
        return FakeResponse({"id": "reimb_2", "state": "DRAFT"})

    monkeypatch.setattr(requests, "post", fake_post)

    result = resource.upload_reimbursement_receipt(
        reimbursee_id="usr_1",
        receipt_file_path=str(receipt),
        idempotency_key="idem-key-1",
    )

    assert result == {"id": "reimb_2", "state": "DRAFT"}
    assert captured["url"] == "https://api.ramp.com/developer/v1/reimbursements/submit-receipt"
    assert captured["data"] == {"idempotency_key": "idem-key-1", "reimbursee_id": "usr_1"}
    assert "receipt" in captured["files"]
    # No Content-Type pinned here -- requests must be free to set its own
    # multipart boundary header, unlike get_client()'s JSON session.
    assert "Content-Type" not in captured["headers"]
    assert captured["headers"]["Authorization"] == "Bearer ramp_business_tok_1"


def test_upload_reimbursement_receipt_includes_reimbursement_id_when_given(resource, monkeypatch, tmp_path):
    import requests

    receipt = tmp_path / "receipt.png"
    receipt.write_bytes(b"fake-image-bytes")
    captured = {}

    def fake_post(url, headers=None, data=None, files=None, timeout=None):
        if url == "https://api.ramp.com/developer/v1/token":
            return FakeResponse({"access_token": "tok", "expires_in": 864000})
        captured["data"] = data
        return FakeResponse({"id": "reimb_3"})

    monkeypatch.setattr(requests, "post", fake_post)

    resource.upload_reimbursement_receipt(
        reimbursee_id="usr_1",
        receipt_file_path=str(receipt),
        idempotency_key="idem-key-2",
        reimbursement_id="reimb_existing",
    )
    assert captured["data"]["reimbursement_id"] == "reimb_existing"


def test_upload_reimbursement_receipt_missing_file_raises(resource, monkeypatch):
    _patch_token(monkeypatch)
    with pytest.raises(FileNotFoundError):
        resource.upload_reimbursement_receipt(
            reimbursee_id="usr_1",
            receipt_file_path="/no/such/file.png",
            idempotency_key="idem-key-3",
        )


# --- create_virtual_card (Vault API) --------------------------------------

def test_create_virtual_card_request_shape(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    captured = {}

    def fake_session_post(self, url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return FakeResponse({
            "spend_limit_id": "sl_1",
            "user_id": "usr_1",
            "card": {"id": "card_1", "pan": "4111111111111111", "cvv": "123", "expiration": "2030-01"},
        })

    monkeypatch.setattr(requests.Session, "post", fake_session_post)

    result = resource.create_virtual_card(
        user_id="usr_1",
        limit_amount=500,
        interval="MONTHLY",
        display_name="Vendor card",
    )

    assert result["card"]["pan"] == "4111111111111111"  # raw response; caller redacts
    assert captured["url"] == "https://api.ramp.com/developer/v1/cards/vault"
    assert captured["json"] == {
        "user_id": "usr_1",
        "spending_restrictions": {
            "interval": "MONTHLY",
            "limit": {"amount": 500, "currency_code": "USD"},
        },
        "display_name": "Vendor card",
    }


def test_create_virtual_card_uses_dedicated_vault_host_when_set(mod, env, monkeypatch):
    import requests

    resource = mod.RampResource(
        client_id_env_var="RAMP_CLIENT_ID",
        client_secret_env_var="RAMP_CLIENT_SECRET",
        vault_base_url="https://demo-vault-api.ramp.com",
    )
    _patch_token(monkeypatch)
    captured = {}

    def fake_session_post(self, url, json=None, timeout=None):
        captured["url"] = url
        return FakeResponse({"spend_limit_id": "sl_1", "card": {"id": "card_1"}})

    monkeypatch.setattr(requests.Session, "post", fake_session_post)

    resource.create_virtual_card(user_id="usr_1", limit_amount=100, interval="MONTHLY")
    assert captured["url"] == "https://demo-vault-api.ramp.com/cards/vault"


# --- update_physical_card --------------------------------------------------

def test_update_physical_card_request_shape(resource, monkeypatch):
    import requests

    _patch_token(monkeypatch)
    captured = {}

    def fake_session_patch(self, url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return FakeResponse({"id": "card_1", "display_name": "New name"})

    monkeypatch.setattr(requests.Session, "patch", fake_session_patch)

    result = resource.update_physical_card(card_id="card_1", display_name="New name")

    assert result == {"id": "card_1", "display_name": "New name"}
    assert captured["url"] == "https://api.ramp.com/developer/v1/cards/physical/card_1"
    assert captured["json"] == {"display_name": "New name"}


def test_update_physical_card_requires_at_least_one_field(resource, monkeypatch):
    _patch_token(monkeypatch)
    with pytest.raises(ValueError, match="at least one of"):
        resource.update_physical_card(card_id="card_1")


def test_update_physical_card_has_no_spend_limit_field():
    """Structural guarantee: this method cannot touch a spend limit because
    Ramp's API exposes no such field on this endpoint -- not a design choice
    this resource could relax even if asked. (The surrounding docstring
    mentions "spend_limit" in prose explaining why it's absent -- this
    checks the actual request-body construction, not the docstring.)"""
    import pathlib

    source = pathlib.Path(__file__).resolve().parent.parent.joinpath("component.py").read_text()
    update_fn_start = source.index("def update_physical_card")
    update_fn_body = source[update_fn_start:update_fn_start + 1200]
    assert 'body["spend_limit"' not in update_fn_body
    assert "spend_limit=" not in update_fn_body
