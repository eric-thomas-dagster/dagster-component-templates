"""Committed tests for SendGridResource.send_email() -- the v3 Mail Send payload shape
(POST /v3/mail/send), distinct from upsert_contacts_bulk's Marketing contacts API.

Mocks ONLY requests.post (the paid/external call); payload construction (personalizations,
cc/bcc, html alternative, base64 attachments, reply_to) runs for real.
"""
import base64

import dagster as dg
import pytest
import requests

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeResponse:
    def __init__(self, status_code=202, text=""):
        self.status_code = status_code
        self.text = text

    def json(self):
        raise ValueError("empty body")  # real /v3/mail/send returns 202 with no body


@pytest.fixture()
def captured_post(monkeypatch):
    calls = []

    def _fake_post(url, json=None, headers=None, timeout=None):
        calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return _FakeResponse(status_code=202)

    monkeypatch.setattr(requests, "post", _fake_post)
    return calls


def _resource(mod, **overrides):
    kwargs = dict(api_key_env_var="SENDGRID_TEST_KEY")
    kwargs.update(overrides)
    return mod.SendGridResource(**kwargs)


def test_send_email_requires_from_addr(mod, monkeypatch):
    monkeypatch.setenv("SENDGRID_TEST_KEY", "SG.test")
    resource = _resource(mod)
    with pytest.raises(dg.Failure, match="from_addr is required"):
        resource.send_email(to="a@b.com", subject="s", body="b")


def test_send_email_plain_text(mod, monkeypatch, captured_post):
    monkeypatch.setenv("SENDGRID_TEST_KEY", "SG.test")
    resource = _resource(mod)
    resource.send_email(to="a@b.com", subject="Hi", body="hello", from_addr="me@example.com")

    assert len(captured_post) == 1
    call = captured_post[0]
    assert call["url"] == "https://api.sendgrid.com/v3/mail/send"
    assert call["headers"]["Authorization"] == "Bearer SG.test"
    body = call["json"]
    assert body["from"] == {"email": "me@example.com"}
    assert body["subject"] == "Hi"
    assert body["personalizations"] == [{"to": [{"email": "a@b.com"}]}]
    assert body["content"] == [{"type": "text/plain", "value": "hello"}]
    assert "reply_to" not in body
    assert "attachments" not in body


def test_send_email_cc_bcc_html_reply_to_multi_to(mod, monkeypatch, captured_post):
    monkeypatch.setenv("SENDGRID_TEST_KEY", "SG.test")
    resource = _resource(mod)
    resource.send_email(
        to="a@b.com, c@d.com",
        subject="Report",
        body="plain",
        from_addr="reports@example.com",
        cc=["cc1@example.com"],
        bcc=["hidden@example.com"],
        html_body="<p>html</p>",
        reply_to="replies@example.com",
    )
    body = captured_post[0]["json"]
    p = body["personalizations"][0]
    assert p["to"] == [{"email": "a@b.com"}, {"email": "c@d.com"}]
    assert p["cc"] == [{"email": "cc1@example.com"}]
    assert p["bcc"] == [{"email": "hidden@example.com"}]
    assert body["reply_to"] == {"email": "replies@example.com"}
    assert {"type": "text/html", "value": "<p>html</p>"} in body["content"]


def test_send_email_attachment_is_base64_encoded(mod, monkeypatch, captured_post):
    monkeypatch.setenv("SENDGRID_TEST_KEY", "SG.test")
    resource = _resource(mod)
    resource.send_email(
        to="a@b.com", subject="s", body="b", from_addr="me@example.com",
        attachments=[("report.csv", b"a,b\n1,2", "text/csv")],
    )
    body = captured_post[0]["json"]
    att = body["attachments"][0]
    assert att["filename"] == "report.csv"
    assert att["type"] == "text/csv"
    assert base64.b64decode(att["content"]) == b"a,b\n1,2"


def test_send_email_raises_on_non_2xx(mod, monkeypatch):
    monkeypatch.setenv("SENDGRID_TEST_KEY", "SG.test")

    def _fake_post_error(url, json=None, headers=None, timeout=None):
        return _FakeResponse(status_code=401, text="Unauthorized")

    monkeypatch.setattr(requests, "post", _fake_post_error)
    resource = _resource(mod)
    with pytest.raises(dg.Failure, match="401"):
        resource.send_email(to="a@b.com", subject="s", body="b", from_addr="me@example.com")
