"""Committed tests for SMTPResource.send_email() -- cc/bcc/html_body/attachments/reply_to.

Mocks ONLY smtplib.SMTP (the paid/external call); everything else (EmailMessage
construction, recipient-list building, bcc-not-a-header behavior, attachment
MIME typing) runs for real.
"""
import smtplib

import pytest

from .conftest import load_component_module


@pytest.fixture()
def mod():
    return load_component_module()


class _FakeSMTP:
    instances: list = []

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.started_tls = False
        self.login_args = None
        self.sent = None
        _FakeSMTP.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        self.started_tls = True

    def login(self, username, password):
        self.login_args = (username, password)

    def send_message(self, msg, from_addr=None, to_addrs=None):
        self.sent = {"msg": msg, "from_addr": from_addr, "to_addrs": to_addrs}


@pytest.fixture(autouse=True)
def _patch_smtp(monkeypatch):
    _FakeSMTP.instances = []
    monkeypatch.setattr(smtplib, "SMTP", _FakeSMTP)
    yield


def _resource(mod, **overrides):
    kwargs = dict(host="smtp.example.com", port=587, username="me@example.com", password="secret", use_tls=True)
    kwargs.update(overrides)
    return mod.SMTPResource(**kwargs)


def test_plain_text_single_recipient(mod):
    resource = _resource(mod)
    resource.send_email(to="a@b.com", subject="Hi", body="hello")

    fake = _FakeSMTP.instances[-1]
    assert fake.host == "smtp.example.com" and fake.port == 587
    assert fake.started_tls is True
    assert fake.login_args == ("me@example.com", "secret")
    assert fake.sent["from_addr"] == "me@example.com"  # defaults to username
    assert fake.sent["to_addrs"] == ["a@b.com"]
    msg = fake.sent["msg"]
    assert msg["Subject"] == "Hi"
    assert msg["To"] == "a@b.com"
    assert msg.get_content().strip() == "hello"


def test_cc_bcc_html_reply_to_and_attachment(mod):
    resource = _resource(mod)
    resource.send_email(
        to="a@b.com, c@d.com",
        subject="Report",
        body="plain version",
        from_addr="reports@example.com",
        cc=["cc1@example.com"],
        bcc=["hidden@example.com"],
        html_body="<p>html version</p>",
        reply_to="replies@example.com",
        attachments=[("report.csv", b"a,b\n1,2", "text/csv")],
    )

    fake = _FakeSMTP.instances[-1]
    msg = fake.sent["msg"]
    assert msg["Cc"] == "cc1@example.com"
    assert msg["Reply-To"] == "replies@example.com"
    assert "Bcc" not in msg  # never a header -- real BCC behavior
    assert fake.sent["to_addrs"] == ["a@b.com", "c@d.com", "cc1@example.com", "hidden@example.com"]
    assert fake.sent["from_addr"] == "reports@example.com"

    assert msg.is_multipart()
    html_parts = [p for p in msg.walk() if p.get_content_type() == "text/html"]
    assert html_parts and "html version" in html_parts[0].get_content()

    attachments = list(msg.iter_attachments())
    assert len(attachments) == 1
    assert attachments[0].get_filename() == "report.csv"
    assert attachments[0].get_content_type() == "text/csv"
    assert attachments[0].get_content() == "a,b\n1,2"  # text/* attachments decode to str


def test_use_tls_false_skips_starttls(mod):
    resource = _resource(mod, use_tls=False)
    resource.send_email(to="a@b.com", subject="s", body="b")
    assert _FakeSMTP.instances[-1].started_tls is False


def test_timeout_seconds_passed_to_smtp_constructor(mod):
    resource = _resource(mod)
    resource.send_email(to="a@b.com", subject="s", body="b", timeout_seconds=5)
    assert _FakeSMTP.instances[-1].timeout == 5
