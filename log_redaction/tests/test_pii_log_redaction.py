import io
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT.parent))

from log_redaction.pii_log_redaction import (  # noqa: E402
    install_pii_log_redaction,
    redact_text,
    uninstall_pii_log_redaction,
)


@pytest.fixture(autouse=True)
def _restore_streams():
    orig_out, orig_err = sys.stdout, sys.stderr
    yield
    uninstall_pii_log_redaction()
    sys.stdout, sys.stderr = orig_out, orig_err


def test_redacts_email_and_phone():
    out = redact_text("Contact jane@acme.com or call 555-123-4567", redact_secrets=False)
    assert "[EMAIL_ADDRESS]" in out
    assert "[PHONE_NUMBER]" in out
    assert "jane@acme.com" not in out
    assert "555-123-4567" not in out


def test_redacts_ssn_credit_card_ip():
    out = redact_text(
        "SSN 123-45-6789, card 4111-1111-1111-1111, ip 10.0.0.5",
        pii_entities=["US_SSN", "CREDIT_CARD", "IP_ADDRESS"],
        redact_secrets=False,
    )
    assert "[US_SSN]" in out and "123-45-6789" not in out
    assert "[CREDIT_CARD]" in out and "4111-1111-1111-1111" not in out
    assert "[IP_ADDRESS]" in out and "10.0.0.5" not in out


def test_redacts_secrets_by_default():
    out = redact_text("key=AKIAABCDEFGHIJKLMNOP token=ghp_" + "a" * 36)
    assert "[AWS_ACCESS_KEY]" in out
    assert "[GITHUB_TOKEN]" in out
    assert "AKIAABCDEFGHIJKLMNOP" not in out


def test_redacts_private_key_header():
    out = redact_text("-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIB...", redact_secrets=True, pii_entities=[])
    assert "[PRIVATE_KEY]" in out


def test_no_op_on_clean_text():
    text = "just a normal log line with nothing sensitive"
    assert redact_text(text) == text


def test_entities_not_requested_are_left_alone():
    out = redact_text("jane@acme.com", pii_entities=[], redact_secrets=False)
    assert out == "jane@acme.com"


def test_install_wraps_real_stdout_and_redacts_prints(capsys):
    install_pii_log_redaction(redact_secrets=True, pii_entities=["EMAIL_ADDRESS"])
    print("contact admin@acme.com about key AKIAABCDEFGHIJKLMNOP")
    sys.stdout.flush()
    captured = capsys.readouterr()
    assert "[EMAIL_ADDRESS]" in captured.out
    assert "[AWS_ACCESS_KEY]" in captured.out
    assert "admin@acme.com" not in captured.out


def test_install_is_idempotent():
    install_pii_log_redaction()
    first = sys.stdout
    install_pii_log_redaction()
    assert sys.stdout is first  # didn't double-wrap


def test_uninstall_restores_original_stream():
    original = sys.stdout
    install_pii_log_redaction()
    assert sys.stdout is not original
    uninstall_pii_log_redaction()
    assert sys.stdout is original


def test_pattern_split_across_two_writes_is_still_caught(capsys):
    install_pii_log_redaction(redact_secrets=False, pii_entities=["EMAIL_ADDRESS"])
    sys.stdout.write("contact jane@acme")
    sys.stdout.write(".com please\n")
    captured = capsys.readouterr()
    assert "[EMAIL_ADDRESS]" in captured.out
    assert "jane@acme.com" not in captured.out


def test_stderr_also_redacted(capsys):
    install_pii_log_redaction(redact_secrets=False, pii_entities=["EMAIL_ADDRESS"])
    print("error contacting jane@acme.com", file=sys.stderr)
    captured = capsys.readouterr()
    assert "[EMAIL_ADDRESS]" in captured.err


def test_custom_mask_format():
    out = redact_text("jane@acme.com", redact_secrets=False, mask_format="<<{entity}>>")
    assert out == "<<EMAIL_ADDRESS>>"


def test_enable_presidio_without_dependency_raises_clear_error():
    try:
        import presidio_analyzer  # noqa: F401
        pytest.skip("presidio_analyzer is installed in this environment; nothing to assert here")
    except ImportError:
        pass
    with pytest.raises(ImportError, match="enable_presidio"):
        redact_text("some text", pii_entities=["PERSON"], enable_presidio=True)
