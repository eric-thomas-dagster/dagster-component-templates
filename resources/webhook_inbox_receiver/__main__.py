"""Standalone HTTP receiver for inbound webhook callbacks (e.g. a document-processing /
OCR-extraction vendor that processes a job asynchronously and POSTs a callback when it
finishes). This is NOT a Dagster sensor or asset -- Dagster sensors are poll loops
(`SensorEvaluationContext`, evaluated on an interval by the Dagster daemon); there is no
mechanism for a sensor to bind a port and receive a pushed HTTP request inline. See
`sensors/webhook_inbox_sensor/README.md` for the two-piece pattern this is one half of.

Run as its own always-on process:

    python -m dagster_community_components.resources.webhook_inbox_receiver \
        --port 8080 --inbox-path /var/lib/webhooks/inbox.jsonl

Every field also has an env-var equivalent (WEBHOOK_INBOX_RECEIVER_<FIELD>), so it can be
driven entirely by environment in a container/systemd unit with no CLI flags at all.

What it does, in order, per POST request:
  1. Reads the raw request body.
  2. If `signature_secret_env_var` is configured, verifies an HMAC-SHA{...} signature on
     that raw body against a header (`signature_header_name`) using a constant-time
     comparison, and responds 401 on mismatch WITHOUT writing anything to the inbox.
     If NOT configured, signature verification is skipped entirely -- see the security
     note below.
  3. Parses the body as JSON; responds 400 on anything that isn't valid JSON (still only
     after signature verification, so an attacker can't use a malformed body to probe
     whether verification is even enabled).
  4. Appends ONE JSONL line (received timestamp, remote address, request path, parsed
     body) to `inbox_path` under a lock, and responds 200 immediately. It does NOT run
     any downstream processing itself -- `sensors/webhook_inbox_sensor` polls this same
     file and turns new lines into Dagster RunRequests. Vendors generally retry
     aggressively on anything but a fast 2xx, so the handler never blocks on slow work.

SECURITY NOTE: when no `signature_secret_env_var` is configured, this receiver accepts ANY
POST to its configured path with a valid JSON body -- there is no authentication at all.
That is an insecure default, suitable only for a trusted network / local development. Set
`signature_header_name` + `signature_secret_env_var` (matching whatever HMAC scheme your
vendor signs callbacks with) for anything reachable from the open internet, and terminate
TLS in front of this process (a reverse proxy) -- TLS termination is out of scope here.
"""
import argparse
import hashlib
import hmac
import json
import os
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

# BEGIN inbox jsonl append (inlined per component; mirrors the exact lock / open-append /
# makedirs pattern used by write_outcome_ledger_entry in
# assets/ai/github_pr_tools/component.py -- keep copies identical if either changes)
_INBOX_LOCK = threading.Lock()


def append_inbox_line(inbox_path: str, record: dict) -> None:
    with _INBOX_LOCK:
        os.makedirs(os.path.dirname(os.path.abspath(inbox_path)) or ".", exist_ok=True)
        with open(inbox_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
# END inbox jsonl append


class ReceiverConfig:
    def __init__(
        self,
        inbox_path: str,
        webhook_path: str = "/webhook",
        signature_header_name: Optional[str] = None,
        signature_secret: Optional[str] = None,
        signature_algorithm: str = "sha256",
        signature_prefix: str = "",
    ):
        self.inbox_path = inbox_path
        self.webhook_path = webhook_path
        self.signature_header_name = signature_header_name
        self.signature_secret = signature_secret
        self.signature_algorithm = signature_algorithm
        self.signature_prefix = signature_prefix or ""


def verify_signature(config: "ReceiverConfig", body_bytes: bytes, header_value: Optional[str]) -> bool:
    """Constant-time HMAC verification. Returns False (reject) whenever a secret is
    configured but the header is missing, malformed, or doesn't match -- never raises."""
    if not header_value:
        return False
    value = header_value.strip()
    if config.signature_prefix and value.startswith(config.signature_prefix):
        value = value[len(config.signature_prefix):]
    try:
        digestmod = getattr(hashlib, config.signature_algorithm)
    except AttributeError:
        return False
    expected = hmac.new(config.signature_secret.encode("utf-8"), body_bytes, digestmod).hexdigest()
    return hmac.compare_digest(expected, value)


class _InboxServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128  # socketserver's default of 5 drops/resets connections under
                              # even modest concurrent bursts (e.g. a vendor retry storm)

    def __init__(self, server_address, handler_cls, config: ReceiverConfig):
        self.config = config
        super().__init__(server_address, handler_cls)


class WebhookInboxHandler(BaseHTTPRequestHandler):
    server: _InboxServer  # type: ignore[assignment]

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802 -- BaseHTTPRequestHandler's own naming convention
        config = self.server.config
        if self.path != config.webhook_path:
            self._send_json(404, {"error": f"no such webhook path: {self.path!r}"})
            return

        content_length = int(self.headers.get("Content-Length") or 0)
        body_bytes = self.rfile.read(content_length) if content_length else b""

        if config.signature_secret:
            header_value = (
                self.headers.get(config.signature_header_name) if config.signature_header_name else None
            )
            if not verify_signature(config, body_bytes, header_value):
                self._send_json(401, {"error": "signature verification failed"})
                return

        try:
            if not body_bytes:
                raise ValueError("empty body")
            parsed_body = json.loads(body_bytes.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            self._send_json(400, {"error": f"invalid JSON body: {e}"})
            return

        record = {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "remote_addr": self.client_address[0],
            "path": self.path,
            "body": parsed_body,
        }
        append_inbox_line(config.inbox_path, record)
        self._send_json(200, {"status": "accepted"})

    def log_message(self, *args) -> None:  # silence BaseHTTPRequestHandler's default stderr logging
        pass


def build_server(host: str, port: int, config: ReceiverConfig) -> _InboxServer:
    return _InboxServer((host, port), WebhookInboxHandler, config)


def _env(name: str, default=None):
    return os.environ.get(f"WEBHOOK_INBOX_RECEIVER_{name}", default)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default=_env("HOST", "0.0.0.0"), help="Interface to bind (env WEBHOOK_INBOX_RECEIVER_HOST).")
    parser.add_argument("--port", type=int, default=int(_env("PORT", "8080")), help="Port to listen on (env WEBHOOK_INBOX_RECEIVER_PORT).")
    parser.add_argument("--path", default=_env("PATH", "/webhook"), help="POST path to accept callbacks on (env WEBHOOK_INBOX_RECEIVER_PATH).")
    parser.add_argument("--inbox-path", default=_env("INBOX_PATH"), required=_env("INBOX_PATH") is None,
                         help="Local JSONL file to append accepted webhooks to (env WEBHOOK_INBOX_RECEIVER_INBOX_PATH).")
    parser.add_argument("--signature-header-name", default=_env("SIGNATURE_HEADER_NAME"),
                         help="Header carrying the vendor's HMAC signature, e.g. X-Signature-256 (env WEBHOOK_INBOX_RECEIVER_SIGNATURE_HEADER_NAME). "
                              "Leave unset to skip signature verification entirely -- see the security note in this module's docstring.")
    parser.add_argument("--signature-secret-env-var", default=_env("SIGNATURE_SECRET_ENV_VAR"),
                         help="Name of the env var holding the shared HMAC secret (env WEBHOOK_INBOX_RECEIVER_SIGNATURE_SECRET_ENV_VAR). "
                              "The secret's VALUE is read from that env var at startup, never passed on the command line.")
    parser.add_argument("--signature-algorithm", default=_env("SIGNATURE_ALGORITHM", "sha256"),
                         help="hashlib algorithm name used for the HMAC digest (env WEBHOOK_INBOX_RECEIVER_SIGNATURE_ALGORITHM).")
    parser.add_argument("--signature-prefix", default=_env("SIGNATURE_PREFIX", ""),
                         help="Prefix some vendors put before the hex digest, e.g. 'sha256=' (env WEBHOOK_INBOX_RECEIVER_SIGNATURE_PREFIX).")
    args = parser.parse_args(argv)

    signature_secret = None
    if args.signature_secret_env_var:
        signature_secret = os.environ.get(args.signature_secret_env_var)
        if not signature_secret:
            print(
                f"ERROR: --signature-secret-env-var={args.signature_secret_env_var!r} is set but that env var "
                "is empty or unset. Refusing to start with signature verification half-configured.",
                file=sys.stderr,
            )
            raise SystemExit(1)
    else:
        print(
            "WARNING: no --signature-secret-env-var configured -- this receiver will accept ANY POST request "
            "with a valid JSON body, unauthenticated. Suitable only for a trusted network / local development.",
            file=sys.stderr,
        )

    config = ReceiverConfig(
        inbox_path=args.inbox_path,
        webhook_path=args.path,
        signature_header_name=args.signature_header_name,
        signature_secret=signature_secret,
        signature_algorithm=args.signature_algorithm,
        signature_prefix=args.signature_prefix,
    )
    server = build_server(args.host, args.port, config)
    print(f"webhook_inbox_receiver listening on http://{args.host}:{args.port}{args.path} -> {args.inbox_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
