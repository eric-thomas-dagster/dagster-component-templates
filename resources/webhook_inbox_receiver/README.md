# Webhook Inbox Receiver

A small, standalone HTTP receiver for inbound webhook callbacks. **This is not a Dagster
`Component`/sensor/asset** -- it produces no Dagster definitions at all. It's a plain Python
process you run yourself, same shape as `resources/notion_mcp_login`'s sign-in CLI: a
`main(argv)` entrypoint you invoke with `python -m <module path>`, not something you reference
from a `defs.yaml`.

## Why this exists, and why it's paired with a separate sensor

Dagster sensors are **poll loops** -- the Dagster daemon evaluates a `SensorEvaluationContext`
on an interval. There is no Dagster mechanism for a sensor to bind a port and synchronously
receive a pushed HTTP request; every "react to an external event" sensor in this repo
(`sqs_monitor`, `s3_monitor`, `http_poll_sensor`) works by polling something already durable.
An inbound webhook is the opposite of that: a vendor pushes a POST at you whenever its own
async job happens to finish, and something has to be listening, synchronously, right then.

So this capability is split into two processes:

1. **This component** -- an always-on HTTP server (stdlib `http.server.ThreadingHTTPServer`
   only, no new dependency) that accepts the vendor's POST, verifies it, and durably records it
   to a local JSONL file as fast as possible.
2. **`sensors/webhook_inbox_sensor`** -- an ordinary Dagster sensor that polls that same file,
   exactly like `sqs_monitor` polls SQS, and turns new lines into `RunRequest`s.

Read `sensors/webhook_inbox_sensor/README.md` for the full worked example and the alternative
AWS-native pattern (`sqs_monitor` behind API Gateway/Lambda/SQS) for teams that don't want to
run this extra process themselves.

## Running it

```bash
python -m dagster_community_components.resources.webhook_inbox_receiver \
  --host 0.0.0.0 \
  --port 8080 \
  --path /webhook \
  --inbox-path /var/lib/webhooks/inbox.jsonl \
  --signature-header-name X-Signature-256 \
  --signature-secret-env-var OCR_VENDOR_WEBHOOK_SECRET \
  --signature-algorithm sha256 \
  --signature-prefix "sha256="
```

Every flag has an environment-variable equivalent (`WEBHOOK_INBOX_RECEIVER_<FIELD>`, e.g.
`WEBHOOK_INBOX_RECEIVER_PORT`, `WEBHOOK_INBOX_RECEIVER_INBOX_PATH`), so it can be driven
entirely by environment in a container or systemd unit with no CLI flags at all.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--host` | `WEBHOOK_INBOX_RECEIVER_HOST` | `0.0.0.0` | Interface to bind |
| `--port` | `WEBHOOK_INBOX_RECEIVER_PORT` | `8080` | Port to listen on |
| `--path` | `WEBHOOK_INBOX_RECEIVER_PATH` | `/webhook` | POST path to accept callbacks on |
| `--inbox-path` | `WEBHOOK_INBOX_RECEIVER_INBOX_PATH` | *(required)* | Local JSONL file to append accepted webhooks to |
| `--signature-header-name` | `WEBHOOK_INBOX_RECEIVER_SIGNATURE_HEADER_NAME` | *(unset)* | Header carrying the vendor's HMAC signature, e.g. `X-Signature-256` |
| `--signature-secret-env-var` | `WEBHOOK_INBOX_RECEIVER_SIGNATURE_SECRET_ENV_VAR` | *(unset)* | Name of the env var holding the shared HMAC secret (the secret's value itself is never passed on the command line) |
| `--signature-algorithm` | `WEBHOOK_INBOX_RECEIVER_SIGNATURE_ALGORITHM` | `sha256` | `hashlib` algorithm name for the HMAC digest |
| `--signature-prefix` | `WEBHOOK_INBOX_RECEIVER_SIGNATURE_PREFIX` | `""` | Prefix some vendors put before the hex digest, e.g. `sha256=` |

## Request handling

- Only `POST` to the configured `--path` is accepted; anything else gets `404`.
- If `--signature-secret-env-var` is configured: the HMAC is computed as
  `hmac.new(secret.encode(), raw_body_bytes, <algorithm>).hexdigest()`, the configured
  `--signature-prefix` is stripped from the header value if present, and the two are compared
  with `hmac.compare_digest` (constant-time). A missing or mismatched signature gets `401`,
  and **nothing is written to the inbox file**. Signature verification happens before JSON
  parsing, so a bad-signature request can't be used to probe whether verification is enabled.
- If `--signature-secret-env-var` is NOT configured: **every** POST with a valid JSON body is
  accepted, unauthenticated. This is an insecure default, documented as such -- it is suitable
  only for a trusted network or local development. Configure signature verification for
  anything reachable from the open internet.
- A non-JSON (or empty) body gets `400`.
- An accepted request gets one JSONL line appended to `--inbox-path`:
  `{"received_at": "<UTC ISO 8601>", "remote_addr": "<client IP>", "path": "<request path>",
  "body": <parsed JSON>}`, and an immediate `200`. The handler does no other processing --
  vendors generally retry aggressively on anything but a fast 2xx response, so this must never
  block on slow downstream work; `webhook_inbox_sensor` does the actual processing on its own
  schedule.

## TLS

This receiver only ever speaks plain HTTP. Put it behind a reverse proxy (nginx, an ALB/NLB,
Caddy, etc.) that terminates TLS -- that is out of scope for this component.

## Concurrency and durability

The server is a `ThreadingHTTPServer` -- each request is handled on its own thread. Appends to
the inbox file are serialized by a single in-process lock (the same lock/open-append/makedirs
shape used by this repo's `write_outcome_ledger_entry` helper in
`assets/ai/github_pr_tools/component.py`), so concurrent requests never interleave their writes
into a corrupted line. This receiver assumes it is the ONLY process writing to a given
`--inbox-path` -- it does not coordinate across multiple receiver processes sharing one file.

## Requirements

- Python 3.8+. No third-party dependencies -- `http.server`, `hmac`, `hashlib`, and `json` are
  all stdlib.

## License

MIT License
