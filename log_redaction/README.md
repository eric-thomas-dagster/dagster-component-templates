# PII / secret redaction for compute logs — code-location level

Unlike [`compute_log_managers/`](../compute_log_managers/) — **instance-level**
infrastructure configured in `dagster.yaml`, which only works where you
control the Dagster instance (self-hosted OSS, or a Dagster+ **Hybrid**
agent's own `dagster.yaml`/Helm `computeLogs` values) — this package is a
plain Python function you call once, at import time, in your own code
location's `definitions.py`:

```python
# definitions.py
from dagster_community_components.log_redaction import install_pii_log_redaction

install_pii_log_redaction()  # call this BEFORE any asset/op code can run

import dagster as dg
defs = dg.Definitions(...)
```

## Why this exists as a separate thing from `compute_log_managers/`

**Dagster+ Serverless gives you no agent and no instance config to customize
at all** — there is no `dagster.yaml` a Serverless customer can edit, because
Dagster+ fully manages the instance. A `ComputeLogManager` (this package's
`splunk`/`otlp`/`tee` siblings, or your own) is therefore **Hybrid/OSS-only**.

`install_pii_log_redaction()` works everywhere — Serverless, Hybrid, and
OSS — because it doesn't touch instance config at all. Each Dagster code
location runs as its own Python process (the gRPC user code server);
replacing `sys.stdout`/`sys.stderr` once, at import time of that process,
covers every `print()` call and every op/asset that subsequently runs in it.

## The real trade-off — read this before you adopt it

|  | `compute_log_managers/` (instance-level) | `log_redaction/` (code-location level, this package) |
|---|---|---|
| Works on Dagster+ Serverless | ❌ No — no agent/instance config exists to set | ✅ Yes |
| Requires agent/platform-team access | ✅ Yes (Hybrid agent `dagster.yaml`/Helm values) | ❌ No — any code location owner can add it themselves |
| Coverage | One change, applies to **every** code location on that instance | Per code location — **you must add it to each one separately** |
| Org-wide enforcement | Automatic (there's only one instance config) | **Manual** — nothing stops a new or existing code location from forgetting this call |

That last row is the real cost of the Serverless-compatible approach: there
is no single switch. A code location that doesn't call
`install_pii_log_redaction()` has fully unredacted compute logs, and nothing
in Dagster itself will tell you that. Treat it the same way you'd treat a
required lint rule or security dependency:

- Add `dagster-community-components` (or wherever you vendor this module) to
  **every** code location's `requirements.txt`/`pyproject.toml`, not just the
  ones you remember have sensitive output.
- Add a CI check in your own pipeline that greps each code location's
  `definitions.py` for the `install_pii_log_redaction(` call (or, better,
  ship a shared internal base module that every code location imports, so
  there's one place to update instead of N).
- If you're on Hybrid (not Serverless) and want automatic, un-forgettable,
  org-wide coverage instead, use the instance-level `compute_log_managers/`
  package — it trades "self-serve per code location" for "one change covers
  everything," which may be the better trade for you if agent access isn't a
  bottleneck.

## What gets redacted

**PII (regex-based, no extra dependencies)**: `EMAIL_ADDRESS`, `PHONE_NUMBER`,
`US_SSN`, `CREDIT_CARD`, `IP_ADDRESS`. These are high-signal, low-false-positive
patterns — there is no reliable regex for "is this word a person's name" or
"is this a street address," so `PERSON`/`LOCATION`/`DATE_TIME`-style detection
requires `enable_presidio=True` (Microsoft Presidio + spaCy NLP — install the
`presidio-analyzer`, `presidio-anonymizer`, `spacy` packages and run
`python -m spacy download en_core_web_sm`; adds real latency and a higher
false-positive rate than the regex path, so it's opt-in).

**Secrets (`redact_secrets=True`, the default)**: AWS access keys, GitHub/Slack
tokens, PEM private key headers, JWTs. This is usually the higher-value half
of this module — a misconfigured client library logging its own auth header,
or a traceback that includes a connection string, is a more common and more
severe real-world compute-log incident than leaked personal data.

## Testing your own config

```python
from dagster_community_components.log_redaction import redact_text

redact_text("Contact jane@acme.com or call 555-123-4567", redact_secrets=False)
# -> "Contact [EMAIL_ADDRESS] or call [PHONE_NUMBER]"
```

## Limitations

- Catches `print()`/raw `sys.stdout`/`sys.stderr` writes — the same surface
  Dagster's own compute-log capture targets. It does **not** touch structured
  `context.log.info(...)` messages (those go through Dagster's separate event
  log / "Logs" tab, not "compute logs," and need a different mechanism), and
  it can't redact text written directly to a file descriptor by a subprocess
  or C extension that bypasses Python's `sys.stdout` object entirely.
- Buffers partial lines so a pattern split across two `write()` calls is still
  caught, but this means output is flushed on newlines (or an explicit
  `flush()`), not per `write()` call — fine for normal logging, but don't rely
  on sub-line write-ordering guarantees.
- Redaction happens in-process, before the OS ever sees the bytes — there is
  no unredacted copy to recover later (unlike `compute_log_managers/`'s
  `redact_on_storage: false` option, which can preserve an unredacted local
  copy). If you need an audit trail of the original, unredacted output, this
  is the wrong tool; use an instance-level `ComputeLogManager` with its own
  audit-copy mechanism instead.
