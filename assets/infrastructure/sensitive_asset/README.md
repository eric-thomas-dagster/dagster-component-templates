# `SensitiveAssetComponent` + `@sensitive` decorator

PII / secret redaction wrapper for asset compute. Scrubs `context.log.*` calls + `MaterializeResult.metadata` before persistence, so PII never lands in the Dagster event log.

## What it does

- **`context.log` proxy** — every `.info()` / `.warning()` / `.error()` call has its positional string args and `extra=` dict scanned; matching key patterns are redacted before the log message is written.
- **MaterializeResult metadata post-scrub** — after compute returns, matching keys in `.metadata` are replaced. This runs BEFORE the materialization event is emitted, so nothing sensitive lands in the event log.
- **AssetObservation counter** — every run emits `AssetObservation` with `sensitive_redacted_count=N` so audits can prove the redactor ran.

## Match rules

Each configured `key` is:

- A **case-insensitive glob** matched against dict keys (via `fnmatch`).
- A **substring pattern** matched inline against structured strings (`key=value`, `"key": "..."`, etc.) using a regex that catches `key=val`, `key="val"`, `key='val'`, and `key: val`.

Wildcards `*` and `?` supported. Defaults:

```python
["password", "passwd", "secret", "*_secret",
 "token", "*_token", "api_key", "*_api_key",
 "ssn", "credit_card", "cvv", "authorization"]
```

## Redaction strategy

| Strategy | Result |
|---|---|
| `redact` (default) | `[REDACTED]` |
| `hash` | `sha256:XXXXXXXX` (first 8 hex chars) |
| `mask` | `***` + last 4 chars |

## Why this belongs in Dagster

- **Event log = PII risk surface** — every `context.log.info` + every `MaterializeResult.metadata` dict ends up persisted. Wrapping compute stops bleed BEFORE persistence.
- **Per-asset attestation** — SOC2 audits benefit from scoped, per-asset proof over a global logger config.
- **Composable with the rest of the stack** — `@lifecycle` still audits raw data; only the log surface is scrubbed.

## Full YAML example

```yaml
type: dagster_community_components.SensitiveAssetComponent
attributes:
  asset_name: user_export

  compute:
    kind: python
    python: "my_project.exports:build_user_export"

  keys:
    - password
    - "*_token"
    - ssn
    - authorization

  strategy: redact    # redact (default) | hash | mask
```

## `@sensitive` decorator

```python
import dagster as dg
from dagster_community_components import sensitive

@dg.asset
@sensitive(keys=["password", "*_token", "ssn"], strategy="hash")
def user_export(context):
    context.log.info(f"processing user ssn=123-45-6789 token=abcd1234")
    # → info: "processing user ssn=sha256:e5f1a8c9 token=sha256:a1b2c3d4"
    return dg.MaterializeResult(
        metadata={
            "ssn": "123-45-6789",     # → hash
            "row_count": 42,           # untouched
        }
    )
```

## Composes with

- **`@lifecycle`** — audit stage still runs on unredacted data; logs stay safe.
- **`@profile`** — profiles still generated; matched columns get hashed instead of raw.
- **`@log_prints`** — print() output routed through the redactor before landing in the log.
- **`@dry_run`** — dry runs still scrub; validation is safe end-to-end.

## What's not in v1 (roadmap)

- **Microsoft Presidio integration** — swap the regex engine for a real PII detection model. Presidio ships pre-trained recognizers for 50+ PII entity types (SSN, credit cards, names, phone numbers, addresses, medical record numbers, banking data, ...) in 15+ languages, plus configurable NER models. Regex-based scrubbing catches known-format identifiers; Presidio catches semantic PII too ("John Doe was hospitalized on Tuesday" — no regex will find the name). Ship as opt-in via `engine: regex | presidio` field so users can pick the tradeoff (regex = zero deps, Presidio = heavier install + better recall).
- **Value-based regex patterns** — regex the *value* (e.g., detect SSN format regardless of key name).
- **Column-name-based DataFrame redaction** — auto-scrub matching columns on returned DataFrames.
- **Redaction key rotation** — periodically rotate the hash seed for privacy.
- **`context.add_output_metadata` scrubbing** — proxy wraps `context.log` but not `context.add_output_metadata`, so metadata added via that path bypasses the redactor. Wrap it too.
- **Non-`key=value` log format support** — current regex only catches `key=value` fragments. Wrap NER or Presidio to catch prose PII.

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`sensitive_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/sensitive_asset.md) | [`setup_sensitive_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_sensitive_asset_demo.sh) | 100% offline. Demonstrates BOTH shapes side by side: (1) `@sensitive` Python decorator scrubs `ssn=... password=... api_token=...` inline in a `context.log.info` call + matching keys in `MaterializeResult.metadata` (proof: `row_count=42` untouched), (2) `SensitiveAssetComponent { wraps: SyntheticDataGeneratorComponent }` — zero Python, proxies the inner's context.log through the redactor. Both emit the same `sensitive_redacted_count` observations for SOC2 per-asset attestation. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_sensitive_asset_demo.sh | bash
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name. |
| `compute` | `Dict[str, Any]` | `{kind: python, python: 'mod:fn'}`. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Default: ['python', 'sensitive']. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `keys` | `List[str]` | — | Case-insensitive globs against dict keys and structured strings. Defaults to a common PII/secrets list (passwords, tokens, api_key, ssn, credit_card, cvv, authorization). |
| `strategy` | `str` | `"redact"` | 'redact' (default) → [REDACTED]; 'hash' → sha256:xxxxxxxx; 'mask' → ***last4. |

[//]: # (FIELDS:END)
