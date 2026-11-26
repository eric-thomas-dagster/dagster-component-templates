# DataMaskingComponent

Rule-based PII masking — hash, partial-mask (last 4 only), full-redact, or character-substitute. Per-column policies.

## SQL source

Instead of `upstream_asset_key`, set `source` to pull rows directly via SQL
-- no intermediate asset required:

```yaml
source:
  kind: warehouse_query
  resource_key: snowflake_resource   # or database_url_env_var: DATABASE_URL
  sql: "SELECT * FROM my_table"
```

Mutually exclusive with `upstream_asset_key` -- set exactly one. Works with
any resource exposing `.get_engine()` (SQLAlchemy), `.get_connection()`
(DBAPI -- Postgres/Snowflake/DuckDB/MySQL/Databricks SQL), or `.get_client()`
(BigQuery, Redshift).

## StreamSets Field Hasher parity

For `method: hash` rules, three extra options reproduce StreamSets' Field Hasher
output closely enough to join against hashes StreamSets already wrote in
production:

- `algorithm`: `sha256` (default) | `md5` | `sha1` | `sha512` | `murmur3_128`
- `hash_mode`: `in_place` (default, hash the column itself) | `target_field`
  (hash `column`, write it to `output_field`, leave `column` untouched) |
  `record` (hash multiple `columns` concatenated together into `output_field`)
- `field_separator`: only used in `record` mode — appended after **every**
  field's value, including the last one (confirmed from StreamSets' own
  current Field Hasher docs: "Hash Record" mode appends the separator to
  each field before hashing, it is not a between-fields join). Field names
  are never included, only values.

```yaml
rules:
  - column: email
    method: hash
    algorithm: md5          # unsalted, untruncated — matches StreamSets' own MD5 output
  - column: ssn
    method: hash
    algorithm: murmur3_128
    hash_mode: target_field
    output_field: ssn_hash  # original ssn column is left untouched
  - column: first_name      # ignored in 'record' mode except as the rule's required `column` key
    method: hash
    algorithm: sha256
    hash_mode: record
    columns: [first_name, last_name, dob]
    field_separator: "|"
    output_field: identity_hash
```

**Backward compatibility:** the ONLY code path that applies the configured
salt (`salt_env`) and truncates to 16 hex characters is the original one —
`algorithm: sha256` (the default) together with `hash_mode: in_place` (the
default). Every existing `defs.yaml` that never set these new fields is
unaffected. Any other combination — a non-default algorithm, or `sha256`
used in `target_field`/`record` mode — produces a full, **unsalted** hex
digest, because StreamSets' own hasher neither salts nor truncates, and
truncating/salting a parity hash would defeat the entire point of it.

**What's independently verified vs. not:** the `record`-mode concatenation
scheme (separator-after-every-value, values only) is confirmed directly from
StreamSets' own live documentation for the Field Hasher processor. The
MD5/SHA1/SHA256/SHA512 hex output format matches StreamSets' own
`MessageDigest`-based hashing, confirmed by reading StreamSets' public,
open-source `FieldHasherProcessor.java` directly. **MURMUR3_128 byte-for-byte
parity with a live StreamSets instance is NOT independently verified** —
MURMUR3_128 support was added to StreamSets after the last publicly available
open-source snapshot (which only supports `java.security.MessageDigest`
algorithms), so its current implementation is closed-source. This component
uses the `mmh3` package's `mmh3_x64_128_digest()` (the standard
MurmurHash3_x64_128 algorithm, seed 0), hex-encoded the same way every other
algorithm here is — correct per MurmurHash3's own definition (e.g. hashing
the empty string with seed 0 is provably all-zero bytes, which this
component's test suite checks), but not confirmed against a real StreamSets
pipeline's output. Compare against a real StreamSets-produced hash before
relying on this for a production join.

One further StreamSets capability is **not** implemented here: a "Hash Field"
mode that may let multiple *source* fields feed one in-place/target-field
hash (hinted at by StreamSets' current docs mentioning a field separator on
that tab too), distinct from this component's `record` mode's *all-fields
required* `output_field`. That variant's exact mechanics couldn't be
confirmed from source (same closed-source gap as MURMUR3_128) — only the
clearly-documented `record` (whole-record) shape is implemented.

**Type-native hashing IS implemented (`encoding: streamsets`) — a prior note here claiming the premise
didn't hold was itself wrong**, traced to a method name (`generateHashForField`) that does not exist
anywhere in StreamSets' real open-source `FieldHasherProcessor.java`. The actual byte-producing code path
(confirmed by reading it directly, following the real call graph: `performHashingForFields` ->
`generateHash`/`handleHashingForTarget` -> `HashingUtil.getRecordFunnel(...)` ->
`hasher.hashObject(record, recordFunnel)` -> `RecordFunnel.funnel()`, in
`com.streamsets.pipeline.lib.hashing.HashingUtil.java`) dispatches on the field's real StreamSets type and
writes each one through Guava's typed `PrimitiveSink` methods -- `putBoolean`/`putByte`/`putShort`/
`putInt`/`putLong`/`putFloat`/`putDouble`, each its own native little-endian width, DATE/TIME/DATETIME as
an 8-byte little-endian epoch-millis `putLong`, and `putString` (UTF-8) for STRING/DECIMAL only. This is
corroborated independently, not just by re-reading the source: a reported real Data Collector 6.4.1 MD5 of
the INTEGER field `42` (`9824a7030ce67cf3f0efe7529f0c6ecc`) is reproduced exactly by hashing int32
little-endian `42` (`\x2a\x00\x00\x00`) -- see `streamsets_field_bytes()` in `component.py` and
`tests/test_hash_streamsets_native_bytes.py`.

```yaml
rules:
  - column: customer_id        # an INTEGER/LONG column in the source system
    method: hash
    algorithm: md5
    encoding: streamsets        # native per-type bytes, not text -- see above
```

`encoding: streamsets` (default: unset, the original text-hashing behavior above, unaffected) applies to
`in_place`/`target_field`/`record` alike. StreamSets' own type for a value isn't carried through a pandas
DataFrame, so by default this infers the narrowest type Data Collector's own JSON/CSV parsers would have
picked (a bool -> BOOLEAN; an int fitting a signed 32-bit range -> INTEGER, else LONG; a float -> DOUBLE;
anything with a `.timestamp()` method -> DATETIME; anything else -> STRING) -- it CANNOT see a narrower
type (FLOAT/SHORT/BYTE, or an integer-flag meant as BOOLEAN) that only an explicit schema or a Field Type
Converter stage assigned upstream in the real pipeline. Set `field_type` (one of boolean/byte/short/
integer/long/float/double/string/byte_array/datetime) to pin it explicitly when you know it; getting this
wrong produces a different, still-valid-looking StreamSets digest silently, not an error. `field_type` is
per-rule (one column in `in_place`/`target_field` mode) and is rejected with a clear error in `record` mode,
which spans several columns that may each have a different real type -- each column's type there is always
inferred independently per value.

MURMUR3_128's byte-for-byte parity caveat above still applies unchanged with `encoding: streamsets` --
only the input bytes fed to it changed (native vs. text), not the confidence level of the algorithm match
itself.

## StreamSets Field Masker parity

For `method: mask` rules — reproduced directly from StreamSets' own
open-source `FieldMaskProcessor.java`, read directly rather than inferred
(github.com/kiritbasu/datacollector, commit `9a475fd6a`, basic-lib's
`fieldmask` package):

- `mask_mode: fixed_length` — always outputs the literal 10-character string
  `xxxxxxxxxx` (StreamSets' own `FIXED_LENGTH_MASK` constant), regardless of
  the input value or length.
- `mask_mode: variable_length` — one `x` per character of the input.
- `mask_mode: custom` (requires `mask`) — a pattern string: `#` copies the
  next input character through unmasked, `x` emits a literal mask
  character, and any other character is emitted literally — consuming an
  input character only if the input already has that exact same character
  at the current position. This lets one pattern both format AND mask: the
  same `mask: "xxx-xx-####"` pattern produces `xxx-xx-6789` whether the
  input is the already-formatted `123-45-6789` (the `-`s line up and are
  consumed, not duplicated) or the raw `123456789` (the `-`s are inserted
  for free). The loop stops at the end of whichever of `mask`/the input
  value runs out first.
- `mask_mode: regex` (requires `regex` and `groups_to_show`, a list of
  1-based capture-group numbers) — the regex must match the **entire**
  value (Python `re.fullmatch`, matching Java's `Matcher.matches()` — not a
  partial/substring match). A value that doesn't fully match is returned
  **completely unmasked**, not an error. On a match, every character
  becomes `x` except the listed groups' original characters, placed back at
  their **original positions** in the full-length masked string (not
  concatenated separately) — e.g. `regex: "(.*)([0-9]{4})"`,
  `groups_to_show: [2]` against `"1234567890"` → `"xxxxxx7890"`.

```yaml
rules:
  - column: ssn
    method: mask
    mask_mode: custom
    mask: "xxx-xx-####"
  - column: credit_card
    method: mask
    mask_mode: regex
    regex: "(.*)([0-9]{4})"
    groups_to_show: [2]
```

## Dependencies
- `pandas`
- `mmh3` — optional, only needed for `algorithm: murmur3_128`

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name |
| `rules` | `list` | List of {column, method, ...} dicts. method: 'hash' \| 'partial' \| 'redact' \| 'substitute' \| 'pseudonymize' \| 'mask'. For method='hash', optional StreamSets-Field-Hasher-parity fields: `algorithm` ('sha256' default \… _(full docs in schema.json + component README)_ |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | — |
| `group_name` | `str` | `"transforms"` | — |
| `deps` | `list[str]` | — | — |
| `owners` | `list[str]` | — | — |
| `asset_tags` | `Union[Dict[str, Any], List[str]]` | — | — |
| `kinds` | `list[str]` | — | — |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5'. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily' / 'weekly' / 'monthly' / 'hourly' / 'static' / 'dynamic' / None for unpartitioned. |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_values` | `Union[str, List[str]]` | — | Comma-separated values for static partitioning, e.g. 'us,eu,asia'. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Useful for transient errors like network glitches or rate limits. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `Union[str, List[str]]` | — | Upstream DataFrame asset key. Mutually exclusive with `source` -- set exactly one. |
| `source` | `Dict[str, Any]` | — | Pull rows directly via SQL instead of from an upstream asset: {kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. Mutually exclusive with `upstream_asset_key` -- set exactly one. |
| `salt_env` | `str` | `"MASKING_SALT"` | Env var with hash salt (improves resistance to rainbow attacks) |
| `redacted_value` | `str` | `"***"` | Replacement value for 'redact' method |
| `include_preview_metadata` | `bool` | `true` | — |
| `preview_rows` | `int` | `20` | — |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition when partition_type='dynamic'. |

[//]: # (FIELDS:END)

## See also
- [Schema](schema.json) · [Example](example.yaml)