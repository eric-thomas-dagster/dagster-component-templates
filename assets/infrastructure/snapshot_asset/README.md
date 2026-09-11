# `SnapshotAssetComponent` + `@snapshot` decorator

Point-in-time snapshots of asset outputs. After compute succeeds, serialize + write to an fsspec URI keyed by `code_version + timestamp + run_id`. Rollback is an event log query.

## What it does

- After compute produces a value, write a snapshot to `<uri>/<asset_name>/<code_version>/<timestamp>__<run_id>.<ext>`.
- Auto-detect format: `pandas.DataFrame` → Parquet, `dict`/`list` → JSON, `bytes` → binary, `str` → text, else pickle. Explicit `format` override supported.
- Optional `retention_days`: after every write, prune older snapshots from the asset's folder.
- Emit `AssetObservation(snapshot_path, snapshot_bytes, snapshot_format, snapshot_pruned_count)` per write.
- Snapshot write failures are logged as warnings — the asset materialization still succeeds.
- Respects `dry_run=true` run tag: writes are skipped in dry-run mode.

## Path shape

```
<uri>/<asset_name>/<code_version>/<timestamp>__<run_id_short>.<ext>
```

- `code_version` — from the asset's `code_version=` (falls back to `unknown`).
- `timestamp` — UTC `YYYYMMDDTHHMMSSZ`.
- `run_id_short` — first 12 chars of `context.run_id`.

## Why this belongs in Dagster

- **`code_version`-aware paths** — rolling back "the last snapshot before we deployed v3" is a filesystem `find`.
- **AssetObservation events** — rollback UIs and audit tools query the event log for `snapshot_asset=written` observations. No side database.
- **fsspec URIs** — same code writes local / S3 / GCS / Azure.
- **Complementary to `@cached`** — `@cached` skips compute; `@snapshot` always runs but saves a checkpoint. Combine for retro-cache access.

## `@snapshot` vs. the IO manager — when to use which

Your Dagster IO manager already writes DataFrame returns to disk / cloud. Isn't `@snapshot` redundant?

**No — they solve different problems**:

| Concern | IO manager | `@snapshot` |
|---|---|---|
| Purpose | **Latest state** for downstream consumption | **Historical audit trail** |
| Files per (asset, partition) | 1, overwritten every run | N, accumulated over time |
| Path shape | `<asset>/<partition>` (deterministic) | `<uri>/<asset>/<code_version>/<UTC_ts>__<run_id>.<ext>` |
| Loaded by downstream? | Yes — feeds the DAG | No — write-only |
| Rollback support | No (write-2 clobbers write-1) | Yes — pick any prior file by code_version + ts |
| Cost | Required (writes are the point) | Extra write per run |

**Use `@snapshot` when:**
- Your IO manager overwrites (local parquet, `PickledObjectFilesystemIOManager`, most SQL/warehouse IO managers). No natural history.
- Regulated / audit environment: "prove what asset X emitted on 2024-11-14".
- Rollback playbook: "we shipped code_version 2.1 last week, numbers went bad, load snapshot from 2.0 to restore".
- Debug: compare last-week's output to today's without re-running compute.

**Skip `@snapshot` when:**
- **Iceberg / Delta Lake IO manager** — table format handles time-travel natively (`FOR VERSION AS OF ...`). `@snapshot` duplicates it.
- **Warehouse with time-travel** — Snowflake Time Travel, BigQuery table snapshots, similar.
- **Versioned cloud storage** — S3 versioning + lifecycle rule + a way to enumerate versions gives you the history.
- **You never need to look back at old outputs** — the extra write is pure overhead.

Rule of thumb: if your storage layer doesn't remember yesterday's value, `@snapshot` is your history.

## Full YAML example

```yaml
type: dagster_community_components.SnapshotAssetComponent
attributes:
  asset_name: daily_report

  compute:
    kind: python
    python: "my_project.reports:build_daily"

  uri: "s3://backups/report_snapshots"       # or gs://, abfs://, /local/path
  # format: null                              # auto-detect from returned value
  compression: zstd                           # parquet uses pyarrow codec; csv/json/text gain a .zst suffix
  retention_days: 30
  code_version: "2.1.0"
```

## `@snapshot` decorator

```python
import dagster as dg
from dagster_community_components import snapshot

@dg.asset(code_version="2.1.0")
@snapshot(uri="s3://backups/report_snapshots", retention_days=30)
def daily_report(context):
    return build_report()   # DataFrame → Parquet snapshot
```

## Rollback pattern

```python
@dg.asset(deps=[daily_report])
def restore_report(context):
    from dagster import EventRecordsFilter, DagsterEventType
    records = context.instance.get_event_records(
        event_records_filter=EventRecordsFilter(event_type=DagsterEventType.ASSET_OBSERVATION),
        limit=50, ascending=False,
    )
    snapshots = [r for r in records
                 if r.asset_observation
                 and r.asset_observation.tags.get("snapshot_asset") == "written"]
    latest = snapshots[0].asset_observation.metadata["snapshot_path"]
    return pd.read_parquet(latest.value)   # restore from event log
```

## Composes with

- **`@cached`** — read from the snapshot URI instead of recomputing.
- **`@lifecycle`** — snapshot AFTER publish so the snapshot matches production.
- **`@dry_run`** — dry runs don't snapshot (skip when tag active).
- **`@profile`** — snapshot the profile alongside the data.

## Compression

`compression` is a per-format field.

| Format | Supported codecs | How it's applied |
|---|---|---|
| `parquet` | `snappy` (default) / `gzip` / `zstd` / `brotli` / `lz4` | Forwarded to `df.to_parquet(compression=...)`; stored inside the parquet file, no filename suffix. |
| `json` / `text` / `bin` | `gzip` / `bz2` / `zstd` / `xz` | Applied via stdlib codecs (or `zstandard` for zstd); filename gains a `.gz` / `.bz2` / `.zst` / `.xz` suffix so the codec is visible on disk. |
| `pickle` | (none) | Compression flag is ignored; a warning is logged. |

Setting `compression: none` (or leaving it unset) uses the format default. Parquet's default is `snappy`; the text formats default to uncompressed.

## Loading snapshots

`load_snapshot` walks the event log for `AssetObservation(snapshot_asset=written)` entries and rehydrates the payload — no manual filesystem or event-log spelunking needed:

```python
from dagster_community_components import load_snapshot
from dagster import DagsterInstance

instance = DagsterInstance.get()  # or the ambient one inside a sensor/op

# Latest snapshot regardless of version
df = load_snapshot(instance, "daily_report", latest=True)

# Latest snapshot before a rollback event
df = load_snapshot(instance, "daily_report", at_or_before_ts="2024-11-14T00:00:00Z")

# Specific version's most recent snapshot
df = load_snapshot(instance, "daily_report", code_version="v1.2")
```

At least one of `code_version`, `at_or_before_ts`, or `latest=True` is required. The file's extension drives deserialization — parquet → `pd.DataFrame`, `.json` → DataFrame or dict/list, `.pkl` → the unpickled object, `.txt` → `str`, `.bin` → `bytes`. `.gz` / `.bz2` / `.zst` / `.xz` codec suffixes are decoded automatically.

Raises `FileNotFoundError` if no matching snapshot exists.

## What's not in v1 (roadmap)

- **Snapshot diffing** — surface the diff between two snapshots as an observation.

## CLI demos using this template

| Demo | Setup script | What it shows |
|---|---|---|
| [`snapshot_asset.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/snapshot_asset.md) | [`setup_snapshot_asset_demo.sh`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/setup_snapshot_asset_demo.sh) | 100% offline. Demonstrates BOTH shapes side by side: (1) `@snapshot` Python decorator writes a point-in-time parquet after every `@dg.asset` materialization, (2) `SnapshotAssetComponent { wraps: SyntheticDataGeneratorComponent }` — zero Python, wraps an inner data-gen with snapshotting. Two runs per shape prove the per-run timestamped path shape. Both emit the same `snapshot_asset=written` observations with `snapshot_path` / `snapshot_bytes` / `snapshot_format` metadata. |

```bash
curl -fsSL https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/setup_snapshot_asset_demo.sh | bash
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `uri` | `str` | fsspec URI directory for snapshots (e.g., `s3://bucket/dir`, `/local/path`). |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | — | Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode). |
| `code_version` | `str` | — | Optional asset code_version. Written into the snapshot path so rollbacks can filter by version. |
| `group_name` | `str` | — | — |
| `description` | `str` | — | — |
| `owners` | `List[str]` | — | — |
| `tags` | `Dict[str, str]` | — | — |
| `kinds` | `List[str]` | — | Default: ['python', 'snapshot']. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `format` | `str` | — | `parquet` \| `json` \| `pickle` \| `text` \| `bin`. If null, auto-detected from the returned value. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | — |
| `compute` | `Dict[str, Any]` | — | `{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`. |
| `wraps` | `Dict[str, Any]` | — | Wrap another DCC component's assets with point-in-time snapshot writes instead of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', attributes: {...}}`. Mutually exclusive with `compute`. |
| `compression` | `str` | — | Compression codec: `gzip` \| `zstd` \| `snappy` \| `brotli` \| `bz2` \| `xz` \| `none`. Default None = format default (parquet defaults to snappy). Parquet forwards this to `df.to_parquet(compression=...)`. JSON / text /… _(full docs in schema.json + component README)_ |
| `retention_days` | `int` | — | If set, delete snapshots older than N days from this asset's folder after a successful write. |

[//]: # (FIELDS:END)
