# Postgres change data capture (CDC) for dlt_load_collection -- design notes

**Status: investigated, not implemented.** No pip-installable, actively-maintained
path exists today to add a `source_type: "postgres_cdc"` mode to
`DltYamlSourceComponent` without vendoring code into this repo. Per the explicit
instruction this investigation was scoped under, that rules it out: this
component's own "no custom Python module required" design, and this repo's
"no vendored copies of someone else's source" convention, both assume the
underlying dlt engine is a real, versioned pip dependency -- true of
`dlt.sources.sql_database`/`rest_api`/`filesystem` (all built into `dlt` core),
not true of Postgres logical replication.

## What was checked

### 1. dlt's own `pg_replication` verified source

Confirmed directly against dlthub's docs (`https://dlthub.com/docs/dlt-ecosystem/verified-sources/pg_replication`):

- **Distribution: scaffold only.** The one documented setup path is
  `dlt init pg_replication <destination>`, which (per dlt's verified-sources
  model generally, and this page's own `from pg_replication import ...` example)
  copies the source's Python files into the caller's own project directory.
  There is no `pip install dlt-pg-replication`-style package -- the page lists
  only its runtime dependencies (`psycopg2-binary`, `sqlalchemy>=2.0`, optionally
  `pyarrow`/`numpy`), never a package name for the source itself.
- This means using it as designed makes the copied files **this repo's own
  code to maintain** the moment they're copied in -- exactly the vendoring this
  investigation was told not to do.
- dlt core itself (the `dlt` package proper, as opposed to verified-sources) has
  no built-in replication/CDC primitive as of the version this repo depends on
  (`dlt>=1.0.0`; verified installed version 1.30.0) -- `sql_database`,
  `rest_api`, and `filesystem` are the only source engines dlt ships as real
  importable modules, and none of them does logical replication.

### 2. Third-party packages named in the request

- **`pg_legacy_replication`**: not found. Checked `pip index versions
  pg_legacy_replication` (no matching distribution), a general web search, and a
  GitHub search -- the closest real projects under similar names are
  `supabase/pg_replicate` and `dejii/pg_replicate` (both Rust crates, not
  Python/dlt) and `waratuman/pg_replication` (a Ruby gem). None of these wrap
  dlt's Postgres replication source or are Python packages at all.
- **`dlt-plugin`**: not found. No matching PyPI distribution under that name.
- **`dlt_plus`** (dltHub's own commercial package, which *does* exist on PyPI):
  downloaded and inspected its wheel contents directly -- it has no
  replication/postgres/CDC module. It's a pipeline-runner/cache/dbt-generator
  add-on and requires a paid license (ships its own `common/license/` module),
  so even if it had this feature it would be a poor fit for a free, open
  community-components catalog that otherwise needs zero license keys.

**Verdict: no acceptable pip-installable, maintained option exists today.**

## What a real implementation would need (Postgres side), for reference

Confirmed against dlt's own docs for `pg_replication` (not independently
re-verified against Postgres's own docs beyond what's cited there):

- `wal_level = logical` (`ALTER SYSTEM SET wal_level = logical;`, takes effect
  only after a Postgres restart).
- A free replication slot (`max_replication_slots`) and a free WAL sender
  (`max_wal_senders`) per replicated pipeline.
- A publication covering the replicated tables -- only the table owner or a
  superuser can create one; a whole-schema publication needs a superuser.
- The pipeline's role needs the `REPLICATION` attribute and `SELECT` on the
  replicated tables (plus `CREATE` on the schema if using the source's
  snapshot-table initial-load modes).
- Postgres 10+ generally; 15+ for whole-schema replication without an explicit
  table list; 13+ for publishing partitioned tables; 16+ for logical decoding
  from a standby.
- Tables need a primary key or a replica identity index for updates/deletes to
  replicate correctly (otherwise restricted to insert-only).

## How deletes are represented (for reference)

dlt's `pg_replication` defaults to **hard deletes** -- a source-side `DELETE`
removes the row from the destination table too. Setting `hard_delete=False`
switches to **soft deletes**: the row is kept and dlt sets a `_dlt_deleted_ts`
timestamp column to the delete's commit time (not a boolean `is_deleted` flag).
Without `REPLICA IDENTITY FULL` on the source table, a delete event only
carries the key/replica-identity columns, so the retained row's other columns
come through as `NULL`.

## A real, buildable alternative worth a separate look: `xmin`-based tracking

dlt's own `pg_replication` docs name a fallback for exactly this situation
("logical replication doesn't fit"): Postgres's built-in, always-present
`xmin` system column (the id of the transaction that last wrote a row) used as
an incremental cursor against the **already-built-in** `sql_database` source
via its existing `query_adapter_callback`/incremental-cursor mechanism.

This is **not equivalent to CDC** and should never be marketed as one:

- It detects "this row changed since I last looked," not the specific
  before/after values or the operation type.
- It **cannot see deletes at all** -- a physically removed row leaves no `xmin`
  trace to find.
- `xmin` wraps around (~2 billion transactions) and is affected by `VACUUM
  FREEZE`, both real correctness caveats for a long-lived incremental cursor.

Unlike `pg_replication`, this needs no new source engine, no scaffolded files,
and no new Postgres server configuration beyond what `sql_database` already
needs -- it's a possible **enhancement to the existing `source_type:
"sql_database"` mode** (e.g. a `change_tracking: "xmin"` per-resource option),
not a new `source_type`. Worth scoping as its own, separate follow-up if
"notice new/changed rows without a timestamp column" turns out to be a real,
common ask -- explicitly out of scope for this investigation, which was about
Postgres CDC specifically.

## Conclusion

Per the explicit instruction this investigation was scoped under: no acceptable
option exists, so no code was vendored and `source_type: "postgres_cdc"` was
not added. If `pg_legacy_replication`, `dlt-plugin`, or another real,
maintained, pip-installable package surfaces later, re-run this investigation
against it specifically -- this doc's "not found" verdict is current as of
2026-11-07 (dlt 1.30.0) and could go stale.
