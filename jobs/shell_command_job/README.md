# ShellCommandJobComponent

Run a shell command as a Dagster job (no asset materialized) — for cleanup, maintenance, ad-hoc tasks.

## Dependencies
(none)

## What this is
A **Dagster job** (no asset materialized) — the component installs a `dg.job`
plus an optional schedule. Lineage graph stays clean; the work happens
behind the scenes.

## See also
- [Schema](schema.json) · [Example config](example.yaml)
