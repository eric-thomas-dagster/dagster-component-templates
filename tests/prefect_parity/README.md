# Prefect `@task` parity — validation tests

Reproducible tests for the parity claims made in the internal "Prefect vs Dagster for agentic pipelines" doc. Every claim in that doc corresponds to at least one test here that **actually runs** — not just checks that code compiles.

## Latest results

See [`LATEST_RESULTS.txt`](./LATEST_RESULTS.txt) for the timestamped output of the most recent run.

**44 tests / 44 PASS** as of the most recent capture — 23 smoke + 21 real end-to-end.

## Running the tests

Two scripts, run separately or together:

```bash
# From the templates repo root
./.venv/bin/python tests/prefect_parity/smoke.py    # 23 tests, session-wide smoke sweep
./.venv/bin/python tests/prefect_parity/deep.py     # 19 tests, real end-to-end coverage
```

Each script writes color-coded PASS / FAIL lines to stdout and exits non-zero on any failure. No pytest or extra deps required — pure Python + dagster + a few tempdirs.

## What the two scripts cover

### `smoke.py` — session-wide smoke sweep

Validates that every component + decorator shipped in the parity session builds and materializes end-to-end via `demo_mode=true` (RPA / scheduler integrations) or their equivalent test-mode path. Fast — full sweep runs in under a minute.

- **8 batch scheduler integrations** — controlm / runmyjobs / jenkins / rundeck / stonebranch_uac / iws / activebatch / jams
- **4 RPA integrations** — uipath_orchestrator / automation_anywhere / blue_prism / power_automate
- **3 RPA utility components** — rpa_output_parser / rpa_queue_concurrency_lock / rpa_health_check
- **2 schedule components** — cron_schedule / interval_schedule (partitioned)
- **1 asset spec fix** — external_bigquery_table (kinds trimmed to 3)
- **cached_asset** — refresh_cache tag + input_hash_cache_key_fn helper
- **task_asset base cache surface** — CachePolicy composable + cache=True + LRU + NO_CACHE
- **task_asset new gaps** — async / retry_condition_fn / concurrency_pool

### `deep.py` — real end-to-end coverage

Exercises the actual behavior of every non-trivial claim (not just "the def builds"). Slower — under two minutes.

- **rpa_health_check** — 3 scenarios: PASS on fresh Successful / FAIL on Faulted status / FAIL on no materialization
- **rpa_output_parser** — real upstream materializes with `output_payload` metadata; parser reads and normalizes; verifies vendor+status+run_id+output_field all populated in the resulting row
- **cached_asset** — full MISS → HIT → refresh_cache tag → forced MISS across 3 runs, verifying compute-count matches expectations
- **@task retry_condition_fn** — DON'T-retry path (predicate False → single attempt, no retries)
- **@task ROOT_RUN default scoping** — cache does NOT bleed across runs (default behavior)
- **@task CROSS_RUN scoping** — cache DOES survive across runs (opt-in)
- **@task IOManagerBackedTaskCache** — real cache stored + retrieved via `fs_io_manager` backend
- **@task concurrency_pool** — actually BLOCKS: 5 threads against cap=2, peak concurrent measured at exactly 2
- **@task async** — nested-event-loop path: `asyncio.run()` fails when a loop already exists, verifies thread fallback works
- **@task timeout_seconds** — hard-kill compute past deadline
- **@task log_prints** — `print()` redirected to `context.log.info`, verified in run logs
- **@task on_completion / on_failure hooks** — both fire at correct times
- **@task retry_jitter_factor** — jittered delay accepted
- **@task task_run_name** — template renders correctly + materialize doesn't crash
- **@task result_storage_key** — template respected; 2 unique IDs → 2 computes (proves cache keys used the templated string, not the auto-hash)
- **@task viz_return_value** — marker emitted, return value still flows to caller
- **@task + smart_retry composition** — advanced retry classification works when `@task` raises inside a smart_retry-wrapped asset

## Test artifacts you can share

- [`smoke.py`](./smoke.py) — full source, ~200 lines
- [`deep.py`](./deep.py) — full source, ~640 lines
- [`LATEST_RESULTS.txt`](./LATEST_RESULTS.txt) — timestamped output of the most recent run, with per-test PASS lines + host + commit hash

Anyone with the templates repo checked out can reproduce in one command. If a claim in the internal doc changes, add a test here first.

## Non-scope

These tests cover the **DCC `@task` decorator + companion primitives** vs Prefect `@task`. They do NOT cover:

- Real HTTP against live scheduler/RPA servers (the `demo_mode=true` simulator paths exercise the whole component structure; real API paths are the customer's own bake-off)
- Cross-machine concurrency (single-process semaphore behavior is tested; the cross_run event-log-backed pool is verified via pre-seeded observations)
- UI rendering / retry-from-UI (Dagster-frontend patches exist for these — separate testing story)
