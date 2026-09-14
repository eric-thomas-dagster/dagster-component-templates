# RPA Queue Concurrency Lock

**Cross-run concurrency limit for RPA bots (or any bounded-license resource).**

RPA farms have hard concurrency ceilings: a UiPath tenant has N attended-bot
seats, an Automation Anywhere control room has N bot runners, a Blue Prism
resource pool has N workers. When Dagster fans out 100 partitioned
materializations at once, all 100 will happily race for those 5 (or 20 or
50) seats and either overrun the license, saturate the pool, or trigger
queue-side backpressure that surfaces as opaque HTTP 503s.

`RPAQueueConcurrencyLockComponent` gates each materialization on a named
`pool_key`. Multiple assets that reference the same `pool_key` share one
concurrency counter. When the pool is full, incoming materializations
`wait` (poll then request a Dagster retry), `skip` (no-op with observation),
or `fail` (raise `dg.Failure`) depending on `on_capacity`.

## Why this belongs in Dagster

- **Pool state lives in the Dagster event log** — no Redis, no external
  counter, no worker-local dict. Just `AssetObservation` events on a
  synthetic `__pool_<pool_key>` asset key. Restart-safe, worker-safe,
  visible in Dagit.
- **TTL auto-expiry** — a killed run doesn't permanently hold its slot.
  Any acquire older than `ttl_seconds` without a matching release is
  treated as released.
- **Composes** — pair with `partition_lock_asset` (per-partition mutex)
  or `throttle_asset` (inter-run gap) for stacked policies; borrows the
  same event-log pattern.

## Pool state machine

```
              +---------- pool state (event log) -----------+
              |                                             |
              |    active = COUNT(acquire) - COUNT(release) |
              |               where age(acquire) <= ttl     |
              |                                             |
              +---------------------------------------------+
                                     ^
                                     |
                                     |  observations on __pool_<pool_key>
                                     |
     +-------------------------------+----------------------+
     |                               |                      |
     v                               v                      v
+----------+                    +----------+           +----------+
|  run A   |                    |  run B   |           |  run C   |
| acquire  |     ...compute...  | acquire  |           |  see     |
|   -->    |                    |   -->    |           |  active  |
|          |                    |          |           |  == max  |
|          |     ...compute...  |          |           |          |
| release  |                    | release  |           |  policy: |
|   -->    |                    |   -->    |           |  wait |  |
+----------+                    +----------+           |  skip |  |
                                                       |  fail    |
                                                       +----------+
```

Every acquire is tagged with `run_id`. Every release pairs by `run_id`. A
`ttl_seconds` sweep on read discards any acquire older than TTL — so if a
run is killed mid-hold, its slot self-frees.

## Fan-out scenario this solves

```
             Dagster fires 100 partitioned runs of `invoice_bot`
                                 |
                                 v
     +----------+  +----------+  +----------+       +----------+
     | run 001  |  | run 002  |  | run 003  |  ...  | run 100  |
     |          |  |          |  |          |       |          |
     |acquire OK|  |acquire OK|  |wait/skip |       |wait/skip |
     |          |  |          |  |          |       |          |
     +----------+  +----------+  +----------+       +----------+
          |             |             |                  |
          v             v             v                  v
     +---------------------------------------------------------+
     |         UiPath pool: max_concurrent = 5                 |
     |   (only 5 hold the pool slot at any instant)            |
     +---------------------------------------------------------+
```

Without the lock: 100 UiPath API calls, license overrun, 95 opaque HTTP
errors. With the lock: 5 concurrent bots, 95 well-behaved retries. The
UiPath server sees exactly the traffic it's licensed for.

## Fields

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `asset_name` | string | required | Dagster asset that gets wrapped with the pool lock. |
| `pool_key` | string | required | Named pool identifier. Assets sharing this share one counter. |
| `max_concurrent` | integer | required | Max simultaneously-running holders. |
| `on_capacity` | enum(`wait` \| `skip` \| `fail`) | `wait` | Behavior when pool is full. |
| `wait_interval_seconds` | integer | `15` | Poll interval when `on_capacity=wait`. |
| `max_wait_attempts` | integer | `40` | Max in-run polls before `RetryRequested`. |
| `ttl_seconds` | integer | `3600` | Stale-acquire auto-expiry (killed-run protection). |
| `window_seconds` | integer | `7200` | Event-log lookback window (>= ttl_seconds). |
| `kinds` | array | `[python, rpa, queue-lock]` | Asset kinds to attach. |
| `group_name` | string | `rpa_queue_locks` | Dagster asset group. |
| `upstream_asset_key` | string | `null` | Slash-separated upstream to depend on. |
| `partition_type` | enum(`daily` \| `hourly`) | `null` | Partitioning shape (or unpartitioned). |
| `partition_start` | string | `null` | ISO start date when partitioned. |
| `description` | string | `null` | Prose shown in the Dagster UI. |

## Example — pool of 5 shared across three RPA assets

```yaml
# defs.yaml
components:
  - type: dagster_community_components.RPAQueueConcurrencyLockComponent
    attributes:
      asset_name: uipath_invoice_bot_slot
      pool_key: uipath_bot_pool_prod
      max_concurrent: 5
      on_capacity: wait

  - type: dagster_community_components.RPAQueueConcurrencyLockComponent
    attributes:
      asset_name: uipath_expense_bot_slot
      pool_key: uipath_bot_pool_prod        # same pool → shares the 5 seats
      max_concurrent: 5
      on_capacity: wait

  - type: dagster_community_components.RPAQueueConcurrencyLockComponent
    attributes:
      asset_name: uipath_reconcile_bot_slot
      pool_key: uipath_bot_pool_prod        # same pool → shares the 5 seats
      max_concurrent: 5
      on_capacity: skip                     # skip instead of wait for this one
```

At most 5 of `{uipath_invoice_bot_slot, uipath_expense_bot_slot,
uipath_reconcile_bot_slot}` will hold pool seats at any instant.
Incoming materializations for `invoice` and `expense` wait; for
`reconcile` they skip with an observation.

## Intended upstream / downstream

This component owns the *slot* — the actual RPA work sits either upstream
(the pool asset gates before the bot runs) or downstream (bot runs after
seeing the slot metadata). Common patterns:

- `uipath_orchestrator_integration` — the UiPath queue-item / job trigger
  component reads the pool asset as a dep so it only fires when a slot is
  held.
- `automation_anywhere_integration` — same pattern with the AA API.
- `activebatch_integration`, `runmyjobs_integration`, `stonebranch_uac_integration`
  — any external-execution integration where the vendor has its own
  concurrency ceiling.

## `on_capacity` tradeoffs

| Mode | Semantics | Cost | Use when |
| --- | --- | --- | --- |
| `wait` | Poll in-run, then `RetryRequested` (frees worker slot during backoff) | Holds a worker slot while polling in-run | You want back-pressured throughput and can tolerate the retry overhead |
| `skip` | Emit `pool_at_capacity` observation, return no-op `MaterializeResult` | Nothing (returns fast) | You'd rather drop the tick than queue — e.g. hourly refresh where next tick catches up |
| `fail` | Raise `dg.Failure` immediately | Downstream sees a failed materialization | You want visibility on saturation and rely on RetryPolicy for backoff |

## Event log inspection

Every slot event is queryable — `AssetObservation` on `__pool_<pool_key>` with:

- `event`: `acquire` | `release` | `pool_at_capacity`
- `run_id`: the holder's run id (pairs acquire/release)
- `asset`: which wrapped asset triggered it
- `ts_epoch` / `ts`: seconds since epoch + ISO timestamp

A companion sensor can scan for `acquire` events older than `ttl_seconds`
without a matching `release` and alert — those are stuck holders.

## Race disclosure

Like `partition_lock_asset`'s event_log backend, this is a probabilistic
counter, not a distributed atomic. Two runs polling within ~200ms could
both observe `active < max_concurrent` and acquire. Acceptable for
bot-license overrun protection at N=5..50; for hard atomicity, back the
pool with a Postgres advisory lock (see `partition_lock_asset`'s postgres
backend as a template) or use the RPA vendor's own queue backpressure.
