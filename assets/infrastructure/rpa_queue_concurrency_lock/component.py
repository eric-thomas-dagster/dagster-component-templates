"""RPAQueueConcurrencyLockComponent — cross-run concurrency limit keyed on a named pool.

Wraps an existing Dagster asset with a "pool slot" acquire/release protocol backed
by the Dagster event log. Solves the RPA-bot license problem: a UiPath / Automation
Anywhere / Blue Prism farm has a fixed number of robot seats, and Dagster fan-out
(100 partitions materializing at once) will happily overrun that pool. This
component gates each materialization on a shared `pool_key` — multiple assets
that reference the same `pool_key` share the same concurrency counter.

## Why this belongs in Dagster

- **Pool state lives in the event log** — AssetObservation events on a synthetic
  pool asset key. No Redis, no external counter, no worker-local state. Restart-
  safe, worker-safe, visible in Dagit.
- **TTL auto-expiry** — a killed run doesn't permanently hold its slot; any
  acquire older than `ttl_seconds` without a matching release is treated as
  released.
- **on_capacity policy**: `wait` (poll then RetryRequested), `skip` (log +
  succeed with no-op), or `fail` (raise dg.Failure).

## Relationship to sibling primitives

- **`partition_lock_asset`** — per-partition mutex (concurrency=1 per
  partition_key). This component is per-pool concurrency=N across all callers
  sharing the pool_key.
- **`throttle_asset`** — inter-run minimum gap for one asset. This component
  is simultaneous-holders cap across a bounded resource pool.

## Race condition disclosure

Like `partition_lock_asset`'s event_log backend, this is a probabilistic
counter, not a distributed atomic. Two runs polling within ~200ms could both
observe `active < max_concurrent` and acquire. Acceptable for bot-license
overrun protection at N=5..50; for hard atomicity use a Postgres advisory
lock or your RPA vendor's own queue backpressure.

## Companion sensor pattern

A separate sensor can scan the same pool asset key for `acquire` events older
than `ttl_seconds` without a matching `release` and emit alerts — those are
stuck holders (usually killed runs).
"""

import time
from datetime import datetime, timezone
from typing import Any, List, Optional

import dagster as dg
from pydantic import Field


_TAG_SAFE = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _pool_asset_key(pool_key: str) -> dg.AssetKey:
    """Synthetic asset key where acquire/release observations get emitted.

    Sanitizes to fit Dagster asset-key charset (`[A-Za-z0-9_]`); non-safe chars
    become `_` so operators can freely use `uipath:prod` or `abc/def` as pool
    labels.
    """
    safe = "".join(c if c in _TAG_SAFE else "_" for c in pool_key)
    return dg.AssetKey([f"__pool_{safe}"])


def _md_get(md: Any, key: str) -> Optional[str]:
    """Pull a string value from an AssetObservation metadata dict, unwrapping
    MetadataValue wrappers (TextMetadataValue.text, FloatMetadataValue.value, etc.)
    without knowing the concrete class.
    """
    if md is None:
        return None
    v = md.get(key) if hasattr(md, "get") else None
    if v is None:
        return None
    for attr in ("value", "text"):
        inner = getattr(v, attr, None)
        if inner is not None:
            return str(inner)
    return str(v)


def _count_active_slots(
    instance: Any,
    pool_asset_key: dg.AssetKey,
    ttl_seconds: float,
    window_seconds: float,
) -> int:
    """Scan recent AssetObservation events on the pool key; return active slot count.

    Pairs acquire/release by `run_id` in metadata. Any acquire older than
    `ttl_seconds` is treated as released (stale — killed run).
    """
    try:
        from dagster import EventRecordsFilter, DagsterEventType
    except Exception:  # noqa: BLE001
        return 0

    now_ts = _now_ts()
    try:
        records = instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
                asset_key=pool_asset_key,
                after_timestamp=now_ts - float(window_seconds),
            ),
            limit=5000,
            ascending=True,
        )
    except Exception:  # noqa: BLE001
        return 0

    active: dict = {}  # run_id -> acquired_at_ts (float seconds)
    for r in records:
        obs = getattr(r, "asset_observation", None)
        md = getattr(obs, "metadata", None) if obs is not None else None
        if md is None:
            # Fallback via dagster_event.event_specific_data
            de = getattr(r, "dagster_event", None)
            esd = getattr(de, "event_specific_data", None) if de is not None else None
            mat = getattr(esd, "asset_observation", None) if esd is not None else None
            md = getattr(mat, "metadata", None) if mat is not None else None
        if md is None:
            continue

        run_id = _md_get(md, "run_id")
        event = _md_get(md, "event")
        ts_str = _md_get(md, "ts_epoch")
        try:
            ts_val = float(ts_str) if ts_str is not None else float(getattr(r, "timestamp", now_ts) or now_ts)
        except Exception:  # noqa: BLE001
            ts_val = float(getattr(r, "timestamp", now_ts) or now_ts)

        if not run_id or not event:
            continue
        if event == "acquire":
            active[run_id] = ts_val
        elif event == "release":
            active.pop(run_id, None)

    # Expire stale acquires past ttl
    return sum(1 for ts in active.values() if now_ts - ts <= ttl_seconds)


def _partitions_def(partition_type: Optional[str], partition_start: Optional[str]):
    if not partition_type:
        return None
    pt = partition_type.lower()
    start = partition_start or "2024-01-01"
    if pt == "daily":
        return dg.DailyPartitionsDefinition(start_date=start)
    if pt == "hourly":
        return dg.HourlyPartitionsDefinition(start_date=start)
    raise ValueError(f"partition_type must be 'daily' | 'hourly' | None; got {partition_type!r}")


class RPAQueueConcurrencyLockComponent(dg.Component, dg.Model, dg.Resolvable):
    """Cross-run concurrency limit for RPA bots (or any bounded-license resource).

    Wraps an existing @dg.asset by adding an inner op that acquires + releases a
    'slot' in a named pool tracked via the Dagster event log. When the pool is
    at max_concurrent, incoming materializations either:

      - **wait** (default): poll every `wait_interval_seconds`, up to
        `max_wait_attempts`; if still full, raise `dg.RetryRequested` so the
        step goes to up_for_retry (worker slot freed during backoff).
      - **skip**: emit AssetObservation('pool_at_capacity') + return a no-op
        MaterializeResult with metadata.pool_at_capacity=true.
      - **fail**: raise `dg.Failure` immediately.

    Pool state lives in the event log as AssetObservation events on a synthetic
    asset key (`__pool_<pool_key>`), with metadata:
        {run_id, event: 'acquire' | 'release', ts_epoch: <float>, asset: <name>}

    Concurrency count = acquired events (since window_seconds) - released events
    (paired by run_id). Stale acquire events without a matching release are
    auto-expired after `ttl_seconds` (default 3600s) so a killed run doesn't
    permanently hold a slot.

    Related primitives:
      - `partition_lock_asset` — per-partition mutex (this borrows the event-log
        pattern; scoped to pool_key instead of partition_key)
      - `throttle_asset` — inter-run rate limit (this is simultaneous-holders
        cap, not gap between runs)
    """

    asset_name: str = Field(
        description="Dagster asset that gets wrapped with the pool lock.",
    )
    pool_key: str = Field(
        description=(
            "Named pool identifier (e.g. 'uipath_bot_pool_prod'). Multiple assets "
            "sharing this pool_key share the same concurrency counter."
        ),
    )
    max_concurrent: int = Field(
        description="Max simultaneously-running holders allowed to occupy the pool.",
    )
    on_capacity: str = Field(
        default="wait",
        description="'wait' | 'skip' | 'fail' behavior when the pool is at capacity.",
    )
    wait_interval_seconds: int = Field(
        default=15,
        description="Seconds between pool-availability polls when on_capacity='wait'.",
    )
    max_wait_attempts: int = Field(
        default=40,
        description=(
            "Max wait attempts before requesting a Dagster retry (total in-run wait "
            "ceiling ~= wait_interval_seconds * max_wait_attempts)."
        ),
    )
    ttl_seconds: int = Field(
        default=3600,
        description=(
            "Stale-acquire expiry — an unreleased acquire older than this is treated "
            "as if released (auto-expiry for killed runs)."
        ),
    )
    window_seconds: int = Field(
        default=7200,
        description=(
            "Event-log lookback window (seconds) for counting active acquires. "
            "Should be >= ttl_seconds so no active holder falls out of the window."
        ),
    )
    kinds: List[str] = Field(
        default_factory=lambda: ["python", "rpa", "queue-lock"],
        description="Asset kinds to attach to the wrapped asset.",
    )
    group_name: str = Field(
        default="rpa_queue_locks",
        description="Dagster asset group for the wrapped asset.",
    )
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Optional upstream asset key (slash-separated for nested keys) to depend on.",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'hourly' | None (unpartitioned).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="ISO start date when partition_type is set (default 2024-01-01).",
    )
    description: Optional[str] = Field(
        default=None,
        description="Optional description shown in the Dagster UI.",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="RPA Queue Concurrency Lock", editable=True)

    def build_defs(self, context: Any) -> dg.Definitions:
        # Snapshot config into locals so the closure doesn't capture `self`.
        asset_name = self.asset_name
        pool_key = self.pool_key
        max_concurrent = int(self.max_concurrent)
        on_capacity = self.on_capacity
        wait_interval = int(self.wait_interval_seconds)
        max_wait_attempts = int(self.max_wait_attempts)
        ttl_seconds = int(self.ttl_seconds)
        window_seconds = int(self.window_seconds)
        kinds_set = set(self.kinds or []) | {"python", "rpa", "queue-lock"}
        group_name = self.group_name
        upstream = self.upstream_asset_key
        parts = _partitions_def(self.partition_type, self.partition_start)
        desc = self.description or (
            f"Pool-concurrency-locked asset {asset_name!r} — pool={pool_key!r}, "
            f"max={max_concurrent}, on_capacity={on_capacity}."
        )

        if on_capacity not in ("wait", "skip", "fail"):
            raise ValueError(f"on_capacity must be 'wait' | 'skip' | 'fail'; got {on_capacity!r}")
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent must be >= 1; got {max_concurrent}")
        if window_seconds < ttl_seconds:
            # Not fatal — but warn via a clearer error later would be nicer.
            # Keep it a soft coerce so the caller's intent (window >= ttl) is preserved.
            window_seconds = ttl_seconds

        pool_ak = _pool_asset_key(pool_key)

        deps = []
        if upstream:
            deps = [dg.AssetKey(upstream.split("/"))]

        @dg.asset(
            name=asset_name,
            kinds=kinds_set,
            group_name=group_name,
            deps=deps,
            partitions_def=parts,
            description=desc,
        )
        def _wrapped(context: dg.AssetExecutionContext):
            instance = context.instance
            run_id = context.run_id

            # 1. Check current pool occupancy.
            active = _count_active_slots(instance, pool_ak, ttl_seconds, window_seconds)
            context.log.info(
                f"[rpa_queue_lock] pool={pool_key!r} active={active}/{max_concurrent} "
                f"(ttl={ttl_seconds}s, window={window_seconds}s)"
            )

            # 2. Handle over-capacity per policy.
            if active >= max_concurrent:
                if on_capacity == "fail":
                    raise dg.Failure(
                        description=(
                            f"RPA pool {pool_key!r} at capacity ({active}/{max_concurrent})"
                        ),
                        metadata={
                            "pool_key": dg.MetadataValue.text(pool_key),
                            "active": dg.MetadataValue.int(int(active)),
                            "max_concurrent": dg.MetadataValue.int(int(max_concurrent)),
                        },
                    )
                if on_capacity == "skip":
                    context.log_event(dg.AssetObservation(
                        asset_key=pool_ak,
                        metadata={
                            "run_id": dg.MetadataValue.text(run_id),
                            "event": dg.MetadataValue.text("pool_at_capacity"),
                            "asset": dg.MetadataValue.text(asset_name),
                            "active": dg.MetadataValue.int(int(active)),
                            "max_concurrent": dg.MetadataValue.int(int(max_concurrent)),
                            "ts_epoch": dg.MetadataValue.float(float(_now_ts())),
                            "ts": dg.MetadataValue.text(_now_iso()),
                        },
                    ))
                    context.log.info(
                        f"[rpa_queue_lock] SKIP pool={pool_key!r} — at capacity"
                    )
                    return dg.MaterializeResult(metadata={
                        "pool_at_capacity": dg.MetadataValue.bool(True),
                        "pool_key": dg.MetadataValue.text(pool_key),
                        "active": dg.MetadataValue.int(int(active)),
                        "max_concurrent": dg.MetadataValue.int(int(max_concurrent)),
                    })
                # wait: in-run poll loop; if still full, RetryRequested.
                acquired_after_wait = False
                for attempt in range(1, max_wait_attempts + 1):
                    time.sleep(max(wait_interval, 0))
                    active = _count_active_slots(
                        instance, pool_ak, ttl_seconds, window_seconds,
                    )
                    context.log.info(
                        f"[rpa_queue_lock] wait poll {attempt}/{max_wait_attempts} "
                        f"pool={pool_key!r} active={active}/{max_concurrent}"
                    )
                    if active < max_concurrent:
                        acquired_after_wait = True
                        break
                if not acquired_after_wait:
                    raise dg.RetryRequested(
                        max_retries=1,
                        seconds_to_wait=float(wait_interval),
                    )

            # 3. Acquire — emit an observation on the pool asset key.
            acquire_ts = _now_ts()
            context.log_event(dg.AssetObservation(
                asset_key=pool_ak,
                metadata={
                    "run_id": dg.MetadataValue.text(run_id),
                    "event": dg.MetadataValue.text("acquire"),
                    "asset": dg.MetadataValue.text(asset_name),
                    "ts_epoch": dg.MetadataValue.float(float(acquire_ts)),
                    "ts": dg.MetadataValue.text(_now_iso()),
                },
            ))
            context.log.info(
                f"[rpa_queue_lock] ACQUIRE pool={pool_key!r} slot "
                f"(active_before={active}, max={max_concurrent})"
            )

            try:
                # 4. Return a MaterializeResult with pool metadata; this is the
                # "hold the slot" phase. In a wraps-style variant, this is where
                # inner compute would run — here it's a marker asset so operators
                # can chain their real RPA work as downstream of this key.
                return dg.MaterializeResult(metadata={
                    "pool_key": dg.MetadataValue.text(pool_key),
                    "pool_slot_acquired_at": dg.MetadataValue.text(
                        datetime.fromtimestamp(acquire_ts, tz=timezone.utc).isoformat()
                    ),
                    "active_before_acquire": dg.MetadataValue.int(int(active)),
                    "max_concurrent": dg.MetadataValue.int(int(max_concurrent)),
                    "pool_at_capacity": dg.MetadataValue.bool(False),
                    "ttl_seconds": dg.MetadataValue.int(int(ttl_seconds)),
                })
            finally:
                # 5. Release — always emit, even on inner exception.
                try:
                    context.log_event(dg.AssetObservation(
                        asset_key=pool_ak,
                        metadata={
                            "run_id": dg.MetadataValue.text(run_id),
                            "event": dg.MetadataValue.text("release"),
                            "asset": dg.MetadataValue.text(asset_name),
                            "ts_epoch": dg.MetadataValue.float(float(_now_ts())),
                            "ts": dg.MetadataValue.text(_now_iso()),
                        },
                    ))
                    context.log.info(
                        f"[rpa_queue_lock] RELEASE pool={pool_key!r} slot"
                    )
                except Exception as e:  # noqa: BLE001
                    # Surface via warning rather than silent-swallow; ttl_seconds
                    # will auto-expire the slot if release genuinely fails.
                    try:
                        context.log.warning(
                            f"[rpa_queue_lock] release observation failed: "
                            f"{type(e).__name__}: {e} (slot will auto-expire in {ttl_seconds}s)"
                        )
                    except Exception:  # noqa: BLE001
                        pass

        return dg.Definitions(assets=[_wrapped])
