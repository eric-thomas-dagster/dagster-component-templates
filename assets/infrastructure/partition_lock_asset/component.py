"""PartitionLockAssetComponent + `@partition_lock` — cross-run partition-scoped mutex.

Prevent two concurrent runs from materializing the same `partition_key` of
the same asset. The lock is a Dagster `AssetObservation` — no Redis, no
side database. A conflicting run either waits (blocking sleep-poll),
skips (returns None + observation), or fails.

## Why this belongs in Dagster

- **Lock state lives in the event log** — restart-safe, worker-safe.
- **Auto-expires** via a stale-window check. No manual cleanup jobs.
- **Companion sensor pattern** — a monitor can find stuck locks
  (`partition_lock_acquired` observations older than TTL with no
  corresponding `partition_lock_released`).

Race condition disclosure: this is a probabilistic mutex, not a
distributed atomic. Two runs starting within ~200 ms of each other
could both observe an unlocked state and acquire. Acceptable for
"prevent 5-minute concurrent backfill duplicates" — NOT for money
transfers. For strong mutual exclusion, use a warehouse table lock or
Postgres advisory lock inside the compute.

## Two shapes

- **`PartitionLockAssetComponent`** (YAML)
- **`@partition_lock` decorator** (Python)

## Behavior

- On entry, scan recent `ASSET_OBSERVATION` events for tag
  `partition_lock_acquired=<partition_key>` newer than `ttl_seconds`.
  If found, the partition is considered locked.
- **`on_conflict=wait`** — sleep-poll every `poll_seconds` until the
  lock frees or `max_wait_seconds` elapses (then fail).
- **`on_conflict=skip`** — return without materializing; emit
  `partition_lock_skipped` observation.
- **`on_conflict=fail`** — raise `dg.Failure` immediately.
- On acquire, emit `partition_lock_acquired=<partition_key>` observation.
- On success or failure, emit `partition_lock_released=<partition_key>`
  observation (post-compute).

## Composes with

- `@smart_retry` — retries inside a held lock.
- `@throttle` — different orthogonal: throttle is inter-run gap;
  partition_lock is per-partition concurrency=1.
- `@sla`, `@timeout` — measure/kill the lock holder.
- `@lifecycle` — WAP inside the lock — safe atomic swap.
"""

import functools
import importlib
import time
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


_ACQUIRED_TAG = "partition_lock_acquired"
_RELEASED_TAG = "partition_lock_released"
_SKIPPED_TAG = "partition_lock_skipped"


def _lock_observation_records(context: Any, asset_key: dg.AssetKey, limit: int = 200) -> List[Any]:
    try:
        instance = getattr(context, "instance", None)
        if instance is None:
            return []
        from dagster import EventRecordsFilter, DagsterEventType
        return instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                event_type=DagsterEventType.ASSET_OBSERVATION,
                asset_key=asset_key,
            ),
            limit=limit,
            ascending=False,
        )
    except Exception:  # noqa: BLE001
        return []


def _lock_state(records: List[Any], partition_key: str, ttl_seconds: float) -> Optional[Dict[str, Any]]:
    """Return the acquired-lock record if a lock is currently held for `partition_key`.

    A lock is 'held' if the most recent acquired/released event for this
    partition_key is `acquired` AND its timestamp is within ttl_seconds.
    Returns None if no lock is held (never acquired, released, or stale).
    """
    now = time.time()
    for r in records:
        obs = getattr(r, "asset_observation", None)
        if obs is None:
            continue
        tags = getattr(obs, "tags", None) or {}
        # match most recent record referencing this partition
        pk_acquired = tags.get(_ACQUIRED_TAG)
        pk_released = tags.get(_RELEASED_TAG)
        if pk_acquired == partition_key:
            ts = getattr(r, "timestamp", None)
            if ts is None:
                return None
            age = now - float(ts)
            if age <= ttl_seconds:
                return {"acquired_at": float(ts), "age_seconds": age}
            return None
        if pk_released == partition_key:
            return None
    return None


def _emit_observation(context: Any, tags: Dict[str, str], metadata: Optional[Dict[str, Any]] = None) -> None:
    try:
        from dagster import AssetObservation
        asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["partition_lock_asset"])
        if hasattr(context, "log_event"):
            context.log_event(AssetObservation(
                asset_key=asset_key,
                tags=tags,
                metadata=metadata or {},
            ))
    except Exception:  # noqa: BLE001
        pass


def _acquire_lock(
    context: Any,
    asset_key: dg.AssetKey,
    partition_key: str,
    ttl_seconds: float,
    on_conflict: str,
    max_wait_seconds: float,
    poll_seconds: float,
) -> None:
    """Acquire the partition lock or handle the conflict per policy.

    Emits `partition_lock_acquired=<partition_key>` observation on success.
    """
    deadline = time.time() + max_wait_seconds
    while True:
        records = _lock_observation_records(context, asset_key, limit=200)
        held = _lock_state(records, partition_key, ttl_seconds)
        if held is None:
            _emit_observation(
                context,
                tags={_ACQUIRED_TAG: partition_key},
                metadata={
                    "partition_lock_ttl_seconds": dg.MetadataValue.float(float(ttl_seconds)),
                    "partition_key": dg.MetadataValue.text(partition_key),
                },
            )
            try:
                context.log.info(f"@partition_lock: acquired lock for partition_key={partition_key!r}")
            except Exception:  # noqa: BLE001
                pass
            return

        held_age = held.get("age_seconds", 0.0)
        if on_conflict == "fail":
            raise dg.Failure(
                description=f"@partition_lock: lock held for partition_key={partition_key!r} "
                            f"({held_age:.1f}s ago, ttl={ttl_seconds}s)",
                metadata={
                    "partition_key": dg.MetadataValue.text(partition_key),
                    "held_age_seconds": dg.MetadataValue.float(float(round(held_age, 3))),
                    "ttl_seconds": dg.MetadataValue.float(float(ttl_seconds)),
                },
            )
        if on_conflict == "skip":
            _emit_observation(
                context,
                tags={_SKIPPED_TAG: partition_key},
                metadata={
                    "partition_key": dg.MetadataValue.text(partition_key),
                    "held_age_seconds": dg.MetadataValue.float(float(round(held_age, 3))),
                },
            )
            try:
                context.log.info(
                    f"@partition_lock: SKIP partition_key={partition_key!r} — locked {held_age:.1f}s ago"
                )
            except Exception:  # noqa: BLE001
                pass
            raise _LockConflictSkip()

        # wait
        if time.time() >= deadline:
            raise dg.Failure(
                description=f"@partition_lock: wait timeout for partition_key={partition_key!r} "
                            f"after {max_wait_seconds}s",
                metadata={
                    "partition_key": dg.MetadataValue.text(partition_key),
                    "waited_seconds": dg.MetadataValue.float(float(round(max_wait_seconds, 3))),
                },
            )
        try:
            context.log.info(
                f"@partition_lock: waiting for partition_key={partition_key!r} (held {held_age:.1f}s ago)"
            )
        except Exception:  # noqa: BLE001
            pass
        time.sleep(poll_seconds)


def _release_lock(context: Any, partition_key: str) -> None:
    _emit_observation(
        context,
        tags={_RELEASED_TAG: partition_key},
        metadata={"partition_key": dg.MetadataValue.text(partition_key)},
    )


class _LockConflictSkip(Exception):
    """Internal sentinel — signals the caller to skip materialization on skip-policy conflict."""


def _get_partition_key(context: Any, override: Optional[str]) -> str:
    if override:
        return override
    try:
        pk = getattr(context, "partition_key", None)
        if pk:
            return str(pk)
    except Exception:  # noqa: BLE001
        pass
    return "__unpartitioned__"


def partition_lock(
    *,
    ttl_seconds: float = 3600.0,
    on_conflict: str = "wait",
    max_wait_seconds: float = 300.0,
    poll_seconds: float = 5.0,
    partition_key: Optional[str] = None,
) -> Callable:
    """Cross-run partition-scoped mutex via Dagster event log.

    ```python
    @dg.asset(partitions_def=daily_partitions)
    @partition_lock(ttl_seconds=1800, on_conflict="wait", max_wait_seconds=300)
    def daily_report(context):
        return build_report(context.partition_key)
    ```

    Args:
        ttl_seconds: Lock auto-expires after N seconds (protects against
            stuck holders). Default 1 hour.
        on_conflict: `wait` (default) sleep-polls; `skip` returns None;
            `fail` raises dg.Failure.
        max_wait_seconds: Max wait for `on_conflict=wait` before failing.
        poll_seconds: Poll interval for `on_conflict=wait`.
        partition_key: Override for the partition key (defaults to
            `context.partition_key`, or `__unpartitioned__` for
            non-partitioned assets).
    """
    if on_conflict not in ("wait", "skip", "fail"):
        raise ValueError(f"on_conflict must be 'wait', 'skip', or 'fail'; got {on_conflict!r}")

    def _decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            context = None
            if args and hasattr(args[0], "log"):
                context = args[0]
            elif "context" in kwargs and hasattr(kwargs["context"], "log"):
                context = kwargs["context"]
            if context is None:
                raise RuntimeError("@partition_lock requires a Dagster context.")

            pk = _get_partition_key(context, partition_key)
            asset_key = getattr(context, "asset_key", None) or dg.AssetKey(["partition_lock_asset"])

            try:
                _acquire_lock(context, asset_key, pk, ttl_seconds, on_conflict, max_wait_seconds, poll_seconds)
            except _LockConflictSkip:
                return None

            try:
                return fn(*args, **kwargs)
            finally:
                _release_lock(context, pk)

        return _wrapped
    return _decorator


class PartitionLockAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@partition_lock`. Wraps a compute with per-partition mutex."""

    asset_name: str = Field(description="Dagster asset name.")
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Dict[str, Any] = Field(description="`{kind: python, python: 'mod:fn'}`.")

    ttl_seconds: float = Field(
        default=3600.0,
        description="Lock TTL. If the most recent acquired event is older than this, lock is stale + treated as free.",
    )
    on_conflict: str = Field(
        default="wait",
        description="`wait` sleep-polls; `skip` returns None + emits skipped observation; `fail` raises dg.Failure.",
    )
    max_wait_seconds: float = Field(
        default=300.0,
        description="Max wait before failing when on_conflict=wait.",
    )
    poll_seconds: float = Field(
        default=5.0,
        description="Poll interval when on_conflict=wait.",
    )
    partition_key: Optional[str] = Field(
        default=None,
        description="Override partition key. Defaults to context.partition_key.",
    )

    group_name: Optional[str] = Field(default=None)
    description: Optional[str] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None, description="Default: ['python', 'lock'].")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Partition Lock Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        ttl = float(self.ttl_seconds)
        conflict = self.on_conflict
        max_wait = float(self.max_wait_seconds)
        poll = float(self.poll_seconds)
        pk_override = self.partition_key

        if conflict not in ("wait", "skip", "fail"):
            raise ValueError(f"on_conflict must be 'wait', 'skip', or 'fail'; got {conflict!r}")

        kinds_set = set(self.kinds or []) | {"python", "lock"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        ins = {}
        if upstream_asset_key:
            ins["upstream"] = dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"Partition-locked asset {asset_name} (ttl={ttl}s)",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            kinds=kinds_set,
            ins=ins,
        )
        def _asset(context: dg.AssetExecutionContext, **kwargs):
            asset_key = context.asset_key
            pk = _get_partition_key(context, pk_override)

            try:
                _acquire_lock(context, asset_key, pk, ttl, conflict, max_wait, poll)
            except _LockConflictSkip:
                return dg.MaterializeResult(
                    metadata={
                        "partition_lock_skipped": dg.MetadataValue.bool(True),
                        "partition_key": dg.MetadataValue.text(pk),
                    }
                )

            try:
                kind = (compute.get("kind") or "python").lower()
                if kind != "python":
                    raise ValueError(f"PartitionLockAssetComponent supports compute.kind=python only; got {kind!r}")
                ref = compute.get("python")
                if not ref or ":" not in ref:
                    raise ValueError("compute.python must be 'module.path:function_name'")
                mod_path, fn_name = ref.rsplit(":", 1)
                fn = getattr(importlib.import_module(mod_path.strip()), fn_name.strip(), None)
                if not callable(fn):
                    raise ValueError(f"compute.python {ref!r} not callable")

                import inspect
                sig = inspect.signature(fn)
                n_positional = sum(1 for p in sig.parameters.values()
                                   if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY))
                if n_positional == 0:
                    _ = fn()
                elif n_positional == 1:
                    _ = fn(context)
                else:
                    _ = fn(context, kwargs.get("upstream"))

                return dg.MaterializeResult(
                    metadata={
                        "partition_lock_skipped": dg.MetadataValue.bool(False),
                        "partition_key": dg.MetadataValue.text(pk),
                    }
                )
            finally:
                _release_lock(context, pk)

        return dg.Definitions(assets=[_asset])
