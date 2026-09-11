"""PartitionLockAssetComponent + `@partition_lock` — cross-run partition-scoped mutex.

Prevent two concurrent runs from materializing the same `partition_key` of
the same asset. Two backends:

- **`event_log`** (default, soft mutex) — lock lives in Dagster
  `AssetObservation` events. No Redis, no side database. Probabilistic:
  two runs starting within ~200 ms could both observe an unlocked
  state and acquire.
- **`postgres`** (hard mutex) — uses `pg_try_advisory_lock` on a
  Postgres session keyed on `hash(asset_key + partition_key)`. Truly
  atomic acquire; auto-releases on session end (crash-safe).

## Why this belongs in Dagster

- **Lock state lives in the event log** (event_log backend) — restart-safe, worker-safe.
- **Auto-expires** via a stale-window check. No manual cleanup jobs.
- **Companion sensor pattern** — a monitor can find stuck locks
  (`partition_lock_acquired` observations older than TTL with no
  corresponding `partition_lock_released`).

## Race condition disclosure (event_log backend only)

The default `event_log` backend is a **probabilistic mutex**, not a
distributed atomic. Two runs starting within ~200 ms of each other
could both observe an unlocked state and acquire. Acceptable for
"prevent 5-minute concurrent backfill duplicates" — NOT for money
transfers. For strong mutual exclusion, set `backend: postgres` and
supply `postgres_url_env_var` (requires `psycopg` or `psycopg2`).

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
import hashlib
import importlib
import math
import os
import time
from typing import Any, Callable, Dict, List, Optional

import dagster as dg
from pydantic import Field


_ACQUIRED_TAG = "partition_lock_acquired"
_RELEASED_TAG = "partition_lock_released"
_SKIPPED_TAG = "partition_lock_skipped"


# --------------------------------------------------------------------------
# Postgres advisory lock backend
# --------------------------------------------------------------------------


def _pg_lock_key(asset_key: dg.AssetKey, partition_key: str) -> int:
    """Derive a signed int64 key for pg_try_advisory_lock from asset + partition.

    Postgres advisory locks accept a single int64 or two int32s. We use the
    first 8 bytes of a SHA-256, interpreted as signed big-endian.
    """
    text = f"{asset_key.to_user_string()}:{partition_key}".encode()
    raw = hashlib.sha256(text).digest()[:8]
    return int.from_bytes(raw, byteorder="big", signed=True)


def _import_psycopg():
    """Return (module, api_version) tuple where api_version is 2 or 3.

    Prefers psycopg (v3) if both are installed. Raises dg.Failure with
    install hint if neither is present.
    """
    try:
        import psycopg  # type: ignore
        return psycopg, 3
    except ImportError:
        pass
    try:
        import psycopg2  # type: ignore
        return psycopg2, 2
    except ImportError:
        pass
    raise dg.Failure(
        description=(
            "@partition_lock backend=postgres requires psycopg or psycopg2. "
            "Install one: `pip install \"psycopg[binary]\"` OR `pip install psycopg2-binary`."
        )
    )


class _PgAdvisoryLock:
    """Context-manager-style holder for a Postgres advisory lock.

    Usage:
        h = _PgAdvisoryLock(url, key); acquired = h.try_acquire()
        try:
            ... compute ...
        finally:
            h.release()
    """

    def __init__(self, url: str, key: int):
        self.url = url
        self.key = int(key)
        self._psycopg, self._api = _import_psycopg()
        self._conn = None
        self._acquired = False

    def try_acquire(self) -> bool:
        self._conn = self._psycopg.connect(self.url)
        # Some psycopg2 configurations default to a transaction; advisory
        # locks are session-scoped so autocommit is safest.
        try:
            self._conn.autocommit = True
        except Exception:  # noqa: BLE001
            pass
        cur = self._conn.cursor()
        try:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (self.key,))
            row = cur.fetchone()
            self._acquired = bool(row and row[0])
            return self._acquired
        finally:
            cur.close()

    def release(self) -> None:
        try:
            if self._conn is not None and self._acquired:
                try:
                    cur = self._conn.cursor()
                    try:
                        cur.execute("SELECT pg_advisory_unlock(%s)", (self.key,))
                        try:
                            cur.fetchone()
                        except Exception:  # noqa: BLE001
                            pass
                    finally:
                        cur.close()
                except Exception:  # noqa: BLE001
                    pass
        finally:
            self._acquired = False
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self._conn = None


def _get_pg_url(postgres_url_env_var: Optional[str]) -> str:
    if not postgres_url_env_var:
        raise dg.Failure(
            description=(
                "@partition_lock backend=postgres requires `postgres_url_env_var` "
                "pointing at an env var holding a Postgres URL."
            )
        )
    url = os.environ.get(postgres_url_env_var)
    if not url:
        raise dg.Failure(
            description=(
                f"@partition_lock backend=postgres: env var "
                f"{postgres_url_env_var!r} is unset or empty."
            )
        )
    return url


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
    """Emit AssetObservation. Sanitizes tag values (Dagster requires
    `[A-Za-z0-9_.-]{,63}`) — composite partition keys with `/` and long
    hash-like values are moved from tags to metadata automatically.
    Errors surface via log.warning instead of silent swallow.
    """
    from dagster import AssetObservation, MetadataValue
    try:
        asset_key = context.asset_key
    except Exception:  # noqa: BLE001
        asset_key = dg.AssetKey(["partition_lock_asset"])

    safe_tags: Dict[str, str] = {}
    demoted_meta: Dict[str, Any] = {}
    for k, v in (tags or {}).items():
        sv = str(v)
        # Dagster tag values: [A-Za-z0-9_.-]{,63}
        if len(sv) > 63 or any(ch not in _TAG_SAFE for ch in sv):
            demoted_meta[k] = MetadataValue.text(sv)
            safe_tags[k] = "unsafe_in_metadata"  # short marker so filtering still works
        else:
            safe_tags[k] = sv

    merged_meta = dict(metadata or {})
    merged_meta.update(demoted_meta)

    if not hasattr(context, "log_event"):
        return
    try:
        context.log_event(AssetObservation(
            asset_key=asset_key,
            tags=safe_tags,
            metadata=merged_meta,
        ))
    except Exception as e:  # noqa: BLE001
        try:
            context.log.warning(f"@partition_lock: could not emit observation: {type(e).__name__}: {e}")
        except Exception:  # noqa: BLE001
            pass


_TAG_SAFE = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")


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
    On `on_conflict=wait`, raises `dg.RetryRequested` so the step goes to
    up_for_retry (worker slot freed during backoff, poll visible in run graph).
    """
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

    # `wait`: request a Dagster retry so we don't hold the worker slot
    # while backing off. `context.retry_number` (0-based) tracks the poll
    # count; budget = ceil(max_wait_seconds / poll_seconds).
    max_polls = max(1, math.ceil(max_wait_seconds / max(poll_seconds, 0.001)))
    retry_number = int(getattr(context, "retry_number", 0) or 0)
    poll_num = retry_number + 1  # 1-based for logs

    if retry_number >= max_polls:
        raise dg.Failure(
            description=f"@partition_lock: wait timeout for partition_key={partition_key!r} "
                        f"after {max_polls} polls (~{max_wait_seconds}s)",
            metadata={
                "partition_key": dg.MetadataValue.text(partition_key),
                "polls": dg.MetadataValue.int(poll_num),
                "waited_seconds_budget": dg.MetadataValue.float(float(round(max_wait_seconds, 3))),
            },
        )

    try:
        context.log.info(
            f"@partition_lock: waiting for partition_key={partition_key!r} "
            f"(held {held_age:.1f}s ago, poll {poll_num}/{max_polls}) — "
            f"requesting Dagster retry in {poll_seconds}s"
        )
    except Exception:  # noqa: BLE001
        pass

    raise dg.RetryRequested(
        max_retries=max_polls,
        seconds_to_wait=float(poll_seconds),
    )


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


def _acquire_dispatch(
    context: Any,
    asset_key: dg.AssetKey,
    pk: str,
    *,
    backend: str,
    ttl_seconds: float,
    on_conflict: str,
    max_wait_seconds: float,
    poll_seconds: float,
    postgres_url_env_var: Optional[str],
):
    """Return `(release_callable, backend_metadata_dict)` on successful acquire.

    Raises `_LockConflictSkip` on skip-policy conflict, or `dg.Failure`
    on fail/timeout. For the event_log backend, the release callback
    emits the released observation. For the postgres backend, it
    releases the advisory lock + closes the connection.
    """
    backend = (backend or "event_log").lower()
    if backend == "event_log":
        _acquire_lock(
            context, asset_key, pk, ttl_seconds, on_conflict, max_wait_seconds, poll_seconds,
        )

        def _release():
            _release_lock(context, pk)

        return _release, {"backend": "event_log"}

    if backend == "postgres":
        url = _get_pg_url(postgres_url_env_var)
        lock_key = _pg_lock_key(asset_key, pk)
        holder = _PgAdvisoryLock(url, lock_key)
        acquired = False
        try:
            acquired = holder.try_acquire()
        except Exception as e:  # noqa: BLE001
            holder.release()
            raise dg.Failure(
                description=(
                    f"@partition_lock backend=postgres: connect/acquire raised "
                    f"{type(e).__name__}: {e}"
                )
            ) from e

        if not acquired:
            # Conflict: another session holds the lock.
            holder.release()
            if on_conflict == "skip":
                _emit_observation(
                    context,
                    tags={_SKIPPED_TAG: pk},
                    metadata={
                        "partition_key": dg.MetadataValue.text(pk),
                        "backend": dg.MetadataValue.text("postgres"),
                        "pg_advisory_lock_key": dg.MetadataValue.int(int(lock_key)),
                    },
                )
                try:
                    context.log.info(
                        f"@partition_lock[pg]: SKIP partition_key={pk!r} — advisory lock held elsewhere"
                    )
                except Exception:  # noqa: BLE001
                    pass
                raise _LockConflictSkip()
            if on_conflict == "fail":
                raise dg.Failure(
                    description=(
                        f"@partition_lock[pg]: advisory lock held for "
                        f"partition_key={pk!r} (key={lock_key})"
                    )
                )
            # wait: use Dagster retry-request semantics like event_log backend.
            max_polls = max(1, math.ceil(max_wait_seconds / max(poll_seconds, 0.001)))
            retry_number = int(getattr(context, "retry_number", 0) or 0)
            if retry_number >= max_polls:
                raise dg.Failure(
                    description=(
                        f"@partition_lock[pg]: wait timeout for "
                        f"partition_key={pk!r} after {max_polls} polls"
                    )
                )
            raise dg.RetryRequested(
                max_retries=max_polls,
                seconds_to_wait=float(poll_seconds),
            )

        # Acquired.
        _emit_observation(
            context,
            tags={_ACQUIRED_TAG: pk},
            metadata={
                "partition_key": dg.MetadataValue.text(pk),
                "backend": dg.MetadataValue.text("postgres"),
                "pg_advisory_lock_key": dg.MetadataValue.int(int(lock_key)),
            },
        )
        try:
            context.log.info(
                f"@partition_lock[pg]: acquired advisory lock for partition_key={pk!r} (key={lock_key})"
            )
        except Exception:  # noqa: BLE001
            pass

        def _release():
            try:
                holder.release()
            finally:
                _release_lock(context, pk)

        return _release, {
            "backend": "postgres",
            "pg_advisory_lock_key": lock_key,
        }

    raise ValueError(
        f"@partition_lock: unknown backend {backend!r} — must be 'event_log' or 'postgres'"
    )


def partition_lock(
    *,
    ttl_seconds: float = 3600.0,
    on_conflict: str = "wait",
    max_wait_seconds: float = 300.0,
    poll_seconds: float = 5.0,
    partition_key: Optional[str] = None,
    backend: str = "event_log",
    postgres_url_env_var: Optional[str] = None,
) -> Callable:
    """Cross-run partition-scoped mutex via Dagster event log OR Postgres advisory lock.

    ```python
    @dg.asset(partitions_def=daily_partitions)
    @partition_lock(ttl_seconds=1800, on_conflict="wait", max_wait_seconds=300)
    def daily_report(context):
        return build_report(context.partition_key)
    ```

    Args:
        ttl_seconds: Lock auto-expires after N seconds (event_log
            backend only; protects against stuck holders). Default 1 hour.
        on_conflict: `wait` (default) requests a Dagster retry; `skip`
            returns None; `fail` raises dg.Failure.
        max_wait_seconds: Max wait for `on_conflict=wait` before failing.
        poll_seconds: Poll interval for `on_conflict=wait`.
        partition_key: Override for the partition key (defaults to
            `context.partition_key`, or `__unpartitioned__` for
            non-partitioned assets).
        backend: `event_log` (default, soft mutex via AssetObservation
            events — probabilistic acquire) OR `postgres` (hard mutex
            via pg_try_advisory_lock, requires `psycopg` or `psycopg2`).
        postgres_url_env_var: Name of the env var holding a Postgres URL.
            Required when `backend=postgres`. Lock key = signed int64
            derived from `sha256(asset_key + partition_key)`. Lock
            auto-releases on session end.
    """
    if on_conflict not in ("wait", "skip", "fail"):
        raise ValueError(f"on_conflict must be 'wait', 'skip', or 'fail'; got {on_conflict!r}")
    if backend not in ("event_log", "postgres"):
        raise ValueError(f"backend must be 'event_log' or 'postgres'; got {backend!r}")

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
                release, _meta = _acquire_dispatch(
                    context, asset_key, pk,
                    backend=backend,
                    ttl_seconds=ttl_seconds,
                    on_conflict=on_conflict,
                    max_wait_seconds=max_wait_seconds,
                    poll_seconds=poll_seconds,
                    postgres_url_env_var=postgres_url_env_var,
                )
            except _LockConflictSkip:
                return None

            try:
                return fn(*args, **kwargs)
            finally:
                release()

        return _wrapped
    return _decorator


class PartitionLockAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """YAML shape of `@partition_lock`. Two authoring modes:

    1. **Define a new asset from scratch** (original shape): supply
       `asset_name` + `compute: {kind: python, python: 'mod:fn'}`. Builds a
       single asset whose compute is protected by a per-partition mutex.

    2. **Wrap an existing DCC component** (composability): supply
       `wraps: {type: <component_class>, attributes: {...}}`. The inner
       component's assets get materialized as they would normally, but
       each compute is gated by the partition-scoped lock. Preserves
       inner asset partitions, deps, resources, kinds, tags, group,
       description. Direct YAML analog of `@partition_lock @dg.asset` in
       Python.

    `wraps:` and `compute:` are mutually exclusive.
    """

    asset_name: Optional[str] = Field(
        default=None,
        description="Dagster asset name. Required when NOT using `wraps:` (inherited from inner in wraps mode).",
    )
    upstream_asset_key: Optional[str] = Field(default=None)
    compute: Optional[Dict[str, Any]] = Field(
        default=None,
        description="`{kind: python, python: 'mod:fn'}`. Mutually exclusive with `wraps`.",
    )
    wraps: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Wrap another DCC component's assets with per-partition mutex behavior instead "
            "of defining new compute. Shape: `{type: 'dagster_community_components.<Component>', "
            "attributes: {...}}`. Mutually exclusive with `compute`."
        ),
    )

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
    backend: str = Field(
        default="event_log",
        description=(
            "Lock backend: `event_log` (default, soft mutex via AssetObservation events — "
            "races on simultaneous acquire) OR `postgres` (hard mutex via `pg_try_advisory_lock`, "
            "requires a Postgres URL env var + `psycopg` or `psycopg2` installed)."
        ),
    )
    postgres_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var name holding a Postgres URL. Required when `backend=postgres`. Uses "
            "`pg_try_advisory_lock` / `pg_advisory_unlock` keyed on a signed int64 derived from "
            "sha256(asset_key + partition_key). Lock auto-released on session end."
        ),
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
        # Route: wraps-an-inner-component  vs  builds-own-asset
        if self.wraps is not None:
            if self.compute is not None:
                raise ValueError("PartitionLockAssetComponent: `wraps:` and `compute:` are mutually exclusive.")
            return self._build_wrapped(context)

        if self.compute is None:
            raise ValueError("PartitionLockAssetComponent: supply either `compute` (build new asset) or `wraps` (wrap existing component).")
        if not self.asset_name:
            raise ValueError("PartitionLockAssetComponent: `asset_name` required when using `compute:`.")

        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        compute = dict(self.compute)
        ttl = float(self.ttl_seconds)
        conflict = self.on_conflict
        max_wait = float(self.max_wait_seconds)
        poll = float(self.poll_seconds)
        pk_override = self.partition_key
        backend = self.backend
        postgres_url_env_var = self.postgres_url_env_var

        if conflict not in ("wait", "skip", "fail"):
            raise ValueError(f"on_conflict must be 'wait', 'skip', or 'fail'; got {conflict!r}")
        if backend not in ("event_log", "postgres"):
            raise ValueError(f"backend must be 'event_log' or 'postgres'; got {backend!r}")

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
                release, backend_meta = _acquire_dispatch(
                    context, asset_key, pk,
                    backend=backend,
                    ttl_seconds=ttl,
                    on_conflict=conflict,
                    max_wait_seconds=max_wait,
                    poll_seconds=poll,
                    postgres_url_env_var=postgres_url_env_var,
                )
            except _LockConflictSkip:
                return dg.MaterializeResult(
                    metadata={
                        "partition_lock_skipped": dg.MetadataValue.bool(True),
                        "partition_key": dg.MetadataValue.text(pk),
                        "backend": dg.MetadataValue.text(backend),
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

                md = {
                    "partition_lock_skipped": dg.MetadataValue.bool(False),
                    "partition_key": dg.MetadataValue.text(pk),
                    "backend": dg.MetadataValue.text(backend_meta.get("backend", backend)),
                }
                if backend_meta.get("pg_advisory_lock_key") is not None:
                    md["pg_advisory_lock_key"] = dg.MetadataValue.int(
                        int(backend_meta["pg_advisory_lock_key"])
                    )
                return dg.MaterializeResult(metadata=md)
            finally:
                release()

        return dg.Definitions(assets=[_asset])

    # ----------------------------------------------------------------------
    # `wraps:` composability — YAML analog of `@partition_lock @dg.asset` stacking
    # ----------------------------------------------------------------------

    def _build_wrapped(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        """Instantiate the inner component; rewrap each of its assets with
        per-partition mutex around the original compute.
        """
        inner = _resolve_inner_component(self.wraps or {})
        inner_defs = inner.build_defs(context)

        wrapped_assets = []
        for asset_def in list(inner_defs.assets or []):
            if len(asset_def.keys) != 1:
                # Multi-asset AssetsDefinition not supported in v1 — pass through unwrapped.
                wrapped_assets.append(asset_def)
                continue
            wrapped_assets.append(self._wrap_single_asset(asset_def))

        return dg.Definitions(
            assets=wrapped_assets,
            resources=inner_defs.resources,
            sensors=inner_defs.sensors,
            schedules=inner_defs.schedules,
            asset_checks=inner_defs.asset_checks,
            jobs=inner_defs.jobs,
            loggers=inner_defs.loggers,
        )

    def _wrap_single_asset(self, asset_def: "dg.AssetsDefinition") -> "dg.AssetsDefinition":
        """Rebuild one single-key AssetsDefinition with per-partition mutex
        wrapping the original compute. Preserves
        partitions/deps/kinds/tags/group/description/metadata.
        """
        key = next(iter(asset_def.keys))
        specs_by_key = getattr(asset_def, "specs_by_key", {}) or {}
        spec = specs_by_key.get(key)

        inner_op = asset_def.op
        inner_compute = getattr(inner_op.compute_fn, "decorated_fn", None) or inner_op.compute_fn

        ttl = float(self.ttl_seconds)
        conflict = self.on_conflict
        max_wait = float(self.max_wait_seconds)
        poll = float(self.poll_seconds)
        pk_override = self.partition_key
        backend = self.backend
        postgres_url_env_var = self.postgres_url_env_var

        if conflict not in ("wait", "skip", "fail"):
            raise ValueError(f"on_conflict must be 'wait', 'skip', or 'fail'; got {conflict!r}")
        if backend not in ("event_log", "postgres"):
            raise ValueError(f"backend must be 'event_log' or 'postgres'; got {backend!r}")

        inner_kinds = set(getattr(spec, "kinds", None) or []) if spec else set()
        merged_kinds = inner_kinds | set(self.kinds or []) | {"lock"}
        inner_tags = dict(getattr(spec, "tags", None) or {}) if spec else {}
        merged_tags = {**inner_tags, **(self.tags or {})}
        merged_owners = list((spec.owners if spec else []) or []) + (self.owners or [])
        inner_description = (spec.description if spec else None) or f"Partition-locked {key.to_user_string()}"
        merged_description = (
            f"{inner_description}  "
            f"[partition_lock: ttl={ttl}s, on_conflict={conflict}]"
        )
        inner_deps = list(spec.deps) if (spec and getattr(spec, "deps", None)) else []

        @dg.asset(
            key=key,
            partitions_def=asset_def.partitions_def,
            deps=inner_deps,
            group_name=(spec.group_name if spec else None),
            kinds=merged_kinds,
            tags=merged_tags,
            owners=merged_owners,
            description=merged_description,
            metadata=(dict(spec.metadata) if (spec and spec.metadata) else {}),
            code_version=(spec.code_version if spec else None),
        )
        def _partition_lock_wrapped(context: dg.AssetExecutionContext, **kwargs):
            asset_key = context.asset_key
            pk = _get_partition_key(context, pk_override)

            try:
                release, backend_meta = _acquire_dispatch(
                    context, asset_key, pk,
                    backend=backend,
                    ttl_seconds=ttl,
                    on_conflict=conflict,
                    max_wait_seconds=max_wait,
                    poll_seconds=poll,
                    postgres_url_env_var=postgres_url_env_var,
                )
            except _LockConflictSkip:
                # on_conflict=skip: short-circuit without invoking inner compute.
                return dg.MaterializeResult(
                    metadata={
                        "partition_lock_skipped": dg.MetadataValue.bool(True),
                        "partition_key": dg.MetadataValue.text(pk),
                        "backend": dg.MetadataValue.text(backend),
                    }
                )

            try:
                result = inner_compute(context, **kwargs)

                # Merge lock metadata into the inner's MaterializeResult (if any).
                passthrough_meta = {
                    "partition_lock_skipped": dg.MetadataValue.bool(False),
                    "partition_key": dg.MetadataValue.text(pk),
                    "backend": dg.MetadataValue.text(backend_meta.get("backend", backend)),
                }
                if backend_meta.get("pg_advisory_lock_key") is not None:
                    passthrough_meta["pg_advisory_lock_key"] = dg.MetadataValue.int(
                        int(backend_meta["pg_advisory_lock_key"])
                    )
                if isinstance(result, dg.MaterializeResult):
                    merged = dict(result.metadata or {})
                    merged.update(passthrough_meta)
                    return dg.MaterializeResult(
                        asset_key=result.asset_key,
                        metadata=merged,
                        check_results=result.check_results,
                        data_version=result.data_version,
                        tags=result.tags,
                    )
                return result
            finally:
                release()

        return _partition_lock_wrapped


def _resolve_inner_component(wraps: Dict[str, Any]):
    """Resolve `{type: 'mod.path.ClassName' OR 'mod.path:ClassName', attributes: {...}}` → component instance."""
    type_str = wraps.get("type")
    attrs = wraps.get("attributes") or {}
    if not type_str or not isinstance(type_str, str):
        raise ValueError("PartitionLockAssetComponent.wraps requires `type: <fully-qualified-class-name>`.")
    if ":" in type_str:
        mod_path, cls_name = type_str.rsplit(":", 1)
    else:
        mod_path, cls_name = type_str.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_path)
    except ImportError as e:
        raise ValueError(f"PartitionLockAssetComponent.wraps: cannot import {mod_path!r}: {e}") from e
    cls = getattr(mod, cls_name, None)
    if cls is None:
        raise ValueError(f"PartitionLockAssetComponent.wraps: {cls_name!r} not found in {mod_path!r}.")
    try:
        return cls(**attrs)
    except Exception as e:  # noqa: BLE001
        raise ValueError(
            f"PartitionLockAssetComponent.wraps: constructing {type_str} failed: {type(e).__name__}: {e}"
        ) from e
