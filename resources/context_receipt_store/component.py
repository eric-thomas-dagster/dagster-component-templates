"""SQLite-backed context retrieval receipts and Dagster resource component."""

from __future__ import annotations

import json
import math
import os
import queue
import random
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dagster as dg
from pydantic import Field

_ITEM_FIELDS = {
    "item_id",
    "source_asset_key",
    "source_version",
    "rank",
    "score",
    "selection",
    "source_partition",
}
SELECTIONS = ("ranked", "explore")
# Outcome signals -> default value in [-1, 1]. Unknown signals need an explicit value.
OUTCOME_SIGNALS = {
    "thumbs_up": 1.0,
    "thumbs_down": -1.0,
    "accepted": 0.5,
    "edited": -0.3,
    "escalated": -0.7,
    "reopened": -0.7,
    "closed_won": 1.0,
    "closed_lost": -0.5,
}
# How much a receipt's outcome is credited/blamed on an item, by strongest evidence
# that the item was actually used. Retrieved-only is weak: it was merely in the window.
EVIDENCE_WEIGHTS = {"retrieved": 0.5, "cited": 1.0, "verified": 1.5, "counterfactual_tested": 2.0}
SPEC_VERSION = "1"  # receipt-spec.v1.json
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


class DuplicateReceiptError(ValueError):
    """Raised when a caller-assigned receipt_id already exists (safe to treat as success on retry)."""


def new_receipt_id() -> str:
    """Time-sortable ULID, generated client-side so the hot path needs no DB round trip."""
    value = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def rerank(
    candidates: Iterable[Mapping[str, Any]],
    scores: Mapping[str, Mapping[str, Any]],
    *,
    top_k: int | None = None,
    prior_weight: float = 0.2,
    explore_fraction: float = 0.1,
    rng: random.Random | None = None,
) -> list[dict[str, Any]]:
    """Reorder retrieval candidates by learned item priors, with reserved exploration.

    ``candidates`` are ``{"item_id", "score", ...}``; ``scores`` is the result of
    ``ContextReceiptStore.get_item_scores``. With probability ``explore_fraction`` one
    candidate from outside the top ``top_k`` replaces the last slot and is marked
    ``selection="explore"``; record that on the receipt so analysis can discount it.
    Without exploration, top-ranked items get all the credit (rich-get-richer).
    """
    rng = rng or random
    if not 0 <= explore_fraction <= 1:
        raise ValueError("explore_fraction must be between 0 and 1")
    adjusted = []
    for candidate in candidates:
        prior = (scores.get(candidate["item_id"]) or {}).get("prior") or 0.0
        adjusted.append({**candidate, "adjusted_score": float(candidate.get("score") or 0.0) + prior_weight * prior, "selection": "ranked"})
    adjusted.sort(key=lambda c: -c["adjusted_score"])
    if top_k is None or top_k >= len(adjusted):
        return adjusted
    chosen, rest = adjusted[:top_k], adjusted[top_k:]
    if top_k > 0 and rest and rng.random() < explore_fraction:
        pick = dict(rng.choice(rest))
        pick["selection"] = "explore"
        chosen[-1] = pick
    return chosen
# Suggested exclusion reasons; any non-empty string is accepted.
EXCLUSION_REASONS = ("pii", "stale", "out_of_scope", "over_budget", "policy")
_EXCLUDED_FIELDS = {"item_id", "reason", "source_asset_key", "source_version", "source_partition"}
_EVIDENCE_LEVELS = {"cited", "verified", "counterfactual_tested"}


class ContextReceiptStore:
    """Append-only retrieval receipts with source lookups for impact analysis.

    The store intentionally records identifiers, versions, and hashes rather
    than query text, retrieved content, or model output. Use a shared database
    service instead of SQLite when receipts must be written across hosts.
    """

    def __init__(self, database_path: str):
        if not database_path:
            raise ValueError("database_path must be a non-empty path")
        if database_path == ":memory:":
            raise ValueError("database_path must be persistent; ':memory:' is not supported")
        self.database_path = os.path.expanduser(database_path)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.database_path, timeout=30)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA busy_timeout = 30000")
            self._enable_wal(conn)
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS context_receipts (
                    receipt_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    context_name TEXT NOT NULL,
                    context_version TEXT,
                    policy_version TEXT,
                    caller_id TEXT,
                    purpose TEXT,
                    query_hash TEXT,
                    eligible_count INTEGER,
                    excluded_count INTEGER,
                    dagster_asset_key TEXT,
                    dagster_run_id TEXT,
                    trace_id TEXT,
                    mcp_session_id TEXT,
                    mcp_request_id TEXT
                );
                CREATE TABLE IF NOT EXISTS context_receipt_items (
                    receipt_id TEXT NOT NULL REFERENCES context_receipts(receipt_id),
                    item_id TEXT NOT NULL,
                    source_asset_key TEXT,
                    source_version TEXT,
                    source_partition TEXT,
                    rank INTEGER,
                    score REAL,
                    selection TEXT NOT NULL DEFAULT 'ranked',
                    PRIMARY KEY (receipt_id, item_id, rank)
                );
                CREATE TABLE IF NOT EXISTS context_item_scores (
                    item_id TEXT PRIMARY KEY,
                    retrievals INTEGER NOT NULL,
                    outcome_weight REAL NOT NULL,
                    outcome_sum REAL NOT NULL,
                    prior REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS context_receipt_excluded (
                    receipt_id TEXT NOT NULL REFERENCES context_receipts(receipt_id),
                    item_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    source_asset_key TEXT,
                    source_version TEXT,
                    source_partition TEXT,
                    PRIMARY KEY (receipt_id, item_id)
                );
                CREATE INDEX IF NOT EXISTS idx_context_receipt_excluded_item
                    ON context_receipt_excluded(item_id);
                CREATE INDEX IF NOT EXISTS idx_context_receipt_items_item
                    ON context_receipt_items(item_id);
                CREATE INDEX IF NOT EXISTS idx_context_receipt_items_asset
                    ON context_receipt_items(source_asset_key);
                CREATE TABLE IF NOT EXISTS context_receipt_events (
                    event_id TEXT PRIMARY KEY,
                    receipt_id TEXT NOT NULL REFERENCES context_receipts(receipt_id),
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_context_receipt_events_receipt
                    ON context_receipt_events(receipt_id, occurred_at);
                """
            )
            # databases created before a column existed
            for table, column, ddl in (
                ("context_receipts", "trace_id", "TEXT"),
                ("context_receipts", "mcp_session_id", "TEXT"),
                ("context_receipts", "mcp_request_id", "TEXT"),
                ("context_receipt_items", "selection", "TEXT NOT NULL DEFAULT 'ranked'"),
                ("context_receipt_items", "source_partition", "TEXT"),
                ("context_receipt_excluded", "source_partition", "TEXT"),
            ):
                if column not in {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _enable_wal(conn: sqlite3.Connection) -> None:
        # Switching journal mode ignores busy_timeout, so concurrent first
        # opens fail with "database is locked". Skip when already WAL and
        # retry otherwise.
        deadline = time.monotonic() + 30
        while True:
            if conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal":
                return
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    @staticmethod
    def _timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _normalize_item(item: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(item, Mapping):
            raise TypeError("each retrieved item must be a mapping")
        unknown = set(item) - _ITEM_FIELDS
        if unknown:
            raise ValueError(f"unsupported retrieved item field(s): {', '.join(sorted(unknown))}")
        item_id = item.get("item_id")
        if not isinstance(item_id, str) or not item_id.strip():
            raise ValueError("each retrieved item requires a non-empty item_id")
        rank = item.get("rank")
        if rank is not None and (
            not isinstance(rank, int) or isinstance(rank, bool) or rank < 0
        ):
            raise ValueError("item rank must be a non-negative integer")
        for field_name in ("source_asset_key", "source_version", "source_partition"):
            value = item.get(field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"item {field_name} must be a string or None")
        selection = item.get("selection", "ranked")
        if selection not in SELECTIONS:
            raise ValueError(f"item selection must be one of: {', '.join(SELECTIONS)}")
        score = item.get("score")
        if score is not None:
            if not isinstance(score, (int, float)) or isinstance(score, bool) or not math.isfinite(score):
                raise ValueError("item score must be a finite number")
            score = float(score)
        return {
            "item_id": item_id,
            "source_asset_key": item.get("source_asset_key"),
            "source_version": item.get("source_version"),
            "source_partition": item.get("source_partition"),
            "rank": rank,
            "score": score,
            "selection": selection,
        }

    @staticmethod
    def _normalize_excluded(item: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(item, Mapping):
            raise TypeError("each excluded item must be a mapping")
        unknown = set(item) - _EXCLUDED_FIELDS
        if unknown:
            raise ValueError(f"unsupported excluded item field(s): {', '.join(sorted(unknown))}")
        for field_name in ("item_id", "reason"):
            value = item.get(field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"each excluded item requires a non-empty {field_name}")
        for field_name in ("source_asset_key", "source_version", "source_partition"):
            value = item.get(field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"excluded {field_name} must be a string or None")
        return {
            "item_id": item["item_id"],
            "reason": item["reason"],
            "source_asset_key": item.get("source_asset_key"),
            "source_version": item.get("source_version"),
            "source_partition": item.get("source_partition"),
        }

    def record_retrieval(
        self,
        *,
        context_name: str,
        items: Iterable[Mapping[str, Any]],
        excluded: Iterable[Mapping[str, Any]] = (),
        context_version: str | None = None,
        policy_version: str | None = None,
        caller_id: str | None = None,
        purpose: str | None = None,
        query_hash: str | None = None,
        eligible_count: int | None = None,
        excluded_count: int | None = None,
        dagster_asset_key: str | None = None,
        dagster_run_id: str | None = None,
        receipt_id: str | None = None,
        trace_id: str | None = None,
        mcp_session_id: str | None = None,
        mcp_request_id: str | None = None,
    ) -> str:
        """Persist a retrieval fact and return its portable receipt ID.

        ``query_hash`` should be a one-way digest, not the raw query. Item
        mappings accept only IDs, source asset/version references, rank, and
        score so content cannot be accidentally serialized into the receipt.
        """
        if not isinstance(context_name, str) or not context_name.strip():
            raise ValueError("context_name must be a non-empty string")
        for field_name, value in (
            ("eligible_count", eligible_count),
            ("excluded_count", excluded_count),
        ):
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ValueError(f"{field_name} must be a non-negative integer")
        normalized_items = [self._normalize_item(item) for item in items]
        seen_ids: set[str] = set()
        for position, item in enumerate(normalized_items):
            if item["item_id"] in seen_ids:
                raise ValueError(f"duplicate item_id in receipt: {item['item_id']}")
            seen_ids.add(item["item_id"])
            if item["rank"] is None:
                item["rank"] = position
        normalized_excluded = [self._normalize_excluded(item) for item in excluded]
        excluded_ids = [item["item_id"] for item in normalized_excluded]
        if len(set(excluded_ids)) != len(excluded_ids):
            raise ValueError("duplicate item_id in excluded items")
        overlap = seen_ids & set(excluded_ids)
        if overlap:
            raise ValueError(
                f"item(s) cannot be both retrieved and excluded: {', '.join(sorted(overlap))}"
            )
        if excluded_count is None and normalized_excluded:
            excluded_count = len(normalized_excluded)
        elif excluded_count is not None and excluded_count < len(normalized_excluded):
            raise ValueError("excluded_count is smaller than the excluded items listed")
        if receipt_id is None:
            receipt_id = new_receipt_id()
        elif not isinstance(receipt_id, str) or not receipt_id.strip():
            raise ValueError("receipt_id must be a non-empty string")
        created_at = self._timestamp()
        with self._connect() as conn:
            if conn.execute(
                "SELECT 1 FROM context_receipts WHERE receipt_id = ?", (receipt_id,)
            ).fetchone():
                raise DuplicateReceiptError(f"receipt already exists: {receipt_id}")
            conn.execute(
                """
                INSERT INTO context_receipts (
                    receipt_id, created_at, context_name, context_version,
                    policy_version, caller_id, purpose, query_hash,
                    eligible_count, excluded_count, dagster_asset_key, dagster_run_id,
                    trace_id, mcp_session_id, mcp_request_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt_id,
                    created_at,
                    context_name,
                    context_version,
                    policy_version,
                    caller_id,
                    purpose,
                    query_hash,
                    eligible_count,
                    excluded_count,
                    dagster_asset_key,
                    dagster_run_id,
                    trace_id,
                    mcp_session_id,
                    mcp_request_id,
                ),
            )
            conn.executemany(
                """
                INSERT INTO context_receipt_items (
                    receipt_id, item_id, source_asset_key, source_version, source_partition,
                    rank, score, selection
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        receipt_id,
                        item["item_id"],
                        item["source_asset_key"],
                        item["source_version"],
                        item["source_partition"],
                        item["rank"],
                        item["score"],
                        item["selection"],
                    )
                    for item in normalized_items
                ],
            )
            conn.executemany(
                """
                INSERT INTO context_receipt_excluded (
                    receipt_id, item_id, reason, source_asset_key, source_version, source_partition
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        receipt_id,
                        item["item_id"],
                        item["reason"],
                        item["source_asset_key"],
                        item["source_version"],
                        item["source_partition"],
                    )
                    for item in normalized_excluded
                ],
            )
        return receipt_id

    def attach_output(
        self,
        receipt_id: str,
        *,
        output_id: str,
        model: str,
        output_hash: str,
        model_version: str | None = None,
        prefect_run_id: str | None = None,
    ) -> None:
        """Append an output reference to a retrieval receipt without mutating it."""
        if not output_id or not model or not output_hash:
            raise ValueError("output_id, model, and output_hash are required")
        payload = {
            "output_id": output_id,
            "model": model,
            "model_version": model_version,
            "output_hash": output_hash,
            "prefect_run_id": prefect_run_id,
        }
        self._append_event(receipt_id, "output_attached", payload)

    def add_evidence(
        self,
        receipt_id: str,
        *,
        level: str,
        details: Mapping[str, Any],
    ) -> None:
        """Append later evidence; details should contain references/hashes only."""
        if level not in _EVIDENCE_LEVELS:
            raise ValueError(
                f"level must be one of: {', '.join(sorted(_EVIDENCE_LEVELS))}"
            )
        if not isinstance(details, Mapping):
            raise TypeError("details must be a mapping")
        self._append_event(receipt_id, "evidence", {"level": level, "details": dict(details)})

    def _append_event(self, receipt_id: str, event_type: str, payload: Mapping[str, Any]) -> None:
        with self._connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM context_receipts WHERE receipt_id = ?", (receipt_id,)
            ).fetchone()
            if exists is None:
                raise KeyError(f"unknown context receipt: {receipt_id}")
            conn.execute(
                """
                INSERT INTO context_receipt_events (
                    event_id, receipt_id, event_type, occurred_at, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    receipt_id,
                    event_type,
                    self._timestamp(),
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                ),
            )

    def get_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        """Return the immutable retrieval record and its appended events."""
        with self._connect() as conn:
            receipt = conn.execute(
                "SELECT * FROM context_receipts WHERE receipt_id = ?", (receipt_id,)
            ).fetchone()
            if receipt is None:
                return None
            items = conn.execute(
                """
                SELECT item_id, source_asset_key, source_version, source_partition, rank, score, selection
                FROM context_receipt_items
                WHERE receipt_id = ?
                ORDER BY rank, item_id
                """,
                (receipt_id,),
            ).fetchall()
            excluded = conn.execute(
                """
                SELECT item_id, reason, source_asset_key, source_version, source_partition
                FROM context_receipt_excluded
                WHERE receipt_id = ?
                ORDER BY item_id
                """,
                (receipt_id,),
            ).fetchall()
            events = conn.execute(
                """
                SELECT event_id, event_type, occurred_at, payload_json
                FROM context_receipt_events
                WHERE receipt_id = ?
                ORDER BY occurred_at, event_id
                """,
                (receipt_id,),
            ).fetchall()
        result = dict(receipt)
        result["items"] = [dict(item) for item in items]
        result["excluded"] = [dict(item) for item in excluded]
        result["events"] = [
            {
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "occurred_at": event["occurred_at"],
                "payload": json.loads(event["payload_json"]),
            }
            for event in events
        ]
        return result

    def record_outcome(
        self,
        receipt_id: str,
        *,
        signal: str,
        value: float | None = None,
        source: str | None = None,
        outcome_ref: str | None = None,
    ) -> None:
        """Append an outcome (thumbs, human edit, escalation, deal result...) to a receipt.

        ``value`` is in [-1, 1]; known signals default (see ``OUTCOME_SIGNALS``), unknown
        ones need an explicit value. A later outcome with the same ``outcome_ref`` (for
        example a user id flipping thumbs-up to thumbs-down) supersedes the earlier one
        when scoring, so changing your mind is not double-counted.
        """
        if not isinstance(signal, str) or not signal.strip():
            raise ValueError("signal must be a non-empty string")
        if value is None:
            if signal not in OUTCOME_SIGNALS:
                raise ValueError(f"unknown signal {signal!r} requires an explicit value")
            value = OUTCOME_SIGNALS[signal]
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(value)
            or not -1 <= value <= 1
        ):
            raise ValueError("outcome value must be a finite number in [-1, 1]")
        self._append_event(
            receipt_id,
            "outcome",
            {"signal": signal, "value": float(value), "source": source, "outcome_ref": outcome_ref},
        )

    def read_since(
        self, receipt_cursor: int = 0, event_cursor: int = 0, limit: int = 1000
    ) -> dict[str, Any]:
        """Cold-path tail: new receipts and new outcomes since the given cursors.

        Cursors are monotonically increasing row positions, so a consumer (such as
        the Dagster sensor) can resume exactly where it stopped. Late outcomes for
        old receipts arrive through ``event_cursor``.
        """
        with self._connect() as conn:
            receipts = conn.execute(
                "SELECT rowid AS pos, * FROM context_receipts WHERE rowid > ? ORDER BY rowid LIMIT ?",
                (receipt_cursor, limit),
            ).fetchall()
            ids = [r["receipt_id"] for r in receipts]
            marks = ",".join("?" * len(ids))
            items = conn.execute(
                f"SELECT * FROM context_receipt_items WHERE receipt_id IN ({marks})", ids
            ).fetchall() if ids else []
            excluded = conn.execute(
                f"SELECT * FROM context_receipt_excluded WHERE receipt_id IN ({marks})", ids
            ).fetchall() if ids else []
            events = conn.execute(
                """
                SELECT rowid AS pos, receipt_id, payload_json FROM context_receipt_events
                WHERE event_type = 'outcome' AND rowid > ? ORDER BY rowid LIMIT ?
                """,
                (event_cursor, limit),
            ).fetchall()
            outcome_assets: dict[str, list[tuple[str, str | None]]] = {}
            if events:
                oids = sorted({e["receipt_id"] for e in events})
                omarks = ",".join("?" * len(oids))
                for row in conn.execute(
                    f"SELECT DISTINCT receipt_id, source_asset_key, source_partition FROM context_receipt_items "
                    f"WHERE receipt_id IN ({omarks}) AND source_asset_key IS NOT NULL",
                    oids,
                ):
                    outcome_assets.setdefault(row["receipt_id"], []).append(
                        (row["source_asset_key"], row["source_partition"])
                    )
        by_receipt: dict[str, dict[str, Any]] = {}
        for row in receipts:
            record = {k: row[k] for k in row.keys() if k != "pos"}
            record["items"], record["excluded"] = [], []
            by_receipt[row["receipt_id"]] = record
        for row in items:
            by_receipt[row["receipt_id"]]["items"].append(dict(row))
        for row in excluded:
            by_receipt[row["receipt_id"]]["excluded"].append(dict(row))
        outcomes = []
        for row in events:
            payload = json.loads(row["payload_json"])
            payload["receipt_id"] = row["receipt_id"]
            payload["sources"] = outcome_assets.get(row["receipt_id"], [])  # (asset_key, partition|None)
            outcomes.append(payload)
        return {
            "receipts": [by_receipt[r["receipt_id"]] for r in receipts],
            "outcomes": outcomes,
            "receipt_cursor": receipts[-1]["pos"] if receipts else receipt_cursor,
            "event_cursor": events[-1]["pos"] if events else event_cursor,
        }

    def refresh_item_scores(self, prior_strength: float = 5.0) -> int:
        """Recompute per-item quality priors from receipts + outcomes; returns items scored.

        For each receipt the effective outcome is the mean of its outcomes (a later
        outcome with the same ``outcome_ref`` replaces the earlier). Every retrieved
        item is credited/blamed with that outcome weighted by the strongest evidence
        that *it* was used: evidence events may name ``details.item_ids``; unscoped
        evidence applies to all items. ``prior = sum(w*outcome) / (sum(w) + prior_strength)``
        shrinks toward 0 until there is enough evidence. Items retrieved only via
        ``selection="explore"`` are still scored; exploration is what lets them be seen.
        """
        if prior_strength < 0:
            raise ValueError("prior_strength must be >= 0")
        with self._connect() as conn:
            items = conn.execute("SELECT receipt_id, item_id FROM context_receipt_items").fetchall()
            events = conn.execute(
                """
                SELECT receipt_id, event_type, payload_json FROM context_receipt_events
                WHERE event_type IN ('outcome', 'evidence') ORDER BY rowid
                """
            ).fetchall()
        outcomes: dict[str, dict[str, float]] = {}
        evidence: dict[str, list[tuple[float, set[str] | None]]] = {}
        for row in events:
            payload = json.loads(row["payload_json"])
            if row["event_type"] == "outcome":
                key = payload.get("outcome_ref") or f"anon-{len(outcomes.get(row['receipt_id'], {}))}"
                outcomes.setdefault(row["receipt_id"], {})[key] = payload["value"]
            else:
                ids = payload["details"].get("item_ids")
                evidence.setdefault(row["receipt_id"], []).append(
                    (EVIDENCE_WEIGHTS[payload["level"]], set(ids) if isinstance(ids, list) else None)
                )
        stats: dict[str, list[float]] = {}  # item -> [retrievals, weight, weighted_sum]
        for row in items:
            entry = stats.setdefault(row["item_id"], [0, 0.0, 0.0])
            entry[0] += 1
            values = outcomes.get(row["receipt_id"])
            if not values:
                continue
            weight = max(
                [EVIDENCE_WEIGHTS["retrieved"]]
                + [w for w, ids in evidence.get(row["receipt_id"], []) if ids is None or row["item_id"] in ids]
            )
            entry[1] += weight
            entry[2] += weight * (sum(values.values()) / len(values))
        now = self._timestamp()
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO context_item_scores
                    (item_id, retrievals, outcome_weight, outcome_sum, prior, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                    retrievals = excluded.retrievals, outcome_weight = excluded.outcome_weight,
                    outcome_sum = excluded.outcome_sum, prior = excluded.prior,
                    updated_at = excluded.updated_at
                """,
                [
                    (item, int(n), w, total, total / (w + prior_strength) if (w + prior_strength) else 0.0, now)
                    for item, (n, w, total) in stats.items()
                ],
            )
        return len(stats)

    def get_item_scores(self, item_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Read learned priors for ``rerank``; unknown items are simply absent."""
        ids = list(item_ids)
        if not ids:
            return {}
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM context_item_scores WHERE item_id IN ({','.join('?' * len(ids))})",
                ids,
            ).fetchall()
        return {row["item_id"]: dict(row) for row in rows}

    def ingest(self, event: Mapping[str, Any]) -> str:
        """Apply one spec-v1 event (see receipt-spec.v1.json); returns its receipt_id.

        This is the wire format between receipt *writers* (a FastMCP gateway, an agent
        runtime) and the store: a writer can emit these dicts over any transport and a
        consumer applies them here. Unknown spec versions and unknown fields are
        rejected, and a re-sent ``retrieval`` raises ``DuplicateReceiptError``.
        """
        if not isinstance(event, Mapping):
            raise TypeError("event must be a mapping")
        if event.get("spec_version") != SPEC_VERSION:
            raise ValueError(f"unsupported spec_version {event.get('spec_version')!r}; expected {SPEC_VERSION!r}")
        kind = event.get("type")
        body = {k: v for k, v in event.items() if k not in ("spec_version", "type", "created_at", "occurred_at")}
        receipt_id = body.get("receipt_id")
        if kind == "retrieval":
            allowed = {"receipt_id", "context_name", "context_version", "policy_version", "caller_id",
                       "purpose", "query_hash", "eligible_count", "excluded_count", "dagster_asset_key",
                       "dagster_run_id", "items", "excluded", "trace_id", "mcp_session_id", "mcp_request_id"}
            handler = self.record_retrieval
        elif kind == "output":
            allowed = {"receipt_id", "output_id", "model", "model_version", "output_hash", "prefect_run_id"}
            handler = self.attach_output
        elif kind == "evidence":
            allowed = {"receipt_id", "level", "details"}
            handler = self.add_evidence
        elif kind == "outcome":
            allowed = {"receipt_id", "signal", "value", "source", "outcome_ref"}
            handler = self.record_outcome
        else:
            raise ValueError(f"unknown event type {kind!r}")
        unknown = set(body) - allowed
        if unknown:
            raise ValueError(f"unsupported {kind} field(s): {', '.join(sorted(unknown))}")
        if not isinstance(receipt_id, str) or not receipt_id.strip():
            raise ValueError("event requires a non-empty receipt_id")
        if kind == "retrieval":
            handler(**body)
        else:
            handler(body.pop("receipt_id"), **body)
        return receipt_id

    def export_events(self, receipt_id: str) -> list[dict[str, Any]]:
        """Return a receipt as spec-v1 events (retrieval first, then appended events in order)."""
        receipt = self.get_receipt(receipt_id)
        if receipt is None:
            raise KeyError(f"unknown context receipt: {receipt_id}")
        keep = ("context_name", "context_version", "policy_version", "caller_id", "purpose", "query_hash",
                "eligible_count", "excluded_count", "dagster_asset_key", "dagster_run_id",
                "trace_id", "mcp_session_id", "mcp_request_id")
        events: list[dict[str, Any]] = [{
            "spec_version": SPEC_VERSION, "type": "retrieval", "receipt_id": receipt_id,
            "created_at": receipt["created_at"], **{k: receipt[k] for k in keep},
            "items": [dict(i) for i in receipt["items"]],
            "excluded": [dict(e) for e in receipt["excluded"]],
        }]
        for ev in receipt["events"]:
            base = {"spec_version": SPEC_VERSION, "receipt_id": receipt_id, "occurred_at": ev["occurred_at"]}
            payload = ev["payload"]
            if ev["event_type"] == "output_attached":
                events.append({**base, "type": "output", **payload})
            elif ev["event_type"] == "evidence":
                events.append({**base, "type": "evidence", **payload})
            elif ev["event_type"] == "outcome":
                events.append({**base, "type": "outcome", **payload})
        return events

    def asset_usage_since(self, asset_key: str, since: str) -> dict[str, Any]:
        """Window statistics for receipts that used ``asset_key`` (``since`` is an ISO-8601 UTC time).

        Returns the retrieved ``items`` as ``(source_partition, source_version)`` pairs, the
        per-receipt effective outcome (mean of outcomes, later ``outcome_ref`` superseding
        earlier), and how many receipts have an attached output / verified evidence.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT i.receipt_id, i.source_partition, i.source_version
                FROM context_receipt_items i JOIN context_receipts r ON r.receipt_id = i.receipt_id
                WHERE i.source_asset_key = ? AND r.created_at >= ?
                """,
                (asset_key, since),
            ).fetchall()
            ids = sorted({r["receipt_id"] for r in rows})
            events = conn.execute(
                f"SELECT receipt_id, event_type, payload_json FROM context_receipt_events "
                f"WHERE receipt_id IN ({','.join('?' * len(ids))}) ORDER BY rowid",
                ids,
            ).fetchall() if ids else []
        outcomes: dict[str, dict[str, float]] = {}
        with_output: set[str] = set()
        verified: set[str] = set()
        for ev in events:
            payload = json.loads(ev["payload_json"])
            if ev["event_type"] == "outcome":
                key = payload.get("outcome_ref") or f"anon-{len(outcomes.get(ev['receipt_id'], {}))}"
                outcomes.setdefault(ev["receipt_id"], {})[key] = payload["value"]
            elif ev["event_type"] == "output_attached":
                with_output.add(ev["receipt_id"])
            elif ev["event_type"] == "evidence" and payload["level"] in ("verified", "counterfactual_tested"):
                verified.add(ev["receipt_id"])
        return {
            "receipts": len(ids),
            "items": [(r["source_partition"], r["source_version"]) for r in rows],
            "outcome_means": [sum(v.values()) / len(v) for v in outcomes.values()],
            "with_output": len(with_output),
            "verified_outputs": len(with_output & verified),
        }

    def find_by_assets(self, targets: Mapping[str, Iterable[str] | None]) -> dict[str, list[str]]:
        """Receipts that used the given assets, optionally narrowed to partitions.

        ``targets`` maps an asset key to a set of partition keys, or ``None`` for "any
        partition". Returns ``{"receipt_ids": [...], "partition_unknown": [...]}``;
        receipts whose matching item recorded no ``source_partition`` for a partitioned
        target cannot be ruled out, so they are reported separately rather than dropped.
        """
        exact: set[str] = set()
        unknown: set[str] = set()
        with self._connect() as conn:
            for asset_key, partitions in targets.items():
                rows = conn.execute(
                    "SELECT DISTINCT receipt_id, source_partition FROM context_receipt_items "
                    "WHERE source_asset_key = ?",
                    (asset_key,),
                ).fetchall()
                wanted = None if partitions is None else set(partitions)
                for row in rows:
                    if wanted is None or row["source_partition"] in wanted:
                        exact.add(row["receipt_id"])
                    elif row["source_partition"] is None:
                        unknown.add(row["receipt_id"])
        return {"receipt_ids": sorted(exact), "partition_unknown": sorted(unknown - exact)}

    def find_by_item(self, item_id: str) -> list[str]:
        """Return receipt IDs that retrieved a particular stable item ID."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT receipt_id FROM context_receipt_items
                WHERE item_id = ? ORDER BY receipt_id
                """,
                (item_id,),
            ).fetchall()
        return [row["receipt_id"] for row in rows]

    def find_by_source_asset(self, asset_key: str) -> list[str]:
        """Return receipt IDs that included items from a Dagster asset key."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT receipt_id FROM context_receipt_items
                WHERE source_asset_key = ? ORDER BY receipt_id
                """,
                (asset_key,),
            ).fetchall()
        return [row["receipt_id"] for row in rows]


class AsyncReceiptWriter:
    """Fire-and-forget receipt writes so the agent never waits on the store.

    Receipt IDs are assigned up front, so ``record_retrieval`` returns instantly and
    later ``attach_output`` / ``record_outcome`` calls can reference it before the
    row is durable. A single worker applies operations in order with bounded retries;
    operations that still fail land in ``dead_letters`` for inspection/replay. This is
    in-process and not crash-safe: use a durable outbox/queue when losing receipts on
    process death is unacceptable.
    """

    _OPS = ("record_retrieval", "attach_output", "add_evidence", "record_outcome")

    def __init__(self, store: ContextReceiptStore, *, max_retries: int = 5, backoff_seconds: float = 0.1):
        self.store = store
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.dead_letters: list[tuple[str, tuple, dict, BaseException]] = []
        self._queue: queue.Queue = queue.Queue()
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()

    def record_retrieval(self, **kwargs: Any) -> str:
        kwargs["receipt_id"] = kwargs.get("receipt_id") or new_receipt_id()
        self._queue.put(("record_retrieval", (), kwargs))
        return kwargs["receipt_id"]

    def attach_output(self, *args: Any, **kwargs: Any) -> None:
        self._queue.put(("attach_output", args, kwargs))

    def add_evidence(self, *args: Any, **kwargs: Any) -> None:
        self._queue.put(("add_evidence", args, kwargs))

    def record_outcome(self, *args: Any, **kwargs: Any) -> None:
        self._queue.put(("record_outcome", args, kwargs))

    def ingest(self, event: Mapping[str, Any]) -> str:
        """Queue a spec-v1 event; assigns a receipt_id to a retrieval event that lacks one."""
        event = dict(event)
        if event.get("type") == "retrieval" and not event.get("receipt_id"):
            event["receipt_id"] = new_receipt_id()
        self._queue.put(("ingest", (event,), {}))
        return event.get("receipt_id", "")

    def flush(self) -> None:
        """Block until everything queued so far has been written or dead-lettered."""
        self._queue.join()

    def _run(self) -> None:
        while True:
            op, args, kwargs = self._queue.get()
            try:
                self._apply(op, args, kwargs)
            finally:
                self._queue.task_done()

    def _apply(self, op: str, args: tuple, kwargs: dict) -> None:
        for attempt in range(self.max_retries + 1):
            try:
                getattr(self.store, op)(*args, **kwargs)
                return
            except DuplicateReceiptError:
                return  # an earlier attempt actually committed
            except (ValueError, TypeError, KeyError) as exc:
                self.dead_letters.append((op, args, kwargs, exc))  # will never succeed on retry
                return
            except Exception as exc:  # transient storage error
                if attempt == self.max_retries:
                    self.dead_letters.append((op, args, kwargs, exc))
                    return
                time.sleep(self.backoff_seconds * (2**attempt))


def impacted_receipts(
    store: ContextReceiptStore,
    asset_graph: Any,
    asset_key: str,
    partitions: Iterable[str] | None = None,
    *,
    current_time: datetime | None = None,
) -> dict[str, Any]:
    """Recall: receipts that used a bad asset (or partitions of it) or anything downstream.

    ``asset_graph`` is ``defs.resolve_asset_graph()``. Walks child assets, translating the bad
    partitions through each edge's real partition mapping (identity, time-window, all-to-one...),
    then looks up receipts whose items came from any affected asset/partition. Returns
    ``{"affected": {asset_key: [partitions] | None}, "receipt_ids": [...], "partition_unknown": [...]}``
    where ``None`` means every partition. Assets outside the graph are simply not expanded.
    """
    root = dg.AssetKey.from_user_string(asset_key)
    if not asset_graph.has(root):
        raise KeyError(f"asset not in graph: {asset_key}")
    affected: dict[dg.AssetKey, set[str] | None] = {root: None if partitions is None else set(partitions)}
    frontier = [root]
    while frontier:
        parent = frontier.pop()
        parent_node = asset_graph.get(parent)
        parent_parts = affected[parent]
        for child in parent_node.child_keys:
            child_node = asset_graph.get(child)
            if child_node.partitions_def is None or parent_parts is None or parent_node.partitions_def is None:
                new: set[str] | None = None  # unpartitioned on either side: whole child is affected
            else:
                mapping = asset_graph.get_partition_mapping(child, parent)
                subset = parent_node.partitions_def.empty_subset().with_partition_keys(parent_parts)
                new = set(
                    mapping.get_downstream_partitions_for_partitions(
                        subset, parent_node.partitions_def, child_node.partitions_def, current_time=current_time
                    ).get_partition_keys()
                )
            if child in affected:
                old = affected[child]
                merged = None if (old is None or new is None) else old | new
                if merged == old:
                    continue
                affected[child] = merged
            else:
                affected[child] = new
            frontier.append(child)
    found = store.find_by_assets({k.to_user_string(): v for k, v in affected.items()})
    return {
        "affected": {k.to_user_string(): (None if v is None else sorted(v)) for k, v in affected.items()},
        **found,
    }


def build_usage_checks(
    store: ContextReceiptStore,
    asset_key: str,
    *,
    window_hours: float,
    max_stale_share: float,
    min_mean_outcome: float,
    min_verified_share: float,
    min_checked_receipts: int,
) -> list[Any]:
    """Asset checks over receipt data so context debt is a red/green signal on the asset.

    - ``context_staleness`` (ERROR): share of retrieved items served from an out-of-date
      data version in the window must be <= ``max_stale_share``. Items whose current
      version is unknown are not judged.
    - ``context_outcome_quality`` (WARN): mean outcome across receipts must be >=
      ``min_mean_outcome``; passes with a note below ``min_checked_receipts`` samples.
    - ``context_verified_share`` (WARN): share of outputs whose receipt reached verified or
      counterfactual evidence must be >= ``min_verified_share``; passes when there are no outputs.
    """
    key = dg.AssetKey.from_user_string(asset_key)

    def window() -> str:
        return (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()

    @dg.asset_check(asset=key, name="context_staleness", description="Share of context served from an out-of-date version.")
    def staleness(context):
        usage = store.asset_usage_since(asset_key, window())
        judged = stale = 0
        current: dict[str | None, str | None] = {}
        for partition, version in usage["items"]:
            if partition not in current:
                current[partition] = _current_data_version(context.instance, key, partition)
            if current[partition] is None or version is None:
                continue
            judged += 1
            stale += version != current[partition]
        share = stale / judged if judged else 0.0
        return dg.AssetCheckResult(
            passed=share <= max_stale_share,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"stale_share": round(share, 4), "stale_items": stale, "judged_items": judged,
                      "receipts": usage["receipts"], "window_hours": window_hours},
        )

    @dg.asset_check(asset=key, name="context_outcome_quality", description="Mean outcome of answers built from this asset.")
    def outcome_quality(context):
        usage = store.asset_usage_since(asset_key, window())
        means = usage["outcome_means"]
        if len(means) < min_checked_receipts:
            return dg.AssetCheckResult(
                passed=True, severity=dg.AssetCheckSeverity.WARN,
                metadata={"note": "insufficient outcomes to judge", "outcomes": len(means)},
            )
        mean = sum(means) / len(means)
        return dg.AssetCheckResult(
            passed=mean >= min_mean_outcome, severity=dg.AssetCheckSeverity.WARN,
            metadata={"mean_outcome": round(mean, 4), "outcomes": len(means), "window_hours": window_hours},
        )

    @dg.asset_check(asset=key, name="context_verified_share", description="Share of outputs whose context reached verified evidence.")
    def verified_share(context):
        usage = store.asset_usage_since(asset_key, window())
        outputs = usage["with_output"]
        share = usage["verified_outputs"] / outputs if outputs else 1.0
        return dg.AssetCheckResult(
            passed=share >= min_verified_share, severity=dg.AssetCheckSeverity.WARN,
            metadata={"verified_share": round(share, 4), "outputs": outputs, "window_hours": window_hours},
        )

    return [staleness, outcome_quality, verified_share]


def _current_data_version(instance: Any, asset_key: dg.AssetKey, partition: str | None = None) -> str | None:
    """Latest (partition's) materialization data version, or None when unknown (staleness not judged)."""
    record = instance.get_latest_data_version_record(asset_key, partition_key=partition)
    materialization = getattr(record, "asset_materialization", None) if record else None
    if materialization is None:
        return None
    return (materialization.tags or {}).get("dagster/data_version")


def build_usage_sensor(
    store: ContextReceiptStore,
    *,
    name: str,
    interval_seconds: int,
    rebuild_asset_keys: list[str],
    stale_reads_threshold: int,
    prior_strength: float,
) -> dg.SensorDefinition:
    """Cold-path sensor: tail receipts, report aggregated usage, learn priors, request rebuilds.

    Emits one AssetObservation per source asset per tick (aggregated, never one event
    per receipt, or per (asset, partition) for partitioned sources). ``source_version`` on
    receipt items is compared with the asset's (or that partition's) latest
    ``dagster/data_version``; reads of an older version are *stale reads*. Only assets
    listed in ``rebuild_asset_keys`` can be re-materialized by this sensor.
    """
    rebuild_keys = [dg.AssetKey.from_user_string(k) for k in rebuild_asset_keys]

    @dg.sensor(
        name=name,
        minimum_interval_seconds=interval_seconds,
        asset_selection=dg.AssetSelection.assets(*rebuild_keys) if rebuild_keys else None,
        description="Aggregates context receipts onto assets, refreshes item priors, requests rebuilds.",
    )
    def context_receipt_usage_sensor(context: dg.SensorEvaluationContext):
        state = json.loads(context.cursor) if context.cursor else {}
        data = store.read_since(state.get("r", 0), state.get("e", 0))
        if not data["receipts"] and not data["outcomes"]:
            return dg.SkipReason("no new receipts or outcomes")

        usage: dict[tuple[str, str | None], dict[str, Any]] = {}

        def bucket(key: str, partition: str | None) -> dict[str, Any]:
            return usage.setdefault(
                (key, partition),
                {"retrievals": 0, "receipts": set(), "explored": 0, "excluded": {}, "stale": 0,
                 "outcomes": 0, "outcome_sum": 0.0, "versions": {}},
            )

        current: dict[tuple[str, str | None], str | None] = {}
        for receipt in data["receipts"]:
            for item in receipt["items"]:
                key = item["source_asset_key"]
                if not key:
                    continue
                part = item.get("source_partition")
                if (key, part) not in current:
                    current[(key, part)] = _current_data_version(
                        context.instance, dg.AssetKey.from_user_string(key), part
                    )
                b = bucket(key, part)
                b["retrievals"] += 1
                b["receipts"].add(receipt["receipt_id"])
                b["explored"] += item["selection"] == "explore"
                version = item["source_version"]
                if current[(key, part)] is not None and version is not None and version != current[(key, part)]:
                    b["stale"] += 1
            for item in receipt["excluded"]:
                if item["source_asset_key"]:
                    reasons = bucket(item["source_asset_key"], item.get("source_partition"))["excluded"]
                    reasons[item["reason"]] = reasons.get(item["reason"], 0) + 1
        for outcome in data["outcomes"]:
            for key, part in outcome["sources"]:
                b = bucket(key, part)
                b["outcomes"] += 1
                b["outcome_sum"] += outcome["value"]

        stale_state = state.get("stale", {})
        events, run_requests = [], []
        for (key, part), b in sorted(usage.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")):
            version = current.get((key, part))
            state_key = f"{key}|{part or ''}"
            prior = stale_state.get(state_key, {})
            running = (prior.get("n", 0) if prior.get("v") == version else 0) + b["stale"]
            stale_state[state_key] = {"v": version, "n": running}
            metadata: dict[str, Any] = {
                "context_receipts/retrievals": b["retrievals"],
                "context_receipts/receipts": len(b["receipts"]),
                "context_receipts/explored": b["explored"],
                "context_receipts/stale_reads": b["stale"],
                "context_receipts/stale_reads_since_refresh": running,
                "context_receipts/excluded_by_reason": b["excluded"],
                "context_receipts/outcomes": b["outcomes"],
            }
            if b["outcomes"]:
                metadata["context_receipts/mean_outcome"] = round(b["outcome_sum"] / b["outcomes"], 4)
            if version is not None:
                metadata["context_receipts/current_data_version"] = version
            asset_key = dg.AssetKey.from_user_string(key)
            events.append(dg.AssetObservation(asset_key=asset_key, partition=part, metadata=metadata))
            if asset_key in rebuild_keys and running >= stale_reads_threshold:
                run_requests.append(
                    dg.RunRequest(
                        run_key=f"context-rebuild:{key}:{part}:{version}",
                        asset_selection=[asset_key],
                        partition_key=part,
                    )
                )

        store.refresh_item_scores(prior_strength)
        cursor = json.dumps(
            {"r": data["receipt_cursor"], "e": data["event_cursor"], "stale": stale_state}, sort_keys=True
        )
        return dg.SensorResult(asset_events=events, run_requests=run_requests, cursor=cursor)

    return context_receipt_usage_sensor


class ContextReceiptStoreComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a context receipt store for assets and agent gateways."""

    resource_key: str = Field(
        default="context_receipts",
        description="Dagster resource key for the ContextReceiptStore.",
    )
    database_path: str = Field(
        default="./context-receipts.sqlite3",
        description=(
            "SQLite file path for local or single-host prototypes. Use a "
            "shared receipt service/database for multi-host production writes."
        ),
    )

    usage_sensor_enabled: bool = Field(
        default=False,
        description=(
            "Add a sensor that tails receipts, reports aggregated usage as AssetObservations on "
            "the source assets, refreshes learned item priors, and can request rebuilds."
        ),
    )
    usage_sensor_interval_seconds: int = Field(default=60, description="Sensor evaluation interval.")
    rebuild_asset_keys: list[str] = Field(
        default_factory=list,
        description=(
            "Asset keys (e.g. 'support/tickets') the sensor may re-materialize when stale reads "
            "pile up. Empty means observe only, never trigger runs."
        ),
    )
    stale_reads_threshold: int = Field(
        default=25,
        description="Receipt items served from an out-of-date data version before a rebuild is requested.",
    )
    check_asset_keys: list[str] = Field(
        default_factory=list,
        description=(
            "Asset keys to attach context-health asset checks to (staleness, outcome quality, "
            "verified share). The assets must be defined in the same code location."
        ),
    )
    check_window_hours: float = Field(default=24.0, description="Lookback window for the checks.")
    max_stale_share: float = Field(default=0.1, description="Fail context_staleness above this share of stale reads.")
    min_mean_outcome: float = Field(default=-0.2, description="Warn context_outcome_quality below this mean outcome.")
    min_verified_share: float = Field(default=0.5, description="Warn context_verified_share below this share.")
    min_checked_receipts: int = Field(default=5, description="Outcomes needed before the quality check judges.")
    score_prior_strength: float = Field(
        default=5.0,
        description="Shrinkage for item quality priors: higher needs more outcome evidence to move a prior.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        store = ContextReceiptStore(os.path.expanduser(self.database_path))
        sensors = []
        if self.usage_sensor_enabled:
            sensors.append(
                build_usage_sensor(
                    store,
                    name=f"{self.resource_key}_usage_sensor",
                    interval_seconds=self.usage_sensor_interval_seconds,
                    rebuild_asset_keys=self.rebuild_asset_keys,
                    stale_reads_threshold=self.stale_reads_threshold,
                    prior_strength=self.score_prior_strength,
                )
            )
        checks = [
            check
            for key in self.check_asset_keys
            for check in build_usage_checks(
                store, key,
                window_hours=self.check_window_hours,
                max_stale_share=self.max_stale_share,
                min_mean_outcome=self.min_mean_outcome,
                min_verified_share=self.min_verified_share,
                min_checked_receipts=self.min_checked_receipts,
            )
        ]
        return dg.Definitions(
            resources={self.resource_key: dg.ResourceDefinition.hardcoded_resource(store)},
            sensors=sensors,
            asset_checks=checks,
        )
