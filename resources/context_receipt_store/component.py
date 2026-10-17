"""SQLite-backed context retrieval receipts and Dagster resource component."""

from __future__ import annotations

import json
import math
import os
import sqlite3
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
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
}
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
                    dagster_run_id TEXT
                );
                CREATE TABLE IF NOT EXISTS context_receipt_items (
                    receipt_id TEXT NOT NULL REFERENCES context_receipts(receipt_id),
                    item_id TEXT NOT NULL,
                    source_asset_key TEXT,
                    source_version TEXT,
                    rank INTEGER,
                    score REAL,
                    PRIMARY KEY (receipt_id, item_id, rank)
                );
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
        for field_name in ("source_asset_key", "source_version"):
            value = item.get(field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"item {field_name} must be a string or None")
        score = item.get("score")
        if score is not None:
            if not isinstance(score, (int, float)) or isinstance(score, bool) or not math.isfinite(score):
                raise ValueError("item score must be a finite number")
            score = float(score)
        return {
            "item_id": item_id,
            "source_asset_key": item.get("source_asset_key"),
            "source_version": item.get("source_version"),
            "rank": rank,
            "score": score,
        }

    def record_retrieval(
        self,
        *,
        context_name: str,
        items: Iterable[Mapping[str, Any]],
        context_version: str | None = None,
        policy_version: str | None = None,
        caller_id: str | None = None,
        purpose: str | None = None,
        query_hash: str | None = None,
        eligible_count: int | None = None,
        excluded_count: int | None = None,
        dagster_asset_key: str | None = None,
        dagster_run_id: str | None = None,
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
        receipt_id = str(uuid.uuid4())
        created_at = self._timestamp()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO context_receipts (
                    receipt_id, created_at, context_name, context_version,
                    policy_version, caller_id, purpose, query_hash,
                    eligible_count, excluded_count, dagster_asset_key, dagster_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            conn.executemany(
                """
                INSERT INTO context_receipt_items (
                    receipt_id, item_id, source_asset_key, source_version, rank, score
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        receipt_id,
                        item["item_id"],
                        item["source_asset_key"],
                        item["source_version"],
                        item["rank"],
                        item["score"],
                    )
                    for item in normalized_items
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
                SELECT item_id, source_asset_key, source_version, rank, score
                FROM context_receipt_items
                WHERE receipt_id = ?
                ORDER BY rank, item_id
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        store = ContextReceiptStore(os.path.expanduser(self.database_path))
        return dg.Definitions(
            resources={self.resource_key: dg.ResourceDefinition.hardcoded_resource(store)}
        )
