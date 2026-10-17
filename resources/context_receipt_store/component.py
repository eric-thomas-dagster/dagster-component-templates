"""SQLite-backed context retrieval receipts and Dagster resource component."""


import functools
import hashlib
import hmac
import inspect
import json
import logging
import math
import os
import queue
import random
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import dagster as dg
from pydantic import Field

logger = logging.getLogger(__name__)

_ITEM_FIELDS = {
    "item_id",
    "source_asset_key",
    "source_version",
    "rank",
    "score",
    "selection",
    "source_partition",
    "source_code_version",
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
    "reformulated": -0.2,  # implicit: the same session re-asked shortly after (see derive_implicit_outcomes)
}
# How much a receipt's outcome is credited/blamed on an item, by strongest evidence
# that the item was actually used. Retrieved-only is weak: it was merely in the window.
EVIDENCE_WEIGHTS = {"retrieved": 0.5, "cited": 1.0, "verified": 1.5, "counterfactual_tested": 2.0}
SPEC_VERSION = "1"  # receipt-spec.v1.json
_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


# BEGIN receipt-warehouse-ddl (canonical; inlined verbatim, byte-identical, by every component that creates or reads the receipt tables -- drift-tested)
RECEIPT_TABLE_NAMES = ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events", "context_item_scores")
RECEIPT_DIALECTS = ("bigquery", "snowflake", "databricks", "postgres", "mysql", "duckdb", "sqlite")


def receipt_table(dataset: str, name: str, prefix: str = "", dialect: str = "bigquery") -> str:
    """Table reference for a receipt table. `dataset` is the namespace: `project.dataset` (bigquery), `database.schema` (snowflake),
    `catalog.schema` (databricks), `schema` (postgres/duckdb), `database` (mysql); "" for sqlite or the connection default."""
    if name not in RECEIPT_TABLE_NAMES:
        raise ValueError(f"unknown receipt table {name!r}; valid: {RECEIPT_TABLE_NAMES}")
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    parts = [p for p in dataset.strip("`").split(".") if p] + [f"{prefix}{name}"]
    if dialect == "bigquery":
        return "`" + ".".join(parts) + "`"          # BigQuery reads `a.b.c` as one path
    if dialect in ("databricks", "mysql"):
        return ".".join(f"`{p}`" for p in parts)
    if dialect in ("postgres", "duckdb", "sqlite"):
        return ".".join(f'"{p}"' for p in parts)
    return ".".join(parts)                          # snowflake: unquoted, case-insensitive


_RECEIPT_TYPES = {  # (string, long string, integer, float, timestamp)
    "bigquery": ("STRING", "STRING", "INT64", "FLOAT64", "TIMESTAMP"),
    "snowflake": ("VARCHAR", "VARCHAR", "BIGINT", "DOUBLE", "TIMESTAMP_TZ"),
    "databricks": ("STRING", "STRING", "BIGINT", "DOUBLE", "TIMESTAMP"),
    "postgres": ("TEXT", "TEXT", "BIGINT", "DOUBLE PRECISION", "TIMESTAMPTZ"),
    "mysql": ("VARCHAR(255)", "LONGTEXT", "BIGINT", "DOUBLE", "DATETIME(6)"),   # UTC by convention
    "duckdb": ("VARCHAR", "VARCHAR", "BIGINT", "DOUBLE", "TIMESTAMPTZ"),
    "sqlite": ("TEXT", "TEXT", "INTEGER", "REAL", "TEXT"),                       # ISO-8601 UTC strings
}


def receipt_table_ddl(dataset: str, prefix: str = "", dialect: str = "bigquery") -> list:
    """CREATE TABLE IF NOT EXISTS statements for the receipt tables (same columns as the SQLite store, per resources/context_receipt_store/SPEC.md).
    Event payloads stay JSON text in `payload_json`; priors and feedback are computed from them (SQL: JSON_VALUE and friends, or in Python)."""
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    S, L, I, F, T = _RECEIPT_TYPES[dialect]
    K = "`rank`" if dialect == "mysql" else "rank"        # RANK is reserved in MySQL 8
    t = lambda n: receipt_table(dataset, n, prefix, dialect)  # noqa: E731
    return [
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipts')} (
  receipt_id {S} NOT NULL, created_at {T} NOT NULL, context_name {S} NOT NULL, context_version {S}, policy_version {S},
  caller_id {S}, purpose {S}, query_hash {S}, eligible_count {I}, excluded_count {I}, dagster_asset_key {S},
  dagster_run_id {S}, trace_id {S}, mcp_session_id {S}, mcp_request_id {S}, dagster_partition_key {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_items')} (
  receipt_id {S} NOT NULL, item_id {S} NOT NULL, source_asset_key {S}, source_version {S}, source_partition {S},
  source_code_version {S}, {K} {I}, score {F}, selection {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_excluded')} (
  receipt_id {S} NOT NULL, item_id {S} NOT NULL, reason {S} NOT NULL, source_asset_key {S}, source_version {S},
  source_partition {S}, source_code_version {S})""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_events')} (
  event_id {S} NOT NULL, receipt_id {S} NOT NULL, event_type {S} NOT NULL, occurred_at {T} NOT NULL, payload_json {L} NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_item_scores')} (
  item_id {S} NOT NULL, retrievals {I} NOT NULL, outcome_weight {F} NOT NULL, outcome_sum {F} NOT NULL, prior {F} NOT NULL,
  updated_at {T} NOT NULL)""",
    ]


# Columns added after the first release: (table, column, kind). A deployed table created from an older DDL lacks them, so
# creators probe each one and run the ALTER only when the probe fails (never a destructive step; existing rows read NULL).
RECEIPT_ADDED_COLUMNS = (("context_receipts", "dagster_partition_key", "string"),)


def receipt_column_upgrades(dataset: str, prefix: str = "", dialect: str = "bigquery") -> list:
    """``[(probe_sql, alter_sql)]`` per late-added column: run ``probe_sql``; if it raises (column missing), run ``alter_sql``."""
    if dialect not in RECEIPT_DIALECTS:
        raise ValueError(f"unknown dialect {dialect!r}; valid: {RECEIPT_DIALECTS}")
    S, L, I, F, T = _RECEIPT_TYPES[dialect]
    out = []
    for table, column, kind in RECEIPT_ADDED_COLUMNS:
        ref = receipt_table(dataset, table, prefix, dialect)
        out.append((f"SELECT {column} FROM {ref} WHERE 1 = 0", f"ALTER TABLE {ref} ADD COLUMN {column} {S if kind == 'string' else L}"))
    return out
# END receipt-warehouse-ddl


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
_EXCLUDED_FIELDS = {"item_id", "reason", "source_asset_key", "source_version", "source_partition", "source_code_version"}
_EVIDENCE_LEVELS = {"cited", "verified", "counterfactual_tested"}


CONTEXT_POLICY_METADATA_KEY = "context/policy"
SENSITIVITY_LEVELS = ("public", "internal", "confidential", "restricted")


def _as_datetime(value: Any) -> datetime | None:
    """Parse a datetime/ISO string; a bare date ("2026-10-31") means "through the end of that day"."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value)
    if len(text) == 10:
        return datetime.fromisoformat(text).replace(tzinfo=timezone.utc) + timedelta(days=1)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class CallerContext:
    """Who is asking, as the gateway knows it (from auth, never from the model's own claims)."""

    clearance: str = "internal"
    access_tags: tuple[str, ...] = ()
    scope_value: str | None = None  # e.g. the account id this caller may see
    allow_pii: bool = False
    now: datetime | None = None

    def __post_init__(self) -> None:
        if self.clearance not in SENSITIVITY_LEVELS:
            raise ValueError(f"clearance must be one of {SENSITIVITY_LEVELS}, got {self.clearance!r}")


@dataclass(frozen=True)
class ContextPolicy:
    """Policy declared on a context asset (see ``context_asset``) and enforced at the gateway.

    ``scope`` names the chunk-metadata field that identifies a tenant (e.g. "account_id"); callers only see
    chunks whose value equals their ``scope_value`` (fail closed when they have none). ``access_tags`` means
    the caller must hold at least one. ``sensitivity`` is the asset's level versus the caller's clearance;
    chunk-level ``sensitivity_field == "pii"`` (set by the pipeline's sensitivity op) additionally needs
    ``allow_pii``. Freshness comes from ``valid_until_field`` (a bare date is valid through that day) and
    ``max_age_hours`` against ``updated_field``. ``token_budget`` caps what one retrieval may return.
    ``retention_days`` is DECLARED metadata only: nothing purges receipts or chunks on it yet.
    """

    scope: str | None = None
    sensitivity: str = "internal"
    max_age_hours: float | None = None
    token_budget: int | None = None
    retention_days: int | None = None
    access_tags: tuple[str, ...] = ()
    chars_per_token: float = 4.0
    sensitivity_field: str = "sensitivity"
    valid_until_field: str = "valid_until"
    updated_field: str = "updated_at"

    def __post_init__(self) -> None:
        if self.sensitivity not in SENSITIVITY_LEVELS:
            raise ValueError(f"sensitivity must be one of {SENSITIVITY_LEVELS}, got {self.sensitivity!r}")
        for name in ("max_age_hours", "token_budget", "retention_days"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0):
                raise ValueError(f"{name} must be a positive number or None")
        if self.chars_per_token <= 0:
            raise ValueError("chars_per_token must be positive")
        object.__setattr__(self, "access_tags", tuple(self.access_tags))

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["access_tags"] = list(self.access_tags)
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ContextPolicy":
        unknown = set(data) - {f.name for f in fields(cls)}
        if unknown:
            raise ValueError(f"unknown context policy field(s): {', '.join(sorted(unknown))}")
        return cls(**{k: (tuple(v) if k == "access_tags" else v) for k, v in data.items()})

    def evaluate(self, metadata: Mapping[str, Any], caller: CallerContext) -> str | None:
        """Why this chunk must be withheld from ``caller`` (a receipt exclusion reason), or None to serve it.

        Order: out_of_scope, policy (access tags / clearance), pii, stale."""
        if self.scope is not None:
            if caller.scope_value is None or str(metadata.get(self.scope)) != str(caller.scope_value):
                return "out_of_scope"
        if self.access_tags and not (set(self.access_tags) & set(caller.access_tags)):
            return "policy"
        if SENSITIVITY_LEVELS.index(self.sensitivity) > SENSITIVITY_LEVELS.index(caller.clearance):
            return "policy"
        if metadata.get(self.sensitivity_field) == "pii" and not caller.allow_pii:
            return "pii"
        now = caller.now or datetime.now(timezone.utc)
        expires = _as_datetime(metadata.get(self.valid_until_field))
        if expires is not None and now >= expires:
            return "stale"
        updated = _as_datetime(metadata.get(self.updated_field))
        if self.max_age_hours is not None and updated is not None and now - updated > timedelta(hours=self.max_age_hours):
            return "stale"
        return None

    def apply(
        self, candidates: Iterable[Mapping[str, Any]], caller: CallerContext
    ) -> tuple[list[Mapping[str, Any]], list[dict[str, Any]]]:
        """Split ranked candidates into (kept, excluded) ready for ``record_retrieval``.

        Each candidate has ``item_id``, optional ``metadata`` (the chunk's index metadata), optional ``text``
        or ``tokens`` (for the budget) and optional ``source_asset_key`` / ``source_version`` /
        ``source_partition``. Candidates are considered in the order given (rank order); the budget is greedy:
        one that does not fit is excluded as ``over_budget`` and later, smaller ones may still fit."""
        kept: list[Mapping[str, Any]] = []
        excluded: list[dict[str, Any]] = []
        used = 0
        for candidate in candidates:
            reason = self.evaluate(candidate.get("metadata") or {}, caller)
            if reason is None and self.token_budget is not None:
                tokens = candidate.get("tokens")
                if tokens is None:
                    tokens = math.ceil(len(candidate.get("text") or "") / self.chars_per_token)
                if used + tokens > self.token_budget:
                    reason = "over_budget"
                else:
                    used += tokens
            if reason is None:
                kept.append(candidate)
            else:
                excluded.append({"item_id": candidate["item_id"], "reason": reason,
                                 **{k: candidate[k] for k in ("source_asset_key", "source_version", "source_partition", "source_code_version")
                                    if candidate.get(k) is not None}})
        return kept, excluded


def _validate_bundle(df: Any, id_column: str, text_column: str) -> None:
    for column in (id_column, text_column):
        if column not in df.columns:
            raise dg.Failure(description=f"context asset must return a DataFrame with column {column!r}; got {list(df.columns)}")
    ids = df[id_column]
    if ids.isna().any() or (ids.astype(str).str.strip() == "").any():
        raise dg.Failure(description=f"context asset: {id_column!r} has null/empty values")
    if ids.duplicated().any():
        dupes = ids[ids.duplicated()].astype(str).head(5).tolist()
        raise dg.Failure(description=f"context asset: {id_column!r} must be unique; duplicates include {dupes}")
    texts = df[text_column]
    if texts.isna().any() or (texts.astype(str).str.strip() == "").any():
        raise dg.Failure(description=f"context asset: {text_column!r} has null/empty values")


def _bundle_version(df: Any, id_column: str, text_column: str) -> str:
    """Content-derived version: changes only when a chunk id or its text changes."""
    h = hashlib.sha256()
    pairs = sorted(
        (str(i), hashlib.sha256(str(t).encode()).hexdigest()) for i, t in zip(df[id_column], df[text_column])
    )
    for chunk_id, digest in pairs:
        h.update(f"{chunk_id}:{digest}\n".encode())
    return h.hexdigest()[:32]


def _finalize_bundle(result: Any, id_column: str, text_column: str, emit_data_version: bool) -> Any:
    value, metadata, data_version = result, {}, None
    if isinstance(result, dg.Output):
        value, metadata, data_version = result.value, dict(result.metadata or {}), result.data_version
    if not hasattr(value, "columns"):  # not a DataFrame: nothing to validate or version
        return result
    _validate_bundle(value, id_column, text_column)
    version = _bundle_version(value, id_column, text_column)
    metadata.update({"context_version": dg.MetadataValue.text(version), "chunks": dg.MetadataValue.int(len(value))})
    if data_version is None and emit_data_version:
        data_version = dg.DataVersion(version)
    return dg.Output(value=value, metadata=metadata, data_version=data_version)


def context_asset(
    *,
    scope: str | None = None,
    sensitivity: str = "internal",
    max_age_hours: float | None = None,
    token_budget: int | None = None,
    retention_days: int | None = None,
    access_tags: Sequence[str] = (),
    id_column: str = "chunk_id",
    text_column: str = "chunk_text",
    emit_data_version: bool = True,
    code_version: str | None = None,
    **policy_extras: Any,
):
    """Declare an asset as governed context: a policy contract plus an output contract.

    Apply AFTER ``@dg.asset``. It (1) records the policy on the asset as ``context/policy`` metadata and
    ``context/*`` tags so a gateway can discover context assets and their rules with ``context_policies``;
    (2) validates what the asset returns (a DataFrame with unique non-empty ``id_column`` and non-empty
    ``text_column``, else the materialization fails); and (3) emits a content-derived data version, because
    Dagster's default changes every run when the asset or its inputs carry no explicit versions, which would make
    every recorded receipt look stale after a no-op rebuild. Set ``code_version`` (or ``@dg.asset(code_version=...)``)
    to name the recipe: Dagster then marks the asset stale when it changes, and a gateway that stamps it as
    ``source_code_version`` can compare outcomes across recipe versions (``outcomes_by_code_version``).

        @context_asset(scope="account_id", sensitivity="confidential", token_budget=4000, max_age_hours=24)
        @dg.asset(partitions_def=daily)
        def account_context(): ...   # returns a DataFrame with chunk_id, chunk_text, account_id, ...

    The gateway then enforces the same declaration: ``context_policies(defs)["account_context"].apply(...)``.
    """
    policy = ContextPolicy(
        scope=scope, sensitivity=sensitivity, max_age_hours=max_age_hours, token_budget=token_budget,
        retention_days=retention_days, access_tags=tuple(access_tags), **policy_extras,
    )

    def decorator(target: Any) -> Any:
        tags = {"context/sensitivity": policy.sensitivity}
        if policy.scope:
            tags["context/scope"] = policy.scope
        return _rebuild_asset(
            target, "@context_asset",
            metadata={CONTEXT_POLICY_METADATA_KEY: dg.MetadataValue.json(policy.to_dict())},
            tags=tags, add_kind="context", code_version=code_version,
            wrap=lambda raw_fn: _wrap_bundle(raw_fn, id_column, text_column, emit_data_version),
        )

    return decorator


def _wrap_bundle(raw_fn: Any, id_column: str, text_column: str, emit_data_version: bool) -> Any:
    @functools.wraps(raw_fn)
    def compute(*args: Any, **kwargs: Any) -> Any:
        return _finalize_bundle(raw_fn(*args, **kwargs), id_column, text_column, emit_data_version)

    return compute


def _rebuild_asset(
    target: Any,
    decorator_name: str,
    *,
    metadata: dict[str, Any],
    tags: dict[str, str],
    add_kind: str | None = None,
    code_version: str | None = None,
    wrap: Any = None,
) -> Any:
    """Re-declare a single ``@dg.asset`` with extra metadata/tags (and optionally a wrapped compute), keeping
    its group, owners, partitions, inputs, deps, automation, retry and backfill settings."""
    if not isinstance(target, dg.AssetsDefinition):
        raise TypeError(f"{decorator_name} must be applied after @dg.asset (it decorates an AssetsDefinition)")
    if len(target.keys) != 1:
        raise ValueError(f"{decorator_name} supports single-asset definitions only")
    key = next(iter(target.keys))
    spec = target.get_asset_spec(key)
    raw_fn = getattr(getattr(target.node_def, "compute_fn", None), "decorated_fn", None)
    if raw_fn is None:
        raise RuntimeError(f"{decorator_name} could not read the compute function from the AssetsDefinition")
    # Real function inputs vs lineage-only deps: only parameters of the compute function can be ``ins``.
    params = set(inspect.signature(raw_fn).parameters)
    ins = {name: dg.AssetIn(key=dep) for name, dep in target.keys_by_input_name.items() if name in params}
    bound = {i.key for i in ins.values()}
    deps = {d.asset_key for d in (spec.deps or [])} | {k for n, k in target.keys_by_input_name.items() if n not in params}
    kinds = set(spec.kinds or ())
    if add_kind and len(kinds) < 3:
        kinds.add(add_kind)
    return dg.asset(
        key=key,
        description=spec.description,
        group_name=spec.group_name,
        owners=list(spec.owners) if spec.owners else None,
        tags={**(spec.tags or {}), **tags},
        metadata={**(spec.metadata or {}), **metadata},
        code_version=code_version or spec.code_version,
        partitions_def=target.partitions_def,
        automation_condition=spec.automation_condition,
        kinds=kinds,
        check_specs=list(target.check_specs) if getattr(target, "check_specs", None) else None,
        ins=ins or None,
        deps=sorted((d for d in deps if d not in bound), key=lambda k: k.to_user_string()) or None,
        retry_policy=getattr(target.node_def, "retry_policy", None),
        backfill_policy=getattr(target, "backfill_policy", None),
    )(wrap(raw_fn) if wrap else raw_fn)


CONTEXT_CONSUMER_METADATA_KEY = "context/consumer"


@dataclass(frozen=True)
class ContextViolation:
    """A consumer asset that sits downstream of context it is not cleared for."""

    consumer: str
    context_asset: str
    reason: str


def context_consumer(
    *, clearance: str = "internal", access_tags: Sequence[str] = (), allow_pii: bool = False
):
    """Declare who an asset is cleared to consume: the clearance and access tags its audience holds.

    Apply after ``@dg.asset``. ``check_context_consumption(defs)`` then fails if the asset has any ancestor
    context asset whose policy (sensitivity above this clearance, or required access tags this consumer
    lacks) it is not cleared for, so a summary built from confidential tickets cannot be wired into an
    asset meant for a wider audience. It looks through intermediate assets, treating them as carrying the
    context's policy forward.
    """
    if clearance not in SENSITIVITY_LEVELS:
        raise ValueError(f"clearance must be one of {SENSITIVITY_LEVELS}, got {clearance!r}")
    declaration = {"clearance": clearance, "access_tags": list(access_tags), "allow_pii": bool(allow_pii)}

    def decorator(target: Any) -> Any:
        return _rebuild_asset(
            target, "@context_consumer",
            metadata={CONTEXT_CONSUMER_METADATA_KEY: dg.MetadataValue.json(declaration)},
            tags={"context/consumer-clearance": clearance},
        )

    return decorator


def validate_context_consumption(source: Any) -> list[ContextViolation]:
    """Every declared consumer's ancestors checked against its clearance; empty list means compliant.

    Scope: asset sensitivity versus clearance, and required access tags. Per-tenant ``scope`` and chunk-level
    PII are enforced at retrieval by the gateway and cannot be judged from the graph."""
    graph = source.resolve_asset_graph() if hasattr(source, "resolve_asset_graph") else source
    violations: list[ContextViolation] = []
    for key in graph.get_all_asset_keys():
        raw = (graph.get(key).metadata or {}).get(CONTEXT_CONSUMER_METADATA_KEY)
        if raw is None:
            continue
        consumer = getattr(raw, "value", raw)
        for ancestor in sorted(graph.get_ancestor_asset_keys(key), key=lambda k: k.to_user_string()):
            policy_raw = (graph.get(ancestor).metadata or {}).get(CONTEXT_POLICY_METADATA_KEY)
            if policy_raw is None:
                continue
            policy = ContextPolicy.from_dict(getattr(policy_raw, "value", policy_raw))
            if SENSITIVITY_LEVELS.index(policy.sensitivity) > SENSITIVITY_LEVELS.index(consumer["clearance"]):
                violations.append(ContextViolation(
                    key.to_user_string(), ancestor.to_user_string(),
                    f"{ancestor.to_user_string()} is {policy.sensitivity}; consumer clearance is {consumer['clearance']}"))
            elif policy.access_tags and not set(policy.access_tags) & set(consumer["access_tags"]):
                violations.append(ContextViolation(
                    key.to_user_string(), ancestor.to_user_string(),
                    f"{ancestor.to_user_string()} requires one of {list(policy.access_tags)}; consumer holds {consumer['access_tags']}"))
    return violations


class ContextPolicyViolation(Exception):
    """Raised by ``check_context_consumption`` with every violation listed."""


def check_context_consumption(source: Any) -> None:
    """Fail loudly (at load time, before any run) if a consumer is wired to context it is not cleared for.

    Call it where definitions are assembled, e.g. ``check_context_consumption(defs)`` at the bottom of
    ``definitions.py``, or in a test."""
    violations = validate_context_consumption(source)
    if violations:
        raise ContextPolicyViolation(
            "context consumption not allowed:\n" + "\n".join(f"  {v.consumer}: {v.reason}" for v in violations))


def context_policies(source: Any) -> dict[str, ContextPolicy]:
    """Read every declared context policy: ``{asset_key: ContextPolicy}`` from ``Definitions`` or an asset graph.

    This is how a gateway discovers which assets are governed context and what rules apply, from the
    same declaration (``@context_asset`` or the pipeline component's ``context_policy``)."""
    graph = source.resolve_asset_graph() if hasattr(source, "resolve_asset_graph") else source
    policies: dict[str, ContextPolicy] = {}
    for key in graph.get_all_asset_keys():
        raw = (graph.get(key).metadata or {}).get(CONTEXT_POLICY_METADATA_KEY)
        if raw is None:
            continue
        policies[key.to_user_string()] = ContextPolicy.from_dict(getattr(raw, "value", raw))
    return policies


def _fold_item_scores(items: Any, events: Any) -> dict[str, list[float]]:
    """Pure aggregation behind ``refresh_item_scores``: item -> [retrievals, weight, weighted_sum] (shared by every storage backend)."""
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
    return stats


def _fold_item_feedback(item_rows: Any, excl_rows: Any, events: Any) -> dict[str, dict[str, Any]]:
    """Pure aggregation behind ``item_feedback`` (rows are mappings: sqlite3.Row or dict); shared by every storage backend."""
    outcomes: dict[str, dict[str, float]] = {}
    evidence: dict[str, list[set[str] | None]] = {}
    for ev in events:
        payload = json.loads(ev["payload_json"])
        if ev["event_type"] == "outcome":
            key = payload.get("outcome_ref") or f"anon-{len(outcomes.get(ev['receipt_id'], {}))}"
            outcomes.setdefault(ev["receipt_id"], {})[key] = payload["value"]
        elif payload.get("level") in ("cited", "verified", "counterfactual_tested"):
            scoped = (payload.get("details") or {}).get("item_ids")
            evidence.setdefault(ev["receipt_id"], []).append(set(scoped) if isinstance(scoped, list) else None)
    out: dict[str, dict[str, Any]] = {}
    
    def entry_for(item_id: str) -> dict[str, Any]:
        return out.setdefault(item_id, {"retrievals": 0, "rated": 0, "_sum": 0.0, "cited": 0, "excluded": {}, "last_retrieved_at": None})
    
    for row in item_rows:
        entry = entry_for(row["item_id"])
        entry["retrievals"] += 1
        if entry["last_retrieved_at"] is None or row["created_at"] > entry["last_retrieved_at"]:
            entry["last_retrieved_at"] = row["created_at"]
        values = outcomes.get(row["receipt_id"])
        if values:
            entry["rated"] += 1
            entry["_sum"] += sum(values.values()) / len(values)
        if any(scope is None or row["item_id"] in scope for scope in evidence.get(row["receipt_id"], [])):
            entry["cited"] += 1
    for row in excl_rows:
        entry = entry_for(row["item_id"])
        entry["excluded"][row["reason"]] = entry["excluded"].get(row["reason"], 0) + 1
    for entry in out.values():
        total = entry.pop("_sum")
        entry["mean_outcome"] = round(total / entry["rated"], 4) if entry["rated"] else None
    return out


def _fold_asset_usage(rows: Any, events: Any, ids: Any) -> dict[str, Any]:
    """Pure aggregation behind ``asset_usage_since`` (shared by every storage backend)."""
    outcomes: dict[str, dict[str, float]] = {}
    with_output: set[str] = set()
    verified: set[str] = set()
    claims: dict[str, tuple[int, int]] = {}  # receipt -> (claims_total, claims_ungrounded), latest report wins
    for ev in events:
        payload = json.loads(ev["payload_json"])
        if ev["event_type"] == "outcome":
            key = payload.get("outcome_ref") or f"anon-{len(outcomes.get(ev['receipt_id'], {}))}"
            outcomes.setdefault(ev["receipt_id"], {})[key] = payload["value"]
        elif ev["event_type"] == "output_attached":
            with_output.add(ev["receipt_id"])
        elif ev["event_type"] == "evidence":
            if payload["level"] in ("verified", "counterfactual_tested"):
                verified.add(ev["receipt_id"])
            details = payload.get("details") or {}
            if isinstance(details.get("claims_total"), int) and isinstance(details.get("claims_ungrounded"), int):
                claims[ev["receipt_id"]] = (details["claims_total"], details["claims_ungrounded"])
    return {
        "claims_total": sum(t for t, _ in claims.values()),
        "claims_ungrounded": sum(u for _, u in claims.values()),
        "receipts": len(ids),
        "items": [(r["source_partition"], r["source_version"]) for r in rows],
        "outcome_means": [sum(v.values()) / len(v) for v in outcomes.values()],
        "with_output": len(with_output),
        "verified_outputs": len(with_output & verified),
    }


def _fold_outcomes_by_code_version(rows: Any, events: Any) -> dict[str, dict[str, Any]]:
    """Pure aggregation behind ``outcomes_by_code_version`` (shared by every storage backend)."""
    per_receipt: dict[str, dict[str, float]] = {}
    for ev in events:
        payload = json.loads(ev["payload_json"])
        key = payload.get("outcome_ref") or f"anon-{len(per_receipt.get(ev['receipt_id'], {}))}"
        per_receipt.setdefault(ev["receipt_id"], {})[key] = payload["value"]
    stats: dict[str, dict[str, Any]] = {}
    for row in rows:
        entry = stats.setdefault(row["cv"], {"receipts": 0, "outcomes": 0, "_sum": 0.0})
        entry["receipts"] += 1
        values = per_receipt.get(row["receipt_id"])
        if values:
            entry["outcomes"] += 1
            entry["_sum"] += sum(values.values()) / len(values)
    return {
        cv: {"receipts": e["receipts"], "outcomes": e["outcomes"],
             "mean_outcome": round(e["_sum"] / e["outcomes"], 4) if e["outcomes"] else None}
        for cv, e in sorted(stats.items())
    }


def fold_maintenance_report(
    feedback: Mapping[str, Mapping[str, Any]],
    *,
    receipts_in_window: int | None = None,
    known_item_ids: Iterable[str] | None = None,
    min_retrievals: int = 5,
    min_outcomes: int = 3,
    bad_below: float = -0.3,
    dead_after_receipts: int = 50,
    good_above: float = 0.3,
    top_n: int = 10,
    min_exclusions: int = 5,
) -> dict[str, Any]:
    """Pure fold behind ``maintenance_report`` (shared by every storage backend): turn ``item_feedback`` output into a to-do list.

    - ``rewrite_candidates``: chunks retrieved >= ``min_retrievals`` times and rated >= ``min_outcomes`` times whose mean
      outcome is <= ``bad_below``; worst first. Each entry carries retrievals / rated / mean_outcome / cited / excluded.
    - ``dead_weight``: ids in ``known_item_ids`` that were never retrieved. Absence is only evidence when enough traffic
      was seen, so it is judged only when ``receipts_in_window >= dead_after_receipts``; otherwise ``None`` and the reason
      is in ``dead_weight_reason``. (Chunks only ever *excluded* count as never retrieved.)
    - ``top_performers``: the mirror image of rewrite candidates (mean >= ``good_above``), best first, at most ``top_n``.
    - ``noisy_exclusions``: chunks the gateway withheld >= ``min_exclusions`` times (all reasons summed; the reasons are listed).
    """
    def entry(item_id: str, e: Mapping[str, Any]) -> dict[str, Any]:
        return {"item_id": item_id, "retrievals": e["retrievals"], "rated": e["rated"], "mean_outcome": e["mean_outcome"],
                "cited": e["cited"], "excluded": dict(e["excluded"])}

    judged = [(i, e) for i, e in feedback.items()
              if e["retrievals"] >= min_retrievals and e["rated"] >= min_outcomes and e["mean_outcome"] is not None]
    rewrite = sorted((entry(i, e) for i, e in judged if e["mean_outcome"] <= bad_below),
                     key=lambda r: (r["mean_outcome"], -r["retrievals"], r["item_id"]))
    top = sorted((entry(i, e) for i, e in judged if e["mean_outcome"] >= good_above),
                 key=lambda r: (-r["mean_outcome"], -r["retrievals"], r["item_id"]))[:top_n]
    noisy = sorted(({**entry(i, e), "excluded_total": sum(e["excluded"].values())} for i, e in feedback.items()
                    if sum(e["excluded"].values()) >= min_exclusions),
                   key=lambda r: (-r["excluded_total"], r["item_id"]))
    dead: list[str] | None = None
    reason: str | None = None
    if known_item_ids is None:
        reason = "known_item_ids not provided"
    elif receipts_in_window is None or receipts_in_window < dead_after_receipts:
        reason = f"only {receipts_in_window or 0} receipt(s) in the window; need >= {dead_after_receipts} before absence means anything"
    else:
        dead = sorted({str(k) for k in known_item_ids if feedback.get(str(k), {}).get("retrievals", 0) == 0})
    return {
        "rewrite_candidates": rewrite,
        "dead_weight": dead,
        "dead_weight_reason": reason,
        "top_performers": top,
        "noisy_exclusions": noisy,
        "summary": {"items_seen": len(feedback), "receipts_in_window": receipts_in_window,
                    "rewrite_candidates": len(rewrite), "dead_weight": None if dead is None else len(dead),
                    "top_performers": len(top), "noisy_exclusions": len(noisy)},
    }


class _Approx:  # noqa: N801  (tiny stand-in so selftest needs no test library; equality within 1e-6)
    def __init__(self, value: float):
        self.value = value

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, (int, float)) and abs(other - self.value) < 1e-6

    def __repr__(self) -> str:
        return f"~{self.value}"


def _receipt_limit(assets: Iterable[str], limits: Mapping[str, float], default_days: float | None) -> float | None:
    """Retention in days for one receipt: the shortest of its assets' limits; an asset with no declared retention uses
    ``default_days`` (the asset's own policy always wins); a receipt that touched no asset uses ``default_days``."""
    values = [limits[a] if a in limits else default_days for a in assets] or [default_days]
    values = [v for v in values if v]
    return min(values) if values else None


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
                    mcp_request_id TEXT,
                    dagster_partition_key TEXT
                );
                CREATE TABLE IF NOT EXISTS context_receipt_items (
                    receipt_id TEXT NOT NULL REFERENCES context_receipts(receipt_id),
                    item_id TEXT NOT NULL,
                    source_asset_key TEXT,
                    source_version TEXT,
                    source_partition TEXT,
                    source_code_version TEXT,
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
                    source_code_version TEXT,
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
                ("context_receipts", "dagster_partition_key", "TEXT"),
                ("context_receipt_items", "selection", "TEXT NOT NULL DEFAULT 'ranked'"),
                ("context_receipt_items", "source_partition", "TEXT"),
                ("context_receipt_excluded", "source_partition", "TEXT"),
                ("context_receipt_items", "source_code_version", "TEXT"),
                ("context_receipt_excluded", "source_code_version", "TEXT"),
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
        for field_name in ("source_asset_key", "source_version", "source_partition", "source_code_version"):
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
            "source_code_version": item.get("source_code_version"),
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
        for field_name in ("source_asset_key", "source_version", "source_partition", "source_code_version"):
            value = item.get(field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"excluded {field_name} must be a string or None")
        return {
            "item_id": item["item_id"],
            "reason": item["reason"],
            "source_asset_key": item.get("source_asset_key"),
            "source_version": item.get("source_version"),
            "source_partition": item.get("source_partition"),
            "source_code_version": item.get("source_code_version"),
        }

    # -- privacy: keyed pseudonyms for caller / session ids ----------------------------------------------------
    _pseudo_salt: Any = None

    def enable_pseudonymization(self, salt: "str | bytes | Any") -> None:
        """Store ``caller_id`` and ``mcp_session_id`` as ``h:`` + HMAC-SHA256(salt, id)[:32] instead of the raw id.

        ``salt`` is a non-empty str/bytes or a zero-argument callable returning one (read on every write, so an env var that
        is unset when the code location loads only fails when a receipt is actually written). Deterministic, so one caller or
        session still groups (``derive_implicit_outcomes``); changing the salt breaks that continuity. Applied inside
        ``_prepare_retrieval`` so no backend can bypass it. Existing rows are not rewritten."""
        if not callable(salt) and not salt:
            raise ValueError("pseudonymization needs a non-empty salt")
        self._pseudo_salt = salt

    @property
    def pseudonymizing(self) -> bool:
        return self._pseudo_salt is not None

    def _pseudonym(self, value: Any) -> Any:
        if value is None or self._pseudo_salt is None:
            return value
        salt = self._pseudo_salt() if callable(self._pseudo_salt) else self._pseudo_salt
        if not salt:
            raise ValueError("pseudonymize_callers is on but the pseudonym salt is empty or unset; refusing to write a raw caller id")
        key = salt if isinstance(salt, bytes) else str(salt).encode()
        return "h:" + hmac.new(key, str(value).encode(), hashlib.sha256).hexdigest()[:32]

    def _prepare_retrieval(
        self, context_name: Any, items: Any, excluded: Any, eligible_count: Any, excluded_count: Any, receipt_id: Any,
        caller_id: Any = None, mcp_session_id: Any = None,
    ) -> "tuple[str, list[dict[str, Any]], list[dict[str, Any]], int | None, Any, Any]":
        """Validate and normalize a retrieval; shared by every storage backend. Returns (receipt_id, items, excluded,
        excluded_count, caller_id, mcp_session_id); the two ids come back pseudonymized when that is enabled."""
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
        return receipt_id, normalized_items, normalized_excluded, excluded_count, self._pseudonym(caller_id), self._pseudonym(mcp_session_id)

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
        dagster_partition_key: str | None = None,
    ) -> str:
        """Persist a retrieval fact and return its portable receipt ID.

        ``dagster_partition_key`` is the partition of the *producing* asset's run (the rag/agentic asset that answered; a
        multi-dimension key is Dagster's ``a|b`` string), as opposed to each item's ``source_partition``.

        ``query_hash`` should be a one-way digest, not the raw query. Item
        mappings accept only IDs, source asset/version references, rank, and
        score so content cannot be accidentally serialized into the receipt.
        """
        receipt_id, normalized_items, normalized_excluded, excluded_count, caller_id, mcp_session_id = self._prepare_retrieval(
            context_name, items, excluded, eligible_count, excluded_count, receipt_id, caller_id, mcp_session_id
        )
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
                    trace_id, mcp_session_id, mcp_request_id, dagster_partition_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    dagster_partition_key,
                ),
            )
            conn.executemany(
                """
                INSERT INTO context_receipt_items (
                    receipt_id, item_id, source_asset_key, source_version, source_partition,
                    source_code_version, rank, score, selection
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        receipt_id,
                        item["item_id"],
                        item["source_asset_key"],
                        item["source_version"],
                        item["source_partition"],
                        item["source_code_version"],
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
                    receipt_id, item_id, reason, source_asset_key, source_version, source_partition,
                    source_code_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        receipt_id,
                        item["item_id"],
                        item["reason"],
                        item["source_asset_key"],
                        item["source_version"],
                        item["source_partition"],
                        item["source_code_version"],
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
                SELECT item_id, source_asset_key, source_version, source_partition, source_code_version,
                       rank, score, selection
                FROM context_receipt_items
                WHERE receipt_id = ?
                ORDER BY rank, item_id
                """,
                (receipt_id,),
            ).fetchall()
            excluded = conn.execute(
                """
                SELECT item_id, reason, source_asset_key, source_version, source_partition, source_code_version
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
        """Cold-path tail: new receipts and new events since the given cursors.

        Cursors are monotonically increasing row positions, so a consumer (such as
        the Dagster sensor) can resume exactly where it stopped. Late events for
        old receipts arrive through ``event_cursor``: ``events`` carries every event
        (outcome, evidence, output_attached) with its id, time and raw payload, and
        ``outcomes`` is the outcome subset, enriched with the receipt's sources.
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
                SELECT rowid AS pos, event_id, receipt_id, event_type, occurred_at, payload_json FROM context_receipt_events
                WHERE rowid > ? ORDER BY rowid LIMIT ?
                """,
                (event_cursor, limit),
            ).fetchall()
            outcome_assets: dict[str, list[tuple[str, str | None, str | None]]] = {}
            if any(e["event_type"] == "outcome" for e in events):
                oids = sorted({e["receipt_id"] for e in events if e["event_type"] == "outcome"})
                omarks = ",".join("?" * len(oids))
                for row in conn.execute(
                    f"SELECT DISTINCT receipt_id, source_asset_key, source_partition, source_code_version FROM context_receipt_items "
                    f"WHERE receipt_id IN ({omarks}) AND source_asset_key IS NOT NULL",
                    oids,
                ):
                    outcome_assets.setdefault(row["receipt_id"], []).append(
                        (row["source_asset_key"], row["source_partition"], row["source_code_version"])
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
            if row["event_type"] != "outcome":
                continue
            payload = json.loads(row["payload_json"])
            payload["receipt_id"] = row["receipt_id"]
            payload["sources"] = outcome_assets.get(row["receipt_id"], [])  # (asset_key, partition|None, code_version|None)
            outcomes.append(payload)
        return {
            "receipts": [by_receipt[r["receipt_id"]] for r in receipts],
            "outcomes": outcomes,
            "events": [{k: row[k] for k in ("event_id", "receipt_id", "event_type", "occurred_at", "payload_json")} for row in events],
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
        stats = _fold_item_scores(items, events)
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

    def item_feedback(
        self, asset_key: str, *, since: str = "", code_version: str | None = None
    ) -> dict[str, dict[str, Any]]:
        """What happened to each chunk of ``asset_key`` -- the read side a BUILD needs to learn from.

        ``{item_id: {"retrievals", "rated", "mean_outcome", "cited", "excluded", "last_retrieved_at"}}`` over receipts
        created at/after ``since`` (ISO-8601 UTC; "" = all time). With ``code_version`` only items served from that
        recipe count: after a recipe change (chunking, embedding model, filters) old feedback describes different
        chunks and must not be applied to the new ones.

        - ``retrievals``: receipts that returned the item (a receipt counts once however many slots it used).
        - ``rated``: how many of those have at least one outcome; ``mean_outcome`` is their mean effective outcome
          (a receipt's effective outcome is the mean of its outcomes, a later outcome with the same ``outcome_ref``
          superseding the earlier -- exactly how ``refresh_item_scores`` counts), or ``None`` when none are rated.
        - ``cited``: receipts where evidence of level cited/verified/counterfactual_tested applies to the item
          (item-scoped evidence only to its ``details.item_ids``; unscoped evidence to every item of the receipt).
        - ``excluded``: ``{reason: n}`` times the gateway withheld the item (pii / stale / policy / ...). An item that was
          only ever excluded appears with ``retrievals == 0``.
        """
        item_where = "i.source_asset_key = ? AND r.created_at >= ?"
        excl_where = "e.source_asset_key = ? AND r.created_at >= ?"
        params: list[Any] = [asset_key, since or ""]
        if code_version is not None:
            item_where += " AND COALESCE(i.source_code_version, 'unknown') = ?"
            excl_where += " AND COALESCE(e.source_code_version, 'unknown') = ?"
            params.append(code_version)
        with self._connect() as conn:
            item_rows = conn.execute(
                f"SELECT DISTINCT i.receipt_id, i.item_id, r.created_at FROM context_receipt_items i "
                f"JOIN context_receipts r ON r.receipt_id = i.receipt_id WHERE {item_where}", params,
            ).fetchall()
            excl_rows = conn.execute(
                f"SELECT e.item_id, e.reason FROM context_receipt_excluded e "
                f"JOIN context_receipts r ON r.receipt_id = e.receipt_id WHERE {excl_where}", params,
            ).fetchall()
            ids = sorted({row["receipt_id"] for row in item_rows})
            events: list[Any] = []
            for start in range(0, len(ids), 500):  # stay under SQLite's bound-variable limit
                batch = ids[start:start + 500]
                events.extend(conn.execute(
                    f"SELECT receipt_id, event_type, payload_json FROM context_receipt_events "
                    f"WHERE event_type IN ('outcome', 'evidence') AND receipt_id IN ({','.join('?' * len(batch))}) ORDER BY rowid",
                    batch,
                ).fetchall())
        return _fold_item_feedback(item_rows, excl_rows, events)

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
                       "dagster_run_id", "items", "excluded", "trace_id", "mcp_session_id", "mcp_request_id",
                       "dagster_partition_key"}
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
                "trace_id", "mcp_session_id", "mcp_request_id", "dagster_partition_key")
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
        return _fold_asset_usage(rows, events, ids)

    def derive_implicit_outcomes(self, *, window_seconds: float = 120.0, since: str | None = None) -> int:
        """Write weak implicit outcomes from session behaviour; returns how many were created.

        When one caller in one MCP session retrieves again within ``window_seconds`` with a *different*
        ``query_hash``, the earlier receipt gets a ``reformulated`` outcome (value -0.2, source
        ``implicit``): the first retrieval probably did not answer the question. Idempotent per
        (earlier, later) pair via ``outcome_ref``. A heuristic: multi-part tasks look like reformulation
        too, so keep the weight small (it is) and leave this off unless you want it. Needs
        ``mcp_session_id`` and ``query_hash`` on receipts.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT receipt_id, created_at, caller_id, mcp_session_id, query_hash
                FROM context_receipts
                WHERE mcp_session_id IS NOT NULL AND query_hash IS NOT NULL AND created_at >= ?
                ORDER BY rowid
                """,
                (since or "",),
            ).fetchall()
            existing = {
                json.loads(r["payload_json"]).get("outcome_ref")
                for r in conn.execute(
                    "SELECT payload_json FROM context_receipt_events WHERE event_type = 'outcome'"
                )
            }
        previous: dict[tuple[str, str | None], Any] = {}
        created = 0
        for row in rows:
            key = (row["mcp_session_id"], row["caller_id"])
            prior = previous.get(key)
            previous[key] = row
            if prior is None or prior["query_hash"] == row["query_hash"]:
                continue
            gap = (_as_datetime(row["created_at"]) - _as_datetime(prior["created_at"])).total_seconds()
            ref = f"implicit:reformulated:{row['receipt_id']}"
            if gap <= window_seconds and ref not in existing:
                self.record_outcome(prior["receipt_id"], signal="reformulated", source="implicit", outcome_ref=ref)
                created += 1
        return created

    def outcomes_by_code_version(self, asset_key: str, since: str = "") -> dict[str, dict[str, Any]]:
        """How answers built from ``asset_key`` turned out, per code version (recipe) of the asset.

        ``{code_version: {"receipts": n, "outcomes": m, "mean_outcome": x | None}}``. Receipts whose items did
        not record a code version are grouped under ``"unknown"``. A receipt that mixes several code versions of
        the asset counts toward each. This is how a recipe change (chunk size, embedding model, filters) is judged
        by what users actually thought, not by a guess.
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT i.receipt_id, COALESCE(i.source_code_version, 'unknown') AS cv
                FROM context_receipt_items i JOIN context_receipts r ON r.receipt_id = i.receipt_id
                WHERE i.source_asset_key = ? AND r.created_at >= ?
                """,
                (asset_key, since),
            ).fetchall()
            ids = sorted({r["receipt_id"] for r in rows})
            events = conn.execute(
                f"SELECT receipt_id, payload_json FROM context_receipt_events WHERE event_type = 'outcome' "
                f"AND receipt_id IN ({','.join('?' * len(ids))}) ORDER BY rowid",
                ids,
            ).fetchall() if ids else []
        return _fold_outcomes_by_code_version(rows, events)

    def enforce_retention(
        self, retention_days: Mapping[str, float | None], *, now: datetime | None = None, default_days: float | None = None,
    ) -> dict[str, int]:
        """Delete receipts past their retention; the one deletion this append-only store performs.

        ``retention_days`` maps asset key to the retention declared in its policy (``None`` = keep forever). A
        receipt expires ``min(retention of the assets it used)`` days after it was created, so a receipt that
        touched a 30-day asset and a 90-day asset goes at 30 days. Receipts that touched no asset with a
        retention are kept -- unless ``default_days`` is set, which applies to every asset whose policy declares
        no retention (and to receipts that touched no asset); an asset's own policy always wins. Returns
        ``{"receipts_deleted": n}``. Aggregated item priors are statistics, not receipts, and are not removed.
        """
        limits = {k: float(v) for k, v in retention_days.items() if v}
        if default_days is not None and default_days <= 0:
            raise ValueError("default_days must be positive or None")
        if not limits and not default_days:
            return {"receipts_deleted": 0}
        now = now or datetime.now(timezone.utc)
        cutoff = (now - timedelta(days=min([*limits.values(), *([float(default_days)] if default_days else [])]))).isoformat()
        with self._connect() as conn:
            candidates = conn.execute(
                "SELECT receipt_id, created_at FROM context_receipts WHERE created_at < ?", (cutoff,)
            ).fetchall()
            doomed: list[str] = []
            for row in candidates:
                assets = {r[0] for r in conn.execute(
                    "SELECT source_asset_key FROM context_receipt_items WHERE receipt_id = ? AND source_asset_key IS NOT NULL "
                    "UNION SELECT source_asset_key FROM context_receipt_excluded WHERE receipt_id = ? AND source_asset_key IS NOT NULL",
                    (row["receipt_id"], row["receipt_id"]))}
                limit = _receipt_limit(assets, limits, default_days)
                if limit and _as_datetime(row["created_at"]) + timedelta(days=limit) <= now:
                    doomed.append(row["receipt_id"])
        self.delete_receipts(doomed)
        return {"receipts_deleted": len(doomed)}

    def delete_receipts(self, receipt_ids: Iterable[str]) -> int:
        """Delete these receipts and their items, exclusions and events (nothing else); returns how many receipts existed.
        Backs ``enforce_retention`` and ``selftest`` cleanup."""
        ids = sorted({str(i) for i in receipt_ids})
        existed = 0
        with self._connect() as conn:
            for i in range(0, len(ids), 500):
                batch = ids[i:i + 500]
                marks = ",".join("?" * len(batch))
                existed += conn.execute(f"SELECT COUNT(1) FROM context_receipts WHERE receipt_id IN ({marks})", batch).fetchone()[0]
                for table in ("context_receipt_events", "context_receipt_excluded", "context_receipt_items", "context_receipts"):
                    conn.execute(f"DELETE FROM {table} WHERE receipt_id IN ({marks})", batch)
        return existed

    def delete_item_scores(self, item_ids: Iterable[str]) -> int:
        """Delete the learned scores of exactly these items (``selftest`` cleanup); returns how many rows existed."""
        ids = sorted({str(i) for i in item_ids})
        existed = 0
        with self._connect() as conn:
            for i in range(0, len(ids), 500):
                batch = ids[i:i + 500]
                marks = ",".join("?" * len(batch))
                existed += conn.execute(f"SELECT COUNT(1) FROM context_item_scores WHERE item_id IN ({marks})", batch).fetchone()[0]
                conn.execute(f"DELETE FROM context_item_scores WHERE item_id IN ({marks})", batch)
        return existed

    def ensure_tables(self) -> None:
        """Create the tables if missing (the SQLite store also does this on every connection)."""
        with self._connect():
            pass

    def _selftest_cursors(self) -> tuple[int, int]:
        """(receipt_cursor, event_cursor) at 'now', so ``selftest`` reads only its own tail instead of the whole store."""
        with self._connect() as conn:
            return (conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM context_receipts").fetchone()[0],
                    conn.execute("SELECT COALESCE(MAX(rowid), 0) FROM context_receipt_events").fetchone()[0])

    def find_by_assets(self, targets: Mapping[str, Iterable[str] | None]) -> dict[str, list[str]]:
        """Receipts that used the given assets, optionally narrowed to partitions.

        Partition semantics: ``targets`` names *source* assets and *source* partitions (an item's ``source_asset_key`` /
        ``source_partition``: where a retrieved chunk came from). It says nothing about which partition of the *producing*
        asset (the rag/agentic asset that answered) a receipt belongs to; that is the receipt's ``dagster_partition_key``,
        queried with ``receipts_for_partition``.

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

    def receipts_for_partition(self, dagster_asset_key: str, partition_key: str | None) -> list[str]:
        """Receipt IDs answered by ``dagster_asset_key``'s run for ``partition_key`` (the *producing* asset's partition,
        ``receipt.dagster_partition_key``; multi-dimension keys are ``a|b``). ``None`` matches receipts recorded without a
        partition key. Not to be confused with an item's ``source_partition`` (see ``find_by_assets``)."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT receipt_id FROM context_receipts WHERE dagster_asset_key = ? AND "
                + ("dagster_partition_key IS NULL" if partition_key is None else "dagster_partition_key = ?") + " ORDER BY receipt_id",
                (dagster_asset_key,) if partition_key is None else (dagster_asset_key, partition_key),
            ).fetchall()
        return [row["receipt_id"] for row in rows]

    # -- maintenance report ------------------------------------------------------------------------------------
    def maintenance_report(
        self, asset_key: str, *, since: str = "", known_item_ids: Iterable[str] | None = None, min_retrievals: int = 5,
        min_outcomes: int = 3, bad_below: float = -0.3, dead_after_receipts: int = 50, good_above: float = 0.3, top_n: int = 10,
        min_exclusions: int = 5, code_version: str | None = None,
    ) -> dict[str, Any]:
        """What to rewrite, delete or keep in ``asset_key``'s chunks, from real usage. Backend-independent: it reads
        ``item_feedback`` (and, only when ``known_item_ids`` is given, the receipt count) and hands both to the pure
        ``fold_maintenance_report`` -- see that function for the definitions of each section."""
        known = None if known_item_ids is None else list(known_item_ids)
        feedback = self.item_feedback(asset_key, since=since, code_version=code_version)
        receipts = None
        if known is not None:  # traffic is judged for the same slice the feedback covers: one recipe's receipts, not all recipes'
            if code_version is None:
                receipts = self.asset_usage_since(asset_key, since)["receipts"]
            else:
                receipts = self.outcomes_by_code_version(asset_key, since).get(code_version, {}).get("receipts", 0)
        return fold_maintenance_report(
            feedback, receipts_in_window=receipts, known_item_ids=known, min_retrievals=min_retrievals, min_outcomes=min_outcomes,
            bad_below=bad_below, dead_after_receipts=dead_after_receipts, good_above=good_above, top_n=top_n, min_exclusions=min_exclusions)

    # -- self-test ---------------------------------------------------------------------------------------------
    def _sync_target(self) -> "ContextReceiptStore":
        writer = getattr(self, "_writer", None)
        return writer.store if writer is not None else self

    def selftest(self, *, prior_strength: float = 5.0) -> list[dict[str, Any]]:
        """Exercise this backend end to end with synthetic receipts and report ``[{step, ok, detail}]``.

        Every id is a fresh uuid, so concurrent runs never collide and nothing real is touched. Steps: create tables, record,
        duplicate detection, outcome/evidence/output events, get_receipt, read_since, item_feedback numbers, score refresh,
        outcomes_by_code_version, asset_usage_since, find_by_*, maintenance_report, then cleanup of exactly the synthetic rows
        (verified). A failing step is reported and the rest still run; cleanup always runs.

        One side effect on a live store: ``refresh_item_scores`` is the normal store-wide recompute (what the usage sensor does
        every tick), so this step reads every receipt and rewrites every item's score row (same values, new ``updated_at``).
        Pass the ``prior_strength`` your sensor uses so the recompute is a no-op in value."""
        run = uuid.uuid4().hex[:12]
        pre = f"selftest-{run}"
        r1, r2 = f"{pre}-r1", f"{pre}-r2"
        asset, cv = f"selftest/{run}", f"{pre}-cv"
        a, b, z, dead = (f"{pre}-{x}" for x in ("a", "b", "z", "dead"))
        caller, session = f"{pre}-caller", f"{pre}-session"
        results: list[dict[str, Any]] = []
        state: dict[str, Any] = {"cursors": (0, 0)}
        dead_before = len(getattr(self, "dead_letters", None) or [])

        def check(cond: Any, message: str) -> None:
            if not cond:
                raise AssertionError(message)

        def step(name: str, fn: Any) -> None:
            try:
                detail = fn()
                results.append({"step": name, "ok": True, "detail": detail or "ok"})
            except BaseException as exc:  # noqa: BLE001  (a broken backend must be reported, never crash the run)
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                results.append({"step": name, "ok": False, "detail": f"{type(exc).__name__}: {exc}"})

        def flush(raise_on_dead: bool = True) -> None:
            flusher = getattr(self, "flush", None)
            if callable(flusher):
                flusher()
            dead_letters = getattr(self, "dead_letters", None) or []
            if raise_on_dead:
                if len(dead_letters) > dead_before:
                    raise AssertionError(f"{len(dead_letters) - dead_before} async write(s) dead-lettered: {dead_letters[-1][3]!r}")

        def item(item_id: str, rank: int) -> dict[str, Any]:
            return {"item_id": item_id, "source_asset_key": asset, "source_version": "v1", "source_code_version": cv, "rank": rank, "score": 0.5}

        def tables() -> str:
            self.ensure_tables()
            state["cursors"] = self._selftest_cursors()
            return "tables ready"

        def record() -> str:
            self.record_retrieval(
                context_name="selftest", receipt_id=r1, items=[item(a, 0), item(b, 1)], eligible_count=3,
                excluded=[{"item_id": z, "reason": "policy", "source_asset_key": asset, "source_code_version": cv}],
                caller_id=caller, mcp_session_id=session, query_hash="selftest", dagster_asset_key=asset)
            self.record_retrieval(context_name="selftest", receipt_id=r2, items=[item(a, 0)], caller_id=caller, mcp_session_id=session)
            flush()
            return "2 receipts written"

        def duplicate() -> str:
            try:
                self._sync_target().record_retrieval(context_name="selftest", receipt_id=r1, items=[])
            except DuplicateReceiptError:
                return "DuplicateReceiptError raised"
            raise AssertionError("re-recording an existing receipt_id was accepted")

        def events() -> str:
            self.record_outcome(r1, signal="thumbs_up", outcome_ref=f"{pre}-u1")
            self.add_evidence(r1, level="cited", details={"item_ids": [a]})
            self.attach_output(r1, output_id=f"{pre}-o1", model="selftest", output_hash="0" * 8)
            self.record_outcome(r2, signal="thumbs_down")
            flush()
            return "4 events written"

        def get() -> str:
            rec = self.get_receipt(r1)
            check(rec is not None, "get_receipt returned None for a receipt just written")
            check([i["item_id"] for i in rec["items"]] == [a, b], f"items {[i['item_id'] for i in rec['items']]}")
            check([e["item_id"] for e in rec["excluded"]] == [z] and rec["excluded"][0]["reason"] == "policy", "excluded item missing")
            check({e["event_type"] for e in rec["events"]} == {"outcome", "evidence", "output_attached"}, f"events {[e['event_type'] for e in rec['events']]}")
            check(rec["created_at"], "created_at empty")
            expected = self._pseudonym(caller)
            check(rec["caller_id"] == expected, f"caller_id stored as {rec['caller_id']!r}, expected {expected!r}")
            check(rec["mcp_session_id"] == self._pseudonym(session), "mcp_session_id not stored as expected")
            if self.pseudonymizing:
                check(rec["caller_id"] != caller and str(rec["caller_id"]).startswith("h:"), "raw caller_id was stored with pseudonymization on")
            check(self.get_receipt(f"{pre}-missing") is None, "get_receipt invented a receipt")
            return "receipt, items, exclusion and events round-trip" + ("; caller pseudonymized" if self.pseudonymizing else "")

        def tail() -> str:
            rc, ec = state["cursors"]
            mine = {r1, r2}
            got_receipts: dict[str, Any] = {}
            got_events: list[Any] = []
            got_outcomes: list[Any] = []
            for _ in range(200):
                page = self.read_since(rc, ec, limit=1000)
                got_receipts.update({r["receipt_id"]: r for r in page["receipts"] if r["receipt_id"] in mine})
                got_events += [e for e in page["events"] if e["receipt_id"] in mine]
                got_outcomes += [o for o in page["outcomes"] if o["receipt_id"] in mine]
                if (page["receipt_cursor"], page["event_cursor"]) == (rc, ec):
                    break
                rc, ec = page["receipt_cursor"], page["event_cursor"]
            check(set(got_receipts) == mine, f"read_since returned receipts {sorted(got_receipts)}")
            check(len(got_receipts[r1]["items"]) == 2 and len(got_receipts[r1]["excluded"]) == 1, "read_since receipt lost its items/exclusions")
            types = sorted(e["event_type"] for e in got_events)
            check(types == ["evidence", "outcome", "outcome", "output_attached"], f"read_since events {types}")
            check(len(got_outcomes) == 2 and all(any(src[0] == asset for src in o["sources"]) for o in got_outcomes), "outcomes lost their sources")
            return "2 receipts and all 3 event types read back"

        def feedback() -> str:
            fb = self.item_feedback(asset)
            check(set(fb) == {a, b, z}, f"item_feedback items {sorted(fb)}")
            check(fb[a]["retrievals"] == 2 and fb[a]["rated"] == 2 and fb[a]["cited"] == 1, f"a: {fb[a]}")
            check(fb[a]["mean_outcome"] == _Approx(0.0), f"a mean {fb[a]['mean_outcome']}")
            check(fb[b]["retrievals"] == 1 and fb[b]["rated"] == 1 and fb[b]["cited"] == 0 and fb[b]["mean_outcome"] == _Approx(1.0), f"b: {fb[b]}")
            check(fb[z]["retrievals"] == 0 and fb[z]["excluded"] == {"policy": 1}, f"z: {fb[z]}")
            return "retrievals / rated / cited / mean_outcome / excluded as expected"

        def scores() -> str:
            self.refresh_item_scores(prior_strength)
            got = self.get_item_scores([a, b, f"{pre}-nope"])
            check(set(got) == {a, b}, f"scores for {sorted(got)}")
            check(got[a]["retrievals"] == 2 and got[a]["outcome_weight"] == _Approx(1.5) and got[a]["outcome_sum"] == _Approx(0.5), f"a: {got[a]}")
            check(got[a]["prior"] == _Approx(0.5 / (1.5 + prior_strength)), f"a prior {got[a]['prior']}")
            check(got[b]["retrievals"] == 1 and got[b]["outcome_weight"] == _Approx(0.5), f"b: {got[b]}")
            return "refresh + get_item_scores match hand-computed priors"

        def by_code_version() -> str:
            got = self.outcomes_by_code_version(asset)
            check(got == {cv: {"receipts": 2, "outcomes": 2, "mean_outcome": 0.0}}, f"outcomes_by_code_version {got}")
            return "per-code-version outcomes as expected"

        def usage() -> str:
            u = self.asset_usage_since(asset, "")
            check(u["receipts"] == 2 and len(u["items"]) == 3, f"usage {u['receipts']} receipts / {len(u['items'])} items")
            check(u["with_output"] == 1 and u["verified_outputs"] == 0, f"usage outputs {u['with_output']}/{u['verified_outputs']}")
            check(sorted(round(x, 6) for x in u["outcome_means"]) == [-1.0, 1.0], f"outcome_means {u['outcome_means']}")
            return "asset_usage_since as expected"

        def find() -> str:
            both = sorted([r1, r2])
            check(self.find_by_item(a) == both and self.find_by_item(b) == [r1], "find_by_item")
            check(self.find_by_source_asset(asset) == both, "find_by_source_asset")
            check(self.find_by_assets({asset: None})["receipt_ids"] == both, "find_by_assets")
            return "find_by_item / find_by_source_asset / find_by_assets"

        def maintenance() -> str:
            rep = self.maintenance_report(asset, known_item_ids=[a, b, dead], min_retrievals=1, min_outcomes=1, bad_below=0.0,
                                          dead_after_receipts=1, good_above=0.3)
            check([r["item_id"] for r in rep["rewrite_candidates"]] == [a], f"rewrite_candidates {rep['rewrite_candidates']}")
            check([r["item_id"] for r in rep["top_performers"]] == [b], f"top_performers {rep['top_performers']}")
            check(rep["dead_weight"] == [dead], f"dead_weight {rep['dead_weight']}")
            return "maintenance_report as expected"

        def cleanup() -> str:
            try:
                flush(raise_on_dead=False)
            except Exception:  # noqa: BLE001
                pass
            problems: list[str] = []
            for label, fn, ids in (("receipts", self.delete_receipts, [r1, r2]), ("scores", self.delete_item_scores, [a, b, z, dead])):
                for attempt in (1, 2):
                    try:
                        fn(ids)
                        break
                    except Exception as exc:  # noqa: BLE001
                        if attempt == 2:
                            problems.append(f"delete {label} failed: {type(exc).__name__}: {exc}")
            try:
                left = [i for i in (r1, r2) if self.get_receipt(i) is not None]
                if left:
                    problems.append(f"receipts still present: {left}")
                if self.find_by_source_asset(asset) or self.find_by_item(a) or self.find_by_item(b):
                    problems.append("item rows still present")
                if self.get_item_scores([a, b, z, dead]):
                    problems.append("synthetic item scores still present")
            except Exception as exc:  # noqa: BLE001
                problems.append(f"could not verify cleanup: {type(exc).__name__}: {exc}")
            check(not problems, "; ".join(problems))
            return "synthetic rows deleted and verified gone"

        try:
            for name, fn in (("create_tables", tables), ("record_retrieval", record), ("duplicate_detection", duplicate),
                             ("outcome_evidence_output_events", events), ("get_receipt", get), ("read_since", tail),
                             ("item_feedback", feedback), ("refresh_and_get_item_scores", scores),
                             ("outcomes_by_code_version", by_code_version), ("asset_usage_since", usage), ("find_by", find),
                             ("maintenance_report", maintenance)):
                step(name, fn)
        finally:
            step("cleanup", cleanup)
        return results


# ── BigQuery backend ──────────────────────────────────────────────────────────────────────────────────────
# The same receipts, in BigQuery tables (DDL: the canonical block above), for a shop whose warehouse is BigQuery and whose
# agents/gateways run on several hosts. Same method surface and results as the SQLite store (a parity test runs one scenario
# through both); the row folding is the shared _fold_* code, only reads/writes differ.
#
# Writes go through LOAD jobs, not streaming inserts: streaming-buffer rows cannot be DELETEd/UPDATEd for up to ~90 minutes,
# which would break `enforce_retention` and score refreshes. A load job costs seconds, not milliseconds -- put an
# AsyncReceiptWriter in front of it on any hot path (the component does so with `async_writes: true`).

def _bq_iso(value: Any) -> Any:
    """BigQuery TIMESTAMP -> the ISO-8601 UTC string the SQLite store returns."""
    if isinstance(value, datetime):
        return (value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)).isoformat()
    return value


_EPOCH = "1970-01-01T00:00:00+00:00"
_BQ_RECEIPT_COLS = ("receipt_id", "created_at", "context_name", "context_version", "policy_version", "caller_id", "purpose", "query_hash",
                    "eligible_count", "excluded_count", "dagster_asset_key", "dagster_run_id", "trace_id", "mcp_session_id", "mcp_request_id",
                    "dagster_partition_key")
_BQ_TYPES = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "TIMESTAMP": "TIMESTAMP", "STRING": "STRING"}


def _missing_columns_message(alters: list, errors: list) -> str:
    return ("create_tables=False, so the receipt tables are yours to manage, but they are missing a column this release writes. "
            "Run these statements once, then retry:\n  " + ";\n  ".join(alters) + ";\n"
            f"(the existence probe failed with: {errors[0][:200]!r}; if the table itself does not exist, create it first or set create_tables=True)")


class BigQueryContextReceiptStore(ContextReceiptStore):
    """Receipts in BigQuery. ``client_factory()`` returns a ``google.cloud.bigquery.Client`` or a context manager yielding one
    (dagster-gcp's ``BigQueryResource.get_client``)."""

    def __init__(self, dataset: str, *, client_factory: Any, table_prefix: str = "", create_tables: bool = True):
        if not dataset or "." not in dataset.strip("`"):
            raise ValueError("dataset must be 'project.dataset'")
        self.dataset = dataset.strip("`")
        self.table_prefix = table_prefix
        self.database_path = f"bigquery://{self.dataset}/{table_prefix}"
        self._client_factory = client_factory
        self._create_tables = create_tables
        self._ready = False
        self._ready_lock = threading.Lock()

    # -- plumbing ---------------------------------------------------------------------------------------------
    def _t(self, name: str) -> str:
        return receipt_table(self.dataset, name, self.table_prefix)

    @contextmanager
    def _client(self) -> Iterator[Any]:
        made = self._client_factory()
        if hasattr(made, "__enter__"):
            with made as client:
                yield client
        else:
            yield made

    @staticmethod
    def _params(bq: Any, params: Mapping[str, Any]) -> list:
        out = []
        for name, value in params.items():
            if isinstance(value, (list, tuple)):
                out.append(bq.ArrayQueryParameter(name, "STRING", [str(v) for v in value]))
            elif isinstance(value, bool):
                out.append(bq.ScalarQueryParameter(name, "BOOL", value))
            elif isinstance(value, int):
                out.append(bq.ScalarQueryParameter(name, "INT64", value))
            elif isinstance(value, float):
                out.append(bq.ScalarQueryParameter(name, "FLOAT64", value))
            else:
                out.append(bq.ScalarQueryParameter(name, "STRING", None if value is None else str(value)))
        return out

    def _prepare(self) -> None:
        """``create_tables=True``: create/upgrade the tables. ``create_tables=False`` (you manage them): run only the column probe, once."""
        if self._create_tables:
            self.ensure_tables()
        else:
            self._verify_columns()

    def _verify_columns(self) -> None:
        with self._ready_lock:
            if self._ready:
                return
            missing, errors = [], []
            with self._client() as client:
                for probe, alter in receipt_column_upgrades(self.dataset, self.table_prefix):
                    try:
                        client.query(probe).result()
                    except Exception as exc:  # noqa: BLE001
                        missing.append(alter)
                        errors.append(str(exc))
            if missing:
                raise RuntimeError(_missing_columns_message(missing, errors))
            self._ready = True

    def ensure_tables(self) -> None:
        with self._ready_lock:
            if self._ready:
                return
            with self._client() as client:
                for ddl in receipt_table_ddl(self.dataset, self.table_prefix):
                    client.query(ddl).result()
                for probe, alter in receipt_column_upgrades(self.dataset, self.table_prefix):   # tables made by an older release
                    try:
                        client.query(probe).result()
                    except Exception:  # noqa: BLE001  (column missing: the probe's SELECT fails)
                        client.query(alter).result()
            self._ready = True

    def _query(self, sql: str, **params: Any) -> list:
        from google.cloud import bigquery as bq  # imported lazily: only the BigQuery backend needs it

        self._prepare()
        with self._client() as client:
            job = client.query(sql, job_config=bq.QueryJobConfig(query_parameters=self._params(bq, params)))
            return [dict(r) for r in job.result()]

    def _load(self, table: str, rows: list, columns: Sequence[tuple]) -> None:
        """Append ``rows`` (dicts) to ``table`` with a load job. ``columns``: [(name, bigquery_type)]."""
        if not rows:
            return
        from google.cloud import bigquery as bq

        self._prepare()
        cfg = bq.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=[bq.SchemaField(n, _BQ_TYPES[t]) for n, t in columns],
        )
        with self._client() as client:
            client.load_table_from_json(rows, table.strip("`"), job_config=cfg).result()

    @staticmethod
    def _ts(value: Any) -> str:
        return value or _EPOCH

    # -- writes -----------------------------------------------------------------------------------------------
    def record_retrieval(self, *, context_name: str, items: Iterable[Mapping[str, Any]], excluded: Iterable[Mapping[str, Any]] = (),
                         context_version: str | None = None, policy_version: str | None = None, caller_id: str | None = None,
                         purpose: str | None = None, query_hash: str | None = None, eligible_count: int | None = None,
                         excluded_count: int | None = None, dagster_asset_key: str | None = None, dagster_run_id: str | None = None,
                         receipt_id: str | None = None, trace_id: str | None = None, mcp_session_id: str | None = None,
                         mcp_request_id: str | None = None, dagster_partition_key: str | None = None) -> str:
        receipt_id, norm_items, norm_excluded, excluded_count, caller_id, mcp_session_id = self._prepare_retrieval(
            context_name, items, excluded, eligible_count, excluded_count, receipt_id, caller_id, mcp_session_id)
        # One round trip on the happy path: does the receipt exist (duplicate), and did a crashed earlier attempt leave
        # items behind (they are written before the receipt row, which is the commit marker)?
        seen = self._query(
            f"SELECT (SELECT COUNT(1) FROM {self._t('context_receipts')} WHERE receipt_id = @r) AS r, "
            f"(SELECT COUNT(1) FROM {self._t('context_receipt_items')} WHERE receipt_id = @r) AS i, "
            f"(SELECT COUNT(1) FROM {self._t('context_receipt_excluded')} WHERE receipt_id = @r) AS e", r=receipt_id)[0]
        if seen["r"]:
            raise DuplicateReceiptError(f"receipt already exists: {receipt_id}")
        if seen["i"]:
            self._query(f"DELETE FROM {self._t('context_receipt_items')} WHERE receipt_id = @r", r=receipt_id)
        if seen["e"]:
            self._query(f"DELETE FROM {self._t('context_receipt_excluded')} WHERE receipt_id = @r", r=receipt_id)
        src = ("source_asset_key", "source_version", "source_partition", "source_code_version")
        self._load(self._t("context_receipt_items"), [{"receipt_id": receipt_id, **i} for i in norm_items],
                   [("receipt_id", "STRING"), ("item_id", "STRING"), *[(c, "STRING") for c in src], ("rank", "INT64"), ("score", "FLOAT64"), ("selection", "STRING")])
        self._load(self._t("context_receipt_excluded"), [{"receipt_id": receipt_id, **e} for e in norm_excluded],
                   [("receipt_id", "STRING"), ("item_id", "STRING"), ("reason", "STRING"), *[(c, "STRING") for c in src]])
        values = dict(zip(_BQ_RECEIPT_COLS, (receipt_id, self._timestamp(), context_name, context_version, policy_version, caller_id, purpose,
                                             query_hash, eligible_count, excluded_count, dagster_asset_key, dagster_run_id, trace_id,
                                             mcp_session_id, mcp_request_id, dagster_partition_key)))
        self._load(self._t("context_receipts"), [values],
                   [(c, "TIMESTAMP" if c == "created_at" else "INT64" if c.endswith("_count") else "STRING") for c in _BQ_RECEIPT_COLS])
        return receipt_id

    def _append_event(self, receipt_id: str, event_type: str, payload: Mapping[str, Any]) -> None:
        if not self._query(f"SELECT 1 AS x FROM {self._t('context_receipts')} WHERE receipt_id = @r LIMIT 1", r=receipt_id):
            raise KeyError(f"unknown context receipt: {receipt_id}")
        self._load(self._t("context_receipt_events"), [{
            "event_id": str(uuid.uuid4()), "receipt_id": receipt_id, "event_type": event_type, "occurred_at": self._timestamp(),
            "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":")),
        }], [("event_id", "STRING"), ("receipt_id", "STRING"), ("event_type", "STRING"), ("occurred_at", "TIMESTAMP"), ("payload_json", "STRING")])

    # -- reads ------------------------------------------------------------------------------------------------
    def get_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        rows = self._query(f"SELECT * FROM {self._t('context_receipts')} WHERE receipt_id = @r", r=receipt_id)
        if not rows:
            return None
        items = self._query(
            f"SELECT item_id, source_asset_key, source_version, source_partition, source_code_version, rank, score, selection "
            f"FROM {self._t('context_receipt_items')} WHERE receipt_id = @r ORDER BY rank, item_id", r=receipt_id)
        excluded = self._query(
            f"SELECT item_id, reason, source_asset_key, source_version, source_partition, source_code_version "
            f"FROM {self._t('context_receipt_excluded')} WHERE receipt_id = @r ORDER BY item_id", r=receipt_id)
        events = self._query(
            f"SELECT event_id, event_type, occurred_at, payload_json FROM {self._t('context_receipt_events')} "
            f"WHERE receipt_id = @r ORDER BY occurred_at, event_id", r=receipt_id)
        result = {k: (_bq_iso(v) if k == "created_at" else v) for k, v in rows[0].items()}
        result["items"], result["excluded"] = items, excluded
        result["events"] = [{"event_id": e["event_id"], "event_type": e["event_type"], "occurred_at": _bq_iso(e["occurred_at"]),
                             "payload": json.loads(e["payload_json"])} for e in events]
        return result

    @staticmethod
    def _page(rows: list, key: str, limit: int) -> list:
        """BigQuery has no row position; cursors are microsecond timestamps. A page that ends mid-microsecond would lose the
        rest of that microsecond on resume, so a full page drops its trailing tie group (unless it is the whole page)."""
        if len(rows) < limit:
            return rows
        last = rows[-1][key]
        kept = [r for r in rows if r[key] != last]
        return kept or rows

    def read_since(self, receipt_cursor: int = 0, event_cursor: int = 0, limit: int = 1000) -> dict[str, Any]:
        """Same contract as the SQLite store; cursors are microseconds since the epoch of ``created_at`` / ``occurred_at``."""
        receipts = self._page(self._query(
            f"SELECT UNIX_MICROS(created_at) AS pos, * FROM {self._t('context_receipts')} WHERE UNIX_MICROS(created_at) > @c "
            f"ORDER BY pos, receipt_id LIMIT @n", c=int(receipt_cursor), n=int(limit)), "pos", int(limit))
        ids = [r["receipt_id"] for r in receipts]
        items = self._query(f"SELECT * FROM {self._t('context_receipt_items')} WHERE receipt_id IN UNNEST(@ids)", ids=ids) if ids else []
        excluded = self._query(f"SELECT * FROM {self._t('context_receipt_excluded')} WHERE receipt_id IN UNNEST(@ids)", ids=ids) if ids else []
        events = self._page(self._query(
            f"SELECT UNIX_MICROS(occurred_at) AS pos, event_id, receipt_id, event_type, occurred_at, payload_json "
            f"FROM {self._t('context_receipt_events')} WHERE UNIX_MICROS(occurred_at) > @c ORDER BY pos, event_id LIMIT @n",
            c=int(event_cursor), n=int(limit)), "pos", int(limit))
        outcome_assets: dict[str, list[tuple[str, str | None, str | None]]] = {}
        if any(e["event_type"] == "outcome" for e in events):
            oids = sorted({e["receipt_id"] for e in events if e["event_type"] == "outcome"})
            for row in self._query(
                f"SELECT DISTINCT receipt_id, source_asset_key, source_partition, source_code_version FROM {self._t('context_receipt_items')} "
                f"WHERE receipt_id IN UNNEST(@ids) AND source_asset_key IS NOT NULL", ids=oids):
                outcome_assets.setdefault(row["receipt_id"], []).append((row["source_asset_key"], row["source_partition"], row["source_code_version"]))
        by_receipt: dict[str, dict[str, Any]] = {}
        for row in receipts:
            record = {k: (_bq_iso(v) if k == "created_at" else v) for k, v in row.items() if k != "pos"}
            record["items"], record["excluded"] = [], []
            by_receipt[row["receipt_id"]] = record
        for row in items:
            by_receipt[row["receipt_id"]]["items"].append(row)
        for row in excluded:
            by_receipt[row["receipt_id"]]["excluded"].append(row)
        outcomes = []
        for row in events:
            if row["event_type"] != "outcome":
                continue
            payload = json.loads(row["payload_json"])
            payload["receipt_id"] = row["receipt_id"]
            payload["sources"] = outcome_assets.get(row["receipt_id"], [])
            outcomes.append(payload)
        return {"receipts": [by_receipt[r["receipt_id"]] for r in receipts], "outcomes": outcomes,
                "events": [{**{k: row[k] for k in ("event_id", "receipt_id", "event_type", "payload_json")}, "occurred_at": _bq_iso(row["occurred_at"])} for row in events],
                "receipt_cursor": receipts[-1]["pos"] if receipts else receipt_cursor,
                "event_cursor": events[-1]["pos"] if events else event_cursor}

    def _events_for(self, ids: list, types: str) -> list:
        out: list = []
        for start in range(0, len(ids), 5000):
            out.extend(self._query(
                f"SELECT receipt_id, event_type, payload_json FROM {self._t('context_receipt_events')} "
                f"WHERE event_type IN ({types}) AND receipt_id IN UNNEST(@ids) ORDER BY occurred_at, event_id", ids=ids[start:start + 5000]))
        return out

    def refresh_item_scores(self, prior_strength: float = 5.0) -> int:
        if prior_strength < 0:
            raise ValueError("prior_strength must be >= 0")
        items = self._query(f"SELECT receipt_id, item_id FROM {self._t('context_receipt_items')}")
        events = self._query(
            f"SELECT receipt_id, event_type, payload_json FROM {self._t('context_receipt_events')} "
            f"WHERE event_type IN ('outcome', 'evidence') ORDER BY occurred_at, event_id")
        stats = _fold_item_scores(items, events)
        now = self._timestamp()
        rows = [{"item_id": item, "retrievals": int(n), "outcome_weight": w, "outcome_sum": total,
                 "prior": total / (w + prior_strength) if (w + prior_strength) else 0.0, "updated_at": now}
                for item, (n, w, total) in stats.items()]
        if not rows:
            return 0
        # staging + one MERGE: readers never see a half-written score table
        staging = receipt_table(self.dataset, "context_item_scores", self.table_prefix).replace("context_item_scores`", "context_item_scores_staging`")
        self._query(f"CREATE TABLE IF NOT EXISTS {staging} AS SELECT * FROM {self._t('context_item_scores')} WHERE FALSE")
        self._query(f"TRUNCATE TABLE {staging}")
        self._load(staging, rows, [("item_id", "STRING"), ("retrievals", "INT64"), ("outcome_weight", "FLOAT64"), ("outcome_sum", "FLOAT64"),
                                   ("prior", "FLOAT64"), ("updated_at", "TIMESTAMP")])
        self._query(
            f"MERGE {self._t('context_item_scores')} t USING {staging} s ON t.item_id = s.item_id "
            f"WHEN MATCHED THEN UPDATE SET retrievals = s.retrievals, outcome_weight = s.outcome_weight, outcome_sum = s.outcome_sum, "
            f"prior = s.prior, updated_at = s.updated_at "
            f"WHEN NOT MATCHED THEN INSERT (item_id, retrievals, outcome_weight, outcome_sum, prior, updated_at) "
            f"VALUES (s.item_id, s.retrievals, s.outcome_weight, s.outcome_sum, s.prior, s.updated_at)")
        return len(stats)

    def get_item_scores(self, item_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        ids = list(item_ids)
        if not ids:
            return {}
        rows = self._query(f"SELECT * FROM {self._t('context_item_scores')} WHERE item_id IN UNNEST(@ids)", ids=ids)
        return {r["item_id"]: {**r, "updated_at": _bq_iso(r["updated_at"])} for r in rows}

    def item_feedback(self, asset_key: str, *, since: str = "", code_version: str | None = None) -> dict[str, dict[str, Any]]:
        item_where = "i.source_asset_key = @a AND r.created_at >= TIMESTAMP(@since)"
        excl_where = "e.source_asset_key = @a AND r.created_at >= TIMESTAMP(@since)"
        params: dict[str, Any] = {"a": asset_key, "since": self._ts(since)}
        if code_version is not None:
            item_where += " AND COALESCE(i.source_code_version, 'unknown') = @cv"
            excl_where += " AND COALESCE(e.source_code_version, 'unknown') = @cv"
            params["cv"] = code_version
        item_rows = self._query(
            f"SELECT DISTINCT i.receipt_id AS receipt_id, i.item_id AS item_id, r.created_at AS created_at "
            f"FROM {self._t('context_receipt_items')} i JOIN {self._t('context_receipts')} r ON r.receipt_id = i.receipt_id WHERE {item_where}", **params)
        for row in item_rows:
            row["created_at"] = _bq_iso(row["created_at"])
        excl_rows = self._query(
            f"SELECT e.item_id AS item_id, e.reason AS reason FROM {self._t('context_receipt_excluded')} e "
            f"JOIN {self._t('context_receipts')} r ON r.receipt_id = e.receipt_id WHERE {excl_where}", **params)
        events = self._events_for(sorted({r["receipt_id"] for r in item_rows}), "'outcome', 'evidence'")
        return _fold_item_feedback(item_rows, excl_rows, events)

    def asset_usage_since(self, asset_key: str, since: str) -> dict[str, Any]:
        rows = self._query(
            f"SELECT i.receipt_id AS receipt_id, i.source_partition AS source_partition, i.source_version AS source_version "
            f"FROM {self._t('context_receipt_items')} i JOIN {self._t('context_receipts')} r ON r.receipt_id = i.receipt_id "
            f"WHERE i.source_asset_key = @a AND r.created_at >= TIMESTAMP(@since)", a=asset_key, since=self._ts(since))
        ids = sorted({r["receipt_id"] for r in rows})
        events = self._events_for(ids, "'outcome', 'output_attached', 'evidence'")
        return _fold_asset_usage(rows, events, ids)

    def outcomes_by_code_version(self, asset_key: str, since: str = "") -> dict[str, dict[str, Any]]:
        rows = self._query(
            f"SELECT DISTINCT i.receipt_id AS receipt_id, COALESCE(i.source_code_version, 'unknown') AS cv "
            f"FROM {self._t('context_receipt_items')} i JOIN {self._t('context_receipts')} r ON r.receipt_id = i.receipt_id "
            f"WHERE i.source_asset_key = @a AND r.created_at >= TIMESTAMP(@since)", a=asset_key, since=self._ts(since))
        events = self._events_for(sorted({r["receipt_id"] for r in rows}), "'outcome'")
        return _fold_outcomes_by_code_version(rows, events)

    def derive_implicit_outcomes(self, *, window_seconds: float = 120.0, since: str | None = None) -> int:
        rows = self._query(
            f"SELECT receipt_id, created_at, caller_id, mcp_session_id, query_hash FROM {self._t('context_receipts')} "
            f"WHERE mcp_session_id IS NOT NULL AND query_hash IS NOT NULL AND created_at >= TIMESTAMP(@since) ORDER BY created_at, receipt_id",
            since=self._ts(since))
        existing = {json.loads(r["payload_json"]).get("outcome_ref") for r in self._query(
            f"SELECT payload_json FROM {self._t('context_receipt_events')} WHERE event_type = 'outcome'")}
        previous: dict[tuple[str, str | None], Any] = {}
        created = 0
        for row in rows:
            key = (row["mcp_session_id"], row["caller_id"])
            prior = previous.get(key)
            previous[key] = row
            if prior is None or prior["query_hash"] == row["query_hash"]:
                continue
            gap = (_as_datetime(_bq_iso(row["created_at"])) - _as_datetime(_bq_iso(prior["created_at"]))).total_seconds()
            ref = f"implicit:reformulated:{row['receipt_id']}"
            if gap <= window_seconds and ref not in existing:
                self.record_outcome(prior["receipt_id"], signal="reformulated", source="implicit", outcome_ref=ref)
                created += 1
        return created

    def enforce_retention(self, retention_days: Mapping[str, float | None], *, now: datetime | None = None,
                          default_days: float | None = None) -> dict[str, int]:
        limits = {k: float(v) for k, v in retention_days.items() if v}
        if default_days is not None and default_days <= 0:
            raise ValueError("default_days must be positive or None")
        if not limits and not default_days:
            return {"receipts_deleted": 0}
        now = now or datetime.now(timezone.utc)
        cutoff = (now - timedelta(days=min([*limits.values(), *([float(default_days)] if default_days else [])]))).isoformat()
        candidates = self._query(f"SELECT receipt_id, created_at FROM {self._t('context_receipts')} WHERE created_at < TIMESTAMP(@c)", c=cutoff)
        ids = [c["receipt_id"] for c in candidates]
        assets: dict[str, set] = {}
        for start in range(0, len(ids), 5000):
            for row in self._query(
                f"SELECT receipt_id, source_asset_key FROM {self._t('context_receipt_items')} WHERE receipt_id IN UNNEST(@ids) AND source_asset_key IS NOT NULL "
                f"UNION DISTINCT SELECT receipt_id, source_asset_key FROM {self._t('context_receipt_excluded')} WHERE receipt_id IN UNNEST(@ids) AND source_asset_key IS NOT NULL",
                ids=ids[start:start + 5000]):
                assets.setdefault(row["receipt_id"], set()).add(row["source_asset_key"])
        doomed = []
        for row in candidates:
            limit = _receipt_limit(assets.get(row["receipt_id"], ()), limits, default_days)
            if limit and _as_datetime(_bq_iso(row["created_at"])) + timedelta(days=limit) <= now:
                doomed.append(row["receipt_id"])
        self.delete_receipts(doomed)
        return {"receipts_deleted": len(doomed)}

    def delete_receipts(self, receipt_ids: Iterable[str]) -> int:
        ids = sorted({str(i) for i in receipt_ids})
        existed = 0
        for start in range(0, len(ids), 5000):
            batch = ids[start:start + 5000]
            existed += self._query(f"SELECT COUNT(1) AS n FROM {self._t('context_receipts')} WHERE receipt_id IN UNNEST(@ids)", ids=batch)[0]["n"]
            for table in ("context_receipt_events", "context_receipt_excluded", "context_receipt_items", "context_receipts"):
                self._query(f"DELETE FROM {self._t(table)} WHERE receipt_id IN UNNEST(@ids)", ids=batch)
        return existed

    def delete_item_scores(self, item_ids: Iterable[str]) -> int:
        ids = sorted({str(i) for i in item_ids})
        existed = 0
        for start in range(0, len(ids), 5000):
            batch = ids[start:start + 5000]
            existed += self._query(f"SELECT COUNT(1) AS n FROM {self._t('context_item_scores')} WHERE item_id IN UNNEST(@ids)", ids=batch)[0]["n"]
            self._query(f"DELETE FROM {self._t('context_item_scores')} WHERE item_id IN UNNEST(@ids)", ids=batch)
        return existed

    def _selftest_cursors(self) -> tuple[int, int]:
        micros = int((datetime.now(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)) / timedelta(microseconds=1)) - 5_000_000
        return micros, micros

    def find_by_assets(self, targets: Mapping[str, Iterable[str] | None]) -> dict[str, list[str]]:
        exact: set[str] = set()
        unknown: set[str] = set()
        for asset_key, partitions in targets.items():
            rows = self._query(f"SELECT DISTINCT receipt_id, source_partition FROM {self._t('context_receipt_items')} WHERE source_asset_key = @a", a=asset_key)
            wanted = None if partitions is None else set(partitions)
            for row in rows:
                if wanted is None or row["source_partition"] in wanted:
                    exact.add(row["receipt_id"])
                elif row["source_partition"] is None:
                    unknown.add(row["receipt_id"])
        return {"receipt_ids": sorted(exact), "partition_unknown": sorted(unknown - exact)}

    def find_by_item(self, item_id: str) -> list[str]:
        return [r["receipt_id"] for r in self._query(
            f"SELECT DISTINCT receipt_id FROM {self._t('context_receipt_items')} WHERE item_id = @i ORDER BY receipt_id", i=item_id)]

    def find_by_source_asset(self, asset_key: str) -> list[str]:
        return [r["receipt_id"] for r in self._query(
            f"SELECT DISTINCT receipt_id FROM {self._t('context_receipt_items')} WHERE source_asset_key = @a ORDER BY receipt_id", a=asset_key)]

    def receipts_for_partition(self, dagster_asset_key: str, partition_key: str | None) -> list[str]:
        where = "dagster_partition_key IS NULL" if partition_key is None else "dagster_partition_key = @p"
        params = {"a": dagster_asset_key} if partition_key is None else {"a": dagster_asset_key, "p": partition_key}
        return [r["receipt_id"] for r in self._query(
            f"SELECT receipt_id FROM {self._t('context_receipts')} WHERE dagster_asset_key = @a AND {where} ORDER BY receipt_id", **params)]


# ── Generic SQL backend (SQLAlchemy) ─────────────────────────────────────────────────────────────────────
# The same receipts in any database SQLAlchemy can reach: Postgres, MySQL/MariaDB, Snowflake, Databricks, DuckDB, SQLite, ...
# One class, portable SQL (bound parameters, expanding IN lists, no vendor functions); the dialect decides table quoting and
# column types via the canonical block above. The row folding is the shared _fold_* code, so results match the other stores
# (a parity test runs one scenario through each). Writes use one transaction per call where the database has them.

_SQL_DRIVER_HINTS = {  # URL scheme -> what to pip install (these drivers are optional: only the backend you use needs one)
    "postgresql": "sqlalchemy psycopg2-binary", "postgres": "sqlalchemy psycopg2-binary", "mysql": "sqlalchemy pymysql",
    "mariadb": "sqlalchemy pymysql", "snowflake": "sqlalchemy snowflake-sqlalchemy", "databricks": "sqlalchemy databricks-sqlalchemy",
    "duckdb": "sqlalchemy duckdb-engine", "sqlite": "sqlalchemy",
}


def _create_sql_engine(url: str) -> Any:
    """``sqlalchemy.create_engine`` with an actionable error when the SQLAlchemy driver for this database is not installed."""
    scheme = url.split(":", 1)[0].split("+", 1)[0].lower()
    hint = _SQL_DRIVER_HINTS.get(scheme)
    try:
        import sqlalchemy

        return sqlalchemy.create_engine(url)
    except (ImportError, Exception) as exc:  # noqa: BLE001  (NoSuchModuleError subclasses Exception, not ImportError)
        missing = isinstance(exc, ImportError) or type(exc).__name__ == "NoSuchModuleError"
        if missing and hint:
            raise ImportError(f"backend: sql with a {scheme!r} URL needs its SQLAlchemy driver, which is not installed: "
                              f"pip install {hint}  (underlying error: {exc})") from exc
        raise


_SQL_DIALECT_NAMES = {"postgresql": "postgres", "postgres": "postgres", "mysql": "mysql", "mariadb": "mysql", "sqlite": "sqlite",
                      "duckdb": "duckdb", "snowflake": "snowflake", "databricks": "databricks"}


class SqlContextReceiptStore(ContextReceiptStore):
    """Receipts in a SQL database. ``engine`` is a SQLAlchemy ``Engine`` (or a URL string); ``schema`` is the namespace the five
    tables live in (``database.schema`` for Snowflake, ``catalog.schema`` for Databricks, ``schema`` for Postgres, ``database``
    for MySQL, "" for the connection default)."""

    def __init__(self, engine: Any, *, schema: str = "", table_prefix: str = "", dialect: str | None = None, create_tables: bool = True):
        """``engine``: a SQLAlchemy Engine, a URL string, or a zero-argument callable returning an Engine (built on first use, so
        an unset environment variable does not break loading the code location)."""
        self._engine_obj: Any = None
        self._engine_factory: Any = None
        if isinstance(engine, str):
            self._engine_factory = lambda url=engine: _create_sql_engine(url)
        elif callable(engine) and not hasattr(engine, "dialect"):
            self._engine_factory = engine
        else:
            self._engine_obj = engine
        self._dialect_override = dialect
        self.schema, self.table_prefix = schema, table_prefix
        self._create_tables = create_tables
        self._ready = False
        self._ready_lock = threading.Lock()
        if dialect is not None and dialect not in _SQL_DIALECT_NAMES:
            raise ValueError(f"unsupported SQL dialect {dialect!r}; supported: {sorted(set(_SQL_DIALECT_NAMES))}")
        if self._engine_obj is not None:
            self.dialect  # fail fast on an unsupported database

    @property
    def _engine(self) -> Any:
        if self._engine_obj is None:
            self._engine_obj = self._engine_factory()
        return self._engine_obj

    @property
    def dialect(self) -> str:  # type: ignore[override]
        name = self._dialect_override or self._engine.dialect.name
        if name not in _SQL_DIALECT_NAMES:
            raise ValueError(f"unsupported SQL dialect {name!r}; supported: {sorted(set(_SQL_DIALECT_NAMES))}")
        return _SQL_DIALECT_NAMES[name]

    @property
    def database_path(self) -> str:  # type: ignore[override]
        return f"sql://{self.schema}/{self.table_prefix}"

    @property
    def _rank(self) -> str:
        return "`rank`" if self.dialect == "mysql" else "rank"     # RANK is reserved in MySQL 8

    # -- plumbing ---------------------------------------------------------------------------------------------
    def _t(self, name: str) -> str:
        return receipt_table(self.schema, name, self.table_prefix, self.dialect)

    def _tsv(self, value: Any) -> Any:
        """A timestamp as this database wants to be handed one."""
        dt = _as_datetime(value)
        if dt is None:
            return None
        dt = dt.astimezone(timezone.utc)
        if self.dialect == "sqlite":
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")             # fixed width: text order == time order
        if self.dialect == "mysql":
            return dt.replace(tzinfo=None)                               # DATETIME(6), UTC by convention
        return dt

    @staticmethod
    def _iso(value: Any) -> Any:
        return _bq_iso(_as_datetime(value)) if value is not None else None

    def _prepare(self) -> None:
        """``create_tables=True``: create/upgrade the tables. ``create_tables=False`` (you manage them): run only the column probe, once."""
        if self._create_tables:
            self.ensure_tables()
        else:
            self._verify_columns()

    def _verify_columns(self) -> None:
        import sqlalchemy as sa

        with self._ready_lock:
            if self._ready:
                return
            missing, errors = [], []
            for probe, alter in receipt_column_upgrades(self.schema, self.table_prefix, self.dialect):
                try:
                    with self._engine.begin() as conn:
                        conn.execute(sa.text(probe))
                except Exception as exc:  # noqa: BLE001
                    missing.append(alter)
                    errors.append(str(exc))
            if missing:
                raise RuntimeError(_missing_columns_message(missing, errors))
            self._ready = True

    def ensure_tables(self) -> None:
        import sqlalchemy as sa

        with self._ready_lock:
            if self._ready:
                return
            with self._engine.begin() as conn:
                for ddl in receipt_table_ddl(self.schema, self.table_prefix, self.dialect):
                    conn.execute(sa.text(ddl))
            for probe, alter in receipt_column_upgrades(self.schema, self.table_prefix, self.dialect):   # tables made by an older release
                try:
                    with self._engine.begin() as conn:      # its own transaction: a failed probe aborts a Postgres transaction
                        conn.execute(sa.text(probe))
                except Exception:  # noqa: BLE001  (column missing)
                    with self._engine.begin() as conn:
                        conn.execute(sa.text(alter))
            if self.dialect in ("postgres", "sqlite", "duckdb"):         # warehouses have no secondary indexes
                for table, cols in (("context_receipts", "receipt_id"), ("context_receipt_items", "item_id"),
                                    ("context_receipt_items", "receipt_id"), ("context_receipt_items", "source_asset_key"),
                                    ("context_receipt_excluded", "receipt_id"), ("context_receipt_events", "receipt_id, occurred_at")):
                    index = f"idx_{self.table_prefix}{table}_{cols.split(',')[0]}".replace(" ", "")
                    try:
                        with self._engine.begin() as conn:
                            conn.execute(sa.text(f'CREATE INDEX IF NOT EXISTS "{index}" ON {self._t(table)} ({cols})'))
                    except Exception:  # an index is an optimization, never a reason to fail
                        pass
            self._ready = True

    def _query(self, sql: str, **params: Any) -> list:
        import sqlalchemy as sa

        self._prepare()
        stmt = sa.text(sql)
        lists = [k for k, v in params.items() if isinstance(v, (list, tuple))]
        if lists:
            stmt = stmt.bindparams(*[sa.bindparam(k, expanding=True) for k in lists])
        with self._engine.begin() as conn:
            result = conn.execute(stmt, params)
            return [dict(r._mapping) for r in result] if result.returns_rows else []

    def _in_chunks(self, ids: list, size: int = 500) -> Iterator[list]:
        for start in range(0, len(ids), size):
            yield ids[start:start + size]

    # -- writes -----------------------------------------------------------------------------------------------
    def record_retrieval(self, *, context_name: str, items: Iterable[Mapping[str, Any]], excluded: Iterable[Mapping[str, Any]] = (),
                         context_version: str | None = None, policy_version: str | None = None, caller_id: str | None = None,
                         purpose: str | None = None, query_hash: str | None = None, eligible_count: int | None = None,
                         excluded_count: int | None = None, dagster_asset_key: str | None = None, dagster_run_id: str | None = None,
                         receipt_id: str | None = None, trace_id: str | None = None, mcp_session_id: str | None = None,
                         mcp_request_id: str | None = None, dagster_partition_key: str | None = None) -> str:
        import sqlalchemy as sa

        receipt_id, norm_items, norm_excluded, excluded_count, caller_id, mcp_session_id = self._prepare_retrieval(
            context_name, items, excluded, eligible_count, excluded_count, receipt_id, caller_id, mcp_session_id)
        self._prepare()
        created_at = self._tsv(self._timestamp())
        with self._engine.begin() as conn:
            seen = conn.execute(sa.text(
                f"SELECT (SELECT COUNT(1) FROM {self._t('context_receipts')} WHERE receipt_id = :r) AS r, "
                f"(SELECT COUNT(1) FROM {self._t('context_receipt_items')} WHERE receipt_id = :r) AS i, "
                f"(SELECT COUNT(1) FROM {self._t('context_receipt_excluded')} WHERE receipt_id = :r) AS e"), {"r": receipt_id}).one()
            if seen.r:
                raise DuplicateReceiptError(f"receipt already exists: {receipt_id}")
            if seen.i:   # leftovers of a crashed earlier attempt (databases without transactions)
                conn.execute(sa.text(f"DELETE FROM {self._t('context_receipt_items')} WHERE receipt_id = :r"), {"r": receipt_id})
            if seen.e:
                conn.execute(sa.text(f"DELETE FROM {self._t('context_receipt_excluded')} WHERE receipt_id = :r"), {"r": receipt_id})
            if norm_items:
                conn.execute(sa.text(
                    f"INSERT INTO {self._t('context_receipt_items')} (receipt_id, item_id, source_asset_key, source_version, source_partition, "
                    f"source_code_version, {self._rank}, score, selection) VALUES (:receipt_id, :item_id, :source_asset_key, :source_version, "
                    f":source_partition, :source_code_version, :rank, :score, :selection)"),
                    [{"receipt_id": receipt_id, **i} for i in norm_items])
            if norm_excluded:
                conn.execute(sa.text(
                    f"INSERT INTO {self._t('context_receipt_excluded')} (receipt_id, item_id, reason, source_asset_key, source_version, "
                    f"source_partition, source_code_version) VALUES (:receipt_id, :item_id, :reason, :source_asset_key, :source_version, "
                    f":source_partition, :source_code_version)"),
                    [{"receipt_id": receipt_id, **e} for e in norm_excluded])
            conn.execute(sa.text(   # the receipt row goes last: it is the commit marker
                f"INSERT INTO {self._t('context_receipts')} ({', '.join(_BQ_RECEIPT_COLS)}) VALUES ({', '.join(':' + c for c in _BQ_RECEIPT_COLS)})"),
                dict(zip(_BQ_RECEIPT_COLS, (receipt_id, created_at, context_name, context_version, policy_version, caller_id, purpose, query_hash,
                                            eligible_count, excluded_count, dagster_asset_key, dagster_run_id, trace_id, mcp_session_id,
                                            mcp_request_id, dagster_partition_key))))
        return receipt_id

    def _append_event(self, receipt_id: str, event_type: str, payload: Mapping[str, Any]) -> None:
        import sqlalchemy as sa

        if not self._query(f"SELECT 1 AS x FROM {self._t('context_receipts')} WHERE receipt_id = :r LIMIT 1", r=receipt_id):
            raise KeyError(f"unknown context receipt: {receipt_id}")
        with self._engine.begin() as conn:
            conn.execute(sa.text(
                f"INSERT INTO {self._t('context_receipt_events')} (event_id, receipt_id, event_type, occurred_at, payload_json) "
                f"VALUES (:event_id, :receipt_id, :event_type, :occurred_at, :payload_json)"),
                {"event_id": str(uuid.uuid4()), "receipt_id": receipt_id, "event_type": event_type,
                 "occurred_at": self._tsv(self._timestamp()), "payload_json": json.dumps(payload, sort_keys=True, separators=(",", ":"))})

    # -- reads ------------------------------------------------------------------------------------------------
    def get_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        rows = self._query(f"SELECT * FROM {self._t('context_receipts')} WHERE receipt_id = :r", r=receipt_id)
        if not rows:
            return None
        items = self._query(
            f"SELECT item_id, source_asset_key, source_version, source_partition, source_code_version, {self._rank} AS rank, score, selection "
            f"FROM {self._t('context_receipt_items')} WHERE receipt_id = :r ORDER BY {self._rank}, item_id", r=receipt_id)
        excluded = self._query(
            f"SELECT item_id, reason, source_asset_key, source_version, source_partition, source_code_version "
            f"FROM {self._t('context_receipt_excluded')} WHERE receipt_id = :r ORDER BY item_id", r=receipt_id)
        events = self._query(
            f"SELECT event_id, event_type, occurred_at, payload_json FROM {self._t('context_receipt_events')} "
            f"WHERE receipt_id = :r ORDER BY occurred_at, event_id", r=receipt_id)
        result = {k: (self._iso(v) if k == "created_at" else v) for k, v in rows[0].items()}
        result["items"], result["excluded"] = items, excluded
        result["events"] = [{"event_id": e["event_id"], "event_type": e["event_type"], "occurred_at": self._iso(e["occurred_at"]),
                             "payload": json.loads(e["payload_json"])} for e in events]
        return result

    @staticmethod
    def _micros(value: Any) -> int:
        return (_as_datetime(value) - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(microseconds=1)

    def _from_micros(self, micros: int) -> Any:
        return self._tsv(datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=int(micros)))

    def read_since(self, receipt_cursor: int = 0, event_cursor: int = 0, limit: int = 1000) -> dict[str, Any]:
        """Same contract as the SQLite store; cursors are microseconds since the epoch of ``created_at`` / ``occurred_at``."""
        receipts = self._page(self._query(
            f"SELECT * FROM {self._t('context_receipts')} WHERE created_at > :c ORDER BY created_at, receipt_id LIMIT :n",
            c=self._from_micros(receipt_cursor), n=int(limit)), "created_at", int(limit))
        for row in receipts:
            row["pos"] = self._micros(row["created_at"])
        ids = [r["receipt_id"] for r in receipts]
        items: list = []
        excluded: list = []
        for chunk in self._in_chunks(ids):
            items += self._query(f"SELECT * FROM {self._t('context_receipt_items')} WHERE receipt_id IN :ids", ids=chunk)
            excluded += self._query(f"SELECT * FROM {self._t('context_receipt_excluded')} WHERE receipt_id IN :ids", ids=chunk)
        events = self._page(self._query(
            f"SELECT event_id, receipt_id, event_type, occurred_at, payload_json FROM {self._t('context_receipt_events')} "
            f"WHERE occurred_at > :c ORDER BY occurred_at, event_id LIMIT :n", c=self._from_micros(event_cursor), n=int(limit)),
            "occurred_at", int(limit))
        for row in events:
            row["pos"] = self._micros(row["occurred_at"])
        outcome_assets: dict[str, list[tuple[str, str | None, str | None]]] = {}
        oids = sorted({e["receipt_id"] for e in events if e["event_type"] == "outcome"})
        for chunk in self._in_chunks(oids):
            for row in self._query(
                f"SELECT DISTINCT receipt_id, source_asset_key, source_partition, source_code_version FROM {self._t('context_receipt_items')} "
                f"WHERE receipt_id IN :ids AND source_asset_key IS NOT NULL", ids=chunk):
                outcome_assets.setdefault(row["receipt_id"], []).append((row["source_asset_key"], row["source_partition"], row["source_code_version"]))
        by_receipt: dict[str, dict[str, Any]] = {}
        for row in receipts:
            record = {k: (self._iso(v) if k == "created_at" else v) for k, v in row.items() if k != "pos"}
            record["items"], record["excluded"] = [], []
            by_receipt[row["receipt_id"]] = record
        for row in items:
            by_receipt[row["receipt_id"]]["items"].append(row)
        for row in excluded:
            by_receipt[row["receipt_id"]]["excluded"].append(row)
        outcomes = []
        for row in events:
            if row["event_type"] != "outcome":
                continue
            payload = json.loads(row["payload_json"])
            payload["receipt_id"] = row["receipt_id"]
            payload["sources"] = outcome_assets.get(row["receipt_id"], [])
            outcomes.append(payload)
        return {"receipts": [by_receipt[r["receipt_id"]] for r in receipts], "outcomes": outcomes,
                "events": [{**{k: row[k] for k in ("event_id", "receipt_id", "event_type", "payload_json")}, "occurred_at": self._iso(row["occurred_at"])}
                           for row in events],
                "receipt_cursor": receipts[-1]["pos"] if receipts else receipt_cursor,
                "event_cursor": events[-1]["pos"] if events else event_cursor}

    @staticmethod
    def _page(rows: list, key: str, limit: int) -> list:
        """Cursors are microsecond timestamps; a full page drops its trailing tie group so no row is lost on resume."""
        if len(rows) < limit:
            return rows
        last = rows[-1][key]
        kept = [r for r in rows if r[key] != last]
        return kept or rows

    def _events_for(self, ids: list, types: str) -> list:
        out: list = []
        for chunk in self._in_chunks(ids):
            out.extend(self._query(
                f"SELECT receipt_id, event_type, payload_json FROM {self._t('context_receipt_events')} "
                f"WHERE event_type IN ({types}) AND receipt_id IN :ids ORDER BY occurred_at, event_id", ids=chunk))
        return out

    def refresh_item_scores(self, prior_strength: float = 5.0) -> int:
        import sqlalchemy as sa

        if prior_strength < 0:
            raise ValueError("prior_strength must be >= 0")
        items = self._query(f"SELECT receipt_id, item_id FROM {self._t('context_receipt_items')}")
        events = self._query(
            f"SELECT receipt_id, event_type, payload_json FROM {self._t('context_receipt_events')} "
            f"WHERE event_type IN ('outcome', 'evidence') ORDER BY occurred_at, event_id")
        stats = _fold_item_scores(items, events)
        now = self._tsv(self._timestamp())
        rows = [{"item_id": item, "retrievals": int(n), "outcome_weight": w, "outcome_sum": total,
                 "prior": total / (w + prior_strength) if (w + prior_strength) else 0.0, "updated_at": now}
                for item, (n, w, total) in stats.items()]
        if rows:
            with self._engine.begin() as conn:    # delete + insert in one transaction: readers never see a half-written table
                for chunk in self._in_chunks([r["item_id"] for r in rows]):
                    conn.execute(sa.text(f"DELETE FROM {self._t('context_item_scores')} WHERE item_id IN :ids").bindparams(
                        sa.bindparam("ids", expanding=True)), {"ids": chunk})
                conn.execute(sa.text(
                    f"INSERT INTO {self._t('context_item_scores')} (item_id, retrievals, outcome_weight, outcome_sum, prior, updated_at) "
                    f"VALUES (:item_id, :retrievals, :outcome_weight, :outcome_sum, :prior, :updated_at)"), rows)
        return len(stats)

    def get_item_scores(self, item_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        ids = list(item_ids)
        out: dict[str, dict[str, Any]] = {}
        for chunk in self._in_chunks(ids):
            for r in self._query(f"SELECT * FROM {self._t('context_item_scores')} WHERE item_id IN :ids", ids=chunk):
                out[r["item_id"]] = {**r, "updated_at": self._iso(r["updated_at"])}
        return out

    def item_feedback(self, asset_key: str, *, since: str = "", code_version: str | None = None) -> dict[str, dict[str, Any]]:
        item_where = "i.source_asset_key = :a AND r.created_at >= :since"
        excl_where = "e.source_asset_key = :a AND r.created_at >= :since"
        params: dict[str, Any] = {"a": asset_key, "since": self._tsv(since or _EPOCH)}
        if code_version is not None:
            item_where += " AND COALESCE(i.source_code_version, 'unknown') = :cv"
            excl_where += " AND COALESCE(e.source_code_version, 'unknown') = :cv"
            params["cv"] = code_version
        item_rows = self._query(
            f"SELECT DISTINCT i.receipt_id AS receipt_id, i.item_id AS item_id, r.created_at AS created_at "
            f"FROM {self._t('context_receipt_items')} i JOIN {self._t('context_receipts')} r ON r.receipt_id = i.receipt_id WHERE {item_where}", **params)
        for row in item_rows:
            row["created_at"] = self._iso(row["created_at"])
        excl_rows = self._query(
            f"SELECT e.item_id AS item_id, e.reason AS reason FROM {self._t('context_receipt_excluded')} e "
            f"JOIN {self._t('context_receipts')} r ON r.receipt_id = e.receipt_id WHERE {excl_where}", **params)
        events = self._events_for(sorted({r["receipt_id"] for r in item_rows}), "'outcome', 'evidence'")
        return _fold_item_feedback(item_rows, excl_rows, events)

    def asset_usage_since(self, asset_key: str, since: str) -> dict[str, Any]:
        rows = self._query(
            f"SELECT i.receipt_id AS receipt_id, i.source_partition AS source_partition, i.source_version AS source_version "
            f"FROM {self._t('context_receipt_items')} i JOIN {self._t('context_receipts')} r ON r.receipt_id = i.receipt_id "
            f"WHERE i.source_asset_key = :a AND r.created_at >= :since", a=asset_key, since=self._tsv(since or _EPOCH))
        ids = sorted({r["receipt_id"] for r in rows})
        return _fold_asset_usage(rows, self._events_for(ids, "'outcome', 'output_attached', 'evidence'"), ids)

    def outcomes_by_code_version(self, asset_key: str, since: str = "") -> dict[str, dict[str, Any]]:
        rows = self._query(
            f"SELECT DISTINCT i.receipt_id AS receipt_id, COALESCE(i.source_code_version, 'unknown') AS cv "
            f"FROM {self._t('context_receipt_items')} i JOIN {self._t('context_receipts')} r ON r.receipt_id = i.receipt_id "
            f"WHERE i.source_asset_key = :a AND r.created_at >= :since", a=asset_key, since=self._tsv(since or _EPOCH))
        return _fold_outcomes_by_code_version(rows, self._events_for(sorted({r["receipt_id"] for r in rows}), "'outcome'"))

    def derive_implicit_outcomes(self, *, window_seconds: float = 120.0, since: str | None = None) -> int:
        rows = self._query(
            f"SELECT receipt_id, created_at, caller_id, mcp_session_id, query_hash FROM {self._t('context_receipts')} "
            f"WHERE mcp_session_id IS NOT NULL AND query_hash IS NOT NULL AND created_at >= :since ORDER BY created_at, receipt_id",
            since=self._tsv(since or _EPOCH))
        existing = {json.loads(r["payload_json"]).get("outcome_ref") for r in self._query(
            f"SELECT payload_json FROM {self._t('context_receipt_events')} WHERE event_type = 'outcome'")}
        previous: dict[tuple[str, str | None], Any] = {}
        created = 0
        for row in rows:
            key = (row["mcp_session_id"], row["caller_id"])
            prior = previous.get(key)
            previous[key] = row
            if prior is None or prior["query_hash"] == row["query_hash"]:
                continue
            gap = (_as_datetime(row["created_at"]) - _as_datetime(prior["created_at"])).total_seconds()
            ref = f"implicit:reformulated:{row['receipt_id']}"
            if gap <= window_seconds and ref not in existing:
                self.record_outcome(prior["receipt_id"], signal="reformulated", source="implicit", outcome_ref=ref)
                created += 1
        return created

    def enforce_retention(self, retention_days: Mapping[str, float | None], *, now: datetime | None = None,
                          default_days: float | None = None) -> dict[str, int]:
        limits = {k: float(v) for k, v in retention_days.items() if v}
        if default_days is not None and default_days <= 0:
            raise ValueError("default_days must be positive or None")
        if not limits and not default_days:
            return {"receipts_deleted": 0}
        now = now or datetime.now(timezone.utc)
        cutoff = self._tsv(now - timedelta(days=min([*limits.values(), *([float(default_days)] if default_days else [])])))
        candidates = self._query(f"SELECT receipt_id, created_at FROM {self._t('context_receipts')} WHERE created_at < :c", c=cutoff)
        ids = [c["receipt_id"] for c in candidates]
        assets: dict[str, set] = {}
        for chunk in self._in_chunks(ids):
            for row in self._query(
                f"SELECT receipt_id, source_asset_key FROM {self._t('context_receipt_items')} WHERE receipt_id IN :ids AND source_asset_key IS NOT NULL "
                f"UNION SELECT receipt_id, source_asset_key FROM {self._t('context_receipt_excluded')} WHERE receipt_id IN :ids AND source_asset_key IS NOT NULL",
                ids=chunk):
                assets.setdefault(row["receipt_id"], set()).add(row["source_asset_key"])
        doomed = []
        for row in candidates:
            limit = _receipt_limit(assets.get(row["receipt_id"], ()), limits, default_days)
            if limit and _as_datetime(row["created_at"]) + timedelta(days=limit) <= now:
                doomed.append(row["receipt_id"])
        self.delete_receipts(doomed)
        return {"receipts_deleted": len(doomed)}

    def delete_receipts(self, receipt_ids: Iterable[str]) -> int:
        import sqlalchemy as sa

        ids = sorted({str(i) for i in receipt_ids})
        existed = 0
        for chunk in self._in_chunks(ids):
            existed += self._query(f"SELECT COUNT(1) AS n FROM {self._t('context_receipts')} WHERE receipt_id IN :ids", ids=chunk)[0]["n"]
        with self._engine.begin() as conn:
            for chunk in self._in_chunks(ids):
                for table in ("context_receipt_events", "context_receipt_excluded", "context_receipt_items", "context_receipts"):
                    conn.execute(sa.text(f"DELETE FROM {self._t(table)} WHERE receipt_id IN :ids").bindparams(
                        sa.bindparam("ids", expanding=True)), {"ids": chunk})
        return existed

    def delete_item_scores(self, item_ids: Iterable[str]) -> int:
        import sqlalchemy as sa

        ids = sorted({str(i) for i in item_ids})
        existed = 0
        for chunk in self._in_chunks(ids):
            existed += self._query(f"SELECT COUNT(1) AS n FROM {self._t('context_item_scores')} WHERE item_id IN :ids", ids=chunk)[0]["n"]
        with self._engine.begin() as conn:
            for chunk in self._in_chunks(ids):
                conn.execute(sa.text(f"DELETE FROM {self._t('context_item_scores')} WHERE item_id IN :ids").bindparams(
                    sa.bindparam("ids", expanding=True)), {"ids": chunk})
        return existed

    def _selftest_cursors(self) -> tuple[int, int]:
        micros = int((datetime.now(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)) / timedelta(microseconds=1)) - 5_000_000
        return micros, micros

    def find_by_assets(self, targets: Mapping[str, Iterable[str] | None]) -> dict[str, list[str]]:
        exact: set[str] = set()
        unknown: set[str] = set()
        for asset_key, partitions in targets.items():
            rows = self._query(f"SELECT DISTINCT receipt_id, source_partition FROM {self._t('context_receipt_items')} WHERE source_asset_key = :a", a=asset_key)
            wanted = None if partitions is None else set(partitions)
            for row in rows:
                if wanted is None or row["source_partition"] in wanted:
                    exact.add(row["receipt_id"])
                elif row["source_partition"] is None:
                    unknown.add(row["receipt_id"])
        return {"receipt_ids": sorted(exact), "partition_unknown": sorted(unknown - exact)}

    def find_by_item(self, item_id: str) -> list[str]:
        return [r["receipt_id"] for r in self._query(
            f"SELECT DISTINCT receipt_id FROM {self._t('context_receipt_items')} WHERE item_id = :i ORDER BY receipt_id", i=item_id)]

    def find_by_source_asset(self, asset_key: str) -> list[str]:
        return [r["receipt_id"] for r in self._query(
            f"SELECT DISTINCT receipt_id FROM {self._t('context_receipt_items')} WHERE source_asset_key = :a ORDER BY receipt_id", a=asset_key)]

    def receipts_for_partition(self, dagster_asset_key: str, partition_key: str | None) -> list[str]:
        where = "dagster_partition_key IS NULL" if partition_key is None else "dagster_partition_key = :p"
        params = {"a": dagster_asset_key} if partition_key is None else {"a": dagster_asset_key, "p": partition_key}
        return [r["receipt_id"] for r in self._query(
            f"SELECT receipt_id FROM {self._t('context_receipts')} WHERE dagster_asset_key = :a AND {where} ORDER BY receipt_id", **params)]


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


class _AsyncWrites:
    """Mixin: fire-and-forget writes for a store whose writes take long (a warehouse load job takes seconds, so an agent must
    not wait on it).

    Reads are synchronous and see only what the background writer has finished -- call ``flush()`` first when you need
    read-your-writes. Argument errors still raise at the call site (validated before queueing); storage errors that survive
    the writer's retries land in ``dead_letters``. In-process and not crash-safe (see ``AsyncReceiptWriter``)."""

    _sync_cls: Any = None

    def __init__(self, *args: Any, writer_max_retries: int = 5, writer_backoff_seconds: float = 0.5, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._writer = AsyncReceiptWriter(self._sync_cls(*args, **kwargs), max_retries=writer_max_retries,
                                          backoff_seconds=writer_backoff_seconds)

    @property
    def dead_letters(self) -> list:
        return self._writer.dead_letters

    def flush(self) -> None:
        self._writer.flush()

    def record_retrieval(self, **kwargs: Any) -> str:
        receipt_id, _items, _excluded, _count, caller_id, session_id = self._prepare_retrieval(
            kwargs.get("context_name"), kwargs.get("items", ()), kwargs.get("excluded", ()), kwargs.get("eligible_count"),
            kwargs.get("excluded_count"), kwargs.get("receipt_id"), kwargs.get("caller_id"), kwargs.get("mcp_session_id"))
        kwargs["items"], kwargs["excluded"] = list(kwargs.get("items", ())), list(kwargs.get("excluded", ()))
        kwargs["receipt_id"] = receipt_id
        if self.pseudonymizing:    # hash before queueing so raw ids never sit in the queue or dead_letters; the inner store has no salt
            kwargs["caller_id"], kwargs["mcp_session_id"] = caller_id, session_id
        return self._writer.record_retrieval(**kwargs)

    def attach_output(self, *args: Any, **kwargs: Any) -> None:
        self._writer.attach_output(*args, **kwargs)

    def add_evidence(self, *args: Any, **kwargs: Any) -> None:
        self._writer.add_evidence(*args, **kwargs)

    def record_outcome(self, *args: Any, **kwargs: Any) -> None:
        self._writer.record_outcome(*args, **kwargs)

    def ingest(self, event: Mapping[str, Any]) -> str:
        if self.pseudonymizing and event.get("type") == "retrieval":
            event = {**event, **{k: self._pseudonym(event[k]) for k in ("caller_id", "mcp_session_id") if event.get(k) is not None}}
        return self._writer.ingest(event)


class AsyncBigQueryContextReceiptStore(_AsyncWrites, BigQueryContextReceiptStore):
    """BigQuery receipts with fire-and-forget writes."""

    _sync_cls = BigQueryContextReceiptStore


class AsyncSqlContextReceiptStore(_AsyncWrites, SqlContextReceiptStore):
    """SQL-database receipts with fire-and-forget writes (worth it for Snowflake/Databricks; unnecessary for a local Postgres)."""

    _sync_cls = SqlContextReceiptStore


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

    Partition semantics: ``partitions`` and ``affected`` are *source* partitions, matched against each receipt item's
    ``source_partition`` (where a retrieved chunk came from). A receipt's own ``dagster_partition_key`` (the partition of the
    producing rag/agentic asset's run) is separate; look those up with ``store.receipts_for_partition(asset_key, partition)``.
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


@dataclass(frozen=True)
class HealthThresholds:
    max_stale_share: float = 0.1
    min_mean_outcome: float = -0.2
    min_verified_share: float = 0.5
    max_ungrounded_rate: float = 0.1
    min_checked_receipts: int = 5


def _asset_health(
    store: "ContextReceiptStore", instance: Any, asset_key: str, window_hours: float, t: HealthThresholds
) -> dict[str, Any]:
    """One evaluation of a context asset's health over a rolling window: the metrics, and for each of the
    four checks whether it passed, its severity, and its numbers. Shared by the asset checks and
    ``context_health`` so a check and an agent asking "is this context healthy?" can never disagree."""
    key = dg.AssetKey.from_user_string(asset_key)
    since = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
    usage = store.asset_usage_since(asset_key, since)

    judged = stale = 0
    current: dict[str | None, str | None] = {}
    for partition, version in usage["items"]:
        if partition not in current:
            current[partition] = _current_data_version(instance, key, partition)
        if current[partition] is None or version is None:
            continue
        judged += 1
        stale += version != current[partition]
    stale_share = stale / judged if judged else 0.0

    means = usage["outcome_means"]
    judged_outcomes = len(means) >= t.min_checked_receipts
    mean_outcome = sum(means) / len(means) if means else None

    outputs = usage["with_output"]
    verified_share = usage["verified_outputs"] / outputs if outputs else 1.0

    claims = usage["claims_total"]
    ungrounded_rate = usage["claims_ungrounded"] / claims if claims else 0.0

    record = instance.get_latest_data_version_record(key)
    tags = (getattr(getattr(record, "asset_materialization", None), "tags", None) or {}) if record else {}
    return {
        "receipts": usage["receipts"],
        "window_hours": window_hours,
        "current_data_version": tags.get("dagster/data_version"),
        "current_code_version": tags.get("dagster/code_version"),
        "checks": {
            "context_staleness": {
                "passed": stale_share <= t.max_stale_share, "severity": "ERROR",
                "metadata": {"stale_share": round(stale_share, 4), "stale_items": stale, "judged_items": judged,
                             "receipts": usage["receipts"], "window_hours": window_hours}},
            "context_outcome_quality": {
                "passed": (not judged_outcomes) or mean_outcome >= t.min_mean_outcome, "severity": "WARN",
                "metadata": ({"mean_outcome": round(mean_outcome, 4), "outcomes": len(means), "window_hours": window_hours}
                             if judged_outcomes else {"note": "insufficient outcomes to judge", "outcomes": len(means)})},
            "context_verified_share": {
                "passed": verified_share >= t.min_verified_share, "severity": "WARN",
                "metadata": {"verified_share": round(verified_share, 4), "outputs": outputs, "window_hours": window_hours}},
            "context_grounding": {
                "passed": ungrounded_rate <= t.max_ungrounded_rate, "severity": "WARN",
                "metadata": ({"ungrounded_rate": round(ungrounded_rate, 4), "claims": claims,
                              "ungrounded_claims": usage["claims_ungrounded"], "window_hours": window_hours}
                             if claims else {"note": "no claim-level verification reported", "claims": 0})},
        },
    }


def context_health(
    store: "ContextReceiptStore",
    instance: Any,
    asset_keys: Iterable[str],
    *,
    window_hours: float = 24.0,
    thresholds: HealthThresholds | None = None,
) -> dict[str, dict[str, Any]]:
    """Is this context healthy right now? ``{asset_key: {status, checks, current versions, receipts}}``.

    ``status`` is ``unhealthy`` when staleness fails (the ERROR check), ``degraded`` when only WARN checks
    fail, else ``healthy``. Same numbers the asset checks report, so a gateway can put it in results, or an agent
    runtime can degrade, without waiting for someone to look at a red check. In-process: pass the Dagster
    instance and the receipt store; serving it over a network API is a deployment choice outside this module."""
    t = thresholds or HealthThresholds()
    out: dict[str, dict[str, Any]] = {}
    for asset_key in asset_keys:
        health = _asset_health(store, instance, asset_key, window_hours, t)
        failed = {name: c for name, c in health["checks"].items() if not c["passed"]}
        health["status"] = (
            "unhealthy" if any(c["severity"] == "ERROR" for c in failed.values())
            else "degraded" if failed else "healthy")
        health["failing"] = sorted(failed)
        out[asset_key] = health
    return out


def build_usage_checks(
    store: ContextReceiptStore,
    asset_key: str,
    *,
    window_hours: float,
    max_stale_share: float,
    min_mean_outcome: float,
    min_verified_share: float,
    min_checked_receipts: int,
    max_ungrounded_rate: float = 0.1,
) -> list[Any]:
    """Asset checks over receipt data so context debt is a red/green signal on the asset.

    - ``context_staleness`` (ERROR): share of retrieved items served from an out-of-date data version in the
      window must be <= ``max_stale_share``. Items whose current version is unknown are not judged.
    - ``context_outcome_quality`` (WARN): mean outcome across receipts must be >= ``min_mean_outcome``;
      passes with a note below ``min_checked_receipts`` samples.
    - ``context_verified_share`` (WARN): share of outputs whose receipt reached verified or counterfactual
      evidence must be >= ``min_verified_share``; passes when there are no outputs.
    - ``context_grounding`` (WARN): share of claims a verifier reported as ungrounded (evidence ``details``
      ``claims_total`` / ``claims_ungrounded``) must be <= ``max_ungrounded_rate``; passes when no verifier
      has reported claims. The verifier that produces those counts is not part of this module.
    """
    key = dg.AssetKey.from_user_string(asset_key)
    thresholds = HealthThresholds(max_stale_share, min_mean_outcome, min_verified_share, max_ungrounded_rate, min_checked_receipts)

    def make(name: str, description: str):
        @dg.asset_check(asset=key, name=name, description=description)
        def check(context):
            verdict = _asset_health(store, context.instance, asset_key, window_hours, thresholds)["checks"][name]
            return dg.AssetCheckResult(
                passed=verdict["passed"], severity=getattr(dg.AssetCheckSeverity, verdict["severity"]),
                metadata=verdict["metadata"])
        return check

    return [
        make("context_staleness", "Share of context served from an out-of-date version."),
        make("context_outcome_quality", "Mean outcome of answers built from this asset."),
        make("context_verified_share", "Share of outputs whose context reached verified evidence."),
        make("context_grounding", "Share of claims a verifier found ungrounded."),
    ]


def build_maintenance_check(
    store: ContextReceiptStore,
    asset_key: str,
    *,
    window_days: float = 30.0,
    min_retrievals: int = 5,
    min_outcomes: int = 3,
    bad_below: float = -0.3,
    known_item_ids: Any = None,
    dead_after_receipts: int = 50,
) -> Any:
    """WARN-severity asset check ``context_maintenance``: counts of rewrite candidates / dead weight / noisy exclusions
    (``maintenance_report``) in metadata, with the worst few chunk ids listed. It is not "failed" by data quality alone: it is
    WARN, so it never fails a run. ``known_item_ids`` is an iterable or a zero-argument callable returning one; without it
    dead weight is not judged."""
    key = dg.AssetKey.from_user_string(asset_key)

    @dg.asset_check(asset=key, name="context_maintenance", description="Chunks worth rewriting, dead weight and noisy exclusions, from real usage.")
    def check(context):
        known = known_item_ids() if callable(known_item_ids) else known_item_ids
        report = store.maintenance_report(
            asset_key, since=(datetime.now(timezone.utc) - timedelta(days=window_days)).isoformat(), known_item_ids=known,
            min_retrievals=min_retrievals, min_outcomes=min_outcomes, bad_below=bad_below, dead_after_receipts=dead_after_receipts)
        summary = report["summary"]
        dead = report["dead_weight"]
        metadata: dict[str, Any] = {
            "rewrite_candidates": summary["rewrite_candidates"],
            "dead_weight": summary["dead_weight"] if dead is not None else dg.MetadataValue.text(report["dead_weight_reason"] or "not judged"),
            "top_performers": summary["top_performers"], "noisy_exclusions": summary["noisy_exclusions"],
            "receipts_in_window": summary["receipts_in_window"] if summary["receipts_in_window"] is not None else dg.MetadataValue.text("not counted"),
            "window_days": window_days,
            "worst_chunks": dg.MetadataValue.json([r["item_id"] for r in report["rewrite_candidates"][:10]]),
        }
        needs_work = bool(report["rewrite_candidates"] or dead or report["noisy_exclusions"])
        return dg.AssetCheckResult(passed=not needs_work, severity=dg.AssetCheckSeverity.WARN, metadata=metadata)

    return check


def build_selftest_asset(store: ContextReceiptStore, resource_key: str, *, prior_strength: float = 5.0) -> Any:
    """Asset ``<resource_key>_selftest``: runs ``store.selftest()``; one materialization verifies the configured backend.
    Fails (with the step table as metadata) when any step failed."""
    columns = [dg.TableColumn("step", "string"), dg.TableColumn("ok", "bool"), dg.TableColumn("detail", "string")]

    @dg.asset(name=f"{resource_key}_selftest", group_name="context_receipts",
              description="Verifies the configured receipt backend end to end with synthetic receipts it then deletes.")
    def selftest_asset(context: dg.AssetExecutionContext):
        results = store.selftest(prior_strength=prior_strength)
        table = dg.MetadataValue.table(records=[dg.TableRecord(r) for r in results], schema=dg.TableSchema(columns=columns))
        failed = [r for r in results if not r["ok"]]
        metadata = {"steps": table, "passed": len(results) - len(failed), "failed": len(failed), "backend": store.database_path}
        if failed:
            raise dg.Failure(description="receipt store self-test failed: " + "; ".join(f"{r['step']}: {r['detail']}" for r in failed),
                             metadata=metadata)
        return dg.MaterializeResult(metadata=metadata)

    return selftest_asset


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
    implicit_signals: bool = False,
    reformulation_window_seconds: float = 120.0,
    enforce_retention: bool = False,
    default_retention_days: float | None = None,
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
        if enforce_retention:
            policies = context_policies(context.repository_def.asset_graph)
            deleted = store.enforce_retention(
                {k: p.retention_days for k, p in policies.items()}, default_days=default_retention_days)["receipts_deleted"]
            if deleted:
                context.log.info(f"retention: deleted {deleted} receipt(s) past their policy retention")
        if implicit_signals:  # written before reading so they are scored in this same tick
            store.derive_implicit_outcomes(
                window_seconds=reformulation_window_seconds,
                since=(datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
            )
        data = store.read_since(state.get("r", 0), state.get("e", 0))
        if not data["receipts"] and not data["outcomes"]:
            return dg.SkipReason("no new receipts or outcomes")

        usage: dict[tuple[str, str | None], dict[str, Any]] = {}

        def bucket(key: str, partition: str | None) -> dict[str, Any]:
            return usage.setdefault(
                (key, partition),
                {"retrievals": 0, "receipts": set(), "explored": 0, "excluded": {}, "stale": 0,
                 "outcomes": 0, "outcome_sum": 0.0, "versions": {}, "by_code_version": {}},
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
            for key, part, code_version in outcome["sources"]:
                b = bucket(key, part)
                b["outcomes"] += 1
                b["outcome_sum"] += outcome["value"]
                cv = b["by_code_version"].setdefault(code_version or "unknown", [0, 0.0])
                cv[0] += 1
                cv[1] += outcome["value"]

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
                metadata["context_receipts/outcomes_by_code_version"] = {
                    cv: {"outcomes": n, "mean_outcome": round(total / n, 4)} for cv, (n, total) in b["by_code_version"].items()
                }
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
            "SQLite file path for local or single-host prototypes (backend: sqlite). Use backend: bigquery "
            "for receipts shared across hosts."
        ),
    )
    backend: str = Field(
        default="sqlite",
        description="Where receipts live: 'sqlite' (a local file), 'bigquery' (tables in a BigQuery dataset), or 'sql' (any SQLAlchemy database: Postgres, MySQL, Snowflake, Databricks, DuckDB). The warehouse backends are shared across hosts and queryable from SQL-mode pipelines.",
    )
    sql_url_env_var: Optional[str] = Field(
        default=None,
        description="backend: sql -- env var holding a SQLAlchemy URL (postgresql://..., mysql+pymysql://..., snowflake://..., databricks://..., duckdb:///file.duckdb). Needs that database's SQLAlchemy driver installed.",
    )
    sql_schema: str = Field(
        default="",
        description="backend: sql -- namespace holding the five receipt tables: 'database.schema' for Snowflake, 'catalog.schema' for Databricks, 'schema' for Postgres, 'database' for MySQL; empty for the connection default.",
    )
    sql_table_prefix: str = Field(default="", description="backend: sql -- prefix for the five receipt table names.")
    bigquery_dataset: Optional[str] = Field(
        default=None,
        description="backend: bigquery -- 'project.dataset' holding the receipt tables (created on first use if missing). The same dataset a SQL-mode context_engineering_pipeline `receipts: {dataset: ...}` reads.",
    )
    bigquery_table_prefix: str = Field(default="", description="backend: bigquery -- prefix for the five receipt table names (e.g. 'prod_'), to keep several environments in one dataset.")
    bigquery_location: Optional[str] = Field(default=None, description="backend: bigquery -- BigQuery location, e.g. 'US' or 'EU'.")
    gcp_credentials_env_var: Optional[str] = Field(
        default=None,
        description="backend: bigquery -- env var holding the service-account JSON (base64-encoded, as dagster-gcp expects). Omit to use Application Default Credentials.",
    )
    async_writes: bool = Field(
        default=True,
        description="backend: bigquery or sql -- queue writes on a background thread (a load job takes seconds, so an agent should not wait on it). Reads see only finished writes; call flush() for read-your-writes. Not crash-safe.",
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
    max_ungrounded_rate: float = Field(default=0.1, description="Warn context_grounding above this share of ungrounded claims (reported by a verifier in evidence details).")
    enforce_retention: bool = Field(
        default=False,
        description=(
            "Have the usage sensor delete receipts past the retention_days declared in the policies of the assets "
            "they used (the shortest applicable retention wins). Reads policies from every context asset in the "
            "code location. This is the only deletion the receipt store performs."
        ),
    )
    default_retention_days: Optional[float] = Field(
        default=None,
        description=(
            "Retention in days for receipts of assets whose context policy declares none (and receipts that touched no asset). "
            "An asset's own retention_days always wins. Applied by the usage sensor only when enforce_retention is true. "
            "None keeps such receipts forever; nothing is deleted by default."
        ),
    )
    pseudonymize_callers: bool = Field(
        default=False,
        description=(
            "Store caller_id and mcp_session_id as a keyed hash (h: + HMAC-SHA256, 32 hex chars) instead of the raw id, in every "
            "backend. Deterministic, so a caller/session still groups. Needs pseudonym_salt_env_var. Existing rows are not rewritten."
        ),
    )
    pseudonym_salt_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding the secret salt for pseudonymize_callers (required when it is true). Changing it breaks continuity with earlier receipts.",
    )
    selftest_asset: bool = Field(
        default=False,
        description="Add an asset '<resource_key>_selftest' that exercises the configured backend with synthetic receipts (then deletes them) and fails if any step fails.",
    )
    maintenance_check_asset_keys: list[str] = Field(
        default_factory=list,
        description="Asset keys to attach a WARN-severity 'context_maintenance' check to: counts of chunks worth rewriting, dead weight and noisy exclusions. Never fails a run.",
    )
    maintenance_window_days: float = Field(default=30.0, description="Lookback window for the context_maintenance check.")
    maintenance_min_retrievals: int = Field(default=5, description="A chunk needs this many retrievals before the maintenance check judges it.")
    maintenance_min_outcomes: int = Field(default=3, description="A chunk needs this many rated retrievals before the maintenance check judges it.")
    maintenance_bad_below: float = Field(default=-0.3, description="Chunks whose mean outcome is at or below this are rewrite candidates.")
    implicit_signals_enabled: bool = Field(
        default=False,
        description=(
            "Derive weak implicit outcomes from session behaviour: a caller re-asking within "
            "reformulation_window_seconds with a different query marks the earlier receipt 'reformulated' (-0.2). "
            "A heuristic that needs mcp_session_id and query_hash on receipts; off by default."
        ),
    )
    reformulation_window_seconds: float = Field(default=120.0, description="Max gap between two retrievals to count as a reformulation.")
    score_prior_strength: float = Field(
        default=5.0,
        description="Shrinkage for item quality priors: higher needs more outcome evidence to move a prior.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.backend == "sqlite":
            store: ContextReceiptStore = ContextReceiptStore(os.path.expanduser(self.database_path))
        elif self.backend == "bigquery":
            if not self.bigquery_dataset:
                raise ValueError("backend: bigquery requires bigquery_dataset ('project.dataset')")
            project = self.bigquery_dataset.strip("`").split(".")[0]
            location, env_var = self.bigquery_location, self.gcp_credentials_env_var

            def client_factory():
                try:
                    from dagster_gcp import BigQueryResource  # dagster-gcp is only needed for this backend
                except ImportError as exc:
                    raise ImportError("backend: bigquery needs dagster-gcp (it brings google-cloud-bigquery): pip install dagster-gcp") from exc

                creds = dg.EnvVar(env_var).get_value() if env_var else None
                return BigQueryResource(project=project, location=location, gcp_credentials=creds).get_client()

            cls = AsyncBigQueryContextReceiptStore if self.async_writes else BigQueryContextReceiptStore
            store = cls(self.bigquery_dataset, client_factory=client_factory, table_prefix=self.bigquery_table_prefix)
        elif self.backend == "sql":
            if not self.sql_url_env_var:
                raise ValueError("backend: sql requires sql_url_env_var (the env var holding the SQLAlchemy URL)")
            url_env = self.sql_url_env_var

            def engine_factory():
                return _create_sql_engine(dg.EnvVar(url_env).get_value())

            cls = AsyncSqlContextReceiptStore if self.async_writes else SqlContextReceiptStore
            store = cls(engine_factory, schema=self.sql_schema, table_prefix=self.sql_table_prefix)
        else:
            raise ValueError(f"backend must be 'sqlite', 'bigquery' or 'sql', got {self.backend!r}")
        if self.pseudonymize_callers:
            if not self.pseudonym_salt_env_var:
                raise ValueError("pseudonymize_callers requires pseudonym_salt_env_var (the env var holding the secret salt)")
            salt_env = self.pseudonym_salt_env_var

            def salt() -> str:
                value = dg.EnvVar(salt_env).get_value()
                if not value:
                    raise ValueError(f"pseudonymize_callers is on but env var {salt_env} is not set")
                return value

            store.enable_pseudonymization(salt)
        if self.default_retention_days is not None:
            if self.default_retention_days <= 0:
                raise ValueError("default_retention_days must be positive")
            if not (self.enforce_retention and self.usage_sensor_enabled):
                logger.warning("default_retention_days is set but nothing will be deleted: it needs enforce_retention: true and usage_sensor_enabled: true")
        retention_active = self.enforce_retention and self.usage_sensor_enabled
        if self.backend in ("bigquery", "sql") and not retention_active and not self.pseudonymize_callers:
            logger.warning(
                "context_receipt_store (%s): receipts in a shared warehouse contain caller ids and session ids, and neither "
                "retention (enforce_retention with usage_sensor_enabled) nor pseudonymize_callers is configured; consider "
                "pseudonymize_callers: true and/or default_retention_days.", self.backend)
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
                    implicit_signals=self.implicit_signals_enabled,
                    enforce_retention=self.enforce_retention,
                    default_retention_days=self.default_retention_days,
                    reformulation_window_seconds=self.reformulation_window_seconds,
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
                max_ungrounded_rate=self.max_ungrounded_rate,
            )
        ]
        checks += [
            build_maintenance_check(
                store, key, window_days=self.maintenance_window_days, min_retrievals=self.maintenance_min_retrievals,
                min_outcomes=self.maintenance_min_outcomes, bad_below=self.maintenance_bad_below)
            for key in self.maintenance_check_asset_keys
        ]
        return dg.Definitions(
            resources={self.resource_key: dg.ResourceDefinition.hardcoded_resource(store)},
            assets=[build_selftest_asset(store, self.resource_key, prior_strength=self.score_prior_strength)] if self.selftest_asset else [],
            sensors=sensors,
            asset_checks=checks,
        )
