"""ContextEngineeringPipelineComponent — one YAML builds a governed,
searchable, citeable knowledge base out of raw unstructured text.

Same "pipeline component" shape as `ml_pipeline`, `rag_pipeline`,
`warehouse_pipeline`, `polars_pipeline`: **one YAML file declares the
whole pipeline**, a `steps:` list defines the chain in reading order,
each step's output is a DataFrame the next step can read via `source:`
(or, if omitted, the immediately-prior step — you don't have to repeat
`source:` on every line).

This is the "build the knowledge base" half of what the industry calls
"context engineering" (dbt Labs' `dbt_context_engineering` package is
the reference example: chunk → embed → classify → semantic search,
governed the same way metrics are). It's designed to pair with the
existing `rag_pipeline` component for the "query the knowledge base"
half — this pipeline's `write_vector_store` step's output is exactly
what `rag_pipeline`'s `retrieve`/`hybrid_search` ops expect to query
against.

Example (support tickets → a searchable, classified knowledge base):

    type: dagster_component_templates.ContextEngineeringPipelineComponent
    attributes:
      asset_name: support_kb
      upstream_asset_key: raw_support_tickets
      id_column: ticket_id
      text_column: body
      metadata_columns: [customer_id, created_at]
      steps:
        - {id: chunks, op: chunk, chunk_size: 500, chunk_overlap: 50}
        - {id: classified, op: classify, candidate_labels: [billing, technical, shipping, other]}
        - {id: embedded, op: embed, provider: sentence_transformers, model: all-MiniLM-L6-v2}
        - {id: indexed, op: write_vector_store, provider: chromadb,
           connection_string: /tmp/context_kb, collection_name: support_tickets}

Standardization — that's the point, same as `ml_pipeline`. Every
context-engineering pipeline in the org uses the same YAML shape, the
same ops, the same output conventions.

Op coverage:

- chunk:             fixed-size chunking with sentence-boundary snapping
                      and configurable overlap; threads `id_column` (and
                      any `metadata_columns`) through to every chunk row
                      as a citation back to the source row.
- classify:           HuggingFace zero-shot classification (local, free,
                      no API key) tags each chunk with a category BEFORE
                      embedding -- this is dbt's "relevance vs. similarity"
                      trick: raw cosine similarity ranks short boilerplate
                      over long relevant analysis unless chunks are
                      pre-filtered by category first.
- embed:              sentence-transformers (local, free), OpenAI, or
                      Cohere.
- write_vector_store: ChromaDB (local, free), Pinecone, or Qdrant.

Every op is free/local by default (sentence-transformers + HuggingFace
zero-shot + ChromaDB) -- no API key required to run this end to end,
matching dbt's own stated philosophy that this kind of governance
tooling should be cheap to validate, not another paid call per test.
"""
import ast
import hashlib
import json
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    DataVersion,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine()
    (SQLAlchemy) OR .get_connection() (DB-API) -- works out of the box
    with duckdb_resource, postgres_resource, mysql_resource,
    snowflake_resource, bigquery_resource, and any custom resource
    implementing the same duck-typed interface. Falls back to a bare
    SQLAlchemy engine via `database_url_env_var` when no Dagster resource
    is registered -- same dual pattern already used by this repo's
    reverse_etl components (e.g. greenhouse_candidate_update). This is
    what makes the pipeline "broadly usable against DuckDB and other
    databases" rather than requiring the raw text to already be a
    materialized Dagster asset."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            # get_connection() is a @contextmanager (confirmed live against
            # dagster_duckdb.DuckDBResource) -- calling it without `with` hands
            # back a _GeneratorContextManager, not a connection, and pd.read_sql
            # fails with AttributeError. Must be entered via `with`.
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            # "get_client" means something different per vendor -- there's no
            # universal calling convention, so dispatch on the CLIENT's own
            # shape rather than assume one. Verified against the real APIs,
            # not guessed:
            client = resource.get_client()
            if hasattr(client, "query"):
                # BigQuery (google.cloud.bigquery.Client): .query(sql) returns
                # a QueryJob; .to_dataframe() blocks until done and returns a
                # pandas DataFrame directly -- no .result() call needed first.
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                # Redshift Data API (dagster_aws RedshiftClient):
                # execute_query(sql, fetch_results=True) returns bare
                # List[Tuple] with NO column names attached -- a
                # RealDictCursor factory is required to get dict rows a
                # DataFrame can use with correct column names.
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        import os
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


def _build_partitions_def(
    partition_type, partition_start, partition_values, dynamic_partition_name,
):
    """Construct a Dagster partitions_def from the canonical partition fields.
    Canonical implementation — copied as-is per FIELD_CONVENTIONS.md."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition,
    )
    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# ── chunk op ────────────────────────────────────────────────────────────

def _chunk_fixed(text: str, chunk_size: int, chunk_overlap: int, preserve_sentences: bool) -> List[str]:
    """Fixed-size chunking with overlap, optionally snapping to a nearby
    sentence boundary -- same approach as document_chunker's chunk_fixed
    (duplicated, not imported, per this repo's standalone-component
    convention)."""
    chunks: List[str] = []
    start = 0
    text_len = len(text)
    while start < text_len:
        end = start + chunk_size
        if preserve_sentences and end < text_len:
            window = text[max(0, end - 100): min(text_len, end + 100)]
            sentence_ends = [m.end() for m in re.finditer(r"[.!?]\s", window)]
            if sentence_ends:
                closest = min(sentence_ends, key=lambda p: abs((max(0, end - 100) + p) - end))
                end = max(0, end - 100) + closest
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end >= text_len:
            break
        start = max(end - chunk_overlap, start + 1)
    return chunks


_RECURSIVE_SEPARATORS = ["\n\n", "\n", ". ", " "]
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*?)\s*$", re.MULTILINE)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _to_chars(value: float, unit: str, chars_per_token: float) -> int:
    """chunk_size/overlap are in characters, or in estimated tokens (chars_per_token, default 4)."""
    if unit not in ("chars", "tokens"):
        raise ValueError(f"chunk op: length_unit must be 'chars' or 'tokens', got {unit!r}")
    return int(value * chars_per_token) if unit == "tokens" else int(value)


def _chunk_recursive(text: str, size: int, overlap: int, separators: Optional[List[str]] = None) -> List[str]:
    """Split on the coarsest separator that works (paragraph, line, sentence, word), pack pieces up
    to ``size`` characters, and carry trailing whole pieces (<= ``overlap`` chars) into the next chunk."""
    separators = _RECURSIVE_SEPARATORS if separators is None else separators
    text = text.strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]
    sep = next((sp for sp in separators if sp in text), None)
    if sep is None:
        return _chunk_fixed(text, size, overlap, False)
    rest = separators[separators.index(sep) + 1:]
    atoms: List[str] = []
    for part in text.split(sep):
        if not part.strip():
            continue
        atoms.extend([part] if len(part) <= size else _chunk_recursive(part, size, overlap, rest))
    chunks: List[str] = []
    current: List[str] = []
    length = 0
    for atom in atoms:
        added = len(atom) + (len(sep) if current else 0)
        if current and length + added > size:
            chunks.append(sep.join(current).strip())
            tail: List[str] = []
            tail_len = 0
            for prev in reversed(current):
                if tail_len + len(prev) + len(sep) > overlap:
                    break
                tail.insert(0, prev)
                tail_len += len(prev) + len(sep)
            current, length = tail, sum(len(a) for a in tail) + len(sep) * max(len(tail) - 1, 0)
            added = len(atom) + (len(sep) if current else 0)
        current.append(atom)
        length += added
    if current:
        chunks.append(sep.join(current).strip())
    return [c for c in chunks if c]


def _chunk_markdown(text: str, size: int, overlap: int) -> List[Tuple[str, str]]:
    """Section-aware chunking: split at markdown headings, chunk each section recursively, and
    return (chunk, "H1 > H2 > H3" breadcrumb) so every chunk can cite where in the document it lives."""
    sections: List[Tuple[str, str]] = []
    stack: List[Tuple[int, str]] = []
    cursor = 0
    path = ""
    for match in _HEADING_RE.finditer(text):
        sections.append((path, text[cursor:match.start()]))
        level, title = len(match.group(1)), match.group(2)
        while stack and stack[-1][0] >= level:
            stack.pop()
        stack.append((level, title))
        path = " > ".join(t for _, t in stack)
        cursor = match.end()
    sections.append((path, text[cursor:]))
    out: List[Tuple[str, str]] = []
    for section_path, body in sections:
        for chunk in _chunk_recursive(body, size, overlap):
            out.append((chunk, section_path))
    return out


_CODE_BOUNDARIES: Dict[str, "re.Pattern"] = {
    "javascript": re.compile(r"^(?:export\s+)?(?:default\s+)?(?:async\s+)?(?:function\b|class\b|interface\b|type\s+\w+\s*=|const\s+\w+\s*=\s*(?:async\s*)?(?:\(|function))", re.MULTILINE),
    "go": re.compile(r"^(?:func|type)\b", re.MULTILINE),
    "rust": re.compile(r"^(?:pub(?:\([^)]*\))?\s+)?(?:fn|struct|enum|impl|trait|mod)\b", re.MULTILINE),
    "java": re.compile(r"^ {0,4}(?:@\w+[^\n]*\n\s*)*(?:public|private|protected|internal|static|abstract|final|class|interface|enum)\b", re.MULTILINE),
    "ruby": re.compile(r"^(?:def|class|module)\b", re.MULTILINE),
    "generic": re.compile(r"^(?:def|class|function|func|fn|sub)\b", re.MULTILINE),
}
_CODE_LANGUAGE_ALIASES = {
    "py": "python", "python": "python",
    "js": "javascript", "jsx": "javascript", "ts": "javascript", "tsx": "javascript", "javascript": "javascript", "typescript": "javascript",
    "go": "go", "golang": "go", "rs": "rust", "rust": "rust",
    "java": "java", "kt": "java", "kotlin": "java", "cs": "java", "csharp": "java", "scala": "java",
    "rb": "ruby", "ruby": "ruby",
}


def _normalize_language(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip().lower().lstrip(".")
    if "." in text or "/" in text:  # a path: use its extension
        text = text.rsplit(".", 1)[-1]
    return _CODE_LANGUAGE_ALIASES.get(text)


def _pack_units(units: List[Tuple[str, str]], size: int, overlap: int) -> List[Tuple[str, str]]:
    """Merge adjacent small code units up to ``size`` chars; split any single oversized unit recursively."""
    out: List[Tuple[str, str]] = []
    buf_label, buf_text, extra = "", "", 0

    def flush():
        nonlocal buf_label, buf_text, extra
        if buf_text.strip():
            out.append((buf_label + (f" +{extra}" if extra else ""), buf_text.strip("\n")))
        buf_label, buf_text, extra = "", "", 0

    for label, text in units:
        if len(text) > size:
            flush()
            out.extend((label, piece) for piece in _chunk_recursive(text, size, overlap))
            continue
        if buf_text and len(buf_text) + 1 + len(text) > size:
            flush()
        if buf_text:
            buf_text += "\n" + text
            extra += 1
        else:
            buf_label, buf_text = label, text
    flush()
    return out


def _code_units_python(text: str) -> Optional[List[Tuple[str, str]]]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return None
    lines = text.splitlines()
    units: List[Tuple[str, str]] = []
    cursor = 0  # next unconsumed 0-based line
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        start = min([node.lineno] + [d.lineno for d in node.decorator_list]) - 1
        if start > cursor and "\n".join(lines[cursor:start]).strip():
            units.append(("module", "\n".join(lines[cursor:start])))
        units.append((node.name, "\n".join(lines[start:node.end_lineno])))
        cursor = node.end_lineno
    if cursor < len(lines) and "\n".join(lines[cursor:]).strip():
        units.append(("module", "\n".join(lines[cursor:])))
    return units


def _chunk_code(text: str, size: int, overlap: int, language: Optional[str]) -> List[Tuple[str, str]]:
    """Code-aware chunking: one unit per top-level function/class (Python via ``ast``, so decorators and
    exact extents are right; other languages by a boundary-keyword heuristic, not a parser), small adjacent
    units packed together, oversized units split recursively. Returns (chunk, unit label) so chunks cite the
    symbol they came from. Unparseable Python and unknown languages fall back to the generic heuristic."""
    units = _code_units_python(text) if language == "python" else None
    if units is None:
        pattern = _CODE_BOUNDARIES.get(language or "", _CODE_BOUNDARIES["generic"])
        starts = [m.start() for m in pattern.finditer(text)]
        if not starts:
            return [(chunk, "code") for chunk in _chunk_recursive(text, size, overlap)]
        bounds = ([0] if starts[0] > 0 else []) + starts + [len(text)]
        units = []
        for a, b in zip(bounds, bounds[1:]):
            body = text[a:b].rstrip()
            if body.strip():
                first = body.strip().splitlines()[0].strip()
                units.append(("header" if a == 0 and starts[0] > 0 else first[:60], body))
    return [(chunk, label) for label, chunk in _pack_units(units, size, overlap)]


class _SafeFormat(dict):
    def __missing__(self, key):
        return ""


def _do_chunk(df: pd.DataFrame, step: dict, id_column: str, text_column: str, metadata_columns: List[str], context) -> pd.DataFrame:
    """Knobs: strategy (fixed|recursive|markdown|code: per-function/class chunks for source files, see
    language / language_column / path_column), chunk_id_prefix (REQUIRED when several assets share one
    vector collection and their source ids can overlap), chunk_size/chunk_overlap with length_unit
    (chars|tokens, tokens estimated via chars_per_token), preserve_sentences (fixed only),
    min_chunk_chars, max_chunks_per_source, chunk_id_mode (positional|content), header_template
    (prepended to each chunk's text, may use any carried column and {section_path})."""
    strategy = step.get("strategy", "fixed")
    if strategy not in ("fixed", "recursive", "markdown", "code"):
        raise ValueError(f"chunk op: strategy must be fixed, recursive, markdown or code, got {strategy!r}")
    unit = step.get("length_unit", "chars")
    cpt = float(step.get("chars_per_token", 4.0))
    chunk_size = max(1, _to_chars(step.get("chunk_size", 500), unit, cpt))
    chunk_overlap = _to_chars(step.get("chunk_overlap", 50), unit, cpt)
    if chunk_overlap >= chunk_size:
        raise ValueError("chunk op: chunk_overlap must be smaller than chunk_size")
    preserve_sentences = step.get("preserve_sentences", True)
    out_text_col = step.get("output_column", "chunk_text")
    min_chars = int(step.get("min_chunk_chars", 0))
    max_chunks = step.get("max_chunks_per_source")
    id_mode = step.get("chunk_id_mode", "positional")
    if id_mode not in ("positional", "content"):
        raise ValueError(f"chunk op: chunk_id_mode must be 'positional' or 'content', got {id_mode!r}")
    header_template = step.get("header_template")
    id_prefix = str(step.get("chunk_id_prefix") or "")
    code_language = _normalize_language(step.get("language"))
    language_column, path_column = step.get("language_column"), step.get("path_column")

    rows: List[Dict[str, Any]] = []
    skipped = dropped_small = capped = 0
    for _, row in df.iterrows():
        source_id = row[id_column]
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        if not text.strip():
            skipped += 1
            continue
        if strategy == "fixed":
            pieces = [(c, "") for c in _chunk_fixed(text, chunk_size, chunk_overlap, preserve_sentences)]
        elif strategy == "recursive":
            pieces = [(c, "") for c in _chunk_recursive(text, chunk_size, chunk_overlap)]
        elif strategy == "markdown":
            pieces = _chunk_markdown(text, chunk_size, chunk_overlap)
        else:
            lang = code_language
            if lang is None and language_column and language_column in df.columns:
                lang = _normalize_language(row[language_column])
            if lang is None and path_column and path_column in df.columns:
                lang = _normalize_language(row[path_column])
            pieces = _chunk_code(text, chunk_size, chunk_overlap, lang)
        kept = [(c, sp) for c, sp in pieces if len(c) >= min_chars]
        dropped_small += len(pieces) - len(kept)
        if max_chunks is not None and len(kept) > int(max_chunks):
            capped += len(kept) - int(max_chunks)
            kept = kept[: int(max_chunks)]
        seen_ids: set = set()
        for i, (body, section_path) in enumerate(kept):
            record: Dict[str, Any] = {id_column: source_id}
            for col in metadata_columns:
                if col in df.columns:
                    record[col] = row[col]
            chunk_text = body
            if header_template:
                fields = _SafeFormat({**{k: ("" if pd.isna(v) else v) for k, v in row.items() if k != text_column}, "section_path": section_path})
                chunk_text = header_template.format_map(fields) + body
            digest = _sha256(chunk_text)
            chunk_id = f"{id_prefix}{source_id}_{digest[:12]}" if id_mode == "content" else f"{id_prefix}{source_id}_{i}"
            if chunk_id in seen_ids:  # identical text twice in one source (content ids): keep both
                chunk_id = f"{chunk_id}-{i}"
            seen_ids.add(chunk_id)
            record.update({
                "chunk_id": chunk_id,
                "chunk_index": i,
                "total_chunks": len(kept),
                out_text_col: chunk_text,
                "content_hash": digest,
            })
            if strategy in ("markdown", "code"):
                record["section_path"] = section_path
            rows.append(record)
    if skipped:
        context.log.warning(f"chunk: skipped {skipped} row(s) with empty {text_column!r}")
    if dropped_small:
        context.log.info(f"chunk: dropped {dropped_small} chunk(s) shorter than min_chunk_chars={min_chars}")
    if capped:
        context.log.warning(f"chunk: max_chunks_per_source={max_chunks} dropped {capped} chunk(s)")
    out = pd.DataFrame(rows)
    out.attrs["stats"] = {"dropped_small": dropped_small, "capped": capped}
    return out


# ── filter / dedupe / sensitivity / stamp ops (python mode) ─────────────

def _text_column(df: pd.DataFrame, step: dict) -> str:
    col = step.get("text_column", "chunk_text")
    if col not in df.columns:
        raise ValueError(f"{step.get('op', 'op')} op: text_column={col!r} not in upstream columns: {list(df.columns)}")
    return col


def _rehash(df: pd.DataFrame, text_col: str) -> pd.DataFrame:
    if "content_hash" in df.columns:
        df = df.copy()
        df["content_hash"] = df[text_col].astype(str).map(_sha256)
    return df


def _do_filter(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Drop low-quality chunks. Knobs: min_chars, max_chars, min_alnum_ratio (drop mostly-symbol
    chunks), exclude_patterns (regexes, e.g. boilerplate/footers/signatures)."""
    col = _text_column(df, step)
    texts = df[col].astype(str)
    keep = pd.Series(True, index=df.index)
    reasons: Dict[str, int] = {}

    def apply(name: str, mask: pd.Series) -> None:
        nonlocal keep
        newly = keep & ~mask
        if newly.any():
            reasons[name] = int(newly.sum())
        keep = keep & mask

    if step.get("min_chars") is not None:
        apply("min_chars", texts.str.len() >= int(step["min_chars"]))
    if step.get("max_chars") is not None:
        apply("max_chars", texts.str.len() <= int(step["max_chars"]))
    if step.get("min_alnum_ratio") is not None:
        ratio = texts.map(lambda t: (sum(ch.isalnum() for ch in t) / len(t)) if t else 0.0)
        apply("min_alnum_ratio", ratio >= float(step["min_alnum_ratio"]))
    for pattern in step.get("exclude_patterns") or []:
        compiled = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
        apply(f"exclude:{pattern}", ~texts.map(lambda t, c=compiled: bool(c.search(t))))
    out = df[keep].reset_index(drop=True)
    out.attrs["stats"] = {"dropped": int((~keep).sum()), "reasons": reasons}
    context.log.info(f"filter: dropped {int((~keep).sum())} of {len(df)} chunks {reasons}")
    return out


def _normalize_for_dedupe(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", text.lower())).strip()


def _do_dedupe(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Drop duplicate chunks (first wins). mode: exact (byte-identical) or normalized (case,
    punctuation and whitespace-insensitive). within: optional columns -- only dedupe inside the
    same value of these (e.g. per customer) so one tenant's chunk never suppresses another's."""
    col = _text_column(df, step)
    mode = step.get("mode", "normalized")
    if mode not in ("exact", "normalized"):
        raise ValueError(f"dedupe op: mode must be 'exact' or 'normalized', got {mode!r}")
    key = df[col].astype(str) if mode == "exact" else df[col].astype(str).map(_normalize_for_dedupe)
    key = key.map(_sha256)
    subset = pd.DataFrame({"_k": key})
    for extra in step.get("within") or []:
        if extra not in df.columns:
            raise ValueError(f"dedupe op: within column {extra!r} not in upstream columns: {list(df.columns)}")
        subset[extra] = df[extra].astype(str)
    dup = subset.duplicated(keep="first")
    out = df[~dup.values].reset_index(drop=True)
    out.attrs["stats"] = {"duplicates_dropped": int(dup.sum())}
    context.log.info(f"dedupe ({mode}): dropped {int(dup.sum())} duplicate chunk(s)")
    return out


def _luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 1:
            d = d * 2 - 9 if d * 2 > 9 else d * 2
        total += d
    return total % 10 == 0


_PII_DETECTORS: Dict[str, "re.Pattern"] = {
    "email": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "phone": re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}(?!\d)"),
    "credit_card": re.compile(r"\b(?:\d[ -]?){13,19}\b"),
    "ipv4": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
}


def _find_pii(text: str, detectors: List[str]) -> List[Tuple[str, int, int]]:
    hits: List[Tuple[str, int, int]] = []
    for name in detectors:
        for m in _PII_DETECTORS[name].finditer(text):
            if name == "credit_card":
                digits = re.sub(r"\D", "", m.group(0))
                if not (13 <= len(digits) <= 19 and _luhn_ok(digits)):
                    continue
            hits.append((name, m.start(), m.end()))
    return hits


def _do_sensitivity(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Detect PII with regexes (email, phone, ssn, credit_card [Luhn-checked], ipv4). action:
    tag (default) adds `sensitivity` ('pii'|'none') and `pii_types`; redact also replaces matches with
    [EMAIL]/[PHONE]/...; drop removes affected chunks. The `sensitivity` column is what a gateway
    policy keys on to withhold chunks (receipt exclusion reason 'pii'). Regex detection, not NER:
    it misses names and free-form identifiers, so treat it as a floor, not a guarantee."""
    col = _text_column(df, step)
    detectors = step.get("detectors") or list(_PII_DETECTORS)
    unknown = [d for d in detectors if d not in _PII_DETECTORS]
    if unknown:
        raise ValueError(f"sensitivity op: unknown detector(s) {unknown}; valid: {sorted(_PII_DETECTORS)}")
    action = step.get("action", "tag")
    if action not in ("tag", "redact", "drop"):
        raise ValueError(f"sensitivity op: action must be tag, redact or drop, got {action!r}")
    out_col = step.get("output_column", "sensitivity")
    types_col = step.get("types_column", "pii_types")
    out = df.copy()
    flags, types, texts = [], [], []
    for text in out[col].astype(str):
        hits = _find_pii(text, detectors)
        flags.append("pii" if hits else "none")
        types.append(",".join(sorted({h[0] for h in hits})))
        if action == "redact" and hits:
            for name, a, b in sorted(hits, key=lambda h: -h[1]):
                text = text[:a] + f"[{name.upper()}]" + text[b:]
        texts.append(text)
    out[out_col] = flags
    out[types_col] = types
    if action == "redact":
        out[col] = texts
        out = _rehash(out, col)
    if action == "drop":
        out = out[out[out_col] == "none"].reset_index(drop=True)
    flagged = sum(f == "pii" for f in flags)
    out.attrs["stats"] = {"pii_chunks": flagged}
    context.log.info(f"sensitivity ({action}): {flagged} of {len(df)} chunks contain PII")
    return out


def _do_stamp(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Add governance columns that travel to the vector store as filterable metadata, so a gateway can
    enforce policy at query time. columns: {name: literal-or-"{column}" template} (e.g. scope:
    "{customer_id}", access_tags: "support"). ttl_days adds valid_until_column (default valid_until,
    ISO date) so a gateway can withhold chunks past their date (receipt reason 'stale')."""
    out = df.copy()
    for name, template in (step.get("columns") or {}).items():
        out[name] = [str(template).format_map(_SafeFormat({k: v for k, v in row.items()})) for row in out.to_dict("records")]
    if step.get("ttl_days") is not None:
        from datetime import datetime, timedelta, timezone
        out[step.get("valid_until_column", "valid_until")] = (datetime.now(timezone.utc) + timedelta(days=float(step["ttl_days"]))).date().isoformat()
    return out


# ── classify op ───────────────────────────────────────────────────────

def _do_classify(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Tag each chunk with a category before embedding/search -- two modes:

    - mode='zero_shot' (default): HuggingFace zero-shot classification --
      local, free, no API key. Same pattern as zero_shot_classifier
      (duplicated, not imported).
    - mode='llm': any litellm-supported model judges the category via a
      real completion call -- costs money per chunk, but can apply real
      judgment a fixed zero-shot label set can't (nuanced/ambiguous
      categories, multi-factor rules described in the prompt).
    """
    candidate_labels = step.get("candidate_labels")
    if not candidate_labels:
        raise ValueError("classify op requires `candidate_labels` (a list of category names).")
    text_column = step.get("text_column", "chunk_text")
    output_column = step.get("output_column", "category")
    batch_size = step.get("batch_size", 16)
    mode = step.get("mode", "zero_shot")

    if text_column not in df.columns:
        raise ValueError(f"classify op: text_column={text_column!r} not in upstream columns: {list(df.columns)}")

    out = df.copy()
    texts = out[text_column].fillna("").astype(str).tolist()

    if mode == "zero_shot":
        try:
            from transformers import pipeline
        except ImportError:
            raise ImportError("classify op mode='zero_shot' requires transformers: pip install transformers torch")
        model_name = step.get("model", "facebook/bart-large-mnli")
        multi_label = step.get("multi_label", False)
        output_scores = step.get("output_scores", False)

        context.log.info(f"classify: loading zero-shot classifier {model_name!r} for {len(df)} chunks")
        classifier = pipeline("zero-shot-classification", model=model_name)
        results: List[dict] = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_results = classifier(batch, candidate_labels=candidate_labels, multi_label=multi_label)
            if isinstance(batch_results, dict):
                batch_results = [batch_results]
            results.extend(batch_results)

        out[output_column] = [r["labels"][0] for r in results]
        if output_scores:
            for label in candidate_labels:
                out[f"score_{label}"] = [dict(zip(r["labels"], r["scores"])).get(label, 0.0) for r in results]

    elif mode == "llm":
        import os
        import json
        try:
            from litellm import completion
        except ImportError:
            raise ImportError("classify op mode='llm' requires litellm: pip install litellm")
        model_name = step.get("model", "gpt-4o-mini")
        api_key_env_var = step.get("api_key_env_var", "OPENAI_API_KEY")
        llm_max_retries = step.get("llm_max_retries", 2)
        prompt_prefix = step.get("prompt_prefix")

        categories: List[Optional[str]] = []
        for text in texts:
            prompt_parts = []
            if prompt_prefix:
                prompt_parts.append(prompt_prefix)
            prompt_parts.append(
                f"Classify the following text into exactly one of these categories: {candidate_labels}\n\n"
                f"Text:\n{text}\n\n"
                'Return only a JSON object like {"category": "<one of the listed categories>"}.'
            )
            try:
                resp = completion(
                    model=model_name,
                    messages=[{"role": "user", "content": "\n\n".join(prompt_parts)}],
                    api_key=os.environ.get(api_key_env_var),
                    num_retries=llm_max_retries,
                )
                raw = resp.choices[0].message.content.strip()
                if raw.startswith("```"):
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                parsed = json.loads(raw)
                category = parsed.get("category") if isinstance(parsed, dict) else None
                if category not in candidate_labels:
                    category = None
            except Exception as e:
                context.log.warning(f"classify (llm): failed for a chunk: {e}")
                category = None
            categories.append(category)
        out[output_column] = categories
    else:
        raise ValueError(f"classify op: unsupported mode {mode!r}. Valid: 'zero_shot', 'llm'.")

    return out


# ── embed op ────────────────────────────────────────────────────────────

def _do_embed_raw(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    provider = step.get("provider", "sentence_transformers")
    text_column = step.get("text_column", "chunk_text")
    output_column = step.get("output_column", "embedding")
    batch_size = step.get("batch_size", 32)
    model = step.get("model")

    if text_column not in df.columns:
        raise ValueError(f"embed op: text_column={text_column!r} not in upstream columns: {list(df.columns)}")

    out = df.copy()
    texts = out[text_column].astype(str).tolist()
    embeddings: List[List[float]] = []

    if provider == "sentence_transformers":
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError("embed op provider='sentence_transformers' requires: pip install sentence-transformers")
        model_name = model or "all-MiniLM-L6-v2"
        context.log.info(f"embed: loading sentence-transformers model {model_name!r} for {len(out)} chunks")
        model_obj = SentenceTransformer(model_name)
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = model_obj.encode(batch, normalize_embeddings=step.get("normalize_embeddings", True), show_progress_bar=False, batch_size=batch_size)
            embeddings.extend(batch_embeddings.tolist())

    elif provider == "openai":
        import os
        try:
            import openai
        except ImportError:
            raise ImportError("embed op provider='openai' requires: pip install openai")
        model_name = model or "text-embedding-3-small"
        api_key_env_var = step.get("api_key_env_var", "OPENAI_API_KEY")
        client = openai.OpenAI(api_key=os.environ.get(api_key_env_var))
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            resp = client.embeddings.create(model=model_name, input=batch)
            embeddings.extend([d.embedding for d in resp.data])

    elif provider == "cohere":
        import os
        try:
            import cohere
        except ImportError:
            raise ImportError("embed op provider='cohere' requires: pip install cohere")
        model_name = model or "embed-english-v3.0"
        api_key_env_var = step.get("api_key_env_var", "COHERE_API_KEY")
        client = cohere.Client(api_key=os.environ.get(api_key_env_var))
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            resp = client.embed(texts=batch, model=model_name, input_type="search_document")
            embeddings.extend(resp.embeddings)

    elif provider == "litellm":
        # Universal gateway -- one interface across OpenAI, Azure, Bedrock,
        # Vertex, Ollama (local), VoyageAI, Mistral, and everything else
        # litellm supports, instead of a separate SDK integration per
        # provider. model is litellm's own "<provider>/<model>" format,
        # e.g. "text-embedding-3-small", "azure/my-embed-deployment",
        # "ollama/nomic-embed-text", "bedrock/amazon.titan-embed-text-v2:0".
        import os
        try:
            import litellm
        except ImportError:
            raise ImportError("embed op provider='litellm' requires: pip install litellm")
        model_name = model or "text-embedding-3-small"
        api_key_env_var = step.get("api_key_env_var")
        api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            resp = litellm.embedding(model=model_name, input=batch, api_key=api_key)
            embeddings.extend([d["embedding"] for d in resp.data])
    else:
        raise ValueError(f"embed op: unsupported provider {provider!r}. Valid: sentence_transformers, openai, cohere, litellm.")

    out[output_column] = embeddings
    out["embedding_model"] = model_name
    return out


def _reuse_embeddings(df: pd.DataFrame, step: dict, output_column: str, context) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split df into (already-embedded, still-needs-embedding) using an existing ChromaDB collection:
    a chunk is reused when the collection holds the same chunk_id with the same content_hash AND the
    same embedding_model, so only new or changed chunks pay for an embedding call."""
    cfg = step["reuse_from"]
    if cfg.get("provider", "chromadb") != "chromadb":
        raise ValueError("embed op reuse_from: only provider='chromadb' is supported")
    for needed in ("chunk_id", "content_hash"):
        if needed not in df.columns:
            raise ValueError(f"embed op reuse_from needs column {needed!r} (produced by the chunk op)")
    import chromadb
    model_name = step.get("model") or {"sentence_transformers": "all-MiniLM-L6-v2", "openai": "text-embedding-3-small",
                                         "cohere": "embed-english-v3.0", "litellm": "text-embedding-3-small"}.get(step.get("provider", "sentence_transformers"))
    client = chromadb.PersistentClient(path=cfg.get("connection_string") or "./chroma_db")
    try:
        collection = client.get_collection(name=cfg.get("collection_name", "context_engineering_kb"))
    except Exception:
        return df.iloc[0:0], df  # nothing indexed yet
    existing: Dict[str, Tuple[str, str, Any]] = {}
    ids = df["chunk_id"].astype(str).tolist()
    for i in range(0, len(ids), 500):
        got = collection.get(ids=ids[i:i + 500], include=["metadatas", "embeddings"])
        for cid, meta, emb in zip(got["ids"], got["metadatas"], got["embeddings"]):
            existing[cid] = ((meta or {}).get("content_hash"), (meta or {}).get("embedding_model"), emb)
    reusable = []
    for cid, digest in zip(ids, df["content_hash"].astype(str)):
        hit = existing.get(cid)
        reusable.append(bool(hit and hit[0] == digest and hit[1] == model_name and hit[2] is not None))
    mask = pd.Series(reusable, index=df.index)
    reused = df[mask].copy()
    reused[output_column] = [list(map(float, existing[c][2])) for c in reused["chunk_id"].astype(str)]
    reused["embedding_model"] = model_name
    return reused, df[~mask]


def _do_embed(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Embed chunks. Knob `reuse_from: {provider: chromadb, connection_string, collection_name}` skips
    chunks whose chunk_id + content_hash + embedding model already exist in the index (re-embedding only
    what changed); the stats report how many were reused."""
    output_column = step.get("output_column", "embedding")
    if not step.get("reuse_from"):
        return _do_embed_raw(df, step, context)
    reused, todo = _reuse_embeddings(df, step, output_column, context)
    computed = _do_embed_raw(todo, step, context) if len(todo) else todo.assign(**{output_column: [], "embedding_model": []})
    out = pd.concat([reused, computed]).sort_index().reset_index(drop=True)
    out.attrs["stats"] = {"embeddings_reused": len(reused), "embeddings_computed": len(todo)}
    context.log.info(f"embed: reused {len(reused)} embedding(s), computed {len(todo)}")
    return out


# ── write_vector_store op ────────────────────────────────────────────────

def _delete_in_batches(delete_fn, ids: List[str], size: int = 500) -> None:
    for i in range(0, len(ids), size):
        delete_fn(ids[i:i + size])


def _do_write_vector_store(df: pd.DataFrame, step: dict, id_column: str, context, stamp: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """Index chunks. Knobs: provider (chromadb|pinecone|qdrant), sync_mode (replace_sources default |
    upsert | replace_all), batch_size, metadata_columns. `stamp` (see the component's stamp_provenance)
    writes source_asset_key / source_version / source_partition / chunk_id / content_hash /
    embedding_model / source_id onto every chunk so a retrieval gateway can build receipts straight from
    index hits. sync_mode=replace_sources deletes chunks of the source rows in this run that no longer
    exist (a shrunk document no longer leaves stale chunks behind); replace_all makes this asset's
    chunks exactly this run (the whole collection when provenance is not stamped; refused for partitioned
    assets). With provenance stamped, sync only touches this asset's chunks, and a chunk id that already
    belongs to a different asset is an error (set chunk_id_prefix). chromadb is exercised by tests; qdrant and
    pinecone are not exercised against live services; pinecone cannot delete by filter so it
    falls back to upsert with a warning."""
    provider = step.get("provider", "chromadb")
    text_column = step.get("text_column", "chunk_text")
    embedding_column = step.get("embedding_column", "embedding")
    chunk_id_column = step.get("id_column", "chunk_id" if "chunk_id" in df.columns else id_column)
    metadata_columns = step.get("metadata_columns") or [c for c in df.columns if c not in (text_column, embedding_column, chunk_id_column)]
    batch_size = step.get("batch_size", 100)
    sync_mode = step.get("sync_mode", "replace_sources")
    if sync_mode not in ("replace_sources", "upsert", "replace_all"):
        raise ValueError(f"write_vector_store op: sync_mode must be replace_sources, upsert or replace_all, got {sync_mode!r}")
    if sync_mode == "replace_all" and stamp and stamp.get("source_partition") is not None:
        raise ValueError("write_vector_store op: sync_mode='replace_all' would delete other partitions' chunks; use replace_sources")

    for required in (text_column, embedding_column, chunk_id_column):
        if required not in df.columns:
            raise ValueError(f"write_vector_store op: column {required!r} not in upstream columns: {list(df.columns)}")

    meta_df = df[[c for c in metadata_columns if c in df.columns]].astype(str).copy()
    # Always present so sync/orphan logic never depends on user-chosen metadata_columns.
    meta_df["source_id"] = df[id_column].astype(str).values
    meta_df["chunk_id"] = df[chunk_id_column].astype(str).values
    meta_df["content_hash"] = df[text_column].astype(str).map(_sha256).values
    if "embedding_model" in df.columns:
        meta_df["embedding_model"] = df["embedding_model"].astype(str).values
    for key, value in (stamp or {}).items():
        if value is not None:
            meta_df[key] = str(value)
    metadatas = meta_df.to_dict("records")
    ids = df[chunk_id_column].astype(str).tolist()
    new_ids = set(ids)
    source_ids = sorted(set(meta_df["source_id"]))
    stats: Dict[str, Any] = {"upserted": len(df), "orphans_deleted": 0}

    if provider == "chromadb":
        import chromadb
        connection_string = step.get("connection_string") or "./chroma_db"
        collection_name = step.get("collection_name", "context_engineering_kb")
        client = chromadb.PersistentClient(path=connection_string)
        collection = client.get_or_create_collection(name=collection_name)
        this_asset = (stamp or {}).get("source_asset_key")
        if this_asset is not None:
            # Several assets may share a collection, but a chunk id can belong to only one of them.
            for i in range(0, len(ids), 500):
                got = collection.get(ids=ids[i:i + 500], include=["metadatas"])
                clash = [cid for cid, meta in zip(got["ids"], got["metadatas"])
                         if (meta or {}).get("source_asset_key") not in (None, this_asset)]
                if clash:
                    raise ValueError(
                        f"write_vector_store: chunk id(s) {clash[:3]} already belong to a different asset in collection "
                        f"{collection_name!r}; set `chunk_id_prefix` on the chunk step (for example the asset name) so assets "
                        "sharing a collection cannot overwrite each other."
                    )
        embeddings = df[embedding_column].tolist()
        documents = df[text_column].astype(str).tolist()
        for i in range(0, len(df), batch_size):
            j = min(i + batch_size, len(df))
            collection.upsert(ids=ids[i:j], embeddings=embeddings[i:j], documents=documents[i:j], metadatas=metadatas[i:j])
        orphans: List[str] = []
        # Sync is scoped to this asset's own chunks when provenance is stamped, so one asset's rebuild never
        # deletes another's chunks in a shared collection.
        own = {"source_asset_key": {"$eq": this_asset}} if this_asset is not None else None
        if sync_mode == "replace_sources":
            for i in range(0, len(source_ids), 500):
                match = {"source_id": {"$in": source_ids[i:i + 500]}}
                got = collection.get(where={"$and": [match, own]} if own else match, include=[])
                orphans.extend(cid for cid in got["ids"] if cid not in new_ids)
        elif sync_mode == "replace_all":
            got = collection.get(where=own, include=[]) if own else collection.get(include=[])
            orphans = [cid for cid in got["ids"] if cid not in new_ids]
        _delete_in_batches(lambda batch: collection.delete(ids=batch), orphans)
        stats["orphans_deleted"] = len(orphans)
        context.log.info(f"write_vector_store: upserted {len(df)} chunks into chromadb collection {collection_name!r} at {connection_string!r}; removed {len(orphans)} stale chunk(s) ({sync_mode})")

    elif provider == "pinecone":
        try:
            from pinecone import Pinecone
        except ImportError:
            raise ImportError("write_vector_store op provider='pinecone' requires: pip install pinecone-client")
        import os
        api_key_env_var = step.get("api_key_env_var", "PINECONE_API_KEY")
        pc = Pinecone(api_key=os.environ.get(api_key_env_var))
        index = pc.Index(step["collection_name"])
        vectors = []
        for pos, row in enumerate(df.itertuples(index=False)):
            meta = dict(metadatas[pos])
            meta[text_column] = str(df[text_column].iloc[pos])
            vectors.append({"id": ids[pos], "values": df[embedding_column].iloc[pos], "metadata": meta})
        for i in range(0, len(vectors), batch_size):
            index.upsert(vectors=vectors[i:i + batch_size])
        if sync_mode != "upsert":
            context.log.warning("write_vector_store: pinecone cannot delete by metadata filter on serverless indexes; stale chunks of changed documents are NOT removed (behaves as sync_mode='upsert').")
        context.log.info(f"write_vector_store: upserted {len(df)} chunks into pinecone index {step['collection_name']!r}")

    elif provider == "qdrant":
        try:
            from qdrant_client import QdrantClient
            from qdrant_client.models import FieldCondition, Filter, FilterSelector, MatchAny, PointStruct
        except ImportError:
            raise ImportError("write_vector_store op provider='qdrant' requires: pip install qdrant-client")
        import os
        api_key_env_var = step.get("api_key_env_var")
        client = QdrantClient(url=step.get("connection_string") or "localhost", api_key=os.environ.get(api_key_env_var) if api_key_env_var else None)
        points = []
        for pos in range(len(df)):
            payload = dict(metadatas[pos])
            payload[text_column] = str(df[text_column].iloc[pos])
            # Deterministic id from the chunk id: the row index would overwrite unrelated chunks across runs.
            points.append(PointStruct(id=str(uuid.uuid5(uuid.NAMESPACE_URL, ids[pos])), vector=df[embedding_column].iloc[pos], payload=payload))
        for i in range(0, len(points), batch_size):
            client.upsert(collection_name=step["collection_name"], points=points[i:i + batch_size])
        if sync_mode == "replace_sources":
            for i in range(0, len(source_ids), 500):
                client.delete(
                    collection_name=step["collection_name"],
                    points_selector=FilterSelector(filter=Filter(
                        must=[FieldCondition(key="source_id", match=MatchAny(any=source_ids[i:i + 500]))],
                        must_not=[FieldCondition(key="chunk_id", match=MatchAny(any=list(new_ids)))],
                    )),
                )
        elif sync_mode == "replace_all":
            client.delete(
                collection_name=step["collection_name"],
                points_selector=FilterSelector(filter=Filter(
                    must_not=[FieldCondition(key="chunk_id", match=MatchAny(any=list(new_ids)))],
                )),
            )
        context.log.info(f"write_vector_store: upserted {len(df)} chunks into qdrant collection {step['collection_name']!r} ({sync_mode})")
    else:
        raise ValueError(f"write_vector_store op: unsupported provider {provider!r}. Valid: chromadb, pinecone, qdrant.")

    out = df.copy()
    out.attrs["stats"] = stats
    return out


_FRAME_OPS = {
    "classify": lambda df, step, ctx: _do_classify(df, step, ctx),
    "embed": lambda df, step, ctx: _do_embed(df, step, ctx),
    "filter": lambda df, step, ctx: _do_filter(df, step, ctx),
    "dedupe": lambda df, step, ctx: _do_dedupe(df, step, ctx),
    "sensitivity": lambda df, step, ctx: _do_sensitivity(df, step, ctx),
    "stamp": lambda df, step, ctx: _do_stamp(df, step, ctx),
}


# Same declaration as resources/context_receipt_store's @context_asset (kept in sync by
# tests/test_context_policy_contract.py): a gateway reads it back with context_policies().
_CONTEXT_POLICY_METADATA_KEY = "context/policy"
_CONTEXT_POLICY_FIELDS = frozenset({
    "scope", "sensitivity", "max_age_hours", "token_budget", "retention_days", "access_tags",
    "chars_per_token", "sensitivity_field", "valid_until_field", "updated_field",
})
_SENSITIVITY_LEVELS = ("public", "internal", "confidential", "restricted")


def _validate_context_policy(policy: Dict[str, Any]) -> Dict[str, Any]:
    unknown = set(policy) - _CONTEXT_POLICY_FIELDS
    if unknown:
        raise ValueError(f"context_policy: unknown field(s) {sorted(unknown)}; valid: {sorted(_CONTEXT_POLICY_FIELDS)}")
    if policy.get("sensitivity", "internal") not in _SENSITIVITY_LEVELS:
        raise ValueError(f"context_policy: sensitivity must be one of {_SENSITIVITY_LEVELS}, got {policy.get('sensitivity')!r}")
    for name in ("max_age_hours", "token_budget", "retention_days"):
        value = policy.get(name)
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0):
            raise ValueError(f"context_policy: {name} must be a positive number")
    return dict(policy)


def _recipe_fingerprint(steps_cfg: List[dict]) -> str:
    """Hash of the build recipe: every step except where it is written (ids, locations, credentials, batch
    sizes and sync mode do not change what is built). Used as the asset's Dagster ``code_version``."""
    recipe = [
        {k: v for k, v in step.items() if k not in ("id", "source", "connection_string", "api_key_env_var", "batch_size", "reuse_from", "sync_mode")}
        for step in steps_cfg if step.get("op") != "write_vector_store"
    ]
    return hashlib.sha256(json.dumps(recipe, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _content_version(df: pd.DataFrame, text_column: str, steps_cfg: List[dict]) -> str:
    """Content-derived version: hash of every (chunk_id, content_hash) plus the build configuration
    (everything except where it is written). Dagster's default data version derives from the asset's
    code version and its inputs' data versions, so it changes every run when an upstream asset carries no explicit
    version (a typical raw table). This only changes when the knowledge base content or recipe changes, so a
    no-op rebuild does not make receipts look stale."""
    h = hashlib.sha256()
    ids = df["chunk_id"].astype(str) if "chunk_id" in df.columns else pd.Series(range(len(df))).astype(str)
    digests = df["content_hash"].astype(str) if "content_hash" in df.columns else df[text_column].astype(str).map(_sha256)
    for cid, digest in sorted(zip(ids, digests)):
        h.update(f"{cid}:{digest}\n".encode())
    h.update(_recipe_fingerprint(steps_cfg).encode())
    return h.hexdigest()[:32]


# ── execution_mode='sql' ────────────────────────────────────────────────
# Generates and runs ONE server-side query per warehouse so the corpus
# never leaves the database and there's no in-memory row limit -- the
# direct answer to "data never leaves the database, where we can help
# it" for warehouses with a native embedding/completion function.
# validation.level: code -- there is no live warehouse credential in
# this dev environment, so these code paths are structurally tested
# (the emitted SQL is asserted, not executed) rather than run for real.

def _sql_chunk_expr(dialect: str, text_expr: str, chunk_size: int, chunk_overlap: int) -> str:
    step = max(chunk_size - chunk_overlap, 1)
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER({text_expr}, 'none', {chunk_size}, {chunk_overlap})"
    if dialect == "bigquery":
        # BigQuery has no native recursive splitter; emit a GENERATE_ARRAY
        # of substrings at fixed stride -- coarser than Cortex's splitter,
        # documented as such in the README.
        return (
            f"ARRAY(SELECT SUBSTR({text_expr}, pos, {chunk_size}) "
            f"FROM UNNEST(GENERATE_ARRAY(1, LENGTH({text_expr}), {step})) AS pos)"
        )
    if dialect == "databricks":
        return (
            f"ARRAY(SELECT SUBSTR({text_expr}, pos, {chunk_size}) "
            f"FROM (SELECT explode(sequence(1, LENGTH({text_expr}), {step})) AS pos))"
        )
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_classify_expr(dialect: str, text_expr: str, candidate_labels: List[str]) -> str:
    labels_sql = ", ".join(f"'{label}'" for label in candidate_labels)
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.CLASSIFY_TEXT({text_expr}, ARRAY_CONSTRUCT({labels_sql}))"
    if dialect == "bigquery":
        return f"AI.CLASSIFY({text_expr}, [{labels_sql}])"
    if dialect == "databricks":
        return f"ai_classify({text_expr}, ARRAY({labels_sql}))"
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_embed_expr(dialect: str, text_expr: str, model: Optional[str]) -> str:
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.EMBED_TEXT_768('{model or 'snowflake-arctic-embed-m'}', {text_expr})"
    if dialect == "bigquery":
        return f"ML.GENERATE_EMBEDDING(MODEL `{model or 'embedding_model'}`, {text_expr})"
    if dialect == "databricks":
        return f"ai_query('{model or 'databricks-gte-large-en'}', {text_expr})"
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _build_sql_mode_query(
    steps_cfg: List[dict], source_cfg: dict, dialect: str, output_table: str,
    id_column: str, text_column: str, metadata_columns: List[str],
) -> str:
    """Build the single CREATE OR REPLACE TABLE statement implementing the
    chunk -> classify -> embed chain natively in the warehouse. Only the
    ops actually used matter for output shape; write_vector_store has no
    SQL equivalent (there's no in-warehouse vector index for these
    dialects that behaves like chromadb/pinecone/qdrant) so it's skipped
    in this mode -- point rag_pipeline's `retrieve` op straight at
    output_table instead (Snowflake Cortex Search / BigQuery vector
    search / Databricks Vector Search all query a table directly)."""
    chunk_step = next(s for s in steps_cfg if s["op"] == "chunk")
    classify_step = next((s for s in steps_cfg if s["op"] == "classify"), None)
    embed_step = next((s for s in steps_cfg if s["op"] == "embed"), None)

    chunk_size = chunk_step.get("chunk_size", 500)
    chunk_overlap = chunk_step.get("chunk_overlap", 50)
    chunk_expr = _sql_chunk_expr(dialect, text_column, chunk_size, chunk_overlap)

    meta_select = ", ".join(f"src.{c}" for c in metadata_columns)
    meta_select = f", {meta_select}" if meta_select else ""

    lines = [
        f"CREATE OR REPLACE TABLE {output_table} AS",
        "WITH chunked AS (",
        f"  SELECT src.{id_column} AS {id_column},",
        f"         chunk.value::string AS chunk_text,",
        f"         chunk.index AS chunk_index{meta_select}",
        f"  FROM ({source_cfg['sql']}) AS src,",
        f"       LATERAL FLATTEN(input => {chunk_expr}) AS chunk",
        ")",
        "SELECT chunked.*",
    ]
    select_extra = []
    if classify_step is not None:
        candidate_labels = classify_step.get("candidate_labels")
        if not candidate_labels:
            raise ValueError("classify step in execution_mode='sql' requires `candidate_labels`.")
        classify_expr = _sql_classify_expr(dialect, "chunked.chunk_text", candidate_labels)
        select_extra.append(f"       {classify_expr} AS category")
    if embed_step is not None:
        embed_expr = _sql_embed_expr(dialect, "chunked.chunk_text", embed_step.get("model"))
        select_extra.append(f"       {embed_expr} AS embedding")
    if select_extra:
        lines[-1] += ","
        lines.append(",\n".join(select_extra))
    lines.append("FROM chunked")
    return "\n".join(lines)


def _run_sql_mode(
    context, steps_cfg: List[dict], source_cfg: dict, dialect: str, output_table: str,
    id_column: str, text_column: str, metadata_columns: List[str],
) -> Output:
    sql = _build_sql_mode_query(steps_cfg, source_cfg, dialect, output_table, id_column, text_column, metadata_columns)
    resource_key = source_cfg["resource_key"]
    resource = getattr(context.resources, resource_key)

    context.log.info(f"execution_mode='sql' ({dialect}): running server-side chunk/classify/embed into {output_table}")
    if hasattr(resource, "get_engine"):
        engine = resource.get_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql(sql)
            row_count = conn.exec_driver_sql(f"SELECT COUNT(*) FROM {output_table}").scalar()
    elif hasattr(resource, "get_connection"):
        with resource.get_connection() as conn:
            conn.execute(sql)
            row_count = conn.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
    else:
        raise ValueError(f"resource {resource_key!r} must expose .get_engine() or .get_connection().")

    return Output(
        value=None,
        metadata={
            "dagster/row_count": MetadataValue.int(row_count),
            "execution_mode": MetadataValue.text("sql"),
            "sql_dialect": MetadataValue.text(dialect),
            "output_table": MetadataValue.text(output_table),
            "generated_sql": MetadataValue.md(f"```sql\n{sql}\n```"),
        },
    )



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class ContextEngineeringPipelineComponent(Component, Model, Resolvable):
    """Chunk, classify, embed, and index raw unstructured text into a
    governed, searchable, citeable knowledge base -- one YAML, a
    `steps:` list, pairs with `rag_pipeline` for the query side.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame of raw text rows. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull raw text rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource>, sql: <query>}. "
            "resource_key must point at a resource exposing .get_engine() (SQLAlchemy) or "
            ".get_connection() (DB-API) -- works out of the box with duckdb_resource, "
            "postgres_resource, snowflake_resource, bigquery_resource, etc., so this pipeline "
            "runs the same way against DuckDB or any other supported warehouse. Alternatively "
            "set `database_url_env_var` to a bare SQLAlchemy connection string from an "
            "environment variable when no Dagster resource is registered. Mutually "
            "exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    id_column: Union[str, int] = Field(description="Column uniquely identifying each source row -- threaded through to every chunk as a citation back to the source.")
    text_column: Union[str, int] = Field(description="Column containing the raw text to chunk.")
    metadata_columns: Optional[List[str]] = Field(
        default=None,
        description="Additional source columns to carry forward onto every chunk row (e.g. customer_id, created_at) -- available to later steps and written as vector-store metadata for filtered search.",
    )
    max_source_rows: Optional[int] = Field(
        default=None,
        description=(
            "Safety cap on source rows processed per materialize (applied AFTER any SQL "
            "filtering, before chunking). A real corpus is typically far too large to process "
            "in one in-memory run -- pair this with partitioning (partition_type below) to "
            "bound each run to a slice (e.g. one day's new tickets) rather than the whole "
            "historical corpus. None (default) processes every row in the upstream/query "
            "result, which is only safe for a genuinely small corpus or a pre-filtered "
            "partition. Only applies to execution_mode='python' -- execution_mode='sql' runs "
            "the whole corpus through the warehouse's own distributed engine and doesn't need "
            "this cap."
        ),
    )
    execution_mode: str = Field(
        default="python",
        description=(
            "'python' (default): pulls rows into a DataFrame and calls chunk/classify/embed "
            "as Python/API calls -- works with every provider, but data leaves the database "
            "and the corpus must fit in memory (bound it with max_source_rows + partitioning). "
            "'sql': the whole chunk/classify/embed chain runs as ONE query executed by the "
            "warehouse's own distributed engine via a warehouse-native AI function -- data "
            "never leaves the database and there's no in-memory size limit, but only works "
            "for warehouses with a native embedding/completion function (set sql_dialect). "
            "DuckDB has no such function (same limitation dbt's own context-engineering "
            "package has) -- use 'python' with a duckdb_resource `source:` for DuckDB."
        ),
    )
    sql_dialect: Optional[str] = Field(
        default=None,
        description="Required when execution_mode='sql'. One of: 'snowflake_cortex', 'bigquery', 'databricks'. Selects which warehouse-native AI SQL functions to emit.",
    )
    output_table: Optional[str] = Field(
        default=None,
        description="Required when execution_mode='sql'. Fully-qualified destination table name the final chunk/category/embedding rows are written to, in the same database (CREATE OR REPLACE TABLE ... AS ...) -- the knowledge base never leaves the warehouse.",
    )
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered list of {id, op, ...op-specific fields} dicts. Each step's `source:` "
            "names a prior step's id to read from; if omitted, defaults to the immediately "
            "preceding step (so you don't have to repeat `source:` on every line). "
            "Valid ops: 'chunk' (must be first), 'filter', 'dedupe', 'sensitivity', 'stamp', 'classify', 'embed', "
            "'write_vector_store'. filter/dedupe/sensitivity/stamp are python-mode only."
        )
    )

    code_version: Optional[str] = Field(
        default=None,
        description=(
            "Dagster code_version for the asset: the identity of the recipe that builds this context. Defaults to a "
            "fingerprint of the steps (chunking, filters, embedding model...), so changing the recipe marks the asset "
            "stale in Dagster (AutomationCondition.code_version_changed) and every chunk is stamped with it "
            "(`source_code_version`), letting outcomes be compared across recipe versions. Set it explicitly to tie it "
            "to something else, such as a git SHA of this config."
        ),
    )
    emit_data_version: bool = Field(
        default=True,
        description=(
            "Emit a content-derived Dagster data version (hash of every chunk id + content hash + the build recipe). "
            "Dagster's default data version derives from the asset's code version and its inputs' data versions, so it "
            "changes on every run when an upstream asset has no explicit version (a typical raw table); without this a "
            "no-op rebuild would make anything that recorded the old version (such as context receipts) look stale."
        ),
    )
    stamp_provenance: bool = Field(
        default=True,
        description=(
            "Write source_asset_key, source_version (the emitted data version), source_partition, source_id, chunk_id, "
            "content_hash and embedding_model onto every chunk in the vector store, so a retrieval gateway can build "
            "receipts directly from index hits. Python mode, write_vector_store step."
        ),
    )

    context_policy: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Declare the governance policy a retrieval gateway should enforce for this knowledge base: "
            "{scope: <chunk metadata field naming the tenant>, sensitivity: public|internal|confidential|restricted, "
            "max_age_hours, token_budget, retention_days, access_tags: [...]}. Recorded on the asset as "
            "`context/policy` metadata and `context/*` tags; read back with context_policies() in "
            "resources/context_receipt_store. Same declaration as the @context_asset decorator. "
            "Pair with the `stamp` op (scope/valid_until columns) and `sensitivity` op so chunks carry the fields the policy checks."
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners — list of team names or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. Auto-inferred if unset.")

    include_preview_metadata: bool = Field(default=False, description="Include a preview of the output data in metadata.")
    preview_rows: int = Field(default=25, ge=1, le=500, description="Rows to include in the preview metadata when include_preview_metadata is True.")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure. Defines a RetryPolicy.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Maximum acceptable lag in minutes before the asset is considered stale.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule string for the freshness policy.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'static'/'dynamic'/None.")
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO), required for time-based types.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps_cfg = list(self.steps or [])
        if not steps_cfg:
            raise ValueError("context_engineering_pipeline requires at least one step in `steps:`.")
        if steps_cfg[0]["op"] != "chunk":
            raise ValueError("context_engineering_pipeline: the first step must be op='chunk'.")
        valid_ops = {"chunk", "classify", "embed", "write_vector_store", "filter", "dedupe", "sensitivity", "stamp"}
        for s in steps_cfg:
            if "id" not in s or "op" not in s:
                raise ValueError(f"every step needs {{id, op}}; got: {s}")
            if s["op"] not in valid_ops:
                raise ValueError(f"step {s['id']!r} op={s['op']!r} unsupported. Valid: {sorted(valid_ops)}")

        execution_mode = self.execution_mode
        if execution_mode == "sql":
            python_only = sorted({s["op"] for s in steps_cfg} - {"chunk", "classify", "embed"})
            if python_only:
                raise ValueError(f"execution_mode='sql' supports only chunk/classify/embed steps; python-mode-only op(s): {python_only}")
        if execution_mode not in ("python", "sql"):
            raise ValueError(f"execution_mode must be 'python' or 'sql', got {execution_mode!r}.")
        if execution_mode == "sql":
            if self.sql_dialect not in ("snowflake_cortex", "bigquery", "databricks"):
                raise ValueError(
                    "execution_mode='sql' requires sql_dialect to be one of: "
                    "'snowflake_cortex', 'bigquery', 'databricks' (DuckDB and plain "
                    "Postgres/MySQL have no native embedding function -- use "
                    "execution_mode='python' with a `source:` warehouse_query for those)."
                )
            if not self.output_table:
                raise ValueError("execution_mode='sql' requires `output_table` (the destination table for the resulting knowledge base).")
            if not self.source or self.source.get("kind") != "warehouse_query":
                raise ValueError("execution_mode='sql' requires `source: {kind: warehouse_query, resource_key: ..., sql: ...}` -- there's no DataFrame to hand to a SQL engine.")
        if self.upstream_asset_key and self.source:
            raise ValueError("context_engineering_pipeline: set exactly one of `upstream_asset_key` or `source`, not both.")
        if not self.upstream_asset_key and not self.source:
            raise ValueError("context_engineering_pipeline: set one of `upstream_asset_key` or `source`.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name,
        )

        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            freshness_policy = FreshnessPolicy(maximum_lag_minutes=self.freshness_max_lag_minutes, cron_schedule=self.freshness_cron)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        id_column = self.id_column
        text_column = self.text_column
        metadata_columns = self.metadata_columns or []
        max_source_rows = self.max_source_rows
        recipe_code_version = self.code_version or _recipe_fingerprint(steps_cfg)
        emit_data_version = self.emit_data_version
        stamp_provenance = self.stamp_provenance
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        source_cfg = self.source
        sql_dialect = self.sql_dialect
        output_table = self.output_table

        context_policy = _validate_context_policy(self.context_policy) if self.context_policy else None
        _inferred_kinds = self.kinds or ["ai", "context-engineering"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        if context_policy is not None:
            _all_tags["context/sensitivity"] = context_policy.get("sensitivity", "internal")
            if context_policy.get("scope"):
                _all_tags["context/scope"] = context_policy["scope"]
        asset_kwargs: Dict[str, Any] = dict(
            key=AssetKey.from_user_string(asset_name),
            code_version=recipe_code_version,
            description=self.description or f"Context-engineering pipeline ({execution_mode} mode): {' → '.join(s['id'] for s in steps_cfg)}",
            group_name=self.group_name,
            tags=_all_tags,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            freshness_policy=freshness_policy,
            partitions_def=partitions_def,
        )
        if context_policy is not None:
            asset_kwargs["metadata"] = {_CONTEXT_POLICY_METADATA_KEY: MetadataValue.json(context_policy)}
        if self.upstream_asset_key:
            asset_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))}
        if source_cfg and source_cfg.get("resource_key"):
            asset_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

        if execution_mode == "sql":
            @asset(**asset_kwargs)
            def _sql_asset(context: AssetExecutionContext):
                return _run_sql_mode(context, steps_cfg, source_cfg, sql_dialect, output_table, id_column, text_column, metadata_columns)
            return Definitions(assets=[_sql_asset])

        @asset(**asset_kwargs)
        def _asset(context: AssetExecutionContext, **kwargs):
            upstream = kwargs.get("upstream")
            if upstream is not None:
                if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                    upstream = upstream.value
                if isinstance(upstream, dict):
                    _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                    upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
            else:
                upstream = _ingest_warehouse_query(source_cfg, context)

            if id_column not in upstream.columns:
                raise ValueError(f"id_column={id_column!r} not in upstream: {list(upstream.columns)}")
            if text_column not in upstream.columns:
                raise ValueError(f"text_column={text_column!r} not in upstream: {list(upstream.columns)}")

            source_rows_available = len(upstream)
            if max_source_rows is not None and source_rows_available > max_source_rows:
                context.log.warning(
                    f"max_source_rows={max_source_rows} < {source_rows_available} available source rows -- "
                    f"truncating this run. Pair with partitioning to process the full corpus over multiple runs "
                    f"rather than silently dropping rows every time."
                )
                upstream = upstream.head(max_source_rows)

            state: Dict[str, pd.DataFrame] = {}
            step_metadata: Dict[str, Dict[str, Any]] = {}
            last_frame_id: Optional[str] = None
            content_version: Optional[str] = None
            partition = context.partition_key if context.has_partition_key else None

            for step in steps_cfg:
                import time as _time
                t0 = _time.time()
                step_id = step["id"]
                op = step["op"]
                source_id = step.get("source") or last_frame_id

                if op == "chunk":
                    df = _do_chunk(upstream, step, id_column, text_column, metadata_columns, context)
                elif op == "write_vector_store":
                    if source_id is None:
                        raise ValueError(f"step {step_id!r} (op=write_vector_store) has no source and is not preceded by another step.")
                    write_input = state[source_id]
                    content_version = _content_version(write_input, step.get("text_column", "chunk_text"), steps_cfg)
                    stamp = {
                        "source_asset_key": asset_name,
                        "source_version": content_version,
                        "source_partition": partition,
                        "source_code_version": recipe_code_version,
                    } if stamp_provenance else None
                    df = _do_write_vector_store(write_input, step, id_column, context, stamp)
                else:
                    if source_id is None:
                        raise ValueError(f"step {step_id!r} (op={op!r}) has no source and is not preceded by another step.")
                    df = _FRAME_OPS[op](state[source_id], step, context)

                state[step_id] = df
                last_frame_id = step_id
                elapsed = _time.time() - t0
                step_metadata[step_id] = {"op": op, "rows": len(df), "elapsed_seconds": round(elapsed, 3), **(df.attrs.get("stats") or {})}
                context.log.info(f"step {step_id!r} ({op}) → {len(df)} rows in {elapsed:.2f}s")

            final_df = state[steps_cfg[-1]["id"]]
            if content_version is None and "chunk_id" in final_df.columns:
                content_version = _content_version(final_df, "chunk_text" if "chunk_text" in final_df.columns else final_df.columns[0], steps_cfg)

            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(len(final_df)),
                "source_rows": MetadataValue.int(len(upstream)),
                "source_rows_available": MetadataValue.int(source_rows_available),
                "n_steps": MetadataValue.int(len(steps_cfg)),
                "step_chain": MetadataValue.text(" → ".join(s["id"] for s in steps_cfg)),
                "step_metadata": MetadataValue.json(step_metadata),
            }
            if content_version is not None:
                metadata["context_version"] = MetadataValue.text(content_version)
            if include_preview and len(final_df) > 0:
                try:
                    _prev_cols = [c for c in final_df.columns if c != "embedding"]
                    _prev = final_df[_prev_cols]
                    _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False) or "")
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            return Output(
                value=final_df,
                metadata=metadata,
                data_version=DataVersion(content_version) if (emit_data_version and content_version) else None,
            )

        return Definitions(assets=[_asset])
