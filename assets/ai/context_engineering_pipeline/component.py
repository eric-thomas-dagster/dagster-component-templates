"""ContextEngineeringPipelineComponent — one YAML builds a governed,
searchable, citeable knowledge base out of raw unstructured text.

Same "pipeline component" shape as `ml_pipeline`, `rag_pipeline`,
`warehouse_pipeline`, `polars_pipeline`: **one YAML file declares the
whole pipeline**, a `steps:` list defines the chain in reading order,
each step's output is a DataFrame the next step can read via `source:`
(or, if omitted, the immediately-prior step — you don't have to repeat
`source:` on every line).

This is the "build the knowledge base" half of context engineering:
chunk → embed → classify → semantic search, governed the same
discipline-first way metrics are. It's designed to pair with the
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
                      embedding -- a relevance-vs-similarity trick: raw
                      cosine similarity ranks short boilerplate over long
                      relevant analysis unless chunks are pre-filtered by
                      category first.
- embed:              sentence-transformers (local, free), OpenAI, or
                      Cohere.
- write_vector_store: ChromaDB (local, free), Pinecone, or Qdrant.

Every op is free/local by default (sentence-transformers + HuggingFace
zero-shot + ChromaDB) -- no API key required to run this end to end;
governance tooling should be cheap to validate, not another paid call
per test.
"""
import ast
import contextlib
import hashlib
import importlib
import inspect
import weakref
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Component,
    DataVersion,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
    Resolvable,
    asset,
    op,
)
from pydantic import Field


# --- _vendor_client_session: begin (identical in every component that takes a vendor-client resource; drift-tested) ---
_GENERATOR_CLIENTS: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()   # generator -> the client it yielded


@contextlib.contextmanager
def _vendor_client_session(obj):
    """Yield a vendor client (BigQuery-style: .query / .execute_query) from whatever Dagster hands the asset for a resource key. With
    required_resource_keys, dagster-gcp's BigQueryResource (an IAttachDifferentObjectToOpContext) arrives as a bare GENERATOR that yields the
    client; injected as a parameter it is the resource whose .get_client() is a @contextmanager; a plain client is used as is.

    A bare generator is ONE shared object (context.resources hands every caller in the step the same one) and can only be advanced once, so the
    client it yielded is remembered for the later calls and the generator is left to Dagster / the garbage collector: closing it after the
    first use would make the second use in the same step raise StopIteration. Its `with` exit (credential teardown) runs when it is collected."""
    def _is_client(o):
        return hasattr(o, "query") or hasattr(o, "execute_query")

    def _client_of(gen):
        if gen not in _GENERATOR_CLIENTS:
            _GENERATOR_CLIENTS[gen] = next(gen)
        return _GENERATOR_CLIENTS[gen]

    if inspect.isgenerator(obj):
        yield _client_of(obj)
        return
    if hasattr(obj, "get_client"):
        made = obj.get_client()
        if inspect.isgenerator(made):
            try:
                yield next(made)
            finally:
                made.close()        # our own generator: runs its `with` exit / credential teardown
        elif not _is_client(made) and hasattr(made, "__enter__"):
            with made as client:
                yield client
        else:
            yield made
        return
    yield obj
# --- _vendor_client_session: end ---


def _read_with_client(client, sql: str, resource_key: str) -> "pd.DataFrame":
    if hasattr(client, "query"):
        # BigQuery (google.cloud.bigquery.Client): .query(sql) returns a QueryJob; .to_dataframe() blocks until done and
        # returns a pandas DataFrame directly -- no .result() call needed first.
        job = client.query(sql)
        if hasattr(job, "to_dataframe"):
            return job.to_dataframe()
    if hasattr(client, "execute_query"):
        # Redshift Data API (dagster_aws RedshiftClient): execute_query(sql, fetch_results=True) returns bare List[Tuple] with NO
        # column names attached -- a RealDictCursor factory is required to get dict rows a DataFrame can use.
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
    sql = _bind_partition_placeholders(source_config["sql"], context, _standard_sql_str)
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
        if inspect.isgenerator(resource) or hasattr(resource, "get_client") or hasattr(resource, "query") or hasattr(resource, "execute_query"):
            # a vendor client: dagster_gcp's BigQueryResource arrives as a bare generator (required_resource_keys) or as the resource
            # whose get_client() is a @contextmanager (parameter injection); _vendor_client_session handles both, and a bare client.
            with _vendor_client_session(resource) as client:
                return _read_with_client(client, sql, resource_key)
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
    partition_type, partition_start, partition_values, dynamic_partition_name, partition_dimensions=None,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    """Construct a Dagster partitions_def from the canonical partition fields: either the flat fields (partition_type / partition_start /
    partition_values / dynamic_partition_name) or `partition_dimensions` (one axis, or several axes -> MultiPartitionsDefinition).
    Strict: raises ValueError on misconfiguration (time-based type without a start, static without values, dynamic without a name, flat fields
    and partition_dimensions together). Same behaviour as rag_pipeline's copy, minus its flat 'multi' shorthand."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, DynamicPartitionsDefinition, MultiPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type (flat-fields shape) or partition_dimensions (multi-axis shape), not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
            )
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        names = [d.get("name") for d in partition_dimensions]
        if not all(names) or len(set(names)) != len(names):
            raise ValueError("partition_dimensions with several axes need a unique `name` on each")
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start (ISO date).")
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values.")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("partition_type='dynamic' requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


# ── partition-aware source SQL ───────────────────────────────────────────────────────────────────────
# `source.sql` may use {partition_key}, {partition_start}, {partition_end} (time-window partitions, ISO strings) and {partition_key.<dim>}
# (multi-dimension partitions). Each placeholder is replaced by a complete, escaped SQL string literal -- write `WHERE d = {partition_key}`,
# NOT `WHERE d = '{partition_key}'`. `{{` / `}}` are literal braces.

_PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z0-9_\-]+)?)\}")


def _sql_placeholders(sql: str) -> List[str]:
    return _PLACEHOLDER_RE.findall(sql.replace("{{", "\0").replace("}}", "\0"))


def _is_partitioned_run(context) -> bool:
    try:
        return bool(context.has_partition_key or context.has_partition_key_range)
    except Exception:
        return False


def _partition_label_and_keys(context) -> Tuple[Optional[str], List[str]]:
    """(label stamped as source_partition, every partition key the run covers). A one-key run: the key. A single_run backfill range: 'first..last'."""
    if context.has_partition_key:
        return str(context.partition_key), [str(context.partition_key)]
    if context.has_partition_key_range:
        r = context.partition_key_range
        keys: List[str] = []
        try:
            keys = [str(k) for k in context.assets_def.partitions_def.get_partition_keys_in_range(r)]
        except Exception:
            pass
        return (str(r.start) if r.start == r.end else f"{r.start}..{r.end}"), keys or [str(r.start), str(r.end)]
    return None, []


def _partition_bindings(context) -> Dict[str, str]:
    from dagster import MultiPartitionsDefinition, TimeWindowPartitionsDefinition
    b: Dict[str, str] = {}
    label, _ = _partition_label_and_keys(context)
    if label is None:
        return b
    b["partition_key"] = label
    window = None
    if context.has_partition_key:
        key = context.partition_key
        kbd = getattr(key, "keys_by_dimension", None)
        if kbd:
            for dim, val in kbd.items():
                b[f"partition_key.{dim}"] = str(val)
            pdef = context.assets_def.partitions_def
            if isinstance(pdef, MultiPartitionsDefinition):
                for d in pdef.partitions_defs:
                    if isinstance(d.partitions_def, TimeWindowPartitionsDefinition):
                        window = d.partitions_def.time_window_for_partition_key(kbd[d.name])
                        break
    if window is None:
        try:
            window = context.partition_time_window
        except Exception:
            window = None
    if window is not None:
        b["partition_start"], b["partition_end"] = window.start.isoformat(), window.end.isoformat()
    return b


def _bind_partition_placeholders(sql: str, context, quote, what: str = "source.sql") -> str:
    names = _sql_placeholders(sql)
    partitioned = _is_partitioned_run(context)
    unescape = lambda t: t.replace("{{", "{").replace("}}", "}")  # noqa: E731
    if not names:
        if partitioned:
            context.log.warning(
                f"{what} uses no partition placeholder ({{partition_key}}, {{partition_start}}, {{partition_end}}) but the asset is partitioned: "
                "every partition run reads the FULL source. Add a placeholder to the WHERE clause to read just this partition.")
        return unescape(sql)
    if not partitioned:
        raise ValueError(f"{what} uses partition placeholder(s) {sorted(set(names))} but this run has no partition key (asset is not partitioned, or this is an unpartitioned run)")
    bindings = _partition_bindings(context)
    for n in set(names):
        if n not in bindings:
            raise ValueError(f"{what}: placeholder {{{n}}} is not available for this run; available: {sorted(bindings)}")
        if re.search(rf"[\'\"`]\{{{re.escape(n)}\}}|\{{{re.escape(n)}\}}[\'\"`]", sql):
            raise ValueError(f"{what}: {{{n}}} is replaced by a complete quoted string literal; remove the quotes around it")
    protected = sql.replace("{{", "\0L").replace("}}", "\0R")
    out = _PLACEHOLDER_RE.sub(lambda m: quote(bindings[m.group(1)]), protected)
    return out.replace("\0L", "{").replace("\0R", "}")


def _standard_sql_str(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _validate_partition_placeholders(sql: str, partitions_def, what: str = "source.sql") -> None:
    """Build-time check of the placeholders against the declared partitions_def."""
    from dagster import MultiPartitionsDefinition, TimeWindowPartitionsDefinition
    names = set(_sql_placeholders(sql))
    if not names:
        return
    if partitions_def is None:
        raise ValueError(f"{what} uses partition placeholder(s) {sorted(names)} but no partitioning is configured (partition_type / partition_dimensions)")
    dims = [d.name for d in partitions_def.partitions_defs] if isinstance(partitions_def, MultiPartitionsDefinition) else []
    timed = isinstance(partitions_def, TimeWindowPartitionsDefinition) or (
        isinstance(partitions_def, MultiPartitionsDefinition) and any(isinstance(d.partitions_def, TimeWindowPartitionsDefinition) for d in partitions_def.partitions_defs))
    for n in sorted(names):
        if n == "partition_key":
            continue
        if n in ("partition_start", "partition_end"):
            if not timed:
                raise ValueError(f"{what}: {{{n}}} needs a time-window partition (daily/weekly/monthly/hourly)")
        elif n.startswith("partition_key.") and n.split(".", 1)[1] in dims:
            continue
        else:
            raise ValueError(f"{what}: unknown placeholder {{{n}}}; valid: partition_key, partition_start, partition_end"
                             + "".join(f", partition_key.{d}" for d in dims))


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
    (prepended to each chunk's text, may use any carried column and {section_path}), parent_chunk_size
    (small-to-big: embed small child chunks but keep each child's larger parent passage as `parent_text`
    and `parent_id`, so retrieval can match precisely and return the surrounding context)."""
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
    parent_size = step.get("parent_chunk_size")
    if parent_size is not None:
        parent_size = max(1, _to_chars(parent_size, unit, cpt))
        parent_overlap = _to_chars(step.get("parent_chunk_overlap", 0), unit, cpt)
        if parent_size <= chunk_size:
            raise ValueError("chunk op: parent_chunk_size must be larger than chunk_size")
        if parent_overlap >= parent_size:
            raise ValueError("chunk op: parent_chunk_overlap must be smaller than parent_chunk_size")

    def split(text: str, size: int, overlap: int, lang: Optional[str]) -> List[Tuple[str, str]]:
        if strategy == "fixed":
            return [(c, "") for c in _chunk_fixed(text, size, overlap, preserve_sentences)]
        if strategy == "recursive":
            return [(c, "") for c in _chunk_recursive(text, size, overlap)]
        if strategy == "markdown":
            return _chunk_markdown(text, size, overlap)
        return _chunk_code(text, size, overlap, lang)

    rows: List[Dict[str, Any]] = []
    skipped = dropped_small = capped = 0
    for _, row in df.iterrows():
        source_id = row[id_column]
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        if not text.strip():
            skipped += 1
            continue
        lang = code_language
        if strategy == "code":
            if lang is None and language_column and language_column in df.columns:
                lang = _normalize_language(row[language_column])
            if lang is None and path_column and path_column in df.columns:
                lang = _normalize_language(row[path_column])
        if parent_size is None:
            pieces = [(c, sp, None, None) for c, sp in split(text, chunk_size, chunk_overlap, lang)]
        else:
            pieces = [
                (child, sp_child or sp_parent, pidx, parent_body)
                for pidx, (parent_body, sp_parent) in enumerate(split(text, parent_size, parent_overlap, lang))
                for child, sp_child in split(parent_body, chunk_size, chunk_overlap, lang)
            ]
        kept = [piece for piece in pieces if len(piece[0]) >= min_chars]
        dropped_small += len(pieces) - len(kept)
        if max_chunks is not None and len(kept) > int(max_chunks):
            capped += len(kept) - int(max_chunks)
            kept = kept[: int(max_chunks)]
        seen_ids: set = set()
        for i, (body, section_path, parent_idx, parent_body) in enumerate(kept):
            record: Dict[str, Any] = {id_column: source_id}
            for col in metadata_columns:
                if col in df.columns:
                    record[col] = row[col]
            chunk_text = body
            if header_template:
                fields = _SafeFormat({**{k: ("" if pd.isna(v) else v) for k, v in row.items() if k != text_column}, "section_path": section_path})
                chunk_text = header_template.format_map(fields) + body
            # A child's identity includes its parent passage: the text a retrieval returns changes if either does.
            digest = _sha256(chunk_text if parent_body is None else chunk_text + "\x1f" + parent_body)
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
            if parent_body is not None:
                record["parent_id"] = f"{id_prefix}{source_id}_p{parent_idx}"
                record["parent_text"] = parent_body
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


_ENRICH_PROMPTS = {
    "summary": "Write one sentence that summarises this passage. Reply with the sentence only.",
    "keywords": "List up to 8 keywords or short phrases a reader might search for to find this passage, comma-separated. Reply with the list only.",
    "questions": "Write 3 short questions this passage answers, one per line. Reply with the questions only.",
    "context": "Write one short sentence that situates this passage within its source so it can be understood on its own (what it is about and where it comes from). Reply with the sentence only.",
}


def _do_enrich(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Add an LLM-written field to every chunk. mode: summary | keywords | questions | context (a situating
    sentence, the "contextual retrieval" technique). `prepend: true` puts the generated text in front of the
    chunk so it is part of what gets embedded (and hashed, so the version reflects it); otherwise it is just
    a column that travels to the vector store as metadata. One model call per chunk, so it costs money and
    time; `model` is required (any litellm model). For `context` mode the passage's `parent_text` is shown
    to the model when the chunk has one. on_error: raise (default) | skip (leave the field empty)."""
    col = _text_column(df, step)
    mode = step.get("mode", "context")
    if mode not in _ENRICH_PROMPTS:
        raise ValueError(f"enrich op: mode must be one of {sorted(_ENRICH_PROMPTS)}, got {mode!r}")
    if not step.get("model"):
        raise ValueError("enrich op: `model` is required (a litellm model name)")
    on_error = step.get("on_error", "raise")
    if on_error not in ("raise", "skip"):
        raise ValueError(f"enrich op: on_error must be 'raise' or 'skip', got {on_error!r}")
    try:
        import litellm
    except ImportError:
        raise ImportError("enrich op requires: pip install litellm")
    out_col = step.get("output_column", f"enrichment_{mode}")
    out = df.copy()
    values: List[str] = []
    failures = 0
    for _, row in out.iterrows():
        passage = str(row[col])
        source = str(row["parent_text"]) if mode == "context" and "parent_text" in out.columns and pd.notna(row.get("parent_text")) else None
        user = (f"Source passage:\n{source}\n\nChunk:\n{passage}" if source else f"Passage:\n{passage}")
        try:
            resp = litellm.completion(
                model=step["model"], temperature=0, max_tokens=int(step.get("max_tokens", 200)),
                messages=[{"role": "system", "content": _ENRICH_PROMPTS[mode]}, {"role": "user", "content": user}],
            )
            values.append(str(resp.choices[0].message.content).strip())
        except Exception as exc:
            if on_error == "raise":
                raise
            failures += 1
            context.log.warning(f"enrich: skipped one chunk: {exc}")
            values.append("")
    out[out_col] = values
    if step.get("prepend"):
        out[col] = [f"{v}\n\n{t}" if v else t for v, t in zip(values, out[col].astype(str))]
        out = _rehash(out, col)
    out.attrs["stats"] = {"enriched": len(values) - failures, "failed": failures}
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

def _resolve_classify_fn(callable_path: str):
    """Resolve a `module.path:function_name` reference for classify mode='custom'. Same
    colon-separated convention as langgraph_agent's graph_fn / dynamic_fanout_asset / warm_scheduled_job's
    own `_resolve` (duplicated, not imported -- no shared files across components in this repo)."""
    if ":" not in callable_path:
        raise ValueError(
            f"classify op mode='custom': fn={callable_path!r} must be 'module.path:function_name' "
            f"(colon-separated), e.g. 'myproject.classifiers:classify_ticket'."
        )
    module_path, fn_name = callable_path.split(":", 1)
    try:
        mod = importlib.import_module(module_path)
    except ImportError as e:
        raise ImportError(f"classify op mode='custom': cannot import module {module_path!r}: {e}") from e
    try:
        return getattr(mod, fn_name)
    except AttributeError as e:
        raise AttributeError(f"classify op mode='custom': module {module_path!r} has no attribute {fn_name!r}") from e


def _do_classify(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Tag each chunk with a category before embedding/search -- three modes:

    - mode='zero_shot' (default): HuggingFace zero-shot classification --
      local, free, no API key. Same pattern as zero_shot_classifier
      (duplicated, not imported).
    - mode='llm': any litellm-supported model judges the category via a
      real completion call -- costs money per chunk, but can apply real
      judgment a fixed zero-shot label set can't (nuanced/ambiguous
      categories, multi-factor rules described in the prompt).
    - mode='custom': bring your own classifier -- `fn` is a
      'module.path:function_name' reference, called as
      `fn(text, candidate_labels) -> str` once per chunk. For anything a
      fixed zero-shot label set or a generic LLM prompt can't express: a
      trained scikit-learn/XGBoost model, a rules engine, a call to an
      in-house classification service, ensembling several of the above.
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

    elif mode == "custom":
        fn_path = step.get("fn")
        if not fn_path:
            raise ValueError("classify op mode='custom' requires `fn` ('module.path:function_name').")
        fn = _resolve_classify_fn(fn_path)

        categories = []
        for text in texts:
            try:
                category = fn(text, candidate_labels)
            except Exception as e:
                context.log.warning(f"classify (custom): fn={fn_path!r} failed for a chunk: {e}")
                category = None
            if category not in candidate_labels:
                if category is not None:
                    context.log.warning(f"classify (custom): fn={fn_path!r} returned {category!r}, not in candidate_labels; dropped to None")
                category = None
            categories.append(category)
        out[output_column] = categories
    else:
        raise ValueError(f"classify op: unsupported mode {mode!r}. Valid: 'zero_shot', 'llm', 'custom'.")

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

def _is_expired(value: Any, now: datetime) -> bool:
    if value is None or value == "":
        return False
    text = str(value)
    try:
        if len(text) == 10:
            return now >= datetime.fromisoformat(text).replace(tzinfo=timezone.utc) + timedelta(days=1)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return now >= (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc))
    except ValueError:
        return False


def _delete_in_batches(delete_fn, ids: List[str], size: int = 500) -> None:
    for i in range(0, len(ids), size):
        delete_fn(ids[i:i + size])


def _do_write_vector_store(df: pd.DataFrame, step: dict, id_column: str, context, stamp: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    """Index chunks. Knobs: provider (chromadb|pinecone|qdrant), sync_mode (replace_sources default |
    upsert | replace_all), batch_size, metadata_columns. `stamp` (see the component's stamp_provenance)
    writes source_asset_key / source_version / source_partition / chunk_id / content_hash /
    embedding_model / source_id onto every chunk so a retrieval gateway can build receipts straight from
    index hits. purge_expired (chromadb) deletes this asset's chunks whose valid_until has passed.
    sync_mode=replace_sources deletes chunks of the source rows in this run that no longer
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
    # Metadata is stringified, except the feedback_* columns: they stay typed (int / float / bool) so a gateway can
    # filter and sort on them (e.g. where={"feedback_quarantined": False}), which strings would not allow.
    for _col in meta_df.columns:
        if _col.startswith("feedback_"):
            meta_df[_col] = pd.Series(df[_col].tolist(), index=meta_df.index, dtype=object)
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
        # Chunks the receipts feedback quarantined (mode: exclude) are deleted by id: sync only cleans up documents that are still
        # present in this run, and a quarantined chunk may have been its document's only chunk.
        purge = [i for i in (step.get("_purge_chunk_ids") or []) if i not in new_ids]
        if purge:
            _delete_in_batches(lambda batch: collection.delete(ids=batch), purge)
        stats["quarantined_deleted"] = len(purge)
        if step.get("purge_expired"):
            # Physically remove chunks whose valid_until (set by the stamp op's ttl_days) has passed; a bare date is
            # valid through the end of that day. A gateway already withholds them as "stale"; this also frees the index.
            field = step.get("valid_until_field", "valid_until")
            current = collection.get(where=own, include=["metadatas"]) if own else collection.get(include=["metadatas"])
            now = datetime.now(timezone.utc)
            expired = [cid for cid, meta in zip(current["ids"], current["metadatas"])
                       if _is_expired((meta or {}).get(field), now)]
            _delete_in_batches(lambda batch: collection.delete(ids=batch), expired)
            stats["expired_deleted"] = len(expired)
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
    "enrich": lambda df, step, ctx: _do_enrich(df, step, ctx),
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


def _validate_retrieval_eval(cfg: Dict[str, Any], steps_cfg: List[dict], execution_mode: str = "python") -> Dict[str, Any]:
    unknown = set(cfg) - {"queries", "k", "min_recall", "min_mrr", "severity", "blocking"}
    if unknown:
        raise ValueError(f"retrieval_eval: unknown field(s) {sorted(unknown)}")
    queries = cfg.get("queries")
    if not isinstance(queries, list) or not queries:
        raise ValueError("retrieval_eval: `queries` must be a non-empty list of {query, expected: [chunk or source ids]}")
    for q in queries:
        if not isinstance(q, dict) or not str(q.get("query", "")).strip() or not q.get("expected"):
            raise ValueError(f"retrieval_eval: every query needs `query` and a non-empty `expected`; got {q!r}")
    if int(cfg.get("k", 5)) < 1:
        raise ValueError("retrieval_eval: k must be >= 1")
    for name in ("min_recall", "min_mrr"):
        if cfg.get(name) is not None and not 0 <= float(cfg[name]) <= 1:
            raise ValueError(f"retrieval_eval: {name} must be between 0 and 1")
    if cfg.get("severity", "ERROR") not in ("ERROR", "WARN"):
        raise ValueError("retrieval_eval: severity must be ERROR or WARN")
    if execution_mode == "sql":
        # the index IS the output table: it needs the embed step (its model embeds the golden queries) and nothing else
        if not any(s_["op"] == "embed" for s_ in steps_cfg):
            raise ValueError("retrieval_eval in execution_mode='sql' needs an embed step (the output table's `embedding` column is what it searches)")
        return dict(cfg)
    writes = [s_ for s_ in steps_cfg if s_["op"] == "write_vector_store"]
    if not any(s_["op"] == "embed" for s_ in steps_cfg) or not writes or writes[-1].get("provider", "chromadb") != "chromadb":
        raise ValueError("retrieval_eval needs an embed step and a chromadb write_vector_store step (the only index it can query)")
    return dict(cfg)


def _run_retrieval_eval(cfg: Dict[str, Any], steps_cfg: List[dict], asset_name: str, scoped: bool, context) -> AssetCheckResult:
    """Embed each golden query with the same embedding step, query the index this run just wrote, and report
    recall@k (a query is a hit when any expected chunk id or source id is in the top k) and MRR."""
    embed_step = next(s_ for s_ in steps_cfg if s_["op"] == "embed")
    write_step = [s_ for s_ in steps_cfg if s_["op"] == "write_vector_store"][-1]
    queries = cfg["queries"]
    k = int(cfg.get("k", 5))
    embedded = _do_embed_raw(
        pd.DataFrame({"chunk_text": [q["query"] for q in queries]}),
        {**embed_step, "text_column": "chunk_text", "output_column": "embedding"}, context)
    import chromadb
    collection = chromadb.PersistentClient(path=write_step.get("connection_string") or "./chroma_db").get_collection(
        write_step.get("collection_name", "context_engineering_kb"))
    where = {"source_asset_key": {"$eq": asset_name}} if scoped else None
    ranked: List[List[Tuple[str, Any]]] = []
    for embedding in embedded["embedding"]:
        res = collection.query(query_embeddings=[list(embedding)], n_results=k, where=where, include=["metadatas"])
        ranked.append([(cid, (meta or {}).get("source_id")) for cid, meta in zip(res["ids"][0], res["metadatas"][0])])
    return _retrieval_quality_result(cfg, ranked)


def _retrieval_quality_result(cfg: Dict[str, Any], ranked: List[List[Tuple[str, Any]]]) -> AssetCheckResult:
    """The single definition of the `retrieval_quality` check, shared by python and SQL mode. `ranked[i]` is query i's top-k as ordered
    (chunk_id, source_id) pairs. A query is a hit when any expected id equals a returned chunk id or source id; its reciprocal rank is
    1 / (position of the first such result); recall@k = hits / queries and MRR = mean reciprocal rank."""
    queries = cfg["queries"]
    k = int(cfg.get("k", 5))
    hits = 0
    reciprocal = 0.0
    misses: List[str] = []
    for q, results in zip(queries, ranked):
        expected = {str(e) for e in q["expected"]}
        rank = next((i for i, (cid, source_id) in enumerate(results) if cid in expected or source_id in expected), None)
        if rank is None:
            misses.append(q["query"])
        else:
            hits += 1
            reciprocal += 1.0 / (rank + 1)
    recall, mrr = hits / len(queries), reciprocal / len(queries)
    passed = recall >= float(cfg.get("min_recall", 0.8)) and (cfg.get("min_mrr") is None or mrr >= float(cfg["min_mrr"]))
    return AssetCheckResult(
        check_name="retrieval_quality", passed=passed,
        severity=AssetCheckSeverity[cfg.get("severity", "ERROR")],
        metadata={"recall_at_k": MetadataValue.float(round(recall, 4)), "mrr": MetadataValue.float(round(mrr, 4)),
                  "k": MetadataValue.int(k), "queries": MetadataValue.int(len(queries)),
                  "misses": MetadataValue.json(misses[:10])},
    )


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
# this dev environment. Every dialect's SQL is parsed with sqlglot in that
# dialect; the chunk/stamp/receipts statements of all three dialects are also executed
# (sqlglot -> DuckDB) in tests. The AI functions themselves (embedding,
# classification) are never run for real.

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


_SQL_DIALECTS = ("snowflake_cortex", "bigquery", "databricks")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TABLE_RE = re.compile(r"^`?[A-Za-z0-9_\-]+`?(\.`?[A-Za-z0-9_\-]+`?){0,3}$")
_MODEL_RE = re.compile(r"^[A-Za-z0-9_\-./:]+$")
_SQL_RESERVED_COLUMNS = {
    "chunk_id", "chunk_text", "chunk_index", "content_hash", "category", "embedding", "content", "category_signature", "embedding_model", "source_asset_key",
    "source_version", "source_partition", "source_code_version", "feedback_retrievals", "feedback_rated",
    "feedback_mean_outcome", "feedback_cited", "feedback_prior", "feedback_quarantined", "feedback_dead_weight", "feedback_first_seen_at",
    "ml_generate_embedding_result", "ml_generate_embedding_statistics", "ml_generate_embedding_status",
}


def _sql_ident(value: Any, what: str) -> str:
    if not isinstance(value, str) or not _IDENT_RE.match(value):
        raise ValueError(f"execution_mode='sql': {what}={value!r} is not a plain identifier (letters, digits, underscore; not starting with a digit)")
    return value


_BQ_PROJECT_RE = re.compile(r"^[A-Za-z0-9_\-]+$")
_BQ_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _sql_table(value: Any, what: str, dialect: Optional[str] = None) -> str:
    if not isinstance(value, str) or not _TABLE_RE.match(value):
        raise ValueError(f"execution_mode='sql': {what}={value!r} is not a valid table reference (up to 4 dot-separated parts of letters, digits, _ or -)")
    if dialect == "bigquery":
        # BigQuery: [project.]dataset.table, a dash only in the project id. Emitted as ONE backticked path everywhere (DDL, DML, FROM): the docs
        # only allow an unquoted dashed project id in FROM / TABLE clauses, never in CREATE / INSERT / DROP.
        parts = [p.strip("`") for p in value.split(".")]
        ok = len(parts) <= 3 and all(_BQ_NAME_RE.match(p) for p in parts[-2:]) and (len(parts) < 3 or _BQ_PROJECT_RE.match(parts[0]))
        if not ok:
            raise ValueError(f"execution_mode='sql': {what}={value!r} is not a valid BigQuery table reference ([project.]dataset.table; only the "
                             "project id may contain a dash, dataset and table names are letters, digits and _)")
        return "`" + ".".join(parts) + "`"
    return value


def _sql_str(value: Any) -> str:
    """A single-quoted string literal that is safe in BigQuery, Databricks (Spark) and Snowflake (all accept backslash escapes)."""
    s = str(value).replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r")
    return f"'{s}'"


def _sql_chunk_expr(dialect: str, text_expr: str, chunk_size: int, chunk_overlap: int) -> str:
    """An expression that yields the ordered array of (non-empty) chunk strings of `text_expr`."""
    chunk_size = int(chunk_size)
    step = max(chunk_size - int(chunk_overlap), 1)
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER({text_expr}, 'none', {chunk_size}, {int(chunk_overlap)})"
    if dialect == "bigquery":
        # BigQuery has no native recursive splitter: fixed-stride substrings (coarser than Cortex's splitter, see README).
        return (
            f"ARRAY(SELECT piece FROM (SELECT pos, SUBSTR({text_expr}, pos, {chunk_size}) AS piece "
            f"FROM UNNEST(GENERATE_ARRAY(1, LENGTH({text_expr}), {step})) AS pos) WHERE TRIM(piece) != '' ORDER BY pos)"
        )
    if dialect == "databricks":
        # greatest(): Spark raises on sequence(1, 0, step), which is what an empty string would give.
        return (
            f"filter(transform(sequence(1, greatest(length({text_expr}), 1), {step}), pos -> substr({text_expr}, pos, {chunk_size})), "
            f"piece -> length(trim(piece)) > 0)"
        )
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_check_classify_labels(dialect: str, candidate_labels: List[str]) -> None:
    """The documented hard limits of each warehouse's classifier, checked at build time (BigQuery's AI.CLASSIFY documents none)."""
    labels = [str(x) for x in candidate_labels]
    if dialect == "snowflake_cortex":
        # docs.snowflake.com/en/sql-reference/functions/classify_text-snowflake-cortex: 2 to 100 unique labels
        if not 2 <= len(labels) <= 100 or len(set(labels)) != len(labels):
            raise ValueError(f"classify with sql_dialect='snowflake_cortex' needs 2 to 100 unique candidate_labels (CLASSIFY_TEXT limit); "
                             f"got {len(labels)} label(s), {len(set(labels))} unique")
    elif dialect == "databricks":
        # docs: ai_classify (v1) takes 2 to 20 labels, each 1 to 50 characters
        if not 2 <= len(labels) <= 20:
            raise ValueError(f"classify with sql_dialect='databricks' needs 2 to 20 candidate_labels (ai_classify limit); got {len(labels)}")
        bad = [x for x in labels if not 1 <= len(x) <= 50]
        if bad:
            raise ValueError(f"classify with sql_dialect='databricks': every candidate label must be 1 to 50 characters (ai_classify limit); offending: {bad!r}")


def _sql_classify_expr(dialect: str, text_expr: str, candidate_labels: List[str]) -> str:
    labels_sql = ", ".join(_sql_str(label) for label in candidate_labels)
    if dialect == "snowflake_cortex":
        # CLASSIFY_TEXT returns an OBJECT {"label": "..."}, not the label: take the field (docs.snowflake.com .../classify_text-snowflake-cortex).
        return f"SNOWFLAKE.CORTEX.CLASSIFY_TEXT({text_expr}, ARRAY_CONSTRUCT({labels_sql})):label::STRING"
    if dialect == "bigquery":
        return f"AI.CLASSIFY({text_expr}, [{labels_sql}])"
    if dialect == "databricks":
        return f"ai_classify({text_expr}, ARRAY({labels_sql}))"
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_embed_expr(dialect: str, text_expr: str, model: Optional[str]) -> str:
    """Scalar embedding expression (Snowflake, Databricks). BigQuery's ML.GENERATE_EMBEDDING is a TABLE function: see _build_sql_mode_query."""
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.EMBED_TEXT_768({_sql_str(model or 'snowflake-arctic-embed-m')}, {text_expr})"
    if dialect == "databricks":
        return f"ai_query({_sql_str(model or 'databricks-gte-large-en')}, {text_expr})"
    if dialect == "bigquery":
        raise ValueError("BigQuery embeds with the ML.GENERATE_EMBEDDING table function, not a scalar expression")
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_enrich_prompt_expr(mode: str, text_expr: str) -> str:
    """The fixed prompt + passage text every enrich mode sends to the model (see _ENRICH_PROMPTS) --
    CONCAT is the same call in all 3 dialects, so this one expression covers both BigQuery's
    ML.GENERATE_TEXT prompt column and the Snowflake/Databricks scalar form. SQL mode never has
    parent-child chunking, so there is no parent_text to show the model -- always the plain
    "Passage:\\n<text>" shape, same as python mode's own fallback when a chunk has no parent."""
    return f"CONCAT({_sql_str(_ENRICH_PROMPTS[mode])}, {_sql_str(chr(10) + chr(10) + 'Passage:' + chr(10))}, {text_expr})"


def _sql_enrich_expr(dialect: str, text_expr: str, model: str, mode: str) -> str:
    """Scalar text-generation expression (Snowflake, Databricks). BigQuery's ML.GENERATE_TEXT is a
    TABLE function: see _build_sql_mode_query."""
    prompt = _sql_enrich_prompt_expr(mode, text_expr)
    if dialect == "snowflake_cortex":
        return f"SNOWFLAKE.CORTEX.COMPLETE({_sql_str(model)}, {prompt})"
    if dialect == "databricks":
        return f"ai_query({_sql_str(model)}, {prompt})"
    if dialect == "bigquery":
        raise ValueError("BigQuery generates text with the ML.GENERATE_TEXT table function, not a scalar expression")
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _sql_embed_model_id(dialect: str, model: Optional[str]) -> str:
    """The model identity stored next to every embedding (column embedding_model): a stored vector is only reused under the same one."""
    if dialect == "bigquery":
        return model or "embedding_model"
    return model or ("snowflake-arctic-embed-m" if dialect == "snowflake_cortex" else "databricks-gte-large-en")


def _sql_category_signature(candidate_labels: List[str]) -> str:
    """Identity of the label set (column category_signature): a stored category is only reused under the same labels, in the same order."""
    return hashlib.sha256(json.dumps([str(x) for x in candidate_labels]).encode("utf-8")).hexdigest()[:16]


def _sql_hash_expr(dialect: str, text_expr: str) -> str:
    return f"TO_HEX(SHA256({text_expr}))" if dialect == "bigquery" else f"sha2({text_expr}, 256)"


def _sql_version_agg(dialect: str) -> str:
    """Aggregate over (chunk_id, content_hash) columns: the content version of the whole table (same in-statement and on read-back)."""
    if dialect == "bigquery":
        return "TO_HEX(MD5(STRING_AGG(content_hash, '' ORDER BY chunk_id)))"
    if dialect == "databricks":
        # Sort a plain string (chunk_id, a space, the 64-hex content_hash), not an array of structs: struct ordering is not documented. The space sorts
        # below every printable id character, so this is chunk_id order (the same order, hence the same version, as the other dialects).
        return ("md5(array_join(transform(sort_array(collect_list(concat(CAST(chunk_id AS STRING), ' ', content_hash))), "
                "x -> right(x, 64)), ''))")
    return "MD5(LISTAGG(content_hash, '') WITHIN GROUP (ORDER BY chunk_id))"


_RECEIPT_DIALECT_OF = {"bigquery": "bigquery", "snowflake_cortex": "snowflake", "databricks": "databricks"}


def _sql_ts(iso: str, dialect: str = "bigquery") -> str:
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    text = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")
    if dialect == "snowflake_cortex":
        # a bare TIMESTAMP '...' is TIMESTAMP_NTZ in Snowflake (offset dropped, session-zone semantics); receipts' created_at is TIMESTAMP_TZ
        return f"CAST('{text}' AS TIMESTAMP_TZ)"
    return f"TIMESTAMP '{text}'"


def _sql_json_value(dialect: str, path: str, as_type: Optional[str] = None) -> str:
    """Scalar at `$.a.b` of the JSON text column payload_json, as a string (or CAST to `as_type`: 'FLOAT')."""
    if dialect == "snowflake_cortex":
        keys = path[2:]  # '$.a.b' -> 'a.b'
        return f"PARSE_JSON(payload_json):{keys}::{'FLOAT' if as_type else 'STRING'}"
    if dialect == "databricks":
        raw = f"get_json_object(payload_json, '{path}')"
        return f"CAST({raw} AS DOUBLE)" if as_type else raw
    raw = f"JSON_VALUE(payload_json, '{path}')"
    return f"CAST({raw} AS FLOAT64)" if as_type else raw


def _sql_count_if(dialect: str, cond: str) -> str:
    return f"COUNTIF({cond})" if dialect == "bigquery" else f"COUNT_IF({cond})"


def _sql_outcome_ctes(events: str, dialect: str = "bigquery") -> List[Tuple[str, str]]:
    """CTEs giving each receipt's effective outcome, exactly as ContextReceiptStore counts it: the mean over its outcome_refs, a later event
    with the same outcome_ref superseding the earlier; an outcome without a ref never supersedes anything (keyed by its own event id)."""
    return [
        ("outcome_events", (
            f"SELECT receipt_id, COALESCE(NULLIF({_sql_json_value(dialect, '$.outcome_ref')}, ''), event_id) AS outcome_key,\n"
            f"         {_sql_json_value(dialect, '$.value', 'float')} AS outcome_value, occurred_at, event_id\n"
            f"  FROM {events} WHERE event_type = 'outcome'")),
        ("receipt_outcome", (
            "SELECT receipt_id, AVG(outcome_value) AS effective_outcome FROM (\n"
            "    SELECT receipt_id, outcome_value,\n"
            "           ROW_NUMBER() OVER (PARTITION BY receipt_id, outcome_key ORDER BY occurred_at DESC, event_id DESC) AS rn\n"
            "    FROM outcome_events) AS ranked WHERE rn = 1 GROUP BY receipt_id")),
    ]


def _sql_citing_ctes(events: str, dialect: str) -> List[Tuple[str, str]]:
    """`citing_events`: one row per cited/verified/counterfactual_tested evidence event with `is_scoped` (details.item_ids is a JSON array)
    and, when scoped, `scoped_ids` (the array; possibly empty = scoped to nothing)."""
    where = f"event_type = 'evidence' AND {_sql_json_value(dialect, '$.level')} IN ('cited', 'verified', 'counterfactual_tested')"
    if dialect == "snowflake_cortex":
        return [
            ("citing_raw", f"SELECT receipt_id, PARSE_JSON(payload_json):details.item_ids AS ids\n  FROM {events}\n  WHERE {where}"),
            ("citing_events", ("SELECT receipt_id, COALESCE(IS_ARRAY(ids), FALSE) AS is_scoped,\n"
                               "         IFF(COALESCE(IS_ARRAY(ids), FALSE), TO_ARRAY(ids), ARRAY_CONSTRUCT()) AS scoped_ids\n  FROM citing_raw")),
        ]
    if dialect == "databricks":
        return [
            ("citing_raw", f"SELECT receipt_id, get_json_object(payload_json, '$.details.item_ids') AS ids\n  FROM {events}\n  WHERE {where}"),
            ("citing_events", ("SELECT receipt_id, COALESCE(left(ids, 1) = '[', FALSE) AS is_scoped,\n"
                               "         CASE WHEN COALESCE(left(ids, 1) = '[', FALSE) THEN from_json(ids, 'ARRAY<STRING>') END AS scoped_ids\n  FROM citing_raw")),
        ]
    return [("citing_events", (
        f"SELECT receipt_id, payload_json,\n"
        f"         COALESCE(STARTS_WITH(JSON_QUERY(payload_json, '$.details.item_ids'), '['), FALSE) AS is_scoped\n"
        f"  FROM {events}\n  WHERE {where}"))]


def _sql_receipt_feedback_ctes(rcfg: Dict[str, Any], asset_name: str, recipe: Optional[str], since: str,
                               dialect: str = "bigquery") -> List[Tuple[str, str]]:
    """Warehouse-side ContextReceiptStore.item_feedback: retrievals / rated / mean_outcome / cited per item, as one `item_feedback` CTE."""
    rd = _RECEIPT_DIALECT_OF[dialect]
    tbl = lambda n: receipt_table(rcfg["dataset"], n, rcfg.get("table_prefix", ""), rd)  # noqa: E731
    recipe_filter = f" AND COALESCE(i.source_code_version, 'unknown') = {_sql_str(recipe)}" if recipe is not None else ""
    if dialect == "bigquery":
        cite = [
            ("cite_scope", (
                "SELECT receipt_id, CAST(NULL AS STRING) AS item_id FROM citing_events WHERE NOT is_scoped\n"
                "  UNION ALL\n"
                "  SELECT e.receipt_id, scoped_id FROM (\n"
                "    SELECT receipt_id, CASE WHEN is_scoped THEN JSON_VALUE_ARRAY(payload_json, '$.details.item_ids') END AS ids FROM citing_events) AS e,\n"
                "    UNNEST(e.ids) AS scoped_id")),
            ("cited_pairs", (
                "SELECT DISTINCT h.receipt_id, h.item_id FROM item_hits AS h\n"
                "  JOIN cite_scope AS c ON c.receipt_id = h.receipt_id AND (c.item_id IS NULL OR c.item_id = h.item_id)")),
        ]
    else:
        contains = "ARRAY_CONTAINS(h.item_id::VARIANT, c.scoped_ids)" if dialect == "snowflake_cortex" else "array_contains(c.scoped_ids, h.item_id)"
        cite = [("cited_pairs", (
            "SELECT DISTINCT h.receipt_id, h.item_id FROM item_hits AS h\n"
            f"  JOIN citing_events AS c ON c.receipt_id = h.receipt_id AND (NOT c.is_scoped OR {contains})"))]
    return _sql_outcome_ctes(tbl("context_receipt_events"), dialect) + [
        ("item_hits", (
            f"SELECT DISTINCT i.receipt_id, i.item_id FROM {tbl('context_receipt_items')} AS i\n"
            f"  JOIN {tbl('context_receipts')} AS r ON r.receipt_id = i.receipt_id\n"
            f"  WHERE i.source_asset_key = {_sql_str(asset_name)} AND r.created_at >= {_sql_ts(since, dialect)}{recipe_filter}")),
    ] + _sql_citing_ctes(tbl("context_receipt_events"), dialect) + cite + [
        ("item_feedback", (
            f"SELECT h.item_id, COUNT(*) AS retrievals, {_sql_count_if(dialect, 'o.effective_outcome IS NOT NULL')} AS rated,\n"
            f"         ROUND(AVG(o.effective_outcome), 4) AS mean_outcome, {_sql_count_if(dialect, 'c.receipt_id IS NOT NULL')} AS cited\n"
            "  FROM item_hits AS h\n"
            "  LEFT JOIN receipt_outcome AS o ON o.receipt_id = h.receipt_id\n"
            "  LEFT JOIN cited_pairs AS c ON c.receipt_id = h.receipt_id AND c.item_id = h.item_id\n"
            "  GROUP BY h.item_id")),
    ]


def _sql_window_receipts_cte(rcfg: Dict[str, Any], asset_name: str, recipe: Optional[str], since: str, dialect: str) -> str:
    """One row (n): receipts in the window that retrieved from this asset (this recipe only when scoped: see _apply_receipt_feedback)."""
    tbl = lambda n: receipt_table(rcfg["dataset"], n, rcfg.get("table_prefix", ""), _RECEIPT_DIALECT_OF[dialect])  # noqa: E731
    recipe_filter = f" AND COALESCE(i.source_code_version, 'unknown') = {_sql_str(recipe)}" if recipe is not None else ""
    return (f"SELECT COUNT(DISTINCT i.receipt_id) AS n FROM {tbl('context_receipt_items')} AS i\n"
            f"  JOIN {tbl('context_receipts')} AS r ON r.receipt_id = i.receipt_id\n"
            f"  WHERE i.source_asset_key = {_sql_str(asset_name)} AND r.created_at >= {_sql_ts(since, dialect)}{recipe_filter}")


def _sql_dead_expr(prune: Dict[str, Any], dialect: str, now_iso: str) -> str:
    """Dead weight, over feedback_joined's columns: never retrieved in the window, the window had >= never_retrieved_after_receipts receipts
    (absence is no evidence without traffic), and the chunk is at least min_age_days old (first_seen_at; a chunk that has not existed
    for the whole window has had no chance to be retrieved)."""
    cutoff = (datetime.fromisoformat(now_iso) - timedelta(days=float(prune["min_age_days"]))).isoformat()
    return (f"(feedback_retrievals = 0 AND (SELECT n FROM window_receipts) >= {int(prune['never_retrieved_after_receipts'])} "
            f"AND feedback_first_seen_at <= {_sql_ts(cutoff, dialect)})")


def _sql_outcomes_by_code_version_query(rcfg: Dict[str, Any], asset_name: str, since: str, dialect: Optional[str] = None) -> str:
    """SQL equivalent of ContextReceiptStore.outcomes_by_code_version (in the receipts' warehouse dialect: rcfg['sql_dialect'], default
    bigquery). Columns: code_version, receipts, outcomes, mean_outcome."""
    dialect = dialect or rcfg.get("sql_dialect", "bigquery")
    tbl = lambda n: receipt_table(rcfg["dataset"], n, rcfg.get("table_prefix", ""), _RECEIPT_DIALECT_OF[dialect])  # noqa: E731
    ctes = _sql_outcome_ctes(tbl("context_receipt_events"), dialect) + [
        ("receipt_versions", (
            f"SELECT DISTINCT i.receipt_id, COALESCE(i.source_code_version, 'unknown') AS code_version\n"
            f"  FROM {tbl('context_receipt_items')} AS i JOIN {tbl('context_receipts')} AS r ON r.receipt_id = i.receipt_id\n"
            f"  WHERE i.source_asset_key = {_sql_str(asset_name)} AND r.created_at >= {_sql_ts(since, dialect)}")),
    ]
    with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
    return (f"WITH {with_}\n"
            f"SELECT v.code_version, COUNT(*) AS receipts, {_sql_count_if(dialect, 'o.effective_outcome IS NOT NULL')} AS outcomes,\n"
            "       ROUND(AVG(o.effective_outcome), 4) AS mean_outcome\n"
            "FROM receipt_versions AS v LEFT JOIN receipt_outcome AS o ON o.receipt_id = v.receipt_id\n"
            "GROUP BY v.code_version ORDER BY v.code_version")


def _sql_chunk_and_stamp_stage(
    chunk_step: dict, source_cfg: dict, dialect: str, id_column: str, text_column: str, metadata_columns: List[str],
    *, asset_name: str, code_version: str, partition: Optional[str] = None, stamp_provenance: bool = False,
    receipts: Optional[Dict[str, Any]] = None, since: Optional[str] = None, now: Optional[str] = None,
    prior_first_seen: bool = False, out_tbl: Optional[str] = None,
) -> "tuple[List[Tuple[str, str]], List[str], str]":
    """The chunk -> stamp (chunk_id/content_hash) -> [receipts feedback] -> [provenance] CTE chain.

    Extracted so the single-statement SQL-mode path (`_build_sql_mode_query`) and the per_step_ops
    path's 'chunks' stage (`_build_sql_chunks_table_query`) stay byte-identical by construction instead
    of by discipline -- this is the exact code both call, not a parallel reimplementation.

    `out_tbl` (the formatted reference to the EXISTING physical output/chunks table) is only read when
    `receipts.prune` and `prior_first_seen` are both set -- it carries forward each chunk's
    `feedback_first_seen_at` stamp from the table's current contents. Required in that case.

    Returns (ctes, cols, cur): the CTE list built so far, the column list each CTE selects, and the
    name of the last (current) CTE a caller should SELECT FROM to continue the chain.
    """
    id_col = _sql_ident(id_column, "id_column")
    text_col = _sql_ident(text_column, "text_column")
    metas = [_sql_ident(c, "metadata_columns entry") for c in metadata_columns]
    for c in [id_col] + metas:
        if c.lower() in _SQL_RESERVED_COLUMNS:
            raise ValueError(f"execution_mode='sql': column {c!r} collides with a column the component writes ({sorted(_SQL_RESERVED_COLUMNS)[:6]}...); alias it in `source.sql`")
    if len({id_col, *metas}) != len(metas) + 1:
        raise ValueError("execution_mode='sql': id_column / metadata_columns contain duplicates")

    chunk_size = int(chunk_step.get("chunk_size", 500))
    chunk_overlap = int(chunk_step.get("chunk_overlap", 50))
    chunk_expr = _sql_chunk_expr(dialect, f"src.{text_col}", chunk_size, chunk_overlap)
    meta_select = "".join(f",\n         src.{c} AS {c}" for c in metas)
    head = f"src.{id_col} AS {id_col}"
    if dialect == "snowflake_cortex":
        chunk_cte = (f"SELECT {head},\n         chunk.value::string AS chunk_text,\n         chunk.index AS chunk_index{meta_select}\n"
                     f"  FROM ({source_cfg['sql']}) AS src,\n       LATERAL FLATTEN(input => {chunk_expr}) AS chunk")
    elif dialect == "bigquery":
        chunk_cte = (f"SELECT {head},\n         chunk_text,\n         chunk_index{meta_select}\n"
                     f"  FROM ({source_cfg['sql']}) AS src,\n       UNNEST({chunk_expr}) AS chunk_text WITH OFFSET AS chunk_index")
    else:
        chunk_cte = (f"SELECT {head},\n         chunks.chunk_text AS chunk_text,\n         chunks.chunk_index AS chunk_index{meta_select}\n"
                     f"  FROM ({source_cfg['sql']}) AS src\n  LATERAL VIEW posexplode({chunk_expr}) chunks AS chunk_index, chunk_text")

    prefix = _sql_str(chunk_step.get("chunk_id_prefix") or "")
    cols = [id_col, "chunk_text", "chunk_index", *metas]
    ctes: List[Tuple[str, str]] = [("chunked", chunk_cte)]
    cols_sql = lambda alias, cs: ", ".join(f"{alias}.{c}" for c in cs)  # noqa: E731

    ctes.append(("stamped", (
        f"SELECT {cols_sql('chunked', cols)},\n"
        f"         CONCAT({prefix}, CAST(chunked.{id_col} AS STRING), '_', CAST(chunked.chunk_index AS STRING)) AS chunk_id,\n"
        f"         {_sql_hash_expr(dialect, 'chunked.chunk_text')} AS content_hash\n"
        f"  FROM chunked")))
    cols += ["chunk_id", "content_hash"]
    cur = "stamped"

    if receipts is not None:
        recipe = code_version if receipts.get("scope_to_recipe", True) else None
        ctes += _sql_receipt_feedback_ctes(receipts, asset_name, recipe, since or _receipts_since(receipts), dialect)
        scores = receipt_table(receipts["dataset"], "context_item_scores", receipts.get("table_prefix", ""), _RECEIPT_DIALECT_OF[dialect])
        pr = receipts.get("prune")
        now_iso = now or datetime.now(timezone.utc).isoformat()
        seen_select = seen_join = ""
        if pr:
            ctes.append(("window_receipts", _sql_window_receipts_cte(receipts, asset_name, recipe, since or _receipts_since(receipts), dialect)))
            if prior_first_seen:
                ctes.append(("prior_seen", f"SELECT chunk_id, MIN(feedback_first_seen_at) AS first_seen\n  FROM {out_tbl}\n  GROUP BY chunk_id"))
                seen_join = "\n  LEFT JOIN prior_seen AS ps ON ps.chunk_id = stamped.chunk_id"
            seen_select = f",\n         COALESCE({'ps.first_seen, ' if prior_first_seen else ''}{_sql_ts(now_iso, dialect)}) AS feedback_first_seen_at"
        ctes.append(("feedback_joined", (
            f"SELECT {cols_sql('stamped', cols)},\n"
            f"         COALESCE(fb.retrievals, 0) AS feedback_retrievals, COALESCE(fb.rated, 0) AS feedback_rated,\n"
            f"         COALESCE(fb.mean_outcome, 0.0) AS feedback_mean_outcome, COALESCE(fb.cited, 0) AS feedback_cited,\n"
            f"         COALESCE(sc.prior, 0.0) AS feedback_prior{seen_select}\n"
            f"  FROM stamped\n  LEFT JOIN item_feedback AS fb ON fb.item_id = stamped.chunk_id\n"
            f"  LEFT JOIN {scores} AS sc ON sc.item_id = stamped.chunk_id{seen_join}")))
        dead = _sql_dead_expr(pr, dialect, now_iso) if pr else None
        q = receipts.get("quarantine")
        bad = None
        if q:
            bad = (f"(feedback_retrievals >= {int(receipts['min_retrievals'])} AND feedback_rated >= {int(receipts['min_outcomes'])} "
                   f"AND feedback_mean_outcome <= {float(q['mean_outcome_below'])!r})")
        fb_cols = (list(_FEEDBACK_COLUMNS) if receipts.get("stamp", True) else []) + (["feedback_first_seen_at"] if pr else [])
        select = list(cols) + fb_cols
        out_flags: List[str] = []
        excl: List[str] = []
        if bad and q["mode"] == "flag":
            select.append(f"{bad} AS feedback_quarantined")
            out_flags.append("feedback_quarantined")
        elif bad:
            excl.append(f"NOT {bad}")
        if pr and pr["mode"] == "flag":
            select.append(f"{dead} AS feedback_dead_weight")
            out_flags.append("feedback_dead_weight")
        elif pr:
            excl.append(f"NOT {dead}")
        where = f"\n  WHERE {' AND '.join(excl)}" if excl else ""
        ctes.append(("feedback_applied", f"SELECT {', '.join(select)}\n  FROM feedback_joined{where}"))
        cols = cols + fb_cols + out_flags
        cur = "feedback_applied"

    if stamp_provenance:
        part = _sql_str(partition) if partition is not None else "CAST(NULL AS STRING)"
        ctes.append(("versioned", (
            f"SELECT {cols_sql(cur, cols)},\n"
            f"         {_sql_str(asset_name)} AS source_asset_key,\n"
            f"         (SELECT {_sql_version_agg(dialect)} FROM {cur}) AS source_version,\n"
            f"         {part} AS source_partition,\n"
            f"         {_sql_str(code_version)} AS source_code_version\n"
            f"  FROM {cur}")))
        cols += ["source_asset_key", "source_version", "source_partition", "source_code_version"]
        cur = "versioned"
    elif partition is not None:
        ctes.append(("versioned", f"SELECT {cols_sql(cur, cols)}, {_sql_str(partition)} AS source_partition\n  FROM {cur}"))
        cols += ["source_partition"]
        cur = "versioned"

    return ctes, cols, cur


# ── filter / dedupe / sensitivity / stamp in execution_mode='sql' ──────────────────────────────────────
# Portable across bigquery/snowflake_cortex/databricks because REGEXP_CONTAINS/REGEXP_LIKE/RLIKE and
# REGEXP_REPLACE take the same (value, pattern[, replacement]) argument order in all three; the regex
# SYNTAX itself is also compatible for the fixed, component-authored patterns below (character classes,
# \b, \d, \w, \s, quantifiers, non-capturing groups) -- none of them use lookaround or backreferences.

def _sql_regexp_contains(dialect: str, expr: str, pattern_literal: str) -> str:
    if dialect == "bigquery":
        return f"REGEXP_CONTAINS({expr}, {pattern_literal})"
    if dialect == "snowflake_cortex":
        return f"REGEXP_LIKE({expr}, {pattern_literal})"
    return f"({expr} RLIKE {pattern_literal})"  # databricks / Spark SQL


def _sql_regexp_replace(dialect: str, expr: str, pattern_literal: str, replacement_literal: str) -> str:
    return f"REGEXP_REPLACE({expr}, {pattern_literal}, {replacement_literal})"  # same arg order, all 3 dialects


_UNSAFE_REGEX_RE = re.compile(r"\(\?[=!]|\(\?<[=!]|\\[1-9]|\(\?P=")


def _validate_portable_regex(pattern: str, what: str) -> None:
    """BigQuery's regex engine (RE2, used by REGEXP_CONTAINS/REGEXP_REPLACE) has no lookahead,
    lookbehind or backreferences -- a pattern using any of those cannot be translated portably across
    bigquery/snowflake_cortex/databricks. Reject loudly at build time instead of silently mistranslating
    or only failing on one dialect."""
    if _UNSAFE_REGEX_RE.search(pattern):
        raise ValueError(
            f"execution_mode='sql': {what} pattern {pattern!r} uses lookahead/lookbehind/backreferences, "
            f"which BigQuery's RE2 regex engine does not support -- not portable across "
            f"bigquery/snowflake_cortex/databricks. Simplify the pattern, or use execution_mode='python' "
            f"where Python's `re` module supports it."
        )


_SQL_PII_PATTERNS: Dict[str, str] = {
    "email": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
    "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
    "ipv4": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
}
# phone's python pattern needs (?<!\d)/(?!\d) (negative lookbehind/lookahead) -- not RE2-portable.
# credit_card's value in python mode comes from Luhn-checking the regex matches, which has no portable
# SQL equivalent implemented here; a regex-only credit_card match would have a much higher false-positive
# rate (catching phone numbers, account numbers, ...) than python mode's, so it is excluded rather than
# silently shipped weaker.
_SQL_PII_UNSUPPORTED: Dict[str, str] = {
    "phone": "its pattern needs a negative lookahead/lookbehind that BigQuery's RE2 engine does not support",
    "credit_card": "its Luhn-check validation (what keeps its false-positive rate down in python mode) has no portable SQL equivalent here",
}

_STAMP_TEMPLATE_RE = re.compile(r"^(?:[^{}]|\{[a-zA-Z_][a-zA-Z0-9_]*\})*$")
_STAMP_PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def _build_sql_mode_query(
    steps_cfg: List[dict], source_cfg: dict, dialect: str, output_table: str,
    id_column: str, text_column: str, metadata_columns: List[str],
    *, asset_name: str = "", code_version: str = "", partition: Optional[str] = None,
    stamp_provenance: bool = False, receipts: Optional[Dict[str, Any]] = None, since: Optional[str] = None,
    incremental: bool = False, reuse: Optional[Dict[str, bool]] = None, counts_only: bool = False,
    write: str = "replace_table", stage_table: Optional[str] = None,
    now: Optional[str] = None, prior_first_seen: bool = False, preview: Optional[str] = None,
) -> str:
    """Build the single CREATE OR REPLACE TABLE statement implementing chunk -> (stamp, receipts feedback) -> classify -> embed natively in the
    warehouse, in that warehouse's own SQL dialect. write_vector_store has no SQL equivalent (there's no in-warehouse vector index for these
    dialects that behaves like chromadb/pinecone/qdrant) so it's skipped in this mode -- point rag_pipeline's `retrieve` op straight at
    output_table instead.

    Output columns: <id_column>, chunk_text, chunk_index, <metadata_columns>, chunk_id, content_hash; with stamp_provenance also
    source_asset_key / source_version / source_partition / source_code_version; with receipts feedback_* (see _validate_receipts); then
    category + category_signature (classify) and embedding + embedding_model (embed).

    incremental=True: the AI functions only see chunks with no reusable value. `reuse` = {"embed": bool, "classify": bool} says which
    features may read prior values from the EXISTING output table (the caller probed that it exists and has the needed columns; a missing
    table cannot be referenced by the statement at all). A prior value is reusable when its content_hash matches AND its embedding_model /
    category_signature equals the current one, so a changed model or label set re-computes everything. The statement stays one atomic CREATE OR
    REPLACE: chunk -> stamp -> (receipts) -> per feature [prior values (one row per content_hash) | todo = distinct chunk texts with no prior
    value -> AI function over todo only] -> join back on content_hash. counts_only=True returns instead a SELECT of total_rows /
    new_embeddings / new_categories (chunks that will be computed) over the same CTEs, without running any AI function.

    write: 'replace_table' (CREATE OR REPLACE TABLE <output_table>: unpartitioned runs), 'create_table' (CREATE TABLE <output_table>: the first
    partition run, the table must not exist) or 'stage' (CREATE OR REPLACE TABLE <stage_table>: later partition runs, which then swap only their own
    rows into output_table, see _run_sql_mode). A partitioned run (partition is not None) always writes source_partition.

    receipts.prune: every chunk gets feedback_first_seen_at (the earlier build's value, read from the existing output table when
    `prior_first_seen` says it exists with that column, else `now`); feedback_dead_weight (flag) or a WHERE (exclude) follows
    _sql_dead_expr. preview='rewrite' | 'counts' returns instead a SELECT over the same CTEs up to the feedback join (no AI function
    runs): the rewrite candidates, or (dead chunks, receipts in window, chunks) -- read BEFORE the table is replaced, so chunks the
    statement is about to drop are still reported."""
    if preview is not None and receipts is None:
        raise ValueError("preview needs receipts")
    if dialect not in _SQL_DIALECTS:
        raise ValueError(f"unsupported sql_dialect: {dialect!r}")
    if write not in ("replace_table", "create_table", "stage") or (write == "stage") != (stage_table is not None):
        raise ValueError("write must be replace_table / create_table, or stage together with stage_table")
    chunk_step = next(s for s in steps_cfg if s["op"] == "chunk")
    classify_step = next((s for s in steps_cfg if s["op"] == "classify"), None)
    embed_step = next((s for s in steps_cfg if s["op"] == "embed"), None)
    enrich_step = next((s for s in steps_cfg if s["op"] == "enrich"), None)
    out_tbl = _sql_table(output_table, "output_table", dialect)
    target = {"replace_table": f"CREATE OR REPLACE TABLE {out_tbl}", "create_table": f"CREATE TABLE {out_tbl}",
              "stage": f"CREATE OR REPLACE TABLE {stage_table}"}[write]
    id_col = _sql_ident(id_column, "id_column")
    text_col = _sql_ident(text_column, "text_column")
    metas = [_sql_ident(c, "metadata_columns entry") for c in metadata_columns]
    for c in [id_col] + metas:
        if c.lower() in _SQL_RESERVED_COLUMNS:
            raise ValueError(f"execution_mode='sql': column {c!r} collides with a column the component writes ({sorted(_SQL_RESERVED_COLUMNS)[:6]}...); alias it in `source.sql`")
    if len({id_col, *metas}) != len(metas) + 1:
        raise ValueError("execution_mode='sql': id_column / metadata_columns contain duplicates")

    chunk_size = int(chunk_step.get("chunk_size", 500))
    chunk_overlap = int(chunk_step.get("chunk_overlap", 50))
    chunk_expr = _sql_chunk_expr(dialect, f"src.{text_col}", chunk_size, chunk_overlap)
    meta_select = "".join(f",\n         src.{c} AS {c}" for c in metas)
    head = f"src.{id_col} AS {id_col}"
    if dialect == "snowflake_cortex":
        chunk_cte = (f"SELECT {head},\n         chunk.value::string AS chunk_text,\n         chunk.index AS chunk_index{meta_select}\n"
                     f"  FROM ({source_cfg['sql']}) AS src,\n       LATERAL FLATTEN(input => {chunk_expr}) AS chunk")
    elif dialect == "bigquery":
        chunk_cte = (f"SELECT {head},\n         chunk_text,\n         chunk_index{meta_select}\n"
                     f"  FROM ({source_cfg['sql']}) AS src,\n       UNNEST({chunk_expr}) AS chunk_text WITH OFFSET AS chunk_index")
    else:
        chunk_cte = (f"SELECT {head},\n         chunks.chunk_text AS chunk_text,\n         chunks.chunk_index AS chunk_index{meta_select}\n"
                     f"  FROM ({source_cfg['sql']}) AS src\n  LATERAL VIEW posexplode({chunk_expr}) chunks AS chunk_index, chunk_text")

    prefix = _sql_str(chunk_step.get("chunk_id_prefix") or "")
    cols = [id_col, "chunk_text", "chunk_index", *metas]
    ctes: List[Tuple[str, str]] = [("chunked", chunk_cte)]
    cols_sql = lambda alias, cs: ", ".join(f"{alias}.{c}" for c in cs)  # noqa: E731

    ctes.append(("stamped", (
        f"SELECT {cols_sql('chunked', cols)},\n"
        f"         CONCAT({prefix}, CAST(chunked.{id_col} AS STRING), '_', CAST(chunked.chunk_index AS STRING)) AS chunk_id,\n"
        f"         {_sql_hash_expr(dialect, 'chunked.chunk_text')} AS content_hash\n"
        f"  FROM chunked")))
    cols += ["chunk_id", "content_hash"]
    cur = "stamped"
    now_iso = now or datetime.now(timezone.utc).isoformat()

    # filter / dedupe / sensitivity (tag|drop) / stamp, in steps_cfg order -- always positioned here
    # (before receipts feedback and classify/embed) regardless of where they appear in `steps:`, so
    # dropped/deduped chunks never pay for a receipts-feedback join or a warehouse AI function call.
    # None of them touch chunk_text/content_hash (sensitivity's action='redact', which would need
    # content_hash recomputed afterward, is rejected below instead), so this fixed position is safe.
    for _step in steps_cfg:
        _op = _step["op"]
        if _op == "filter":
            _text = f"{cur}.chunk_text"
            _preds: List[str] = []
            if _step.get("min_chars") is not None:
                _preds.append(f"LENGTH({_text}) >= {int(_step['min_chars'])}")
            if _step.get("max_chars") is not None:
                _preds.append(f"LENGTH({_text}) <= {int(_step['max_chars'])}")
            if _step.get("min_alnum_ratio") is not None:
                _alnum = _sql_regexp_replace(dialect, _text, _sql_str(r"[^a-zA-Z0-9]"), _sql_str(""))
                _preds.append(f"(CASE WHEN LENGTH({_text}) = 0 THEN 0.0 ELSE LENGTH({_alnum}) / LENGTH({_text}) END) >= {float(_step['min_alnum_ratio'])!r}")
            for _pattern in _step.get("exclude_patterns") or []:
                _validate_portable_regex(_pattern, "filter exclude_patterns")
                _preds.append(f"NOT {_sql_regexp_contains(dialect, _text, _sql_str(f'(?im){_pattern}'))}")
            _where = f"\n  WHERE {' AND '.join(_preds)}" if _preds else ""
            ctes.append(("filtered", f"SELECT {cols_sql(cur, cols)}\n  FROM {cur}{_where}"))
            cur = "filtered"
        elif _op == "dedupe":
            _mode = _step.get("mode", "normalized")
            if _mode not in ("exact", "normalized"):
                raise ValueError(f"dedupe op: mode must be 'exact' or 'normalized', got {_mode!r}")
            _within = _step.get("within") or []
            for _w in _within:
                if _w not in cols:
                    raise ValueError(f"dedupe op: within column {_w!r} not in upstream columns: {cols}")
            if _mode == "exact":
                _key = f"{cur}.content_hash"
            else:
                # Built via plain variables, not nested f-string expressions: a literal
                # backslash (as in r"\s+") inside an f-string's {...} part is a SyntaxError
                # on Python < 3.12 (PEP 701 only relaxed this in 3.12).
                _lower_text = f"LOWER({cur}.chunk_text)"
                _strip_non_word = _sql_str(r"[^\w\s]")
                _empty = _sql_str("")
                _collapse_ws = _sql_str(r"\s+")
                _single_space = _sql_str(" ")
                _strip_punct = _sql_regexp_replace(dialect, _lower_text, _strip_non_word, _empty)
                _norm = f"TRIM({_sql_regexp_replace(dialect, _strip_punct, _collapse_ws, _single_space)})"
                _key = _sql_hash_expr(dialect, _norm)
            _partition_by = ", ".join([_key] + [f"{cur}.{_w}" for _w in _within])
            ctes.append(("deduped", (
                f"SELECT {cols_sql(cur, cols)}\n  FROM {cur}\n"
                f"  QUALIFY ROW_NUMBER() OVER (PARTITION BY {_partition_by} ORDER BY {cur}.chunk_id) = 1")))
            cur = "deduped"
        elif _op == "sensitivity":
            _detectors = _step.get("detectors") or list(_SQL_PII_PATTERNS)
            _unsupported = [d for d in _detectors if d in _SQL_PII_UNSUPPORTED]
            if _unsupported:
                _reasons = "; ".join(f"{d}: {_SQL_PII_UNSUPPORTED[d]}" for d in _unsupported)
                raise ValueError(
                    f"execution_mode='sql': sensitivity detector(s) {_unsupported} not supported ({_reasons}); "
                    f"use execution_mode='python', or drop them from `detectors`."
                )
            _unknown = [d for d in _detectors if d not in _SQL_PII_PATTERNS]
            if _unknown:
                raise ValueError(f"sensitivity op: unknown detector(s) {_unknown}; valid for execution_mode='sql': {sorted(_SQL_PII_PATTERNS)}")
            _action = _step.get("action", "tag")
            if _action == "redact":
                raise ValueError(
                    "execution_mode='sql': sensitivity action='redact' is not supported (it would require "
                    "re-hashing content_hash mid-statement); use 'tag' or 'drop', or execution_mode='python'."
                )
            if _action not in ("tag", "drop"):
                raise ValueError(f"sensitivity op: action must be tag or drop, got {_action!r}")
            _text = f"{cur}.chunk_text"
            _hit_exprs = {d: _sql_regexp_contains(dialect, _text, _sql_str(f"(?im){_SQL_PII_PATTERNS[d]}")) for d in _detectors}
            _any_hit = " OR ".join(_hit_exprs.values())
            _out_col = _step.get("output_column", "sensitivity")
            _types_col = _step.get("types_column", "pii_types")
            _type_parts = [f"(CASE WHEN {expr} THEN {_sql_str(d)} ELSE NULL END)" for d, expr in _hit_exprs.items()]
            _types_expr = f"CONCAT_WS(',', {', '.join(_type_parts)})"
            ctes.append(("sensitivity_tagged", (
                f"SELECT {cols_sql(cur, cols)},\n"
                f"         (CASE WHEN ({_any_hit}) THEN 'pii' ELSE 'none' END) AS {_out_col},\n"
                f"         {_types_expr} AS {_types_col}\n"
                f"  FROM {cur}")))
            cols = cols + [_out_col, _types_col]
            cur = "sensitivity_tagged"
            if _action == "drop":
                ctes.append(("sensitivity_filtered", f"SELECT {cols_sql(cur, cols)}\n  FROM {cur}\n  WHERE {_out_col} = 'none'"))
                cur = "sensitivity_filtered"
        elif _op == "stamp":
            _select_extra: List[str] = []
            for _name, _template in (_step.get("columns") or {}).items():
                _template = str(_template)
                if not _STAMP_TEMPLATE_RE.match(_template):
                    raise ValueError(
                        f"execution_mode='sql': stamp column {_name!r} template {_template!r} must be a plain "
                        f"literal or simple {{column}} placeholders only (no format specs) -- use execution_mode='python' for anything more complex."
                    )
                _placeholders = _STAMP_PLACEHOLDER_RE.findall(_template)
                for _p in _placeholders:
                    if _p not in cols:
                        raise ValueError(f"execution_mode='sql': stamp column {_name!r} references {{{_p}}}, not in upstream columns: {cols}")
                if not _placeholders:
                    _expr = _sql_str(_template)
                else:
                    _parts: List[str] = []
                    _pos = 0
                    for _m in _STAMP_PLACEHOLDER_RE.finditer(_template):
                        if _m.start() > _pos:
                            _parts.append(_sql_str(_template[_pos:_m.start()]))
                        _parts.append(f"CAST({cur}.{_m.group(1)} AS STRING)")
                        _pos = _m.end()
                    if _pos < len(_template):
                        _parts.append(_sql_str(_template[_pos:]))
                    _expr = f"CONCAT({', '.join(_parts)})" if len(_parts) > 1 else _parts[0]
                _select_extra.append(f"{_expr} AS {_name}")
                cols = cols + [_name]
            if _step.get("ttl_days") is not None:
                _valid_col = _step.get("valid_until_column", "valid_until")
                _valid_date = (datetime.fromisoformat(now_iso) + timedelta(days=float(_step["ttl_days"]))).date().isoformat()
                _select_extra.append(f"{_sql_str(_valid_date)} AS {_valid_col}")
                cols = cols + [_valid_col]
            if _select_extra:
                _base_cols = cols[:-len(_select_extra)]
                ctes.append(("stamped_governance", f"SELECT {cols_sql(cur, _base_cols)},\n         " + ",\n         ".join(_select_extra) + f"\n  FROM {cur}"))
                cur = "stamped_governance"

    if receipts is not None:
        recipe = code_version if receipts.get("scope_to_recipe", True) else None
        ctes += _sql_receipt_feedback_ctes(receipts, asset_name, recipe, since or _receipts_since(receipts), dialect)
        scores = receipt_table(receipts["dataset"], "context_item_scores", receipts.get("table_prefix", ""), _RECEIPT_DIALECT_OF[dialect])
        pr = receipts.get("prune")
        seen_select = seen_join = ""
        if pr:
            ctes.append(("window_receipts", _sql_window_receipts_cte(receipts, asset_name, recipe, since or _receipts_since(receipts), dialect)))
            if prior_first_seen:
                ctes.append(("prior_seen", f"SELECT chunk_id, MIN(feedback_first_seen_at) AS first_seen\n  FROM {out_tbl}\n  GROUP BY chunk_id"))
                seen_join = f"\n  LEFT JOIN prior_seen AS ps ON ps.chunk_id = {cur}.chunk_id"
            seen_select = f",\n         COALESCE({'ps.first_seen, ' if prior_first_seen else ''}{_sql_ts(now_iso, dialect)}) AS feedback_first_seen_at"
        ctes.append(("feedback_joined", (
            f"SELECT {cols_sql(cur, cols)},\n"
            f"         COALESCE(fb.retrievals, 0) AS feedback_retrievals, COALESCE(fb.rated, 0) AS feedback_rated,\n"
            f"         COALESCE(fb.mean_outcome, 0.0) AS feedback_mean_outcome, COALESCE(fb.cited, 0) AS feedback_cited,\n"
            f"         COALESCE(sc.prior, 0.0) AS feedback_prior{seen_select}\n"
            f"  FROM {cur}\n  LEFT JOIN item_feedback AS fb ON fb.item_id = {cur}.chunk_id\n"
            f"  LEFT JOIN {scores} AS sc ON sc.item_id = {cur}.chunk_id{seen_join}")))
        dead = _sql_dead_expr(pr, dialect, now_iso) if pr else None
        if preview == "rewrite":
            with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
            return (f"WITH {with_}\nSELECT chunk_id, {id_col} AS source_id, feedback_retrievals, feedback_rated, feedback_mean_outcome, feedback_cited\n"
                    f"FROM feedback_joined\nWHERE feedback_retrievals >= {int(receipts['min_retrievals'])} AND feedback_rated >= {int(receipts['min_outcomes'])} "
                    f"AND feedback_mean_outcome <= {float(receipts['rewrite_below'])!r}\n"
                    f"ORDER BY feedback_mean_outcome, feedback_retrievals DESC, chunk_id\nLIMIT {REWRITE_CANDIDATES_TOP}")
        if preview == "counts":
            if not pr:
                raise ValueError("preview='counts' needs receipts.prune")
            with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
            return (f"WITH {with_}\nSELECT {_sql_count_if(dialect, dead)}, (SELECT n FROM window_receipts), COUNT(*), "
                    f"{_sql_count_if(dialect, 'feedback_retrievals = 0')}\nFROM feedback_joined")
        q = receipts.get("quarantine")
        bad = None
        if q:
            bad = (f"(feedback_retrievals >= {int(receipts['min_retrievals'])} AND feedback_rated >= {int(receipts['min_outcomes'])} "
                   f"AND feedback_mean_outcome <= {float(q['mean_outcome_below'])!r})")
        fb_cols = (list(_FEEDBACK_COLUMNS) if receipts.get("stamp", True) else []) + (["feedback_first_seen_at"] if pr else [])
        select = list(cols) + fb_cols
        out_flags: List[str] = []
        excl: List[str] = []
        if bad and q["mode"] == "flag":
            select.append(f"{bad} AS feedback_quarantined")
            out_flags.append("feedback_quarantined")
        elif bad:
            excl.append(f"NOT {bad}")
        if pr and pr["mode"] == "flag":
            select.append(f"{dead} AS feedback_dead_weight")
            out_flags.append("feedback_dead_weight")
        elif pr:
            excl.append(f"NOT {dead}")
        where = f"\n  WHERE {' AND '.join(excl)}" if excl else ""
        ctes.append(("feedback_applied", f"SELECT {', '.join(select)}\n  FROM feedback_joined{where}"))
        cols = cols + fb_cols + out_flags
        cur = "feedback_applied"

    if stamp_provenance:
        part = _sql_str(partition) if partition is not None else "CAST(NULL AS STRING)"
        ctes.append(("versioned", (
            f"SELECT {cols_sql(cur, cols)},\n"
            f"         {_sql_str(asset_name)} AS source_asset_key,\n"
            f"         (SELECT {_sql_version_agg(dialect)} FROM {cur}) AS source_version,\n"
            f"         {part} AS source_partition,\n"
            f"         {_sql_str(code_version)} AS source_code_version\n"
            f"  FROM {cur}")))
        cols += ["source_asset_key", "source_version", "source_partition", "source_code_version"]
        cur = "versioned"
    elif partition is not None:
        # a partitioned run must always say which partition owns each row: that is what lets a later run replace only its own rows
        ctes.append(("versioned", f"SELECT {cols_sql(cur, cols)}, {_sql_str(partition)} AS source_partition\n  FROM {cur}"))
        cols += ["source_partition"]
        cur = "versioned"

    reuse = reuse or {}
    sig = None
    classify_expr_for = None
    if classify_step is not None:
        candidate_labels = classify_step.get("candidate_labels")
        if not candidate_labels:
            raise ValueError("classify step in execution_mode='sql' requires `candidate_labels`.")
        sig = _sql_category_signature(candidate_labels)
        _sql_check_classify_labels(dialect, candidate_labels)
        classify_expr_for = lambda t: _sql_classify_expr(dialect, t, candidate_labels)  # noqa: E731
    model = embed_model_id = None
    if embed_step is not None:
        model = embed_step.get("model") or ("embedding_model" if dialect == "bigquery" else None)
        if dialect == "bigquery" and (not isinstance(model, str) or not _MODEL_RE.match(model)):
            raise ValueError(f"execution_mode='sql': embed model {model!r} is not a valid BigQuery model reference (project.dataset.model)")
        embed_model_id = _sql_embed_model_id(dialect, embed_step.get("model"))
    enrich_model = enrich_mode = enrich_output_col = enrich_sig = None
    if enrich_step is not None:
        enrich_mode = enrich_step.get("mode", "context")
        if enrich_mode not in _ENRICH_PROMPTS:
            raise ValueError(f"enrich op: mode must be one of {sorted(_ENRICH_PROMPTS)}, got {enrich_mode!r}")
        if enrich_step.get("prepend"):
            raise ValueError(
                "execution_mode='sql': enrich prepend=True is not supported (it would require re-hashing "
                "content_hash mid-statement); use execution_mode='python', or drop `prepend`."
            )
        enrich_model = enrich_step.get("model")
        if not enrich_model:
            raise ValueError("enrich op requires `model` (any warehouse-native generation model reference).")
        if dialect == "bigquery" and (not isinstance(enrich_model, str) or not _MODEL_RE.match(enrich_model)):
            raise ValueError(f"execution_mode='sql': enrich model {enrich_model!r} is not a valid BigQuery model reference (project.dataset.model)")
        enrich_output_col = enrich_step.get("output_column", f"enrichment_{enrich_mode}")
        enrich_sig = f"{enrich_model}:{enrich_mode}"

    _TAG_PLURAL = {"category": "categories", "embedding": "embeddings", "enrichment": "enrichments"}

    if incremental and (classify_step is not None or embed_step is not None or enrich_step is not None):
        sel = [cols_sql("b", cols)]
        joins: List[str] = []
        counts: List[str] = [f"(SELECT COUNT(*) FROM {cur}) AS total_rows"]

        def feature(tag: str, value_col: str, tag_col: str, tag_val: str, fresh_select: str, can_reuse: bool, extra_where: str = "") -> None:
            # prior_<tag>: the reusable earlier values (one row per content_hash); todo_<tag>: distinct chunk texts without one;
            # new_<tag>: the AI function over todo only; all_<tag>: prior + new, joined back onto every chunk
            if can_reuse:
                ctes.append((f"prior_{tag}", (
                    f"SELECT content_hash, {value_col}\n  FROM {out_tbl}\n  WHERE {tag_col} = {_sql_str(tag_val)} AND {value_col} IS NOT NULL{extra_where}\n"
                    f"  QUALIFY ROW_NUMBER() OVER (PARTITION BY content_hash ORDER BY chunk_id) = 1")))
                ctes.append((f"todo_{tag}", (
                    f"SELECT DISTINCT b.content_hash, b.chunk_text\n  FROM {cur} AS b\n"
                    f"  LEFT JOIN prior_{tag} AS p ON p.content_hash = b.content_hash\n  WHERE p.content_hash IS NULL")))
            else:
                ctes.append((f"todo_{tag}", f"SELECT DISTINCT b.content_hash, b.chunk_text\n  FROM {cur} AS b"))
            counts.append(f"(SELECT COUNT(*) FROM {cur} AS b JOIN todo_{tag} AS t ON t.content_hash = b.content_hash) AS new_{_TAG_PLURAL[tag]}")
            if counts_only:
                return
            ctes.append((f"new_{tag}", fresh_select))
            union = f"SELECT content_hash, {value_col} FROM new_{tag}"
            if can_reuse:
                union = f"SELECT content_hash, {value_col} FROM prior_{tag}\n  UNION ALL\n  {union}"
            ctes.append((f"all_{tag}", union))
            joins.append(f"LEFT JOIN all_{tag} AS {tag}_ ON {tag}_.content_hash = b.content_hash")

        if classify_step is not None:
            feature("category", "category", "category_signature", sig,
                    f"SELECT content_hash, {classify_expr_for('todo_category.chunk_text')} AS category\n  FROM todo_category",
                    bool(reuse.get("classify")))
            sel += ["category_.category AS category", f"{_sql_str(sig)} AS category_signature"]
        if enrich_step is not None:
            if dialect == "bigquery":
                fresh = (f"SELECT content_hash, ml_generate_text_llm_result AS {enrich_output_col}\n"
                         f"  FROM ML.GENERATE_TEXT(MODEL `{enrich_model}`,\n"
                         f"    (SELECT content_hash, {_sql_enrich_prompt_expr(enrich_mode, 'todo_enrichment.chunk_text')} AS prompt FROM todo_enrichment),\n"
                         f"    STRUCT(TRUE AS flatten_json_output))")
            else:
                fresh = (f"SELECT content_hash, {_sql_enrich_expr(dialect, 'todo_enrichment.chunk_text', enrich_model, enrich_mode)} AS {enrich_output_col}\n"
                         f"  FROM todo_enrichment")
            feature("enrichment", enrich_output_col, "enrichment_signature", enrich_sig, fresh, bool(reuse.get("enrich")))
            sel += [f"enrichment_.{enrich_output_col} AS {enrich_output_col}", f"{_sql_str(enrich_sig)} AS enrichment_signature"]
        if embed_step is not None:
            if dialect == "bigquery":
                fresh = (f"SELECT content_hash, ml_generate_embedding_result AS embedding\n"
                         f"  FROM ML.GENERATE_EMBEDDING(MODEL `{model}`,\n"
                         f"    (SELECT content_hash, chunk_text AS content FROM todo_embedding),\n"
                         f"    STRUCT(TRUE AS flatten_json_output))")
            else:
                fresh = f"SELECT content_hash, {_sql_embed_expr(dialect, 'todo_embedding.chunk_text', embed_step.get('model'))} AS embedding\n  FROM todo_embedding"
            feature("embedding", "embedding", "embedding_model", embed_model_id, fresh, bool(reuse.get("embed")),
                    " AND ARRAY_LENGTH(embedding) > 0" if dialect == "bigquery" else "")
            sel += ["embedding_.embedding AS embedding", f"{_sql_str(embed_model_id)} AS embedding_model"]
        with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
        if counts_only:
            return f"WITH {with_}\nSELECT {', '.join(counts)}"
        final = f"SELECT {', '.join(sel)}\nFROM {cur} AS b\n" + "\n".join(joins)
        return f"{target} AS\nWITH {with_}\n{final}"

    if counts_only:
        raise ValueError("counts_only needs incremental=True and a classify, embed or enrich step")
    scalar_extra: List[str] = []
    # Fixed pipeline position classify -> enrich -> embed (same as the incremental path above,
    # regardless of steps_cfg order -- see the module comment on filter/dedupe/sensitivity/stamp for
    # why SQL mode uses fixed positions rather than python mode's flexible step ordering). On BigQuery,
    # AI.CLASSIFY/ML.GENERATE_TEXT/ML.GENERATE_EMBEDDING are all table functions, and a FROM clause can
    # only directly wrap one: whichever of these three is LAST in that fixed order wraps the final
    # SELECT: every earlier one materializes as its own CTE first.
    bq_last_is_embed = dialect == "bigquery" and embed_step is not None
    bq_last_is_enrich = dialect == "bigquery" and enrich_step is not None and embed_step is None
    if classify_step is not None:
        classify_expr = classify_expr_for(f"{cur}.chunk_text")
        if dialect == "bigquery" and (enrich_step is not None or embed_step is not None):
            ctes.append(("classified", f"SELECT {cols_sql(cur, cols)}, {classify_expr} AS category, {_sql_str(sig)} AS category_signature\n  FROM {cur}"))
            cols += ["category", "category_signature"]
            cur = "classified"
        else:
            scalar_extra += [f"{classify_expr} AS category", f"{_sql_str(sig)} AS category_signature"]
    if enrich_step is not None and not bq_last_is_enrich:
        if dialect == "bigquery":
            ctes.append(("enriched", (
                f"SELECT {cols_sql(cur, cols)},\n"
                f"         ml_generate_text_llm_result AS {enrich_output_col}, {_sql_str(enrich_sig)} AS enrichment_signature\n"
                f"  FROM ML.GENERATE_TEXT(MODEL `{enrich_model}`,\n"
                f"    (SELECT {cols_sql(cur, cols)}, {_sql_enrich_prompt_expr(enrich_mode, f'{cur}.chunk_text')} AS prompt FROM {cur}),\n"
                f"    STRUCT(TRUE AS flatten_json_output))")))
            cols += [enrich_output_col, "enrichment_signature"]
            cur = "enriched"
        else:
            scalar_extra += [f"{_sql_enrich_expr(dialect, f'{cur}.chunk_text', enrich_model, enrich_mode)} AS {enrich_output_col}",
                             f"{_sql_str(enrich_sig)} AS enrichment_signature"]

    with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
    if bq_last_is_embed:
        final = (f"SELECT {', '.join(cols)}, ml_generate_embedding_result AS embedding, {_sql_str(embed_model_id)} AS embedding_model\n"
                 f"FROM ML.GENERATE_EMBEDDING(MODEL `{model}`,\n"
                 f"  (SELECT {cols_sql(cur, cols)}, {cur}.chunk_text AS content FROM {cur}),\n"
                 f"  STRUCT(TRUE AS flatten_json_output))")
    elif bq_last_is_enrich:
        final = (f"SELECT {', '.join(cols)}, ml_generate_text_llm_result AS {enrich_output_col}, {_sql_str(enrich_sig)} AS enrichment_signature\n"
                 f"FROM ML.GENERATE_TEXT(MODEL `{enrich_model}`,\n"
                 f"  (SELECT {cols_sql(cur, cols)}, {_sql_enrich_prompt_expr(enrich_mode, f'{cur}.chunk_text')} AS prompt FROM {cur}),\n"
                 f"  STRUCT(TRUE AS flatten_json_output))")
    else:
        if embed_step is not None:
            scalar_extra += [f"{_sql_embed_expr(dialect, f'{cur}.chunk_text', embed_step.get('model'))} AS embedding",
                             f"{_sql_str(embed_model_id)} AS embedding_model"]
        final = "SELECT " + ", ".join([cols_sql(cur, cols)] + scalar_extra) + f"\nFROM {cur}"
    return f"{target} AS\nWITH {with_}\n{final}"


# ── per_step_ops, execution_mode='sql': one real table per step ──────────────────────────────────────────────
#
# User-confirmed scope (see commit message / memory): real per-step tables (not cosmetic fake assets over one
# statement), incremental reuse preserved, 3 steps max (chunk/classify/embed -- write_vector_store genuinely
# has no SQL equivalent, confirmed dbt-labs/dbt-context-engineering's own "search" model is the RETRIEVAL step,
# not a 4th build step, so there's nothing missing here to reach parity with). Unpartitioned only for now
# (validated in build_defs) -- partition-aware per-table staging is real added complexity, tracked as a
# follow-up rather than built speculatively.
#
# `_sql_chunk_and_stamp_stage` (above, shared verbatim with the single-statement path) builds the chunks
# table's content. `_build_sql_feature_table_query` below is a single-feature (classify OR embed) version of
# `_build_sql_mode_query`'s `feature()` closure -- incremental reuse against THIS stage's own prior table
# instead of the shared output_table, since each stage is now a separate table with its own history.


def _build_sql_chunks_table_query(
    chunk_step: dict, source_cfg: dict, dialect: str, chunks_table: str, id_column: str, text_column: str,
    metadata_columns: List[str], *, asset_name: str, code_version: str, stamp_provenance: bool = False,
    receipts: Optional[Dict[str, Any]] = None, since: Optional[str] = None, now: Optional[str] = None,
    prior_first_seen: bool = False, prior_chunks_table: Optional[str] = None,
) -> "tuple[str, List[str]]":
    """The 'chunks' stage: chunk -> stamp -> [receipts feedback] -> [provenance], written to its own
    table. Returns (sql, cols) -- cols is what every later stage's `SELECT *` actually carries forward."""
    ctes, cols, cur = _sql_chunk_and_stamp_stage(
        chunk_step, source_cfg, dialect, id_column, text_column, metadata_columns,
        asset_name=asset_name, code_version=code_version, stamp_provenance=stamp_provenance,
        receipts=receipts, since=since, now=now, prior_first_seen=prior_first_seen, out_tbl=prior_chunks_table,
    )
    cols_sql = lambda alias, cs: ", ".join(f"{alias}.{c}" for c in cs)  # noqa: E731
    with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
    final = f"SELECT {cols_sql(cur, cols)}\nFROM {cur}"
    return f"CREATE OR REPLACE TABLE {chunks_table} AS\nWITH {with_}\n{final}", cols


def _build_sql_feature_table_query(
    *, tag: str, tag_col: str, tag_val: str, dialect: str, prior_table: str, this_table: str,
    base_cols: List[str], fresh_select: str, can_reuse: bool, extra_where: str = "", counts_only: bool = False,
) -> str:
    """One feature (classify OR embed) as its own `CREATE OR REPLACE TABLE`, reading every column of
    `prior_table` (the previous stage's real table) through and adding exactly one new column. When
    `can_reuse`, rows whose content_hash already has a value in `this_table` (THIS stage's own prior
    run, by content_hash -- not `prior_table`) skip recomputation, same content-hash-match logic as
    `_build_sql_mode_query`'s `feature()` closure, just targeting this stage's own table instead of a
    shared output_table (each stage now has its own history to reuse from).

    `fresh_select` is the dialect/feature-specific SELECT that computes `tag` for the rows with no
    reusable value (built by the caller -- BigQuery's embed uses a different TABLE FUNCTION shape than
    everything else, exactly as in the single-statement path).
    """
    cols_sql = lambda alias, cs: ", ".join(f"{alias}.{c}" for c in cs)  # noqa: E731
    ctes: List[Tuple[str, str]] = [("base", f"SELECT * FROM {prior_table}")]
    cur = "base"
    counts = [f"(SELECT COUNT(*) FROM {cur}) AS total_rows"]
    if can_reuse:
        ctes.append(("prior_vals", (
            f"SELECT content_hash, {tag}\n  FROM {this_table}\n  WHERE {tag_col} = {_sql_str(tag_val)} AND {tag} IS NOT NULL{extra_where}\n"
            f"  QUALIFY ROW_NUMBER() OVER (PARTITION BY content_hash ORDER BY chunk_id) = 1")))
        ctes.append(("todo", (
            f"SELECT DISTINCT b.content_hash, b.chunk_text\n  FROM {cur} AS b\n"
            f"  LEFT JOIN prior_vals AS p ON p.content_hash = b.content_hash\n  WHERE p.content_hash IS NULL")))
    else:
        ctes.append(("todo", f"SELECT DISTINCT b.content_hash, b.chunk_text\n  FROM {cur} AS b"))
    counts.append(f"(SELECT COUNT(*) FROM {cur} AS b JOIN todo AS t ON t.content_hash = b.content_hash) AS new_count")
    if counts_only:
        with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
        return f"WITH {with_}\nSELECT {', '.join(counts)}"
    ctes.append(("new_vals", fresh_select))
    union = f"SELECT content_hash, {tag} FROM new_vals"
    if can_reuse:
        union = f"SELECT content_hash, {tag} FROM prior_vals\n  UNION ALL\n  {union}"
    ctes.append(("all_vals", union))
    sel = [cols_sql("b", base_cols), f"v.{tag} AS {tag}", f"{_sql_str(tag_val)} AS {tag_col}"]
    with_ = ",\n".join(f"{n} AS (\n  {b}\n)" for n, b in ctes)
    final = f"SELECT {', '.join(sel)}\nFROM {cur} AS b\n  LEFT JOIN all_vals AS v ON v.content_hash = b.content_hash"
    return f"CREATE OR REPLACE TABLE {this_table} AS\nWITH {with_}\n{final}"


def _build_sql_classify_table_query(
    classify_step: dict, dialect: str, prior_table: str, this_table: str, base_cols: List[str], *, can_reuse: bool,
) -> str:
    candidate_labels = classify_step.get("candidate_labels")
    if not candidate_labels:
        raise ValueError("classify step in execution_mode='sql' requires `candidate_labels`.")
    _sql_check_classify_labels(dialect, candidate_labels)
    sig = _sql_category_signature(candidate_labels)
    classify_expr = _sql_classify_expr(dialect, "todo.chunk_text", candidate_labels)
    fresh = f"SELECT content_hash, {classify_expr} AS category\n  FROM todo"
    return _build_sql_feature_table_query(
        tag="category", tag_col="category_signature", tag_val=sig, dialect=dialect,
        prior_table=prior_table, this_table=this_table, base_cols=base_cols, fresh_select=fresh, can_reuse=can_reuse,
    )


def _build_sql_embed_table_query(
    embed_step: dict, dialect: str, prior_table: str, this_table: str, base_cols: List[str], *, can_reuse: bool,
) -> str:
    model = embed_step.get("model") or ("embedding_model" if dialect == "bigquery" else None)
    if dialect == "bigquery" and (not isinstance(model, str) or not _MODEL_RE.match(model)):
        raise ValueError(f"execution_mode='sql': embed model {model!r} is not a valid BigQuery model reference (project.dataset.model)")
    embed_model_id = _sql_embed_model_id(dialect, embed_step.get("model"))
    if dialect == "bigquery":
        fresh = (f"SELECT content_hash, ml_generate_embedding_result AS embedding\n"
                 f"  FROM ML.GENERATE_EMBEDDING(MODEL `{model}`,\n"
                 f"    (SELECT content_hash, chunk_text AS content FROM todo),\n"
                 f"    STRUCT(TRUE AS flatten_json_output))")
        extra_where = " AND ARRAY_LENGTH(embedding) > 0"
    else:
        fresh = f"SELECT content_hash, {_sql_embed_expr(dialect, 'todo.chunk_text', embed_step.get('model'))} AS embedding\n  FROM todo"
        extra_where = ""
    return _build_sql_feature_table_query(
        tag="embedding", tag_col="embedding_model", tag_val=embed_model_id, dialect=dialect,
        prior_table=prior_table, this_table=this_table, base_cols=base_cols, fresh_select=fresh,
        can_reuse=can_reuse, extra_where=extra_where,
    )


# ── Receipts feedback: let what was retrieved and how it turned out shape the next build ─────────────────────
#
# resources/context_receipt_store records, at query time, which chunks were retrieved and (later) how each answer turned out.
# `receipts:` reads that back at build time so every rebuild learns from real use. It acts on the chunk frame right before
# `write_vector_store`:  stamp (write feedback_* onto each chunk so gateways can rerank), quarantine (flag or leave out
# chunks users keep rating badly), and report (dead weight, best/worst chunks, exclusion reasons, outcomes per recipe).
# An asset check (`recipe_outcomes`) compares the current recipe's real-world outcomes with earlier recipes.

_RECEIPTS_KEYS = {"resource_key", "window_days", "min_retrievals", "min_outcomes", "refresh_priors", "prior_strength",
                  "scope_to_recipe", "stamp", "quarantine", "dead_weight_min_receipts", "regression_check", "dataset", "table_prefix",
                  "rewrite_below", "prune"}
_FEEDBACK_COLUMNS = ["feedback_retrievals", "feedback_rated", "feedback_mean_outcome", "feedback_cited", "feedback_prior"]


def _validate_receipts(cfg: Dict[str, Any], steps_cfg: List[dict], execution_mode: str,
                       sql_dialect: Optional[str] = None, source_cfg: Optional[dict] = None) -> Dict[str, Any]:
    if not isinstance(cfg, dict):
        raise ValueError("receipts must be a mapping")
    unknown = set(cfg) - _RECEIPTS_KEYS
    if unknown:
        raise ValueError(f"receipts: unknown field(s) {sorted(unknown)}; valid: {sorted(_RECEIPTS_KEYS)}")
    sql_mode = execution_mode == "sql"
    if sql_mode:
        if sql_dialect not in _RECEIPT_DIALECT_OF:
            raise ValueError("receipts in execution_mode='sql' is supported for sql_dialect bigquery / snowflake_cortex / databricks only "
                             f"(got {sql_dialect!r}); use execution_mode='python' with a ContextReceiptStore resource for other warehouses")
        dataset = cfg.get("dataset")
        shape = {"bigquery": "'project.dataset' (or 'dataset')", "snowflake_cortex": "'DATABASE.SCHEMA' (or 'SCHEMA')",
                 "databricks": "'catalog.schema' (or 'schema')"}[sql_dialect]
        if not isinstance(dataset, str) or not re.match(r"^[A-Za-z0-9_\-]+(\.[A-Za-z0-9_\-]+)?$", dataset):
            raise ValueError(f"receipts.dataset is required in execution_mode='sql': {shape} holding the receipt tables")
        if not re.match(r"^[A-Za-z0-9_]*$", str(cfg.get("table_prefix", "") or "")):
            raise ValueError("receipts.table_prefix may contain only letters, digits and underscore")
        source_key = (source_cfg or {}).get("resource_key")
        if cfg.get("resource_key") not in (None, source_key):
            raise ValueError(f"receipts.resource_key in execution_mode='sql' must be the warehouse resource the source uses ({source_key!r}); "
                             "the feedback is computed by the same statement that builds the table")
        cfg = {**cfg, "resource_key": source_key, "table_prefix": cfg.get("table_prefix") or "", "sql_dialect": sql_dialect}
    else:
        if "dataset" in cfg or "table_prefix" in cfg:
            raise ValueError("receipts.dataset / receipts.table_prefix apply to execution_mode='sql' only (python mode reads a ContextReceiptStore resource)")
        if not isinstance(cfg.get("resource_key"), str) or not cfg["resource_key"].strip():
            raise ValueError("receipts.resource_key is required: the key of a registered ContextReceiptStoreComponent resource")
    out = {"window_days": 30.0, "min_retrievals": 5, "min_outcomes": 3, "refresh_priors": True, "prior_strength": 5.0,
           "scope_to_recipe": True, "stamp": True, "dead_weight_min_receipts": 50, **cfg}
    if float(out["window_days"]) <= 0:
        raise ValueError("receipts.window_days must be > 0")
    for name in ("min_retrievals", "min_outcomes"):
        if int(out[name]) < 1:
            raise ValueError(f"receipts.{name} must be >= 1")
    writes = [s_ for s_ in steps_cfg if s_["op"] == "write_vector_store"]
    if not writes and not sql_mode:
        raise ValueError("receipts needs a write_vector_store step (feedback is applied to the chunks it writes)")
    q = out.get("quarantine")
    if q is True:
        q = {}
    if q is not None:
        if not isinstance(q, dict) or set(q) - {"mean_outcome_below", "mode"}:
            raise ValueError("receipts.quarantine must be true or {mean_outcome_below: -0.3, mode: flag|exclude}")
        q = {"mean_outcome_below": -0.3, "mode": "flag", **q}
        if not -1 <= float(q["mean_outcome_below"]) <= 1:
            raise ValueError("receipts.quarantine.mean_outcome_below must be in [-1, 1]")
        if q["mode"] not in ("flag", "exclude"):
            raise ValueError("receipts.quarantine.mode must be 'flag' or 'exclude'")
        if q["mode"] == "exclude" and not sql_mode and any(w.get("provider", "chromadb") != "chromadb" for w in writes):
            raise ValueError("receipts.quarantine.mode=exclude is supported for the chromadb provider only (it deletes the chunks by id); "
                             "use mode: flag with other providers")
        out["quarantine"] = q
    if not -1 <= float(out.setdefault("rewrite_below", -0.3)) <= 1:
        raise ValueError("receipts.rewrite_below must be in [-1, 1]")
    pr = out.get("prune")
    if pr is not None:
        if not isinstance(pr, dict) or set(pr) - {"never_retrieved_after_receipts", "mode", "min_age_days"}:
            raise ValueError("receipts.prune must be {never_retrieved_after_receipts: 200, mode: flag|exclude, min_age_days: <defaults to window_days>}")
        pr = {"never_retrieved_after_receipts": 200, "mode": "flag", "min_age_days": float(out["window_days"]), **pr}
        if int(pr["never_retrieved_after_receipts"]) < 1:
            raise ValueError("receipts.prune.never_retrieved_after_receipts must be >= 1")
        if float(pr["min_age_days"]) < 0:
            raise ValueError("receipts.prune.min_age_days must be >= 0")
        if pr["mode"] not in ("flag", "exclude"):
            raise ValueError("receipts.prune.mode must be 'flag' or 'exclude'")
        if not sql_mode and any(w.get("provider", "chromadb") != "chromadb" for w in writes):
            raise ValueError("receipts.prune is supported for the chromadb provider only (it reads each chunk's first_seen_at back from the previous "
                             "index to protect new chunks, and mode=exclude deletes by id)")
        out["prune"] = pr
    rc = out.get("regression_check")
    if rc is not None:
        if not isinstance(rc, dict) or set(rc) - {"min_outcomes", "max_drop", "severity"}:
            raise ValueError("receipts.regression_check must be {min_outcomes: 10, max_drop: 0.15, severity: WARN|ERROR}")
        rc = {"min_outcomes": 10, "max_drop": 0.15, "severity": "WARN", **rc}
        if rc["severity"] not in ("WARN", "ERROR"):
            raise ValueError("receipts.regression_check.severity must be WARN or ERROR")
        out["regression_check"] = rc
    return out


def _receipts_store(cfg: Dict[str, Any], context):
    store = getattr(context.resources, cfg["resource_key"], None)
    if store is None or not hasattr(store, "item_feedback"):
        raise ValueError(f"receipts.resource_key={cfg['resource_key']!r} is not a ContextReceiptStore resource "
                         f"(register one with ContextReceiptStoreComponent)")
    return store


def _receipts_since(cfg: Dict[str, Any]) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=float(cfg["window_days"]))).isoformat()


REWRITE_CANDIDATES_TOP = 20


def _rewrite_candidates(fb: Dict[str, Dict[str, Any]], source_of: Dict[str, str], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Chunks worth a human rewrite: retrieved >= min_retrievals, rated >= min_outcomes, mean outcome <= rewrite_below; worst first
    (ties: more retrievals first, then id), top REWRITE_CANDIDATES_TOP. Same definition and thresholds as the receipt store's
    fold_maintenance_report (pinned by a parity test), restricted to chunks of this build and carrying each chunk's source id.
    Independent of `quarantine`: a quarantined (even excluded) chunk is exactly what needs rewriting, so it stays on the list."""
    rows = [{"chunk_id": i, "source_id": source_of[i], "retrievals": int(v["retrievals"]), "rated": int(v["rated"]),
             "mean_outcome": v["mean_outcome"], "cited": int(v["cited"])}
            for i, v in fb.items() if i in source_of and v["mean_outcome"] is not None
            and v["retrievals"] >= int(cfg["min_retrievals"]) and v["rated"] >= int(cfg["min_outcomes"])
            and v["mean_outcome"] <= float(cfg["rewrite_below"])]
    return sorted(rows, key=lambda r: (r["mean_outcome"], -r["retrievals"], r["chunk_id"]))[:REWRITE_CANDIDATES_TOP]


def _chroma_first_seen(write_step: dict, ids: List[str], context) -> Dict[str, str]:
    """first_seen_at stamped by an earlier build on the chunks still in the index, {} when the collection does not exist yet.
    A chunk missing from the result (new, or its stamp is absent) is treated as first seen NOW, so it is protected from pruning for
    min_age_days. Failing to read the index therefore fails safe (nothing is pruned), never towards deleting."""
    try:
        import chromadb
        client = chromadb.PersistentClient(path=write_step.get("connection_string") or "./chroma_db")
        collection = client.get_collection(name=write_step.get("collection_name", "context_engineering_kb"))
        out: Dict[str, str] = {}
        for i in range(0, len(ids), 500):
            got = collection.get(ids=ids[i:i + 500], include=["metadatas"])
            for cid, meta in zip(got["ids"], got["metadatas"]):
                if (meta or {}).get("feedback_first_seen_at"):
                    out[cid] = str(meta["feedback_first_seen_at"])
        return out
    except Exception as e:  # noqa: BLE001  (first build: no collection; anything else: warn, and treat every chunk as new)
        if "does not exist" not in str(e).lower() and "not found" not in str(e).lower():
            context.log.warning(f"receipts.prune: could not read the previous index ({type(e).__name__}: {e}); every chunk counts as new this build")
        return {}


def _apply_receipt_feedback(df: pd.DataFrame, write_step: dict, cfg: Dict[str, Any], asset_name: str, code_version: str,
                            context, source_id_column: Optional[str] = None, now: Optional[datetime] = None) -> "tuple[pd.DataFrame, dict, dict]":
    """Returns (frame to write, the write step with feedback columns added to its metadata_columns, summary for metadata)."""
    store = _receipts_store(cfg, context)
    since = _receipts_since(cfg)
    chunk_col = write_step.get("id_column", "chunk_id" if "chunk_id" in df.columns else None)
    if chunk_col is None or chunk_col not in df.columns:
        raise ValueError("receipts: the frame reaching write_vector_store has no chunk id column (add a chunk step, or set id_column)")
    if cfg["refresh_priors"]:
        store.refresh_item_scores(prior_strength=float(cfg["prior_strength"]))
    recipe = code_version if cfg["scope_to_recipe"] else None
    fb = store.item_feedback(asset_name, since=since, code_version=recipe)
    ids = df[chunk_col].astype(str).tolist()
    priors = store.get_item_scores(ids)
    frame = df.copy()
    frame["feedback_retrievals"] = [int(fb.get(i, {}).get("retrievals", 0)) for i in ids]
    frame["feedback_rated"] = [int(fb.get(i, {}).get("rated", 0)) for i in ids]
    frame["feedback_mean_outcome"] = [float(fb.get(i, {}).get("mean_outcome") or 0.0) for i in ids]  # 0.0 + feedback_rated=0 means unrated
    frame["feedback_cited"] = [int(fb.get(i, {}).get("cited", 0)) for i in ids]
    frame["feedback_prior"] = [float((priors.get(i) or {}).get("prior") or 0.0) for i in ids]
    now = now or datetime.now(timezone.utc)
    pr = cfg.get("prune")
    usage = store.asset_usage_since(asset_name, since)
    receipts_total = int(usage.get("receipts", 0))
    by_cv = store.outcomes_by_code_version(asset_name, since)
    # Receipts that could have retrieved a chunk of THIS recipe: with scope_to_recipe the feedback only counts this recipe's
    # retrievals, so "zero retrievals" is only evidence against a chunk if this recipe saw traffic (a recipe change must not look like
    # "nothing was ever retrieved").
    receipts_for_prune = int((by_cv.get(recipe) or {}).get("receipts", 0)) if recipe is not None else receipts_total
    pruned: List[str] = []
    protected_new = 0
    new_cols = list(_FEEDBACK_COLUMNS) if cfg["stamp"] else []
    if pr:
        first_seen = _chroma_first_seen(write_step, ids, context)
        stamped = [first_seen.get(i) or now.isoformat() for i in ids]
        frame["feedback_first_seen_at"] = stamped
        new_cols.append("feedback_first_seen_at")
        judged = receipts_for_prune >= int(pr["never_retrieved_after_receipts"])
        cutoff = now - timedelta(days=float(pr["min_age_days"]))
        zero = frame["feedback_retrievals"] == 0
        old_enough = pd.Series([datetime.fromisoformat(t) <= cutoff for t in stamped], index=frame.index)
        dead = (zero & old_enough) if judged else zero & False
        protected_new = int((zero & ~old_enough).sum()) if judged else 0
        pruned = [i for i, b in zip(ids, dead.tolist()) if b]
        if pr["mode"] == "flag":
            frame["feedback_dead_weight"] = dead.astype(bool)
            new_cols.append("feedback_dead_weight")
        else:
            frame = frame[~dead].reset_index(drop=True)
            ids = frame[chunk_col].astype(str).tolist()
    q = cfg.get("quarantine")
    quarantined: List[str] = []
    if q:
        bad = (frame["feedback_retrievals"] >= int(cfg["min_retrievals"])) & (frame["feedback_rated"] >= int(cfg["min_outcomes"])) \
              & (frame["feedback_mean_outcome"] <= float(q["mean_outcome_below"]))
        quarantined = [i for i, b in zip(ids, bad.tolist()) if b]
        if q["mode"] == "flag":
            frame["feedback_quarantined"] = bad.astype(bool)
            new_cols.append("feedback_quarantined")
        else:
            frame = frame[~bad].reset_index(drop=True)
            ids = frame[chunk_col].astype(str).tolist()
    # Keys that no longer apply (stamping switched off, quarantine mode changed, ...) must not linger in the index: Chroma's
    # upsert MERGES metadata, so a stale `feedback_quarantined: True` would keep hiding a recovered chunk. Writing None
    # deletes the key. Other providers replace metadata on upsert, so there the unused columns are simply dropped.
    stale = [c for c in _FEEDBACK_COLUMNS + ["feedback_quarantined", "feedback_dead_weight", "feedback_first_seen_at"] if c not in new_cols]
    if write_step.get("provider", "chromadb") == "chromadb":
        for c in stale:
            frame[c] = pd.Series([None] * len(frame), index=frame.index, dtype=object)
        new_cols = new_cols + stale
    else:
        frame = frame.drop(columns=[c for c in stale if c in frame.columns])
    step = dict(write_step)
    purge = (quarantined if q and q["mode"] == "exclude" else []) + (pruned if pr and pr["mode"] == "exclude" else [])
    if purge:
        step["_purge_chunk_ids"] = list(purge)
    if step.get("metadata_columns"):
        step["metadata_columns"] = list(step["metadata_columns"]) + [c for c in new_cols if c not in step["metadata_columns"]]
    in_frame = set(df[chunk_col].astype(str))
    source_of = dict(zip(df[chunk_col].astype(str), df[source_id_column].astype(str))) if source_id_column in df.columns else {i: "" for i in in_frame}
    never = [i for i in in_frame if fb.get(i, {}).get("retrievals", 0) == 0]
    ranked = sorted(((i, v) for i, v in fb.items() if v.get("mean_outcome") is not None and v["rated"] >= int(cfg["min_outcomes"])),
                    key=lambda kv: kv[1]["mean_outcome"])
    reasons: Dict[str, int] = {}
    for v in fb.values():
        for r, n in v.get("excluded", {}).items():
            reasons[r] = reasons.get(r, 0) + n
    summary = {
        "window_days": float(cfg["window_days"]), "scoped_to_recipe": recipe, "receipts_in_window": receipts_total,
        "chunks_in_build": len(in_frame), "chunks_with_feedback": sum(1 for i in in_frame if fb.get(i, {}).get("retrievals", 0) > 0),
        "chunks_rated": sum(1 for i in in_frame if fb.get(i, {}).get("rated", 0) > 0),
        "quarantine_mode": q["mode"] if q else None, "quarantined": len(quarantined), "quarantined_ids": quarantined[:50],
        "never_retrieved": len(never) if receipts_total >= int(cfg["dead_weight_min_receipts"]) else None,
        "exclusion_reasons": reasons,
        "worst": [{"chunk_id": i, "mean_outcome": v["mean_outcome"], "rated": v["rated"]} for i, v in ranked[:10]],
        "best": [{"chunk_id": i, "mean_outcome": v["mean_outcome"], "rated": v["rated"]} for i, v in ranked[::-1][:10]],
        "rewrite_below": float(cfg["rewrite_below"]),
        "rewrite_candidates": _rewrite_candidates(fb, source_of, cfg),
        "outcomes_by_code_version": by_cv,
    }
    if pr:
        summary.update({
            "prune_mode": pr["mode"], "pruned": len(pruned), "pruned_ids": pruned[:50],
            "prune_judged": receipts_for_prune >= int(pr["never_retrieved_after_receipts"]),
            "prune_receipts_in_window": receipts_for_prune, "prune_protected_new": protected_new,
        })
    return frame, step, summary


def _run_recipe_outcomes_check(cfg: Dict[str, Any], asset_name: str, code_version: str, context,
                               outcomes_by_cv: Optional[Dict[str, Dict[str, Any]]] = None) -> AssetCheckResult:
    """Is the recipe running now doing worse, in production, than an earlier recipe? Judged on user outcomes recorded on receipts.
    Python mode reads them from the ContextReceiptStore resource; sql mode passes `outcomes_by_cv` computed by BigQuery SQL."""
    if outcomes_by_cv is None:
        outcomes_by_cv = _receipts_store(cfg, context).outcomes_by_code_version(asset_name, _receipts_since(cfg))
    return _recipe_outcomes_result(cfg["regression_check"], code_version, outcomes_by_cv)


def _recipe_outcomes_result(rc: Dict[str, Any], code_version: str, by_cv: Dict[str, Dict[str, Any]]) -> AssetCheckResult:
    """The decision, shared by both execution modes (only how `by_cv` is fetched differs)."""
    rated = lambda v: v is not None and v.get("mean_outcome") is not None and v["outcomes"] >= int(rc["min_outcomes"])  # noqa: E731
    cur = by_cv.get(code_version)
    base_meta = {"code_version": code_version, "outcomes_by_code_version": MetadataValue.json(by_cv)}
    severity = AssetCheckSeverity.ERROR if rc["severity"] == "ERROR" else AssetCheckSeverity.WARN
    if not rated(cur):
        return AssetCheckResult(check_name="recipe_outcomes", passed=True, metadata={**base_meta, "status": "insufficient_evidence",
                                "outcomes_for_current_recipe": (cur or {}).get("outcomes", 0), "needed": int(rc["min_outcomes"])},
                                description="Not enough rated outcomes for the current recipe yet to judge it")
    others = {cv: v for cv, v in by_cv.items() if cv not in (code_version, "unknown") and rated(v)}
    if not others:
        return AssetCheckResult(check_name="recipe_outcomes", passed=True, metadata={**base_meta, "status": "no_baseline",
                                "current_mean_outcome": cur["mean_outcome"]}, description="No earlier recipe with enough outcomes to compare against")
    best_cv = max(others, key=lambda cv: others[cv]["mean_outcome"])
    drop = round(others[best_cv]["mean_outcome"] - cur["mean_outcome"], 4)
    passed = drop <= float(rc["max_drop"])
    return AssetCheckResult(
        check_name="recipe_outcomes", passed=passed, severity=severity,
        metadata={**base_meta, "status": "ok" if passed else "regressed", "current_mean_outcome": cur["mean_outcome"],
                  "best_previous_version": best_cv, "best_previous_mean_outcome": others[best_cv]["mean_outcome"], "drop": drop,
                  "max_drop": float(rc["max_drop"])},
        description=(f"recipe {code_version} mean outcome {cur['mean_outcome']:+.2f} vs {others[best_cv]['mean_outcome']:+.2f} for {best_cv}"
                     + ("" if passed else f" -- worse by {drop:.2f}; consider rolling back")),
    )


class _SqlSession:
    """Runs statements through whichever interface the warehouse resource exposes: SQLAlchemy engine, DB-API connection, or a
    BigQuery client (dagster-gcp BigQueryResource: `with resource.get_client() as client`)."""

    def __init__(self, execute, fetch):
        self.execute, self.fetch = execute, fetch


@contextlib.contextmanager
def _sql_session(resource, resource_key: str):
    if hasattr(resource, "get_engine"):
        engine = resource.get_engine()
        with engine.begin() as conn:
            yield _SqlSession(lambda s: conn.exec_driver_sql(s), lambda s: list(conn.exec_driver_sql(s).fetchall()))
    elif hasattr(resource, "get_connection"):
        with resource.get_connection() as conn:
            def run(s):
                if hasattr(conn, "execute"):
                    return conn.execute(s)
                cur = conn.cursor()  # e.g. snowflake-connector connections only have cursors
                cur.execute(s)
                return cur
            yield _SqlSession(run, lambda s: list(run(s).fetchall()))
    elif inspect.isgenerator(resource) or hasattr(resource, "get_client") or hasattr(resource, "query"):
        with _vendor_client_session(resource) as client:
            yield _SqlSession(lambda s: client.query(s).result(), lambda s: list(client.query(s).result()))
    else:
        raise ValueError(f"resource {resource_key!r} must expose .get_engine(), .get_connection() or .get_client() (BigQuery).")


def _sql_fetch_outcomes_by_code_version(context, source_cfg: dict, rcfg: Dict[str, Any], asset_name: str) -> Dict[str, Dict[str, Any]]:
    """ContextReceiptStore.outcomes_by_code_version, computed by warehouse SQL through the asset's own resource."""
    sql = _sql_outcomes_by_code_version_query(rcfg, asset_name, _receipts_since(rcfg))
    with _sql_session(getattr(context.resources, rcfg["resource_key"]), rcfg["resource_key"]) as session:
        rows = session.fetch(sql)
    return {r[0]: {"receipts": int(r[1]), "outcomes": int(r[2]), "mean_outcome": None if r[3] is None else float(r[3])} for r in rows}


def _sql_table_columns(session, dialect: str, tbl: str, context=None) -> List[str]:
    """Column names of `tbl` in ordinal order, [] when it does not exist (INFORMATION_SCHEMA.COLUMNS, so a missing table is not an exception)."""
    parts = [p.strip("`") for p in tbl.split(".")]
    name, schema = parts[-1], parts[-2] if len(parts) >= 2 else None
    if schema is None:
        if dialect == "bigquery":
            raise ValueError(f"table {tbl!r} has no dataset part; BigQuery tables need dataset.table")
        schema_pred = f"LOWER(table_schema) = LOWER({'CURRENT_SCHEMA()' if dialect == 'snowflake_cortex' else 'current_schema()'})"
    else:
        schema_pred = f"LOWER(table_schema) = LOWER({_sql_str(schema)})"
    if dialect == "bigquery":
        # documented forms: `project-id`.dataset.INFORMATION_SCHEMA.COLUMNS and dataset.INFORMATION_SCHEMA.COLUMNS. A missing TABLE gives zero
        # rows, but a missing DATASET raises NotFound (not caught here: the dataset is the user's to create, so that error is the right one).
        info = (f"`{parts[0]}`." if len(parts) >= 3 else "") + f"{schema}.INFORMATION_SCHEMA.COLUMNS"
    else:  # Snowflake / Databricks: a 3-part name selects that database's / catalog's information_schema, otherwise the current one
        info = (f"{parts[-3]}." if len(parts) >= 3 else "") + "information_schema.columns"
    return [str(r[0]) for r in session.fetch(
        f"SELECT column_name FROM {info} WHERE {schema_pred} AND LOWER(table_name) = LOWER({_sql_str(name)}) ORDER BY ordinal_position")]


def _sql_probe_prior_table(session, dialect: str, out_tbl: str, classify_step: Optional[dict], embed_step: Optional[dict], context,
                           columns: Optional[List[str]] = None, enrich_step: Optional[dict] = None) -> Tuple[Dict[str, bool], int]:
    """Which features can read their prior values from the existing output table: {"embed": bool, "classify": bool, "enrich": bool},
    plus its row count. A missing table (first run) or one without the needed columns (built by an older version of this component, or
    before the step existed) reports False for the feature, so that feature is built in full."""
    if columns is None:
        try:
            columns = _sql_table_columns(session, dialect, out_tbl, context)
        except ValueError as e:
            context.log.warning(f"incremental: {e}; building in full")
            return {"embed": False, "classify": False, "enrich": False}, 0
    have = {c.lower() for c in columns}
    base = {"chunk_id", "content_hash"}
    enrich_output_col = enrich_step.get("output_column", f"enrichment_{enrich_step.get('mode', 'context')}") if enrich_step is not None else None
    reuse = {"embed": embed_step is not None and (base | {"embedding", "embedding_model"}) <= have,
             "classify": classify_step is not None and (base | {"category", "category_signature"}) <= have,
             "enrich": enrich_step is not None and (base | {enrich_output_col.lower(), "enrichment_signature"}) <= have}
    prior_rows = int(session.fetch(f"SELECT COUNT(*) FROM {out_tbl}")[0][0]) if any(reuse.values()) else 0
    if have and not any(reuse.values()):
        context.log.info(f"incremental: {out_tbl} exists but lacks the columns needed for reuse (built by an older version?); rebuilding in full")
    return reuse, prior_rows


def _sql_stage_table(out_tbl: str, partition_label: str, dialect: Optional[str] = None) -> str:
    parts = [p.strip("`") for p in out_tbl.split(".")]
    parts[-1] = f"{parts[-1]}__stage_{hashlib.md5(partition_label.encode('utf-8')).hexdigest()[:8]}"
    if dialect == "bigquery":
        return "`" + ".".join(parts) + "`"      # one backticked path: an unquoted dashed project id is not valid in CREATE / INSERT / DROP
    return ".".join(parts)


def _sql_swap_partition_script(dialect: str, out_tbl: str, stage: str, cols: List[str], label: str, keys: List[str]) -> List[str]:
    """The statements that replace ONLY this run's partition rows of out_tbl with the stage table's rows. BigQuery: one transaction script.
    Snowflake: BEGIN / DELETE / INSERT / COMMIT as separate statements (one connection). Databricks (Delta): one atomic INSERT ... REPLACE WHERE."""
    owned = sorted(set([label] + list(keys)))
    cond = f"source_partition IN ({', '.join(_sql_str(k) for k in owned)})"
    col_list = ", ".join(cols)
    insert = f"INSERT INTO {out_tbl} ({col_list}) SELECT {col_list} FROM {stage}"
    if dialect == "databricks":
        return [f"INSERT INTO {out_tbl} REPLACE WHERE {cond} SELECT {col_list} FROM {stage}"]
    delete = f"DELETE FROM {out_tbl} WHERE {cond}"
    if dialect == "bigquery":
        return [f"BEGIN TRANSACTION;\n{delete};\n{insert};\nCOMMIT TRANSACTION;"]
    return ["BEGIN", delete, insert, "COMMIT"]


def _sql_cosine_similarity(dialect: str, a: str, b: str) -> str:
    """Cosine similarity (higher = closer) of two embedding expressions, in the dialect's own functions."""
    if dialect == "bigquery":
        # SAFE.: COSINE_DISTANCE errors on zero-norm / empty vectors and on a dimension mismatch, and BigQuery does not guarantee a WHERE guard runs first
        return f"(1 - SAFE.COSINE_DISTANCE({a}, {b}))"
    if dialect == "snowflake_cortex":
        return f"VECTOR_COSINE_SIMILARITY({a}, {b})"
    if dialect == "databricks":
        # plain Spark array functions (no vector type or index needed): dot / (|a| * |b|), NULL for a zero vector
        norm = lambda x: f"sqrt(aggregate(transform({x}, v -> v * v), CAST(0 AS DOUBLE), (acc, v) -> acc + v))"  # noqa: E731
        dot = f"aggregate(zip_with(CAST({a} AS ARRAY<DOUBLE>), CAST({b} AS ARRAY<DOUBLE>), (x, y) -> x * y), CAST(0 AS DOUBLE), (acc, v) -> acc + v)"
        return f"({dot} / NULLIF({norm(a)} * {norm(b)}, 0))"
    raise ValueError(f"unsupported sql_dialect: {dialect!r}")


def _build_retrieval_eval_query(cfg: Dict[str, Any], steps_cfg: List[dict], dialect: str, output_table: str, id_column: str,
                                *, asset_name: str = "", scoped: bool = False) -> str:
    """The warehouse-side retrieval_eval: embed every golden query with the SAME embedding the build used (same model, same function), score
    it against every chunk of `output_table` by cosine similarity and return each query's top-k as rows (qi, rank, chunk_id, source_id), best first.
    Matching against `expected` and the metrics happen in Python (_retrieval_quality_result), the same code python mode uses.

    Candidates = the WHOLE table: the table is the index (python mode likewise searches the whole collection, every partition), restricted to rows
    whose `embedding_model` is the current one (vectors of two models cannot be compared) and, with stamp_provenance, to this asset's
    `source_asset_key`. Quarantined / dead-weight rows stay candidates (python mode does not filter them either): the check scores what
    the index serves; `exclude` mode has already left them out of the table."""
    if dialect not in _SQL_DIALECTS:
        raise ValueError(f"unsupported sql_dialect: {dialect!r}")
    embed_step = next(s_ for s_ in steps_cfg if s_["op"] == "embed")
    model = embed_step.get("model")
    tbl = _sql_table(output_table, "output_table", dialect)
    id_col = _sql_ident(id_column, "id_column")
    k = int(cfg.get("k", 5))
    golden = "\n    UNION ALL ".join(f"SELECT {i} AS qi, {_sql_str(q['query'])} AS content" for i, q in enumerate(cfg["queries"]))
    if dialect == "bigquery":
        model = model or "embedding_model"
        if not isinstance(model, str) or not _MODEL_RE.match(model):
            raise ValueError(f"execution_mode='sql': embed model {model!r} is not a valid BigQuery model reference (project.dataset.model)")
        embedded = (f"SELECT qi, ml_generate_embedding_result AS qemb\n  FROM ML.GENERATE_EMBEDDING(MODEL `{model}`,\n"
                    f"    (SELECT qi, content FROM golden),\n    STRUCT(TRUE AS flatten_json_output))")
        usable = "ARRAY_LENGTH(c.embedding) > 0 AND ARRAY_LENGTH(q.qemb) = ARRAY_LENGTH(c.embedding)"
        has_vec = "ARRAY_LENGTH(embedding) > 0"
    else:
        embedded = f"SELECT qi, {_sql_embed_expr(dialect, 'content', model)} AS qemb\n  FROM golden"
        usable = "TRUE"      # a NULL query vector scores NULL and drops out below; q.qemb is referenced once, so the engine has no reason to embed a query twice
        has_vec = "embedding IS NOT NULL"
    where = [f"embedding_model = {_sql_str(_sql_embed_model_id(dialect, embed_step.get('model')))}", has_vec]
    if scoped:
        where.append(f"source_asset_key = {_sql_str(asset_name)}")
    return (
        f"WITH golden AS (\n  {golden}\n),\n"
        f"queries AS (\n  {embedded}\n),\n"
        f"candidates AS (\n  SELECT chunk_id, CAST({id_col} AS STRING) AS source_id, embedding\n  FROM {tbl}\n  WHERE {' AND '.join(where)}\n),\n"
        f"scored AS (\n  SELECT q.qi AS qi, c.chunk_id AS chunk_id, c.source_id AS source_id,\n"
        f"         {_sql_cosine_similarity(dialect, 'c.embedding', 'q.qemb')} AS sim\n"
        f"  FROM queries AS q CROSS JOIN candidates AS c\n  WHERE {usable}\n),\n"
        f"ranked AS (\n  SELECT qi, chunk_id, source_id, ROW_NUMBER() OVER (PARTITION BY qi ORDER BY sim DESC, chunk_id) AS rn\n"
        f"  FROM scored\n  WHERE sim IS NOT NULL\n)\n"
        f"SELECT qi, rn, chunk_id, source_id FROM ranked WHERE rn <= {k} ORDER BY qi, rn")


def _run_sql_retrieval_eval(cfg: Dict[str, Any], steps_cfg: List[dict], source_cfg: dict, dialect: str, output_table: str,
                            id_column: str, asset_name: str, scoped: bool, context) -> AssetCheckResult:
    """retrieval_quality for execution_mode='sql': runs _build_retrieval_eval_query through the build's own warehouse resource."""
    sql = _build_retrieval_eval_query(cfg, steps_cfg, dialect, output_table, id_column, asset_name=asset_name, scoped=scoped)
    resource_key = source_cfg["resource_key"]
    with _sql_session(getattr(context.resources, resource_key), resource_key) as session:
        rows = session.fetch(sql)
    ranked: List[List[Tuple[str, Any]]] = [[] for _ in cfg["queries"]]
    for r in rows:                                      # ordered by (qi, rank)
        ranked[int(r[0])].append((str(r[2]), None if r[3] is None else str(r[3])))
    return _retrieval_quality_result(cfg, ranked)


def _run_sql_chunks_stage(
    context, resource_key: str, chunk_step: dict, source_cfg: dict, dialect: str, chunks_table: str,
    id_column: str, text_column: str, metadata_columns: List[str], *, asset_name: str, code_version: str,
    stamp_provenance: bool, receipts_cfg: Optional[Dict[str, Any]],
) -> "tuple[Dict[str, Any], List[str]]":
    """Executes the per_step_ops SQL-mode 'chunks' stage for real. Returns (metadata, cols) -- cols is
    what the next stage's `SELECT *` carries forward."""
    resource = getattr(context.resources, resource_key)
    chunks_tbl = _sql_table(chunks_table, "output_table", dialect)
    with _sql_session(resource, resource_key) as session:
        prior_first_seen = False
        if receipts_cfg is not None and receipts_cfg.get("prune"):
            try:
                existing = {c.lower() for c in _sql_table_columns(session, dialect, chunks_tbl, context)}
                prior_first_seen = "feedback_first_seen_at" in existing
            except ValueError:
                prior_first_seen = False
        sql, cols = _build_sql_chunks_table_query(
            chunk_step, source_cfg, dialect, chunks_tbl, id_column, text_column, metadata_columns,
            asset_name=asset_name, code_version=code_version, stamp_provenance=stamp_provenance,
            receipts=receipts_cfg, prior_first_seen=prior_first_seen, prior_chunks_table=chunks_tbl,
        )
        context.log.info(f"per_step_ops (sql): building chunks table {chunks_table}")
        session.execute(sql)
        row_count = int(session.fetch(f"SELECT COUNT(*) FROM {chunks_tbl}")[0][0])
    metadata = {
        "dagster/row_count": MetadataValue.int(row_count),
        "sql_table": MetadataValue.text(chunks_table),
        "generated_sql": MetadataValue.md(f"```sql\n{sql}\n```"),
    }
    return metadata, cols


def _run_sql_feature_stage(
    context, resource_key: str, step: dict, dialect: str, prior_table: str, this_table: str, base_cols: List[str],
    *, tag: str, tag_col: str, incremental: bool,
) -> Dict[str, Any]:
    """Executes one per_step_ops SQL-mode feature stage (classify OR embed, `tag` is 'category' or
    'embedding') for real. Probes `this_table`'s OWN existing contents (not `prior_table`) for
    incremental reuse -- each stage now has its own history, instead of every feature sharing one
    output_table's history."""
    resource = getattr(context.resources, resource_key)
    prior_tbl = _sql_table(prior_table, "output_table", dialect)
    this_tbl = _sql_table(this_table, "output_table", dialect)
    with _sql_session(resource, resource_key) as session:
        can_reuse = False
        if incremental:
            try:
                existing = {c.lower() for c in _sql_table_columns(session, dialect, this_tbl, context)}
                can_reuse = {"chunk_id", "content_hash", tag, tag_col} <= existing
            except ValueError:
                can_reuse = False
        if tag == "category":
            sql = _build_sql_classify_table_query(step, dialect, prior_tbl, this_tbl, base_cols, can_reuse=can_reuse)
        else:
            sql = _build_sql_embed_table_query(step, dialect, prior_tbl, this_tbl, base_cols, can_reuse=can_reuse)
        context.log.info(f"per_step_ops (sql): building {tag} table {this_table} (reuse={can_reuse})")
        session.execute(sql)
        row_count = int(session.fetch(f"SELECT COUNT(*) FROM {this_tbl}")[0][0])
    return {
        "dagster/row_count": MetadataValue.int(row_count),
        "sql_table": MetadataValue.text(this_table),
        "reused": MetadataValue.bool(can_reuse),
        "generated_sql": MetadataValue.md(f"```sql\n{sql}\n```"),
    }


def _run_sql_mode(
    context, steps_cfg: List[dict], source_cfg: dict, dialect: str, output_table: str,
    id_column: str, text_column: str, metadata_columns: List[str],
    *, asset_name: str = "", code_version: str = "", stamp_provenance: bool = False, emit_data_version: bool = True,
    receipts_cfg: Optional[Dict[str, Any]] = None, incremental: bool = True,
) -> Output:
    partition, partition_keys = _partition_label_and_keys(context)
    source_cfg = {**source_cfg, "sql": _bind_partition_placeholders(source_cfg["sql"], context, _sql_str)}
    resource_key = source_cfg["resource_key"]
    resource = getattr(context.resources, resource_key)
    out_tbl = _sql_table(output_table, "output_table", dialect)
    classify_step = next((s for s in steps_cfg if s["op"] == "classify"), None)
    embed_step = next((s for s in steps_cfg if s["op"] == "embed"), None)
    enrich_step = next((s for s in steps_cfg if s["op"] == "enrich"), None)
    incremental = bool(incremental and (classify_step or embed_step or enrich_step))
    kw = dict(asset_name=asset_name, code_version=code_version, partition=partition, stamp_provenance=stamp_provenance, receipts=receipts_cfg)
    q = (receipts_cfg or {}).get("quarantine")
    prune = (receipts_cfg or {}).get("prune")
    now_dt = datetime.now(timezone.utc)
    if receipts_cfg is not None:
        # one cut-off and one clock for the preview queries and the statement itself, so the report describes exactly what the build did
        kw.update(since=_receipts_since(receipts_cfg), now=now_dt.isoformat())
    # every read-back below describes THIS partition's rows only (the table also holds the other partitions)
    scope = f" WHERE source_partition = {_sql_str(partition)}" if partition is not None else ""
    reuse: Dict[str, bool] = {}
    prior_rows = 0
    inc_counts: Optional[Dict[str, int]] = None
    with _sql_session(resource, resource_key) as session:
        columns = _sql_table_columns(session, dialect, out_tbl, context) if (incremental or partition is not None or prune) else None
        if prune:
            kw["prior_first_seen"] = "feedback_first_seen_at" in {c.lower() for c in columns or []}
        rewrite_rows = prune_counts = None
        if receipts_cfg is not None:
            # read before the table is replaced: prior first_seen_at is still there, and chunks the statement drops (quarantine / prune
            # exclude) are still reported -- the quarantined ones are exactly what needs rewriting
            pv = lambda kind: _build_sql_mode_query(  # noqa: E731
                steps_cfg, source_cfg, dialect, output_table, id_column, text_column, metadata_columns, preview=kind, **kw)
            rewrite_rows = session.fetch(pv("rewrite"))
            if prune:
                prune_counts = session.fetch(pv("counts"))[0]
        if incremental:
            reuse, prior_rows = _sql_probe_prior_table(session, dialect, out_tbl, classify_step, embed_step, context, columns=columns, enrich_step=enrich_step)
        build = lambda **extra: _build_sql_mode_query(  # noqa: E731
            steps_cfg, source_cfg, dialect, output_table, id_column, text_column, metadata_columns,
            incremental=incremental, reuse=reuse, **kw, **extra)
        if incremental and any(reuse.values()):
            # chunks that will pay for an AI call, counted BEFORE the table is changed (afterwards the prior values are gone).
            # Order must match feature()'s own call order inside _build_sql_mode_query (classify -> enrich -> embed): the
            # counts SELECT lists its columns in that same order, and this reads them back by position.
            row = session.fetch(build(counts_only=True))[0]
            names = [n for n, on in (("categories", classify_step), ("enrichments", enrich_step), ("embeddings", embed_step)) if on]
            inc_counts = {"total": int(row[0])}
            for i, name in enumerate(names, start=1):
                inc_counts[name] = int(row[i])
        context.log.info(f"execution_mode='sql' ({dialect}): running server-side chunk/classify/enrich/embed into {output_table}"
                         + (f" (partition {partition!r})" if partition is not None else "")
                         + (f" (incremental, reusing prior: {reuse})" if incremental else ""))
        if partition is None:
            sql = build()
            session.execute(sql)
        elif not columns:
            sql = build(write="create_table")          # first partition run: nothing to preserve
            session.execute(sql)
        else:
            if "source_partition" not in {c.lower() for c in columns}:
                raise ValueError(f"{output_table} exists but has no source_partition column (built by an unpartitioned run or an older version), so this "
                                 "partition's rows cannot be told apart from the rest. Drop or rename it once and re-run.")
            stage = _sql_stage_table(out_tbl, partition, dialect)
            sql = build(write="stage", stage_table=stage)
            try:
                session.execute(sql)                   # the AI calls happen here; output_table is untouched if this fails
                stage_cols = _sql_table_columns(session, dialect, stage, context)
                missing = [c for c in stage_cols if c.lower() not in {x.lower() for x in columns}]
                if missing:
                    raise ValueError(f"{output_table} lacks column(s) {missing} that this recipe now writes (a step or option changed since the table "
                                     "was built). Drop or alter the table once and re-run; the other partitions' rows are untouched.")
                if dialect == "databricks" and [c.lower() for c in stage_cols] != [c.lower() for c in columns]:
                    raise ValueError(f"{output_table}'s columns {columns} differ from what this recipe writes {stage_cols}; INSERT ... REPLACE WHERE matches by "
                                     "position, so it will not run against a table with another column set or order. Drop or alter the table once and re-run.")
                for stmt in _sql_swap_partition_script(dialect, out_tbl, stage, stage_cols, partition, partition_keys):
                    try:
                        session.execute(stmt)
                    except Exception:
                        if dialect == "snowflake_cortex":
                            with contextlib.suppress(Exception):
                                session.execute("ROLLBACK")
                        raise
            finally:
                with contextlib.suppress(Exception):
                    session.execute(f"DROP TABLE IF EXISTS {stage}")
        row_count = int(session.fetch(f"SELECT COUNT(*) FROM {out_tbl}{scope}")[0][0])
        # Read the content version back (same aggregate the statement stamped as source_version).
        content_version = session.fetch(f"SELECT {_sql_version_agg(dialect)} FROM {out_tbl}{scope}")[0][0]
        fb_counts = None
        if receipts_cfg is not None and receipts_cfg.get("stamp", True):
            quarantined = f", {_sql_count_if(dialect, 'feedback_quarantined')}" if q and q["mode"] == "flag" else ""
            row = session.fetch(f"SELECT {_sql_count_if(dialect, 'feedback_retrievals > 0')}{quarantined} FROM {out_tbl}{scope}")[0]
            fb_counts = {"chunks_with_feedback": int(row[0]), "quarantined": int(row[1]) if quarantined else None}

    metadata: Dict[str, Any] = {
        "dagster/row_count": MetadataValue.int(row_count),
        "execution_mode": MetadataValue.text("sql"),
        "sql_dialect": MetadataValue.text(dialect),
        "output_table": MetadataValue.text(output_table),
        "generated_sql": MetadataValue.md(f"```sql\n{sql}\n```"),
    }
    if partition is not None:
        metadata["source_partition"] = MetadataValue.text(partition)
    if content_version is not None:
        metadata["context_version"] = MetadataValue.text(str(content_version))
    if incremental:
        what_changed = {"embed": "embed model", "classify": "candidate_labels", "enrich": "enrich model/mode"}
        for name, step, key in (("embeddings", embed_step, "embed"), ("categories", classify_step, "classify"), ("enrichments", enrich_step, "enrich")):
            if step is None:
                continue
            new = inc_counts[name] if inc_counts else row_count
            metadata[f"reused_{name}"] = MetadataValue.int(row_count - new)
            metadata[f"new_{name}"] = MetadataValue.int(new)
            if reuse.get(key) and prior_rows > 0 and row_count - new == 0 and row_count > 0:
                context.log.warning(
                    f"incremental: the prior {output_table} has {prior_rows} row(s) but none of their {name} could be reused. "
                    f"Expected only if the {what_changed[key]} changed or every chunk's text changed; "
                    "otherwise check that chunking/ids are stable and the table was not rebuilt by another recipe.")
    if receipts_cfg is not None:
        summary: Dict[str, Any] = {
            "window_days": float(receipts_cfg["window_days"]), "rewrite_below": float(receipts_cfg["rewrite_below"]),
            "rewrite_candidates": [{"chunk_id": r[0], "source_id": None if r[1] is None else str(r[1]), "retrievals": int(r[2]), "rated": int(r[3]),
                                    "mean_outcome": float(r[4]), "cited": int(r[5])} for r in rewrite_rows],
        }
        if prune:
            judged = int(prune_counts[1]) >= int(prune["never_retrieved_after_receipts"])
            summary.update({"prune_mode": prune["mode"], "pruned": int(prune_counts[0]), "prune_judged": judged,
                            "prune_receipts_in_window": int(prune_counts[1]), "prune_protected_new": int(prune_counts[3]) - int(prune_counts[0]) if judged else 0})
            metadata["receipts_pruned"] = MetadataValue.int(int(prune_counts[0]))
        metadata["receipts_feedback"] = MetadataValue.json(summary)
        metadata["receipts_scoped_to_recipe"] = MetadataValue.text(code_version if receipts_cfg.get("scope_to_recipe", True) else "all recipes")
        metadata["receipts_quarantine_mode"] = MetadataValue.text(q["mode"] if q else "off")
        if fb_counts:
            metadata["receipts_chunks_with_feedback"] = MetadataValue.int(fb_counts["chunks_with_feedback"])
            if fb_counts["quarantined"] is not None:
                metadata["receipts_quarantined"] = MetadataValue.int(fb_counts["quarantined"])
    return Output(
        value=None, metadata=metadata,
        data_version=DataVersion(str(content_version)) if (emit_data_version and content_version is not None) else None,
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
            "DuckDB has no such function -- use 'python' with a duckdb_resource `source:` "
            "for DuckDB."
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
    incremental: bool = Field(
        default=True,
        description=(
            "execution_mode='sql' only (ignored in python mode, which has embed `reuse_from`). Re-use the embeddings and categories already in "
            "`output_table` instead of paying the warehouse AI function for the whole corpus on every run. A chunk's prior embedding is reused when "
            "its content_hash matches AND the stored embedding_model equals the current model; a prior category when the content_hash matches AND "
            "the stored category_signature (hash of candidate_labels) equals the current one -- so a changed model or label set recomputes everything. "
            "Only chunks with no reusable value reach ML.GENERATE_EMBEDDING / Cortex / ai_query / classify. The statement is still one atomic CREATE OR "
            "REPLACE TABLE. The first run (no table), or a table lacking the needed columns (built by an older version), builds in full. "
            "Set false to recompute everything every run. Materialization metadata: reused_embeddings, new_embeddings, reused_categories, new_categories."
        ),
    )
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered list of {id, op, ...op-specific fields} dicts. Each step's `source:` "
            "names a prior step's id to read from; if omitted, defaults to the immediately "
            "preceding step (so you don't have to repeat `source:` on every line). "
            "Valid ops: 'chunk' (must be first), 'filter', 'dedupe', 'sensitivity', 'stamp', 'enrich', 'classify', 'embed', "
            "'write_vector_store'. 'write_vector_store' is python-mode only (no warehouse-native equivalent); the "
            "rest also work with execution_mode='sql' ('classify'/'embed'/'enrich' each map onto a warehouse-native "
            "AI function; 'filter'/'dedupe'/'sensitivity'/'stamp' are plain SQL), each with its own sub-validation "
            "for the specific parts that have no portable warehouse-SQL equivalent, e.g. sensitivity's "
            "phone/credit_card detectors and action='redact', filter's lookahead/lookbehind patterns, enrich's "
            "prepend=True."
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
            "receipts directly from index hits. Python mode: the write_vector_store step. SQL mode: columns chunk_id, content_hash, source_asset_key, source_version (content hash of the whole table), source_partition, source_code_version on the output table."
        ),
    )
    per_step_ops: bool = Field(
        default=False,
        description=(
            "When True, emit each configured step as its own @dg.op inside one op graph instead of folding "
            "the whole chain into a single @asset -- one visible, individually re-executable-on-failure asset "
            "node per step (chunk/classify/embed/write_vector_store/...), same shape as "
            "AgenticPipelineComponent's per_step_ops. The asset key for the FINAL step stays `asset_name` "
            "unchanged (so any existing upstream_asset_key pointing at this component's output keeps working); "
            "every OTHER step publishes as `<asset_name>_<step id>`, with real step-to-step lineage between "
            "them (not just every step depending on the raw upstream). Supported with execution_mode='sql' "
            "too: each participating step (chunk always, classify/embed if configured) becomes its own real "
            "warehouse table instead of one combined CREATE OR REPLACE TABLE statement, with incremental reuse "
            "preserved per table (write_vector_store has no SQL equivalent either way, so a SQL-mode pipeline "
            "tops out at 3 steps/tables). Not yet supported combined with partitioning in sql mode (per-table "
            "partition staging is a tracked follow-up), nor with freshness_max_lag_minutes in either mode "
            "(AssetsDefinition.from_graph has no per-output freshness-policy plumbing)."
        ),
    )

    receipts: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Learn from real use: read the retrieval receipts recorded by resources/context_receipt_store back into every build. "
            "{resource_key: <registered ContextReceiptStoreComponent>, window_days: 30, min_retrievals: 5, min_outcomes: 3, "
            "refresh_priors: true, scope_to_recipe: true, stamp: true, quarantine: {mean_outcome_below: -0.3, mode: flag|exclude}, "
            "regression_check: {min_outcomes: 10, max_drop: 0.15, severity: WARN|ERROR}}. Right before write_vector_store each chunk is "
            "stamped with feedback_retrievals / feedback_rated / feedback_mean_outcome / feedback_cited / feedback_prior (so a gateway can "
            "rerank), chunks users keep rating badly are flagged or (mode: exclude) left out of the index until their old receipts age out "
            "of the window, and the run reports dead weight, best/worst chunks, exclusion reasons and outcomes per recipe. "
            "regression_check adds the `recipe_outcomes` asset check: is the current recipe doing worse in production than an earlier one? "
            "Feedback is scoped to the current recipe (code_version) by default so a recipe change starts fresh. Python mode reads a ContextReceiptStore resource."
            " In execution_mode='sql' (sql_dialect bigquery / snowflake_cortex / databricks) the same feedback is computed inside the one CREATE TABLE statement by SQL joins against the receipt tables: {dataset: <where the receipt tables live, required: 'project.dataset' on BigQuery, 'DATABASE.SCHEMA' on Snowflake, 'catalog.schema' on Databricks>, table_prefix: '', resource_key: <optional; defaults to source.resource_key and must be the same BigQuery resource>, plus the same window_days / min_retrievals / min_outcomes / scope_to_recipe / stamp / quarantine / regression_check}; refresh_priors and write_vector_store do not apply (feedback_prior is read from context_item_scores as written), and mode: exclude drops the rows from the output table."
        ),
    )
    retrieval_eval: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "A golden set that gates this knowledge base: {queries: [{query, expected: [chunk or source ids]}], k: 5, "
            "min_recall: 0.8, min_mrr: null, severity: ERROR|WARN, blocking: false}. After the index is written, each "
            "query is embedded with the same embed step and run against it; the `retrieval_quality` asset check reports "
            "recall@k and MRR and fails below the thresholds, with `blocking: true` holding downstream assets until it "
            "passes. Pair with code_version: a new recipe must still answer the golden questions. Python mode needs a chromadb index; execution_mode='sql' (bigquery, snowflake_cortex, databricks) runs it warehouse-side by cosine similarity over the whole output table."
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

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO), required for time-based types.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts (type: daily/weekly/monthly/hourly/static/dynamic). "
            "One entry is a single axis; several make a MultiPartitionsDefinition. Mutually exclusive with the flat partition_* fields. "
            "In execution_mode='sql', `source.sql` can use {partition_key.<name>} for each axis."
        ),
    )
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run); 'single_run' materializes a whole backfill range in one run. Leave unset for Dagster's default "
            "(one run per partition). Ignored when the asset is not partitioned. In execution_mode='sql', single_run is only allowed when `source.sql` "
            "uses both {partition_start} and {partition_end} (the query must itself select the whole range) and the asset is not multi-dimension; "
            "otherwise the build raises."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

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
        valid_ops = {"chunk", "classify", "embed", "write_vector_store", "filter", "dedupe", "sensitivity", "stamp", "enrich"}
        for s in steps_cfg:
            if "id" not in s or "op" not in s:
                raise ValueError(f"every step needs {{id, op}}; got: {s}")
            if s["op"] not in valid_ops:
                raise ValueError(f"step {s['id']!r} op={s['op']!r} unsupported. Valid: {sorted(valid_ops)}")

        execution_mode = self.execution_mode
        if execution_mode == "sql":
            # chunk/classify/embed/enrich each map onto a warehouse-native AI function (enrich onto
            # ML.GENERATE_TEXT / CORTEX.COMPLETE / ai_query, the same text-generation family as
            # classify's AI.CLASSIFY/CLASSIFY_TEXT); filter/dedupe/sensitivity/stamp are expressible as
            # plain SQL (see _build_sql_mode_query). Each has its own sub-validation (e.g. sensitivity's
            # phone/credit_card detectors and redact action, filter's exclude_patterns, enrich's
            # prepend=True) rejecting the specific parts that have no portable warehouse-SQL equivalent.
            sql_capable_ops = {"chunk", "classify", "embed", "enrich", "filter", "dedupe", "sensitivity", "stamp"}
            python_only = sorted({s["op"] for s in steps_cfg} - sql_capable_ops)
            if python_only:
                raise ValueError(
                    f"execution_mode='sql' supports chunk/classify/embed/enrich/filter/dedupe/sensitivity/stamp steps; "
                    f"python-mode-only op(s): {python_only}."
                )
            if self.per_step_ops:
                per_step_sql_only = sorted({s["op"] for s in steps_cfg} - {"chunk", "classify", "embed"})
                if per_step_sql_only:
                    raise ValueError(
                        f"execution_mode='sql' with per_step_ops=True supports only chunk/classify/embed steps "
                        f"(one real table per step); filter/dedupe/sensitivity/stamp/enrich aren't available as "
                        f"their own per-step SQL tables yet: {per_step_sql_only}. Drop per_step_ops, or drop those steps."
                    )
            custom_classify = [s["id"] for s in steps_cfg if s["op"] == "classify" and s.get("mode") == "custom"]
            if custom_classify:
                raise ValueError(
                    f"classify step(s) {custom_classify} use mode='custom', not supported with execution_mode='sql' "
                    f"(a bring-your-own Python function can't run inside a warehouse-native SQL statement; use "
                    f"execution_mode='python' for a custom classifier)."
                )
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
            self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        if self.source and self.source.get("sql"):
            _validate_partition_placeholders(self.source["sql"], partitions_def)
        backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            from dagster import MultiPartitionsDefinition
            used = set(_sql_placeholders((self.source or {}).get("sql", "")))
            if execution_mode == "sql":
                if isinstance(partitions_def, MultiPartitionsDefinition):
                    raise ValueError("backfill_policy='single_run' is not supported with multi-dimension partitions in execution_mode='sql'; use multi_run.")
                if not {"partition_start", "partition_end"} <= used:
                    raise ValueError(
                        "execution_mode='sql' with backfill_policy='single_run' is only safe when `source.sql` selects the whole backfill range with "
                        "{partition_start} and {partition_end} (otherwise the run would read the full source or just one partition and stamp it as the range). "
                        "Use those placeholders, or backfill_policy='multi_run'.")
            backfill_policy_obj = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy_obj = BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)

        if self.per_step_ops and execution_mode == "sql" and partitions_def is not None:
            raise ValueError(
                "per_step_ops=True with execution_mode='sql' does not support partitioning yet: each "
                "step's own table would need its own partition-aware stage/swap script (today's single-table "
                "partition staging, per step) -- real added complexity, tracked as a follow-up rather than "
                "built speculatively. Unpartitioned per_step_ops SQL mode is supported."
            )
        if self.per_step_ops and self.freshness_max_lag_minutes is not None:
            raise ValueError(
                "per_step_ops=True does not support freshness_max_lag_minutes: Dagster's graph_multi_asset "
                "has no per-output freshness_policy plumbing today (confirmed directly against the installed "
                "dagster.AssetsDefinition.from_graph -- it accepts tags/owners/code_version/description/metadata "
                "per output but not freshness), so setting both would silently do nothing rather than fail loudly."
            )
        freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            lag = timedelta(minutes=self.freshness_max_lag_minutes)
            if hasattr(FreshnessPolicy, "time_window"):  # current Dagster
                freshness_policy = (
                    FreshnessPolicy.cron(deadline_cron=self.freshness_cron, lower_bound_delta=lag)
                    if self.freshness_cron else FreshnessPolicy.time_window(fail_window=lag)
                )
            else:  # older Dagster
                freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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
        incremental = self.incremental

        context_policy = _validate_context_policy(self.context_policy) if self.context_policy else None
        retrieval_eval = None
        if self.retrieval_eval:
            retrieval_eval = _validate_retrieval_eval(self.retrieval_eval, steps_cfg, execution_mode)
        receipts_cfg = _validate_receipts(self.receipts, steps_cfg, execution_mode, sql_dialect, source_cfg) if self.receipts else None
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
            backfill_policy=backfill_policy_obj,
        )
        check_specs: List[Any] = []
        if retrieval_eval is not None:
            check_specs.append(AssetCheckSpec(
                name="retrieval_quality", asset=AssetKey.from_user_string(asset_name),
                blocking=bool(retrieval_eval.get("blocking", False))))
        if receipts_cfg is not None and receipts_cfg.get("regression_check"):
            check_specs.append(AssetCheckSpec(name="recipe_outcomes", asset=AssetKey.from_user_string(asset_name)))
        if check_specs:
            asset_kwargs["check_specs"] = check_specs
        if context_policy is not None:
            asset_kwargs["metadata"] = {_CONTEXT_POLICY_METADATA_KEY: MetadataValue.json(context_policy)}
        if self.upstream_asset_key:
            asset_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(self.upstream_asset_key))}
        _resource_keys = set()
        if source_cfg and source_cfg.get("resource_key"):
            _resource_keys.add(source_cfg["resource_key"])
        if receipts_cfg is not None:
            _resource_keys.add(receipts_cfg["resource_key"])
        if _resource_keys:
            asset_kwargs["required_resource_keys"] = _resource_keys

        if execution_mode == "sql" and self.per_step_ops:
            return self._build_per_step_ops_sql_defs(
                steps_cfg=steps_cfg, asset_name=asset_name, id_column=id_column, text_column=text_column,
                metadata_columns=metadata_columns, recipe_code_version=recipe_code_version,
                stamp_provenance=stamp_provenance, source_cfg=source_cfg, receipts_cfg=receipts_cfg,
                retrieval_eval=retrieval_eval, sql_dialect=sql_dialect, output_table=output_table,
                incremental=incremental, all_tags=_all_tags, check_specs=check_specs,
            )

        if execution_mode == "sql":
            # The table IS the output; there is nothing for an IO manager to persist (and a single_run backfill needs that: an IO manager
            # cannot store one value for several partitions). dagster_type=Nothing opts out.
            @asset(dagster_type=Nothing, **asset_kwargs)
            def _sql_asset(context: AssetExecutionContext):
                yield _run_sql_mode(context, steps_cfg, source_cfg, sql_dialect, output_table, id_column, text_column, metadata_columns,
                                    asset_name=asset_name, code_version=recipe_code_version, stamp_provenance=stamp_provenance,
                                    emit_data_version=emit_data_version, receipts_cfg=receipts_cfg, incremental=incremental)
                if retrieval_eval is not None:
                    yield _run_sql_retrieval_eval(retrieval_eval, steps_cfg, source_cfg, sql_dialect, output_table, id_column, asset_name,
                                                  stamp_provenance, context)
                if receipts_cfg is not None and receipts_cfg.get("regression_check"):
                    by_cv = _sql_fetch_outcomes_by_code_version(context, source_cfg, receipts_cfg, asset_name)
                    yield _run_recipe_outcomes_check(receipts_cfg, asset_name, recipe_code_version, context, outcomes_by_cv=by_cv)
            return Definitions(assets=[_sql_asset])

        if self.per_step_ops:
            return self._build_per_step_ops_defs(
                steps_cfg=steps_cfg, asset_name=asset_name, id_column=id_column, text_column=text_column,
                metadata_columns=metadata_columns, max_source_rows=max_source_rows,
                recipe_code_version=recipe_code_version, emit_data_version=emit_data_version,
                stamp_provenance=stamp_provenance, include_preview=include_preview, preview_rows=preview_rows,
                source_cfg=source_cfg, receipts_cfg=receipts_cfg, retrieval_eval=retrieval_eval,
                partitions_def=partitions_def, backfill_policy_obj=backfill_policy_obj,
                freshness_policy=freshness_policy, retry_policy=retry_policy, all_tags=_all_tags,
                check_specs=check_specs,
            )

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
            receipts_summary: Optional[Dict[str, Any]] = None

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
                    if receipts_cfg is not None:
                        write_input, step, receipts_summary = _apply_receipt_feedback(
                            write_input, step, receipts_cfg, asset_name, recipe_code_version, context, source_id_column=id_column)
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
            if receipts_summary is not None:
                metadata["receipts_feedback"] = MetadataValue.json(receipts_summary)
                metadata["receipts_quarantined"] = MetadataValue.int(receipts_summary["quarantined"])
                metadata["receipts_chunks_with_feedback"] = MetadataValue.int(receipts_summary["chunks_with_feedback"])
            if include_preview and len(final_df) > 0:
                try:
                    _prev_cols = [c for c in final_df.columns if c != "embedding"]
                    _prev = final_df[_prev_cols]
                    _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                    metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False) or "")
                except Exception as e:
                    context.log.warning(f"preview emission failed: {e}")
            yield Output(
                value=final_df,
                metadata=metadata,
                data_version=DataVersion(content_version) if (emit_data_version and content_version) else None,
            )
            if retrieval_eval is not None:
                yield _run_retrieval_eval(retrieval_eval, steps_cfg, asset_name, stamp_provenance, context)
            if receipts_cfg is not None and receipts_cfg.get("regression_check"):
                yield _run_recipe_outcomes_check(receipts_cfg, asset_name, recipe_code_version, context)

        return Definitions(assets=[_asset])

    def _build_per_step_ops_defs(
        self, *, steps_cfg: List[dict], asset_name: str, id_column: str, text_column: str,
        metadata_columns: List[str], max_source_rows: Optional[int], recipe_code_version: str,
        emit_data_version: bool, stamp_provenance: bool, include_preview: bool, preview_rows: int,
        source_cfg: Optional[dict], receipts_cfg: Optional[Dict[str, Any]], retrieval_eval: Optional[Dict[str, Any]],
        partitions_def, backfill_policy_obj, freshness_policy, retry_policy, all_tags: Dict[str, str],
        check_specs: List[Any],
    ) -> Definitions:
        """`per_step_ops=True`: one @dg.op per configured step inside a single op graph, instead of
        folding the whole chain into one @asset. Each step becomes its own asset node -- individually
        visible, individually metadata'd, and (via Dagster's native re-execution-from-failure)
        independently retryable.

        Built directly on `dg.AssetsDefinition.from_graph` rather than the `@dg.graph_multi_asset`
        convenience decorator, because that decorator's own default -- confirmed by reading
        `AssetsDefinition.from_graph`'s docstring directly -- is "every declared output depends on
        every graph INPUT," not on the op-call topology inside the graph; left alone, every per-step
        asset here would show as depending only on `upstream_asset_key`, with no edges between the
        steps themselves. `internal_asset_deps` is the documented fix, and `@graph_multi_asset` doesn't
        expose that parameter at all, so the lower-level API is used instead.

        The final step keeps the plain `asset_name` key (so existing `upstream_asset_key: <asset_name>`
        references elsewhere keep working unchanged); every other step publishes as `<asset_name>_<step id>`.
        """
        last_step_id = steps_cfg[-1]["id"]
        # AssetsDefinition.from_graph's check_specs must each correspond to a literally-named output on
        # the underlying node (confirmed directly: AssetCheckSpec.get_python_identifier() -- there is no
        # bare-yield auto-wiring the way a plain @asset(check_specs=...) gets for free). Caught here
        # before shipping SQL mode's own copy of this graph-construction pattern.
        check_output_names = [spec.get_python_identifier() for spec in (check_specs or [])]

        def _step_asset_key(step_id: str) -> str:
            return asset_name if step_id == last_step_id else f"{asset_name}_{step_id}"

        def _step_metadata(step_id: str, op_name: str, df: pd.DataFrame, elapsed: float) -> Dict[str, Any]:
            md: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "op": MetadataValue.text(op_name),
                "elapsed_seconds": MetadataValue.float(round(elapsed, 3)),
            }
            for k, v in (df.attrs.get("stats") or {}).items():
                with contextlib.suppress(Exception):
                    md[k] = MetadataValue.text(str(v))
            return md

        # --- one @dg.op per step --------------------------------------------------------------
        ops_by_id: Dict[str, Any] = {}
        for i, step in enumerate(steps_cfg):
            step_id = step["id"]
            op_kind = step["op"]
            op_name = f"{asset_name}_{step_id}"
            is_first = i == 0
            is_last = step_id == last_step_id
            op_required_resource_keys = set()
            if is_first and source_cfg and source_cfg.get("resource_key"):
                op_required_resource_keys.add(source_cfg["resource_key"])
            if op_kind == "write_vector_store" and receipts_cfg is not None:
                op_required_resource_keys.add(receipts_cfg["resource_key"])

            def _make_op(step=step, step_id=step_id, op_kind=op_kind, op_name=op_name,
                         is_first=is_first, is_last=is_last, rrk=op_required_resource_keys):
                def _compute(context: OpExecutionContext, upstream) -> "tuple[pd.DataFrame, Dict[str, Any]]":
                    import time as _time
                    t0 = _time.time()
                    extra_md: Dict[str, Any] = {}

                    if is_first:
                        if upstream is None:
                            upstream_df = _ingest_warehouse_query(source_cfg, context)
                        else:
                            upstream_df = upstream
                            if hasattr(upstream_df, "value") and hasattr(upstream_df, "metadata"):
                                upstream_df = upstream_df.value
                            if isinstance(upstream_df, dict):
                                _frames = [v for v in upstream_df.values() if isinstance(v, pd.DataFrame)]
                                upstream_df = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()
                        if id_column not in upstream_df.columns:
                            raise ValueError(f"id_column={id_column!r} not in upstream: {list(upstream_df.columns)}")
                        if text_column not in upstream_df.columns:
                            raise ValueError(f"text_column={text_column!r} not in upstream: {list(upstream_df.columns)}")
                        source_rows_available = len(upstream_df)
                        if max_source_rows is not None and source_rows_available > max_source_rows:
                            context.log.warning(
                                f"max_source_rows={max_source_rows} < {source_rows_available} available source rows -- "
                                f"truncating this run. Pair with partitioning to process the full corpus over multiple "
                                f"runs rather than silently dropping rows every time."
                            )
                            upstream_df = upstream_df.head(max_source_rows)
                        df = _do_chunk(upstream_df, step, id_column, text_column, metadata_columns, context)
                        extra_md["source_rows"] = MetadataValue.int(len(upstream_df))
                        extra_md["source_rows_available"] = MetadataValue.int(source_rows_available)
                    elif op_kind == "write_vector_store":
                        write_input = upstream
                        local_step = step
                        receipts_summary = None
                        if receipts_cfg is not None:
                            write_input, local_step, receipts_summary = _apply_receipt_feedback(
                                write_input, local_step, receipts_cfg, asset_name, recipe_code_version, context,
                                source_id_column=id_column,
                            )
                        partition = context.partition_key if context.has_partition_key else None
                        content_version = _content_version(write_input, local_step.get("text_column", "chunk_text"), steps_cfg)
                        stamp = {
                            "source_asset_key": asset_name,
                            "source_version": content_version,
                            "source_partition": partition,
                            "source_code_version": recipe_code_version,
                        } if stamp_provenance else None
                        df = _do_write_vector_store(write_input, local_step, id_column, context, stamp)
                        extra_md["context_version"] = MetadataValue.text(content_version)
                        if receipts_summary is not None:
                            extra_md["receipts_feedback"] = MetadataValue.json(receipts_summary)
                            extra_md["receipts_quarantined"] = MetadataValue.int(receipts_summary["quarantined"])
                            extra_md["receipts_chunks_with_feedback"] = MetadataValue.int(receipts_summary["chunks_with_feedback"])
                    else:
                        df = _FRAME_OPS[op_kind](upstream, step, context)

                    if is_last:
                        content_version = None
                        if "chunk_id" in df.columns:
                            content_version = _content_version(
                                df, "chunk_text" if "chunk_text" in df.columns else df.columns[0], steps_cfg)
                        extra_md["n_steps"] = MetadataValue.int(len(steps_cfg))
                        extra_md["step_chain"] = MetadataValue.text(" → ".join(s["id"] for s in steps_cfg))
                        if content_version is not None and op_kind != "write_vector_store":
                            extra_md["context_version"] = MetadataValue.text(content_version)
                        if include_preview and len(df) > 0:
                            try:
                                _prev_cols = [c for c in df.columns if c != "embedding"]
                                _prev = df[_prev_cols]
                                _prev = _prev.sample(min(preview_rows, len(_prev))) if len(_prev) > preview_rows * 10 else _prev.head(preview_rows)
                                extra_md["preview"] = MetadataValue.md(_prev.to_markdown(index=False) or "")
                            except Exception as e:
                                context.log.warning(f"preview emission failed: {e}")

                    elapsed = _time.time() - t0
                    md = {**_step_metadata(step_id, op_kind, df, elapsed), **extra_md}
                    context.log.info(f"step {step_id!r} ({op_kind}) → {len(df)} rows in {elapsed:.2f}s")
                    return df, md

                # The first step's op takes a real `upstream` INPUT only when self.upstream_asset_key is
                # set (the graph wires a real predecessor to it). With `source:` instead, _pipeline_fn
                # calls this op with zero args -- so the parameter must not exist at all here: a
                # declared-but-unconnected `upstream: pd.DataFrame = None` parameter still gets a real
                # Dagster input slot (default value None), and Dagster's own output type check then fails
                # None against the pd.DataFrame annotation. Caught live: per_step_ops=True + `source:` (no
                # upstream_asset_key) crashed with DagsterTypeCheckDidNotPass before this fix -- the
                # existing committed test only ever exercised upstream_asset_key, never `source:` alone.
                takes_upstream_input = not is_first or bool(self.upstream_asset_key)

                if is_last:
                    # Generator form: the only step whose op may also yield AssetCheckResults
                    # (retrieval_eval / regression_check both describe "the built index", i.e.
                    # this step specifically). Mixing a plain `return` with `yield` elsewhere in
                    # the same function would turn every call into a generator unconditionally,
                    # so non-last steps get their own plain-return op below instead.
                    #
                    # When there ARE checks, the op needs an explicit `out=` with one Out per check
                    # output (named exactly `AssetCheckSpec.get_python_identifier()`, not a name of our
                    # choosing -- AssetsDefinition.from_graph matches on it literally) alongside the
                    # primary "result" Out; a bare `yield AssetCheckResult(...)` then auto-produces that
                    # named output, so only the asset-value yield needs `output_name="result"` once
                    # `out=` stops being implicit.
                    op_out = (
                        {"result": Out(), **{n: Out(dagster_type=Nothing, is_required=False) for n in check_output_names}}
                        if check_output_names else None
                    )

                    if takes_upstream_input:
                        @op(name=op_name, required_resource_keys=rrk or None, retry_policy=retry_policy, out=op_out)
                        def _last_step_op(context: OpExecutionContext, upstream: pd.DataFrame = None):
                            df, md = _compute(context, upstream)
                            yield Output(df, metadata=md, output_name="result")
                            if retrieval_eval is not None:
                                yield _run_retrieval_eval(retrieval_eval, steps_cfg, asset_name, stamp_provenance, context)
                            if receipts_cfg is not None and receipts_cfg.get("regression_check"):
                                yield _run_recipe_outcomes_check(receipts_cfg, asset_name, recipe_code_version, context)
                        return _last_step_op

                    @op(name=op_name, required_resource_keys=rrk or None, retry_policy=retry_policy, out=op_out)
                    def _last_step_op_no_input(context: OpExecutionContext):
                        df, md = _compute(context, None)
                        yield Output(df, metadata=md, output_name="result")
                        if retrieval_eval is not None:
                            yield _run_retrieval_eval(retrieval_eval, steps_cfg, asset_name, stamp_provenance, context)
                        if receipts_cfg is not None and receipts_cfg.get("regression_check"):
                            yield _run_recipe_outcomes_check(receipts_cfg, asset_name, recipe_code_version, context)
                    return _last_step_op_no_input

                if takes_upstream_input:
                    @op(name=op_name, required_resource_keys=rrk or None, retry_policy=retry_policy)
                    def _step_op(context: OpExecutionContext, upstream: pd.DataFrame = None) -> pd.DataFrame:
                        df, md = _compute(context, upstream)
                        context.add_output_metadata(md)
                        return df
                    return _step_op

                @op(name=op_name, required_resource_keys=rrk or None, retry_policy=retry_policy)
                def _step_op_no_input(context: OpExecutionContext) -> pd.DataFrame:
                    df, md = _compute(context, None)
                    context.add_output_metadata(md)
                    return df
                return _step_op_no_input

                return _step_op

            ops_by_id[step_id] = _make_op()

        # --- per-output metadata + real step-to-step lineage ------------------------------------
        # graph_multi_asset's own default (confirmed by reading dagster's AssetsDefinition.from_graph
        # source directly) is "every declared output depends on every graph INPUT" -- it does NOT
        # infer lineage from the op-call topology inside the graph. Left alone, all N per-step assets
        # here would show as depending only on upstream_asset_key, with no edges between the steps
        # themselves. internal_asset_deps is the documented override (`from_graph`'s own docstring:
        # "a corrected set of AssetKeys they depend on... either used as input to the asset or
        # produced within the graph") -- the lower-level `AssetsDefinition.from_graph` API accepts it;
        # the `@graph_multi_asset` convenience decorator does not expose it at all, so it's built
        # directly here instead of via that decorator.
        keys_by_output_name: Dict[str, AssetKey] = {}
        code_versions_by_output_name: Dict[str, str] = {}
        descriptions_by_output_name: Dict[str, str] = {}
        owners_by_output_name: Dict[str, List[str]] = {}
        tags_by_output_name: Dict[str, Dict[str, str]] = {}
        internal_asset_deps: Dict[str, set] = {}
        prev_id: Optional[str] = None
        for i, step in enumerate(steps_cfg):
            step_id = step["id"]
            output_name = _step_asset_key(step_id)
            keys_by_output_name[output_name] = AssetKey.from_user_string(output_name)
            code_versions_by_output_name[output_name] = recipe_code_version
            descriptions_by_output_name[output_name] = (
                self.description or f"Context-engineering pipeline step {step_id!r} ({step['op']})")
            if self.owners:
                owners_by_output_name[output_name] = list(self.owners)
            tags_by_output_name[output_name] = all_tags
            if i == 0:
                internal_asset_deps[output_name] = (
                    {AssetKey.from_user_string(self.upstream_asset_key)} if self.upstream_asset_key else set()
                )
            else:
                source_id = step.get("source") or prev_id
                internal_asset_deps[output_name] = {AssetKey.from_user_string(_step_asset_key(source_id))}
            prev_id = step_id

        keys_by_input_name: Dict[str, AssetKey] = {}
        if self.upstream_asset_key:
            keys_by_input_name["upstream"] = AssetKey.from_user_string(self.upstream_asset_key)

        from dagster import AssetsDefinition, GraphIn, GraphOut
        from dagster import graph as _graph

        op_graph = _graph(
            name=f"{asset_name}_pipeline",
            out={**{name: GraphOut() for name in keys_by_output_name}, **{n: GraphOut() for n in check_output_names}},
            ins={name: GraphIn() for name in keys_by_input_name},
        )
        def _pipeline_fn(**kwargs):
            upstream_input = kwargs.get("upstream")
            results: Dict[str, Any] = {}
            check_results: Dict[str, Any] = {}
            prev_id: Optional[str] = None
            for i, step in enumerate(steps_cfg):
                step_id = step["id"]
                source_id = step.get("source") or prev_id
                step_op = ops_by_id[step_id]
                if i == 0:
                    out = step_op(upstream_input) if self.upstream_asset_key else step_op()
                else:
                    if source_id is None or source_id not in results:
                        raise ValueError(f"step {step_id!r} (op={step['op']!r}) has no source and is not preceded by another step.")
                    out = step_op(results[source_id])
                if step_id == last_step_id and check_output_names:
                    out, *check_outs = out   # op has one Out per declared check, in check_output_names order
                    check_results = dict(zip(check_output_names, check_outs))
                results[step_id] = out
                prev_id = step_id
            return {**{_step_asset_key(s["id"]): results[s["id"]] for s in steps_cfg}, **check_results}
        op_graph = op_graph(_pipeline_fn)

        pipeline_asset = AssetsDefinition.from_graph(
            op_graph,
            keys_by_input_name=keys_by_input_name or None,
            keys_by_output_name=keys_by_output_name,
            internal_asset_deps=internal_asset_deps,
            partitions_def=partitions_def,
            group_name=self.group_name,
            backfill_policy=backfill_policy_obj,
            check_specs=check_specs or None,
            code_versions_by_output_name=code_versions_by_output_name,
            descriptions_by_output_name=descriptions_by_output_name,
            tags_by_output_name=tags_by_output_name,
            owners_by_output_name=owners_by_output_name or None,
        )
        return Definitions(assets=[pipeline_asset])

    def _build_per_step_ops_sql_defs(
        self, *, steps_cfg: List[dict], asset_name: str, id_column: str, text_column: str,
        metadata_columns: List[str], recipe_code_version: str, stamp_provenance: bool, source_cfg: dict,
        receipts_cfg: Optional[Dict[str, Any]], retrieval_eval: Optional[Dict[str, Any]], sql_dialect: str,
        output_table: str, incremental: bool, all_tags: Dict[str, str], check_specs: List[Any],
    ) -> Definitions:
        """`per_step_ops=True` + `execution_mode='sql'`: one real table (and one real asset) per
        participating step -- chunk always, classify/embed if configured. write_vector_store has no SQL
        equivalent (same reason `_build_sql_mode_query` stops at embed): there's no "4th step" to add for
        parity -- dbt-labs/dbt-context-engineering's own reference package confirms its 3rd model
        ("search") is the RETRIEVAL step, not a build step, so 3 steps max here is the correct scope, not
        a gap. Unpartitioned only (validated in build_defs): per-table partition staging is real added
        complexity, tracked as a follow-up.

        Each op calls straight into the real execution-layer functions (`_run_sql_chunks_stage` /
        `_run_sql_feature_stage`) -- no DataFrame changes hands between ops (SQL mode never pulls data
        into Python at all), so every op is `Nothing`-typed: Dagster's ordering-only dependency, same as
        the existing single-asset SQL path's own output type.
        """
        resource_key = source_cfg["resource_key"]
        chunk_step = next(s for s in steps_cfg if s["op"] == "chunk")
        classify_step = next((s for s in steps_cfg if s["op"] == "classify"), None)
        embed_step = next((s for s in steps_cfg if s["op"] == "embed"), None)
        last_step_id = steps_cfg[-1]["id"]
        # Same requirement as _build_per_step_ops_defs: AssetsDefinition.from_graph needs each check to
        # be a literally-named output (AssetCheckSpec.get_python_identifier()) on the underlying node --
        # a bare `yield AssetCheckResult(...)` is not auto-wired the way @asset(check_specs=...) wires it.
        check_output_names = [spec.get_python_identifier() for spec in (check_specs or [])]

        def _step_asset_key(step_id: str) -> str:
            return asset_name if step_id == last_step_id else f"{asset_name}_{step_id}"

        def _step_table(step_id: str) -> str:
            return output_table if step_id == last_step_id else f"{output_table}__{step_id}"

        # Column lists are pure static config -- a deterministic function of id_column/metadata_columns/
        # stamp_provenance/receipts, never of the actual data -- so they're computed once here at build
        # time (a dummy `source_cfg['sql']` is safe: only the chunk CTE's FROM clause reads it, which
        # doesn't affect which columns exist) instead of threaded through op outputs at runtime.
        _, cols_after_chunk, _cur = _sql_chunk_and_stamp_stage(
            chunk_step, {"sql": ""}, sql_dialect, id_column, text_column, metadata_columns,
            asset_name=asset_name, code_version=recipe_code_version, stamp_provenance=stamp_provenance,
            receipts=receipts_cfg,
        )
        cols_by_step: Dict[str, List[str]] = {chunk_step["id"]: cols_after_chunk}
        if classify_step is not None:
            cols_by_step[classify_step["id"]] = cols_after_chunk + ["category", "category_signature"]
        if embed_step is not None:
            prior_id = classify_step["id"] if classify_step is not None else chunk_step["id"]
            cols_by_step[embed_step["id"]] = cols_by_step[prior_id] + ["embedding", "embedding_model"]

        ops_by_id: Dict[str, Any] = {}
        prev_step_id: Optional[str] = None
        for step in steps_cfg:
            step_id, op_kind = step["id"], step["op"]
            op_name = f"{asset_name}_{step_id}"
            this_table = _step_table(step_id)
            is_last = step_id == last_step_id
            source_step_id = prev_step_id  # chunk has none; classify/embed read from the immediately-prior step

            def _make_op(step=step, step_id=step_id, op_kind=op_kind, op_name=op_name, this_table=this_table,
                         is_last=is_last, source_step_id=source_step_id):
                # Same optional-plain-input convention as _build_per_step_ops_defs's own ops (an
                # `upstream=None`-defaulted parameter, no explicit Nothing `ins=`/`out=`): SQL mode
                # never threads a real value between ops either, it just needs the dependency edge for
                # ordering, and real step-to-step ASSET lineage comes from internal_asset_deps below,
                # not from this op-call wiring.
                def _compute(context: OpExecutionContext) -> Dict[str, Any]:
                    if op_kind == "chunk":
                        md, _cols = _run_sql_chunks_stage(
                            context, resource_key, step, source_cfg, sql_dialect, this_table,
                            id_column, text_column, metadata_columns, asset_name=asset_name,
                            code_version=recipe_code_version, stamp_provenance=stamp_provenance,
                            receipts_cfg=receipts_cfg,
                        )
                    else:
                        tag = "category" if op_kind == "classify" else "embedding"
                        tag_col = "category_signature" if op_kind == "classify" else "embedding_model"
                        md = _run_sql_feature_stage(
                            context, resource_key, step, sql_dialect, _step_table(source_step_id), this_table,
                            cols_by_step[source_step_id], tag=tag, tag_col=tag_col, incremental=incremental,
                        )
                    if is_last:
                        tbl_ref = _sql_table(this_table, "output_table", sql_dialect)
                        with _sql_session(getattr(context.resources, resource_key), resource_key) as session:
                            content_version = session.fetch(f"SELECT {_sql_version_agg(sql_dialect)} FROM {tbl_ref}")[0][0]
                        md["context_version"] = MetadataValue.text(str(content_version))
                        md["n_steps"] = MetadataValue.int(len(steps_cfg))
                        md["step_chain"] = MetadataValue.text(" → ".join(s["id"] for s in steps_cfg))
                    return md

                if is_last:
                    # Generator form: the only step whose op may also yield AssetCheckResults
                    # (retrieval_eval / regression_check both describe "the built index", i.e. this
                    # step specifically). See _build_per_step_ops_defs for why non-last steps need a
                    # separate plain op instead of branching inside the same function, and for why
                    # `out=` must name each check output explicitly once there are any.
                    op_out = (
                        {"result": Out(dagster_type=Nothing), **{n: Out(dagster_type=Nothing, is_required=False) for n in check_output_names}}
                        if check_output_names else None
                    )

                    @op(name=op_name, required_resource_keys={resource_key}, out=op_out)
                    def _last_stage_op(context: OpExecutionContext, upstream=None) -> Any:
                        md = _compute(context)
                        yield Output(None, metadata=md, output_name="result")
                        if retrieval_eval is not None:
                            yield _run_sql_retrieval_eval(retrieval_eval, steps_cfg, source_cfg, sql_dialect, output_table,
                                                          id_column, asset_name, stamp_provenance, context)
                        if receipts_cfg is not None and receipts_cfg.get("regression_check"):
                            by_cv = _sql_fetch_outcomes_by_code_version(context, source_cfg, receipts_cfg, asset_name)
                            yield _run_recipe_outcomes_check(receipts_cfg, asset_name, recipe_code_version, context, outcomes_by_cv=by_cv)

                    return _last_stage_op

                @op(name=op_name, required_resource_keys={resource_key})
                def _stage_op(context: OpExecutionContext, upstream=None) -> Any:
                    md = _compute(context)
                    context.add_output_metadata(md)
                    return None

                return _stage_op

            ops_by_id[step_id] = _make_op()
            prev_step_id = step_id

        keys_by_output_name: Dict[str, AssetKey] = {}
        code_versions_by_output_name: Dict[str, str] = {}
        descriptions_by_output_name: Dict[str, str] = {}
        owners_by_output_name: Dict[str, List[str]] = {}
        tags_by_output_name: Dict[str, Dict[str, str]] = {}
        internal_asset_deps: Dict[str, set] = {}
        prev_step_id = None
        for step in steps_cfg:
            step_id = step["id"]
            output_name = _step_asset_key(step_id)
            keys_by_output_name[output_name] = AssetKey.from_user_string(output_name)
            code_versions_by_output_name[output_name] = recipe_code_version
            descriptions_by_output_name[output_name] = (
                self.description or f"Context-engineering pipeline step {step_id!r} ({step['op']}, execution_mode=sql)")
            if self.owners:
                owners_by_output_name[output_name] = list(self.owners)
            tags_by_output_name[output_name] = all_tags
            internal_asset_deps[output_name] = (
                {AssetKey.from_user_string(_step_asset_key(prev_step_id))} if prev_step_id is not None else set()
            )
            prev_step_id = step_id

        from dagster import AssetsDefinition, GraphOut
        from dagster import graph as _graph

        op_graph = _graph(
            name=f"{asset_name}_pipeline",
            out={**{name: GraphOut() for name in keys_by_output_name}, **{n: GraphOut() for n in check_output_names}},
        )

        def _pipeline_fn():
            results: Dict[str, Any] = {}
            check_results: Dict[str, Any] = {}
            prev = None
            for step in steps_cfg:
                step_op = ops_by_id[step["id"]]
                out = step_op(prev) if prev is not None else step_op()
                if step["id"] == last_step_id and check_output_names:
                    out, *check_outs = out   # op has one Out per declared check, in check_output_names order
                    check_results = dict(zip(check_output_names, check_outs))
                prev = out
                results[step["id"]] = out
            return {**{_step_asset_key(s["id"]): results[s["id"]] for s in steps_cfg}, **check_results}
        op_graph = op_graph(_pipeline_fn)

        pipeline_asset = AssetsDefinition.from_graph(
            op_graph,
            keys_by_output_name=keys_by_output_name,
            internal_asset_deps=internal_asset_deps,
            group_name=self.group_name,
            check_specs=check_specs or None,
            code_versions_by_output_name=code_versions_by_output_name,
            descriptions_by_output_name=descriptions_by_output_name,
            tags_by_output_name=tags_by_output_name,
            owners_by_output_name=owners_by_output_name or None,
        )
        return Definitions(assets=[pipeline_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat
