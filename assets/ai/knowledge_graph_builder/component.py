"""KnowledgeGraphBuilderComponent -- chunk raw text, extract (subject, relation,
object) triples, resolve entity aliases, and write the result into a real graph
store. Same "pipeline component" shape as `context_engineering_pipeline`: one
YAML, a `steps:` list, each step's output is a DataFrame the next step reads
(via `source:`, or the immediately-prior step if omitted).

    type: dagster_community_components.KnowledgeGraphBuilderComponent
    attributes:
      asset_name: support_kg
      upstream_asset_key: raw_support_tickets
      id_column: ticket_id
      text_column: body
      steps:
        - {id: chunks, op: chunk, chunk_size: 1500, chunk_overlap: 150}
        - {id: triples, op: extract, method: llm, model: gpt-4o-mini, api_key_env_var: OPENAI_API_KEY}
        - {id: resolved, op: resolve, min_mentions: 2}
        - {id: graph, op: write_graph, provider: networkx, path: /tmp/support_kg.json}

Shared chunk-id contract: `chunk`'s algorithm, default id scheme
(`{prefix}{source_id}_{index}`), and content_hash formula (sha256 of the
chunk text) are copied verbatim from context_engineering_pipeline's own
chunk op. Given the SAME source text and the SAME chunk_size/chunk_overlap/
preserve_sentences/chunk_id_prefix, both pipelines produce byte-identical
chunk_id and content_hash values for the same chunk -- a vector knowledge
base (context_engineering_pipeline) and this knowledge graph can be joined
by chunk_id (see hybrid_knowledge_search_server). To chunk the corpus only
ONCE and feed both: run context_engineering_pipeline with per_step_ops:
true, then point THIS component's `upstream_asset_key` at that pipeline's
own `<asset_name>_<chunk step id>` per-step asset and start `steps:` with
op='extract' instead of op='chunk' (the first step may be either).

Op coverage:

- chunk:        fixed-size chunking with sentence-boundary snapping and
                configurable overlap -- same output contract as
                context_engineering_pipeline's own chunk op (chunk_id,
                chunk_index, total_chunks, chunk_text, content_hash, plus
                id_column/metadata_columns carried through), reimplemented
                here rather than imported (this repo's standalone-component
                convention). May be omitted -- start `steps:` with
                op='extract' instead when upstream is already chunk-shaped.
- extract:      method='llm' (litellm; any entity_types/relation_types act as
                a closed vocabulary hint) or method='spacy' (free, local,
                dependency-parse-based subject-verb-object extraction over
                named entities -- no API key, modest precision). Each row of
                the output is ONE triple: subject, relation, object,
                subject_type, object_type, confidence, evidence, plus the
                source id_column/chunk_id/content_hash for provenance.
- resolve:      always normalizes case/punctuation and merges exact aliases
                within the same entity type; optionally also merges near-
                duplicate names above `embedding_similarity_threshold`
                (cosine similarity of name embeddings), optionally confirmed
                by an LLM adjudication call per candidate pair before
                merging. `min_mentions` drops entities (and their triples)
                seen fewer times than this across the whole run.
- write_graph:  provider='networkx' (a local JSON file -- GraphML is not
                supported: its string-only attributes can't round-trip the
                list-valued provenance fields below without lossy
                flattening), 'neo4j', or 'falkordb'. `sync_mode`: 'upsert'
                (default, add/update only), 'replace_sources' (also remove
                triples whose source rows are in this run but that this run
                no longer produces -- a shrunk document stops leaving stale
                edges behind), or 'replace_all' (the graph becomes exactly
                this run's triples; refused when partitioned, same rule as
                `vector_store_writer`'s own replace_all).

Every edge in the resulting graph carries real provenance: which source
row(s) (`source_ids`), which chunk(s) (`chunk_ids` + `content_hashes`, so a
later run can tell whether a chunk changed), and a verbatim evidence snippet
per chunk (`evidences`) that supported the triple.

Incremental extraction: `extract`'s `reuse_from: {provider, ...}` (same
connection fields as `write_graph`) looks up chunks whose id+content_hash
already have provenance recorded in that graph store and reuses their
previously-extracted triples instead of paying the LLM/spaCy call again --
only new or changed chunks are actually (re-)extracted.
"""
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Backoff,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    RetryPolicy,
    asset,
)
from pydantic import Field


# ─── small, duplicated-not-imported helpers (this repo's standalone-component convention) ───

def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _chunk_fixed(text: str, chunk_size: int, chunk_overlap: int, preserve_sentences: bool) -> List[str]:
    """Fixed-size chunking with overlap, optionally snapping to a nearby sentence boundary --
    same approach as context_engineering_pipeline's own _chunk_fixed (duplicated, not imported)."""
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


def _extract_json_block(text: str) -> Any:
    """Parse text as JSON, falling back to the content of a fenced ```json block."""
    text = (text or "").strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1).strip())
    raise ValueError(f"no valid JSON found in text: {text[:200]!r}")


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource exposing .get_engine() (SQLAlchemy), .get_connection()
    (DBAPI), or .get_client() (vendor client), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no resource is registered. Same helper/contract duplicated
    across every dual-ingestion component in this repo."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            client = resource.get_client()
            if hasattr(client, "query"):
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found)."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


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


# ─── chunk op ──────────────────────────────────────────────────────────────────────────────

def _do_chunk(df: pd.DataFrame, step: dict, id_column: str, text_column: str, metadata_columns: List[str], context) -> pd.DataFrame:
    """Knobs: chunk_size (default 1500 chars -- graph extraction wants more context per chunk
    than a typical retrieval chunk), chunk_overlap (default 150), preserve_sentences (default
    True), min_chunk_chars, chunk_id_prefix, output_column (default chunk_text)."""
    chunk_size = int(step.get("chunk_size", 1500))
    chunk_overlap = int(step.get("chunk_overlap", 150))
    if chunk_overlap >= chunk_size:
        raise ValueError("chunk op: chunk_overlap must be smaller than chunk_size")
    preserve_sentences = step.get("preserve_sentences", True)
    out_text_col = step.get("output_column", "chunk_text")
    min_chars = int(step.get("min_chunk_chars", 0))
    id_prefix = str(step.get("chunk_id_prefix") or "")

    rows: List[Dict[str, Any]] = []
    skipped = 0
    for _, row in df.iterrows():
        source_id = row[id_column]
        text = str(row[text_column]) if pd.notna(row[text_column]) else ""
        if not text.strip():
            skipped += 1
            continue
        pieces = [c for c in _chunk_fixed(text, chunk_size, chunk_overlap, preserve_sentences) if len(c) >= min_chars]
        seen_ids: set = set()
        for i, chunk_text in enumerate(pieces):
            record: Dict[str, Any] = {id_column: source_id}
            for col in metadata_columns:
                if col in df.columns:
                    record[col] = row[col]
            digest = _sha256(chunk_text)
            chunk_id = f"{id_prefix}{source_id}_{i}"
            if chunk_id in seen_ids:
                chunk_id = f"{chunk_id}-{i}"
            seen_ids.add(chunk_id)
            record.update({
                "chunk_id": chunk_id,
                "chunk_index": i,
                "total_chunks": len(pieces),
                out_text_col: chunk_text,
                "content_hash": digest,
            })
            rows.append(record)
    if skipped:
        context.log.warning(f"chunk: skipped {skipped} row(s) with empty {text_column!r}")
    out = pd.DataFrame(rows)
    out.attrs["stats"] = {"document_count": len(df), "chunk_count": len(out)}
    return out


# ─── extract op ────────────────────────────────────────────────────────────────────────────

_EXTRACT_PROMPT_TEMPLATE = """Extract factual relationship triples from the text below. Return ONLY a \
JSON array, no prose, no markdown fence. Each element: {{"subject": str, "subject_type": str, \
"relation": str, "object": str, "object_type": str, "confidence": float between 0 and 1, \
"evidence": a short verbatim quote from the text supporting this triple}}.
{entity_hint}{relation_hint}
If nothing qualifies, return [].

Text:
\"\"\"
{text}
\"\"\""""


def _extract_llm_call(text: str, *, model: str, api_key_env_var: Optional[str], entity_types: Optional[List[str]],
                       relation_types: Optional[List[str]], temperature: float = 0.0) -> List[Dict[str, Any]]:
    import litellm
    entity_hint = f"\nUse only these entity types (or 'OTHER' if none fit): {entity_types}." if entity_types else ""
    relation_hint = f"\nUse only these relation types (or your own short verb-phrase if none fit): {relation_types}." if relation_types else ""
    prompt = _EXTRACT_PROMPT_TEMPLATE.format(entity_hint=entity_hint, relation_hint=relation_hint, text=text)
    api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
    resp = litellm.completion(
        model=model, messages=[{"role": "user", "content": prompt}], api_key=api_key, temperature=temperature,
    )
    content = resp.choices[0].message.content or "[]"
    parsed = _extract_json_block(content)
    if not isinstance(parsed, list):
        raise ValueError(f"extract op (llm): model did not return a JSON array: {content[:200]!r}")
    return parsed


def _extract_llm(df: pd.DataFrame, step: dict, id_column: str, text_column: str, context) -> pd.DataFrame:
    model = step.get("model")
    if not model:
        raise ValueError("extract op method='llm' requires `model` (a litellm model string).")
    api_key_env_var = step.get("api_key_env_var")
    entity_types = step.get("entity_types")
    relation_types = step.get("relation_types")
    min_confidence = float(step.get("min_confidence", 0.0))
    max_triples_per_chunk = step.get("max_triples_per_chunk")

    rows: List[Dict[str, Any]] = []
    errors = 0
    for _, row in df.iterrows():
        text = str(row[text_column]) if pd.notna(row.get(text_column)) else ""
        if not text.strip():
            continue
        try:
            triples = _extract_llm_call(
                text, model=model, api_key_env_var=api_key_env_var,
                entity_types=entity_types, relation_types=relation_types,
            )
        except Exception as e:  # noqa: BLE001 -- one bad chunk must not fail the whole extraction run
            errors += 1
            context.log.warning(f"extract (llm): chunk {row.get('chunk_id')!r} failed: {type(e).__name__}: {e}")
            continue
        if max_triples_per_chunk is not None:
            triples = triples[: int(max_triples_per_chunk)]
        for t in triples:
            if not isinstance(t, dict) or not t.get("subject") or not t.get("object") or not t.get("relation"):
                continue
            confidence = t.get("confidence")
            try:
                confidence = float(confidence) if confidence is not None else None
            except (TypeError, ValueError):
                confidence = None
            if confidence is not None and confidence < min_confidence:
                continue
            rows.append({
                id_column: row[id_column],
                "chunk_id": row.get("chunk_id"),
                "content_hash": row.get("content_hash"),
                "subject": str(t["subject"]).strip(),
                "subject_type": (str(t["subject_type"]).strip() if t.get("subject_type") else None),
                "relation": str(t["relation"]).strip(),
                "object": str(t["object"]).strip(),
                "object_type": (str(t["object_type"]).strip() if t.get("object_type") else None),
                "confidence": confidence,
                "evidence": (str(t.get("evidence")).strip() if t.get("evidence") else text[:300]),
            })
    if errors:
        context.log.warning(f"extract (llm): {errors} chunk(s) failed extraction and were skipped")
    return pd.DataFrame(rows)


def _load_spacy(spacy_model: str):
    try:
        import spacy
    except ImportError:
        raise ImportError("extract op method='spacy' requires: pip install spacy")
    try:
        return spacy.load(spacy_model)
    except OSError:
        raise OSError(
            f"spaCy model {spacy_model!r} is not downloaded. Run: python -m spacy download {spacy_model}"
        )


def _entity_span_at(token, ents) -> Optional[Any]:
    for ent in ents:
        if ent.start <= token.i < ent.end:
            return ent
    return None


def _extract_spacy_sentence(sent, ents) -> List[Dict[str, Any]]:
    """Dependency-parse SVO extraction: for each verb, pair its nsubj/nsubjpass child with its
    dobj/attr/oprd child (or, failing that, the object of its first prepositional child). Simple
    and local, not a calibrated extractor -- documented as heuristic, no confidence is reported."""
    triples = []
    for token in sent:
        if token.pos_ != "VERB":
            continue
        subj = next((c for c in token.children if c.dep_ in ("nsubj", "nsubjpass")), None)
        obj = next((c for c in token.children if c.dep_ in ("dobj", "attr", "oprd")), None)
        if obj is None:
            prep = next((c for c in token.children if c.dep_ == "prep"), None)
            if prep is not None:
                obj = next((c for c in prep.children if c.dep_ == "pobj"), None)
        if subj is None or obj is None:
            continue
        subj_ent = _entity_span_at(subj, ents)
        obj_ent = _entity_span_at(obj, ents)
        triples.append({
            "subject": subj_ent.text if subj_ent is not None else subj.text,
            "subject_type": subj_ent.label_ if subj_ent is not None else None,
            "relation": token.lemma_,
            "object": obj_ent.text if obj_ent is not None else obj.text,
            "object_type": obj_ent.label_ if obj_ent is not None else None,
            "evidence": sent.text.strip()[:300],
        })
    return triples


def _extract_spacy(df: pd.DataFrame, step: dict, id_column: str, text_column: str, context) -> pd.DataFrame:
    if step.get("min_confidence") is not None:
        context.log.warning("extract (spacy): min_confidence is ignored -- spaCy's dependency-parse extraction reports no calibrated confidence.")
    nlp = _load_spacy(step.get("spacy_model", "en_core_web_sm"))
    max_triples_per_chunk = step.get("max_triples_per_chunk")

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        text = str(row[text_column]) if pd.notna(row.get(text_column)) else ""
        if not text.strip():
            continue
        doc = nlp(text)
        triples: List[Dict[str, Any]] = []
        for sent in doc.sents:
            triples.extend(_extract_spacy_sentence(sent, list(doc.ents)))
            if max_triples_per_chunk is not None and len(triples) >= int(max_triples_per_chunk):
                break
        if max_triples_per_chunk is not None:
            triples = triples[: int(max_triples_per_chunk)]
        for t in triples:
            rows.append({
                id_column: row[id_column],
                "chunk_id": row.get("chunk_id"),
                "content_hash": row.get("content_hash"),
                "subject": t["subject"], "subject_type": t["subject_type"],
                "relation": t["relation"],
                "object": t["object"], "object_type": t["object_type"],
                "confidence": None,
                "evidence": t["evidence"],
            })
    return pd.DataFrame(rows)


def _reuse_extraction(step: dict, chunk_ids_with_hash: Dict[str, str], id_column: str, context) -> Tuple[pd.DataFrame, set]:
    """Looks up `step['reuse_from']` (same connection shape as write_graph) for chunks whose
    chunk_id+content_hash already exist in the graph store, and returns their previously-written
    triples plus the set of chunk_ids that were successfully reused (skip re-extracting those)."""
    cfg = step.get("reuse_from")
    if not cfg:
        return pd.DataFrame(), set()
    provider = cfg.get("provider", "networkx")
    try:
        rows = _graph_store_triples_for_chunks(provider, cfg, chunk_ids_with_hash, id_column)
    except Exception as e:  # noqa: BLE001 -- a store that isn't reachable/doesn't exist yet must not fail the run
        context.log.warning(f"extract reuse_from: could not read existing graph ({type(e).__name__}: {e}); extracting everything.")
        return pd.DataFrame(), set()
    reused_ids = {r["chunk_id"] for r in rows}
    context.log.info(f"extract reuse_from: reused {len(reused_ids)} unchanged chunk(s) from the existing graph.")
    return pd.DataFrame(rows), reused_ids


def _do_extract(df: pd.DataFrame, step: dict, id_column: str, context) -> pd.DataFrame:
    method = step.get("method", "llm")
    if method not in ("llm", "spacy"):
        raise ValueError(f"extract op: method must be 'llm' or 'spacy', got {method!r}")
    text_column = step.get("text_column", "chunk_text")
    if text_column not in df.columns:
        raise ValueError(f"extract op: text_column={text_column!r} not in upstream columns: {list(df.columns)}")

    to_extract = df
    reused_df = pd.DataFrame()
    if step.get("reuse_from") and "content_hash" in df.columns and "chunk_id" in df.columns:
        chunk_hashes = dict(zip(df["chunk_id"], df["content_hash"]))
        reused_df, reused_ids = _reuse_extraction(step, chunk_hashes, id_column, context)
        if reused_ids:
            to_extract = df[~df["chunk_id"].isin(reused_ids)]

    fresh_df = (_extract_llm if method == "llm" else _extract_spacy)(to_extract, step, id_column, text_column, context)
    out = pd.concat([reused_df, fresh_df], ignore_index=True) if len(reused_df) else fresh_df
    out.attrs["stats"] = {
        "chunks_extracted": len(to_extract), "chunks_reused": len(df) - len(to_extract), "triple_count": len(out),
        "extraction_method": method,
    }
    return out


# ─── resolve op ────────────────────────────────────────────────────────────────────────────

def _normalize_entity_key(name: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", "", str(name).lower())).strip()


def _embed_texts(texts: List[str], provider: str, model: Optional[str], api_key_env_var: Optional[str]) -> List[List[float]]:
    if provider == "sentence_transformers":
        from sentence_transformers import SentenceTransformer
        return SentenceTransformer(model or "all-MiniLM-L6-v2").encode(texts, normalize_embeddings=True, show_progress_bar=False).tolist()
    if provider == "openai":
        import openai
        client = openai.OpenAI(api_key=os.environ.get(api_key_env_var or "OPENAI_API_KEY"))
        resp = client.embeddings.create(model=model or "text-embedding-3-small", input=texts)
        return [d.embedding for d in resp.data]
    if provider == "cohere":
        import cohere
        client = cohere.Client(api_key=os.environ.get(api_key_env_var or "COHERE_API_KEY"))
        resp = client.embed(texts=texts, model=model or "embed-english-v3.0", input_type="clustering")
        return list(resp.embeddings)
    if provider == "litellm":
        import litellm
        api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
        resp = litellm.embedding(model=model or "text-embedding-3-small", input=texts, api_key=api_key)
        return [d["embedding"] for d in resp.data]
    raise ValueError(f"resolve op: unsupported embed_provider {provider!r}. Valid: sentence_transformers, openai, cohere, litellm.")


def _llm_adjudicate_merge(name_a: str, name_b: str, entity_type: Optional[str], model: str, api_key_env_var: Optional[str]) -> bool:
    import litellm
    api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
    type_hint = f" (both tagged as type {entity_type!r})" if entity_type else ""
    prompt = (
        f"Are '{name_a}' and '{name_b}'{type_hint} the same real-world entity? "
        "Answer with exactly one word: yes or no."
    )
    resp = litellm.completion(model=model, messages=[{"role": "user", "content": prompt}], api_key=api_key, temperature=0.0, max_tokens=5)
    return "yes" in (resp.choices[0].message.content or "").strip().lower()


def _cosine(a: List[float], b: List[float]) -> float:
    import math
    dot = sum(x * y for x, y in zip(a, b))
    na, nb = math.sqrt(sum(x * x for x in a)) or 1.0, math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)


def _do_resolve(df: pd.DataFrame, step: dict, context) -> pd.DataFrame:
    """Always merges exact normalized aliases (case/punctuation-insensitive) within the same
    type bucket; optionally also merges near-duplicate names above `embedding_similarity_threshold`
    (cosine similarity, within the same type bucket, O(n^2) over unique names -- capped by
    max_entities_for_embedding_resolve, default 500, to keep this bounded), optionally confirmed
    per candidate pair by an LLM (llm_adjudication=True, adjudication_model/
    adjudication_api_key_env_var). `min_mentions` drops any entity (and every triple
    referencing it) with fewer than that many total mentions."""
    required = ("subject", "relation", "object")
    for col in required:
        if col not in df.columns:
            raise ValueError(f"resolve op: column {col!r} not in upstream columns: {list(df.columns)}")
    if df.empty:
        df = df.copy()
        df.attrs["entities"] = {}
        return df

    min_mentions = int(step.get("min_mentions", 1))
    embedding_threshold = step.get("embedding_similarity_threshold")
    max_entities_for_embedding_resolve = int(step.get("max_entities_for_embedding_resolve", 500))
    llm_adjudication = bool(step.get("llm_adjudication", False))

    # mention occurrences: every (name, type) appearance as a subject or object
    occurrences: List[Tuple[str, Optional[str]]] = []
    for _, row in df.iterrows():
        occurrences.append((row["subject"], row.get("subject_type")))
        occurrences.append((row["object"], row.get("object_type")))

    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}  # (normalized_key, type_bucket) -> {names: Counter, mention_count}
    from collections import Counter
    for name, etype in occurrences:
        type_bucket = etype or ""
        key = (_normalize_entity_key(name), type_bucket)
        g = groups.setdefault(key, {"names": Counter(), "mention_count": 0, "type": etype})
        g["names"][str(name)] += 1
        g["mention_count"] += 1
        if etype and not g["type"]:
            g["type"] = etype

    canonical: Dict[Tuple[str, str], str] = {k: g["names"].most_common(1)[0][0] for k, g in groups.items()}
    group_keys = list(groups.keys())

    # optional embedding-similarity merge within the same type bucket, union-find over group_keys
    parent = {k: k for k in group_keys}

    def find(k):
        while parent[k] != k:
            parent[k] = parent[parent[k]]
            k = parent[k]
        return k

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    if embedding_threshold is not None and len(group_keys) > 1:
        if len(group_keys) > max_entities_for_embedding_resolve:
            context.log.warning(
                f"resolve: {len(group_keys)} unique entities exceeds max_entities_for_embedding_resolve="
                f"{max_entities_for_embedding_resolve}; skipping embedding-similarity merge for this run."
            )
        else:
            by_bucket: Dict[str, List[Tuple[str, str]]] = {}
            for k in group_keys:
                by_bucket.setdefault(k[1], []).append(k)
            embed_provider = step.get("embed_provider", "sentence_transformers")
            embed_model = step.get("embed_model")
            embed_api_key_env_var = step.get("embed_api_key_env_var")
            adjudication_model = step.get("adjudication_model")
            adjudication_api_key_env_var = step.get("adjudication_api_key_env_var")
            for bucket_keys in by_bucket.values():
                if len(bucket_keys) < 2:
                    continue
                names = [canonical[k] for k in bucket_keys]
                vectors = _embed_texts(names, embed_provider, embed_model, embed_api_key_env_var)
                for i in range(len(bucket_keys)):
                    for j in range(i + 1, len(bucket_keys)):
                        if _cosine(vectors[i], vectors[j]) < float(embedding_threshold):
                            continue
                        if llm_adjudication:
                            if not adjudication_model:
                                raise ValueError("resolve op: llm_adjudication=True requires `adjudication_model`.")
                            if not _llm_adjudicate_merge(names[i], names[j], bucket_keys[i][1] or None, adjudication_model, adjudication_api_key_env_var):
                                continue
                        union(bucket_keys[i], bucket_keys[j])

    # final canonical name/type/mention_count/aliases per merged group
    merged: Dict[str, Dict[str, Any]] = {}
    key_to_final: Dict[Tuple[str, str], str] = {}
    for k in group_keys:
        root = find(k)
        root_group = groups[root]
        final_name = canonical[root]
        key_to_final[k] = final_name
        m = merged.setdefault(final_name, {"type": root_group["type"], "aliases": set(), "mention_count": 0})
        m["aliases"].update(groups[k]["names"].keys())
        m["mention_count"] += groups[k]["mention_count"]

    kept_entities = {name for name, m in merged.items() if m["mention_count"] >= min_mentions}

    out_rows = []
    for _, row in df.iterrows():
        s_key = (_normalize_entity_key(row["subject"]), row.get("subject_type") or "")
        o_key = (_normalize_entity_key(row["object"]), row.get("object_type") or "")
        s_final, o_final = key_to_final[s_key], key_to_final[o_key]
        if s_final not in kept_entities or o_final not in kept_entities:
            continue
        new_row = row.to_dict()
        new_row["subject"], new_row["object"] = s_final, o_final
        new_row["subject_type"], new_row["object_type"] = merged[s_final]["type"], merged[o_final]["type"]
        out_rows.append(new_row)

    out = pd.DataFrame(out_rows, columns=df.columns)
    out.attrs["entities"] = {
        name: {"type": m["type"], "aliases": sorted(m["aliases"]), "mention_count": m["mention_count"]}
        for name, m in merged.items() if name in kept_entities
    }
    out.attrs["stats"] = {
        "entities_before": len(groups), "entities_after": len(kept_entities),
        "triples_before": len(df), "triples_after": len(out),
    }
    return out


# ─── graph store I/O (networkx / neo4j / falkordb) ────────────────────────────────────────

def _edge_key(subject: str, relation: str, obj: str) -> str:
    return f"{subject}\x1f{relation}\x1f{obj}"


def _build_networkx_graph(path: str):
    import networkx as nx
    if not os.path.exists(path):
        return nx.MultiDiGraph()
    with open(path) as f:
        data = json.load(f)
    return nx.node_link_graph(data, directed=True, multigraph=True, edges="edges")


def _save_networkx_graph(g, path: str) -> None:
    import networkx as nx
    data = nx.node_link_data(g, edges="edges")
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def _graph_store_triples_for_chunks(provider: str, cfg: dict, chunk_hashes: Dict[str, str], id_column: str) -> List[Dict[str, Any]]:
    """Returns previously-written triple rows (same shape _do_extract produces) for every
    chunk_id in `chunk_hashes` whose stored content_hash still matches."""
    chunk_ids = set(chunk_hashes)
    if not chunk_ids:
        return []
    if provider == "networkx":
        path = cfg.get("path") or f"./{cfg.get('graph_name', 'knowledge_graph')}.json"
        g = _build_networkx_graph(path)
        rows = []
        for u, v, data in g.edges(data=True):
            for i, cid in enumerate(data.get("chunk_ids") or []):
                if cid in chunk_ids and (data.get("content_hashes") or [None])[i] == chunk_hashes[cid]:
                    rows.append({
                        id_column: (data.get("source_ids") or [None])[i], "chunk_id": cid,
                        "content_hash": chunk_hashes[cid], "subject": u, "object": v,
                        "subject_type": g.nodes[u].get("type"), "object_type": g.nodes[v].get("type"),
                        "relation": data.get("relation"), "confidence": data.get("confidence"),
                        "evidence": (data.get("evidences") or [None])[i],
                    })
        return rows
    if provider in ("neo4j", "falkordb"):
        graph_name = cfg.get("graph_name", "knowledge_graph")
        query = (
            "MATCH (a)-[r]->(b) UNWIND range(0, size(r.chunk_ids) - 1) AS i "
            "WITH a, b, r, i WHERE r.chunk_ids[i] IN $chunk_ids AND r.graph = $graph_name "
            "RETURN a.name AS subject, a.type AS subject_type, b.name AS object, b.type AS object_type, "
            "r.relation AS relation, r.confidence AS confidence, r.chunk_ids[i] AS chunk_id, "
            "r.source_ids[i] AS source_id, r.content_hashes[i] AS content_hash, r.evidences[i] AS evidence"
        )
        params = {"chunk_ids": list(chunk_ids), "graph_name": graph_name}
        records = _run_graph_query(provider, cfg, query, params)
        return [
            {id_column: r["source_id"], "chunk_id": r["chunk_id"], "content_hash": r["content_hash"],
             "subject": r["subject"], "subject_type": r["subject_type"], "relation": r["relation"],
             "object": r["object"], "object_type": r["object_type"], "confidence": r["confidence"],
             "evidence": r["evidence"]}
            for r in records if r.get("content_hash") == chunk_hashes.get(r.get("chunk_id"))
        ]
    raise ValueError(f"write_graph op: unsupported provider {provider!r}. Valid: networkx, neo4j, falkordb.")


def _build_neo4j_driver(uri: str, username: Optional[str], password: Optional[str]):
    from neo4j import GraphDatabase
    auth = (username, password) if username else None
    return GraphDatabase.driver(uri, auth=auth)


def _build_falkordb_graph(host: str, port: int, username: Optional[str], password: Optional[str], graph_name: str):
    from falkordb import FalkorDB
    db = FalkorDB(host=host, port=port, username=username, password=password)
    return db.select_graph(graph_name)


def _run_graph_query(provider: str, cfg: dict, query: str, params: dict) -> List[Dict[str, Any]]:
    if provider == "neo4j":
        uri = cfg.get("uri")
        if not uri:
            raise ValueError("write_graph op provider='neo4j' requires `uri`.")
        username = os.environ.get(cfg["username_env_var"]) if cfg.get("username_env_var") else None
        password = os.environ.get(cfg["password_env_var"]) if cfg.get("password_env_var") else None
        driver = _build_neo4j_driver(uri, username, password)
        try:
            with driver.session(database=cfg.get("database", "neo4j")) as session:
                result = session.run(query, params)
                return [dict(r) for r in result]
        finally:
            driver.close()
    if provider == "falkordb":
        host = cfg.get("host", "localhost")
        port = int(cfg.get("port", 6379))
        username = os.environ.get(cfg["username_env_var"]) if cfg.get("username_env_var") else None
        password = os.environ.get(cfg["password_env_var"]) if cfg.get("password_env_var") else None
        g = _build_falkordb_graph(host, port, username, password, cfg.get("graph_name", "knowledge_graph"))
        result = g.query(query, params)
        columns = getattr(result, "header", None)
        if columns:
            col_names = [c[1] if isinstance(c, (list, tuple)) else c for c in columns]
            return [dict(zip(col_names, row)) for row in result.result_set]
        return [dict(zip(params.keys(), row)) for row in result.result_set]  # best-effort fallback
    raise ValueError(f"unsupported provider {provider!r}")


def _do_write_graph(df: pd.DataFrame, step: dict, id_column: str, context, partitioned: bool) -> pd.DataFrame:
    """Knobs: provider (networkx|neo4j|falkordb), sync_mode (upsert default | replace_sources |
    replace_all -- refused when partitioned), graph_name, plus provider connection fields (see
    the module docstring). Merges this run's triples into node/edge provenance: mention_count,
    aliases (nodes); chunk_ids/source_ids/content_hashes/evidences parallel arrays (edges)."""
    provider = step.get("provider", "networkx")
    sync_mode = step.get("sync_mode", "upsert")
    if sync_mode not in ("upsert", "replace_sources", "replace_all"):
        raise ValueError(f"write_graph op: sync_mode must be upsert, replace_sources or replace_all, got {sync_mode!r}")
    if sync_mode == "replace_all" and partitioned:
        raise ValueError("write_graph op: sync_mode='replace_all' would delete other partitions' triples; use replace_sources or upsert.")
    required = ("subject", "relation", "object")
    for col in required:
        if col not in df.columns:
            raise ValueError(f"write_graph op: column {col!r} not in upstream columns: {list(df.columns)}")
    graph_name = step.get("graph_name", "knowledge_graph")

    # Aggregate this run's rows into one record per unique (subject, relation, object).
    edges: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    nodes: Dict[str, Dict[str, Any]] = {}
    source_ids_this_run: set = set()
    for _, row in df.iterrows():
        s, r, o = row["subject"], row["relation"], row["object"]
        key = (s, r, o)
        e = edges.setdefault(key, {"confidence": None, "mention_count": 0, "chunk_ids": [], "source_ids": [], "content_hashes": [], "evidences": []})
        e["mention_count"] += 1
        conf = row.get("confidence")
        if conf is not None and (e["confidence"] is None or conf > e["confidence"]):
            e["confidence"] = conf
        cid, sid, chash, ev = row.get("chunk_id"), row.get(id_column), row.get("content_hash"), row.get("evidence")
        if cid is not None:
            e["chunk_ids"].append(cid)
            e["source_ids"].append(sid)
            e["content_hashes"].append(chash)
            e["evidences"].append((str(ev)[:500] if ev is not None else None))
        if sid is not None:
            source_ids_this_run.add(sid)
        for name, etype in ((s, row.get("subject_type")), (o, row.get("object_type"))):
            n = nodes.setdefault(name, {"type": None, "aliases": {name}, "mention_count": 0})
            n["mention_count"] += 1
            if etype and not n["type"]:
                n["type"] = etype

    if provider == "networkx":
        path = step.get("path") or f"./{graph_name}.json"
        g = _build_networkx_graph(path)
        if sync_mode == "replace_all":
            g.clear()
        elif sync_mode == "replace_sources" and source_ids_this_run:
            for u, v, k, data in list(g.edges(keys=True, data=True)):
                sids = set(data.get("source_ids") or [])
                if sids and sids <= source_ids_this_run and (u, data.get("relation"), v) not in edges:
                    g.remove_edge(u, v, key=k)
            for n in [n for n in g.nodes if g.degree(n) == 0]:
                g.remove_node(n)
        for name, n in nodes.items():
            if g.has_node(name):
                existing = g.nodes[name]
                existing["type"] = n["type"] or existing.get("type")
                existing["aliases"] = sorted(set(existing.get("aliases") or []) | n["aliases"])
                existing["mention_count"] = n["mention_count"]
            else:
                g.add_node(name, type=n["type"], aliases=sorted(n["aliases"]), mention_count=n["mention_count"])
        for (s, r, o), e in edges.items():
            removed = [k for u, v, k, d in g.edges(keys=True, data=True) if u == s and v == o and d.get("relation") == r]
            for k in removed:
                g.remove_edge(s, o, key=k)
            g.add_edge(s, o, relation=r, confidence=e["confidence"], mention_count=e["mention_count"],
                       chunk_ids=e["chunk_ids"], source_ids=e["source_ids"], content_hashes=e["content_hashes"],
                       evidences=e["evidences"])
        _save_networkx_graph(g, path)
        node_count, edge_count = g.number_of_nodes(), g.number_of_edges()
        degrees = dict(g.degree())
        top_entities = sorted(degrees.items(), key=lambda kv: -kv[1])[:20]
        store_location = path

    elif provider in ("neo4j", "falkordb"):
        if sync_mode == "replace_all":
            _run_graph_query(provider, step, "MATCH (n {graph: $graph_name}) DETACH DELETE n", {"graph_name": graph_name})
        for name, n in nodes.items():
            _run_graph_query(
                provider, step,
                "MERGE (n:Entity {name: $name, graph: $graph_name}) "
                "ON CREATE SET n.type = $type, n.aliases = $aliases, n.mention_count = $mention_count "
                "ON MATCH SET n.type = coalesce($type, n.type), n.aliases = $aliases, n.mention_count = $mention_count",
                {"name": name, "graph_name": graph_name, "type": n["type"], "aliases": sorted(n["aliases"]), "mention_count": n["mention_count"]},
            )
        new_edge_keys = []
        for (s, r, o), e in edges.items():
            ekey = _edge_key(s, r, o)
            new_edge_keys.append(ekey)
            _run_graph_query(
                provider, step,
                "MATCH (a:Entity {name: $s, graph: $graph_name}), (b:Entity {name: $o, graph: $graph_name}) "
                "MERGE (a)-[rel:RELATION {relation: $r, graph: $graph_name}]->(b) "
                "SET rel.edge_key = $ekey, rel.confidence = $confidence, rel.mention_count = $mention_count, "
                "rel.chunk_ids = $chunk_ids, rel.source_ids = $source_ids, rel.content_hashes = $content_hashes, "
                "rel.evidences = $evidences",
                {"s": s, "o": o, "r": r, "graph_name": graph_name, "ekey": ekey, "confidence": e["confidence"],
                 "mention_count": e["mention_count"], "chunk_ids": e["chunk_ids"], "source_ids": e["source_ids"],
                 "content_hashes": e["content_hashes"], "evidences": e["evidences"]},
            )
        if sync_mode == "replace_sources" and source_ids_this_run:
            _run_graph_query(
                provider, step,
                "MATCH (a)-[r {graph: $graph_name}]->(b) WHERE any(sid IN r.source_ids WHERE sid IN $source_ids) "
                "AND NOT r.edge_key IN $new_edge_keys DELETE r",
                {"graph_name": graph_name, "source_ids": list(source_ids_this_run), "new_edge_keys": new_edge_keys},
            )
            _run_graph_query(
                provider, step,
                "MATCH (n {graph: $graph_name}) WHERE NOT (n)--() DETACH DELETE n",
                {"graph_name": graph_name},
            )
        counts = _run_graph_query(
            provider, step,
            "MATCH (n {graph: $graph_name}) WITH count(n) AS nodes "
            "OPTIONAL MATCH ()-[r {graph: $graph_name}]->() RETURN nodes, count(r) AS edges",
            {"graph_name": graph_name},
        )
        node_count = counts[0]["nodes"] if counts else len(nodes)
        edge_count = counts[0]["edges"] if counts else len(edges)
        top_entities = sorted(nodes.items(), key=lambda kv: -kv[1]["mention_count"])[:20]
        top_entities = [(name, n["mention_count"]) for name, n in top_entities]
        store_location = f"{provider}:{graph_name}"
    else:
        raise ValueError(f"write_graph op: unsupported provider {provider!r}. Valid: networkx, neo4j, falkordb.")

    entities_this_run = set(nodes)
    chunk_ids_this_run = {cid for e in edges.values() for cid in e["chunk_ids"]}

    out = df.copy()
    out.attrs["stats"] = {
        "node_count": node_count, "edge_count": edge_count, "top_entities": top_entities,
        "store_location": store_location, "sync_mode": sync_mode, "provider": provider,
        "chunk_count": len(chunk_ids_this_run), "entities_linked_to_chunks": len(entities_this_run),
    }
    return out


# BEGIN scaffold partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port_scaffold.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")
# END scaffold partitions-def builder


class KnowledgeGraphBuilderComponent(Component, Model, Resolvable):
    """Chunk text, extract (subject, relation, object) triples, resolve entity aliases, and
    write a provenance-carrying knowledge graph -- one YAML, a `steps:` list."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame of raw text rows. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull raw text rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )
    id_column: Union[str, int] = Field(description="Column uniquely identifying each source row -- threaded through to every triple as provenance.")
    text_column: Union[str, int] = Field(description="Column containing the raw text to chunk.")
    metadata_columns: Optional[List[str]] = Field(default=None, description="Additional source columns carried forward onto every chunk row.")
    max_source_rows: Optional[int] = Field(
        default=None,
        description="Safety cap on source rows processed per materialize. Pair with partitioning to bound each run to a slice of a large corpus.",
    )
    steps: List[Dict[str, Any]] = Field(
        description=(
            "Ordered list of {id, op, ...op-specific fields} dicts. Each step's `source:` names a prior "
            "step's id to read from; defaults to the immediately preceding step. The first step must be "
            "op='chunk'. Valid ops: 'chunk', 'extract' (method: llm|spacy), 'resolve', 'write_graph' "
            "(provider: networkx|neo4j|falkordb, sync_mode: upsert|replace_sources|replace_all)."
        )
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the Dagster catalog. Defaults to ['ai', 'knowledge-graph'] if unset.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")

    retry_policy_max_retries: Optional[int] = Field(default=None, description="Max retries on asset failure.")
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    partition_type: Optional[str] = Field(default=None, description="Partition type: 'daily'/'weekly'/'monthly'/'hourly'/'cron'/'static'/'multi'/'dynamic'/None.")
    partition_cron_schedule: Optional[str] = Field(default=None, description="Required when partition_type='cron'.")
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition_type='cron' keys.")
    partition_end_offset: int = Field(default=0, description="0 (default) excludes the current, in-progress time window; 1 includes it.")
    partition_timezone: Optional[str] = Field(default=None, description="IANA timezone for time-based partitions (defaults to UTC).")
    partition_start: Optional[str] = Field(default=None, description="Partition start date (ISO). Required for time-based types.")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated values for static/multi partitioning.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Name for DynamicPartitionsDefinition.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None, description="Multi-axis partition spec. Overrides the flat partition fields.")
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None, description="Backfill execution strategy for a partitioned asset.")
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None, description="For backfill_policy='multi_run' only.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps", "metadata_columns"), csv=("partition_values",), tags=("asset_tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Knowledge Graph Builder", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        steps_cfg = list(self.steps or [])
        if not steps_cfg:
            raise ValueError("knowledge_graph_builder requires at least one step in `steps:`.")
        first_op = steps_cfg[0].get("op")
        if first_op not in ("chunk", "extract"):
            raise ValueError(
                "knowledge_graph_builder: the first step must be op='chunk', or op='extract' when "
                "`upstream_asset_key` already points at a pre-chunked asset (e.g. context_engineering_pipeline's "
                "own chunk step with per_step_ops=True) -- see the module docstring's shared chunk-id contract."
            )
        valid_ops = {"chunk", "extract", "resolve", "write_graph"}
        for s in steps_cfg:
            if "id" not in s or "op" not in s:
                raise ValueError(f"every step needs {{id, op}}; got: {s}")
            if s["op"] not in valid_ops:
                raise ValueError(f"step {s['id']!r} op={s['op']!r} unsupported. Valid: {sorted(valid_ops)}")
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("knowledge_graph_builder: set exactly one of `upstream_asset_key` or `source`.")

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule, cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )
        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1)

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries, delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        asset_name = self.asset_name
        id_column = self.id_column
        text_column = self.text_column
        metadata_columns = self.metadata_columns or []
        max_source_rows = self.max_source_rows
        source_cfg = self.source
        upstream_asset_key = self.upstream_asset_key
        partitioned = partitions_def is not None
        pre_chunked = first_op == "extract"

        tags = dict(self.asset_tags or {})
        for k in (self.kinds or ["ai", "knowledge-graph"]):
            tags[f"dagster/kind/{k}"] = ""
        tags.setdefault("dagster-community/knowledge-graph", "true")

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=self.description or f"Knowledge graph pipeline: {' → '.join(s['id'] for s in steps_cfg)}",
            group_name=self.group_name,
            tags=tags,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            retry_policy=retry_policy,
            partitions_def=partitions_def,
            backfill_policy=backfill_policy,
            ins=({"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))} if upstream_asset_key else None),
            required_resource_keys=({source_cfg["resource_key"]} if (source_cfg and source_cfg.get("resource_key")) else None),
        )
        def _asset(context: AssetExecutionContext, **kwargs):
            upstream = kwargs.get("upstream")
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
            if pre_chunked:
                if "chunk_id" not in upstream_df.columns:
                    raise ValueError(
                        "knowledge_graph_builder: the first step is op='extract' (pre-chunked upstream), but "
                        f"upstream has no 'chunk_id' column (columns: {list(upstream_df.columns)}). Point "
                        "upstream_asset_key at a chunk-shaped asset (e.g. a chunk op's own output, or "
                        "context_engineering_pipeline's <asset_name>_<chunk step id> per-step asset)."
                    )
            elif text_column not in upstream_df.columns:
                raise ValueError(f"text_column={text_column!r} not in upstream: {list(upstream_df.columns)}")
            source_rows_available = len(upstream_df)
            if max_source_rows is not None and source_rows_available > max_source_rows:
                context.log.warning(f"max_source_rows={max_source_rows} < {source_rows_available} available -- truncating this run.")
                upstream_df = upstream_df.head(max_source_rows)

            state: Dict[str, pd.DataFrame] = {}
            prev_id: Optional[str] = None
            extraction_settings: Dict[str, Any] = {}
            for step in steps_cfg:
                step_id, op = step["id"], step["op"]
                source_id = step.get("source", prev_id)
                df_in = upstream_df if source_id is None else state[source_id]
                if op == "chunk":
                    df_out = _do_chunk(df_in, step, id_column, text_column, metadata_columns, context)
                elif op == "extract":
                    df_out = _do_extract(df_in, step, id_column, context)
                    extraction_settings = {
                        "method": step.get("method", "llm"), "model": step.get("model"),
                        "entity_types": step.get("entity_types"), "relation_types": step.get("relation_types"),
                    }
                elif op == "resolve":
                    df_out = _do_resolve(df_in, step, context)
                else:  # write_graph
                    df_out = _do_write_graph(df_in, step, id_column, context, partitioned)
                state[step_id] = df_out
                prev_id = step_id

            final_df = state[prev_id]
            final_stats = final_df.attrs.get("stats") or {}
            metadata: Dict[str, Any] = {
                "dagster/row_count": MetadataValue.int(len(final_df)),
                "source_rows": MetadataValue.int(len(upstream_df)),
                "n_steps": MetadataValue.int(len(steps_cfg)),
                "step_chain": MetadataValue.text(" → ".join(s["id"] for s in steps_cfg)),
                "extraction_settings": MetadataValue.json(extraction_settings),
            }
            if "node_count" in final_stats:
                metadata["node_count"] = MetadataValue.int(final_stats["node_count"])
                metadata["edge_count"] = MetadataValue.int(final_stats["edge_count"])
                metadata["chunk_count"] = MetadataValue.int(final_stats.get("chunk_count", 0))
                metadata["entities_linked_to_chunks"] = MetadataValue.int(final_stats.get("entities_linked_to_chunks", 0))
                metadata["top_entities"] = MetadataValue.json(final_stats.get("top_entities"))
                metadata["graph_location"] = MetadataValue.text(str(final_stats.get("store_location")))
                metadata["sync_mode"] = MetadataValue.text(str(final_stats.get("sync_mode")))
            sample_cols = [c for c in ("subject", "relation", "object", "confidence") if c in final_df.columns]
            if sample_cols:
                metadata["sample_triples"] = MetadataValue.json(final_df[sample_cols].head(10).to_dict("records"))
            return Output(value=final_df, metadata=metadata)

        return Definitions(assets=[_asset])
