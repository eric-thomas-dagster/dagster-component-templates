"""Vector Store Query Asset Component.

Query vector databases for semantic search using embeddings.
"""

import os
from typing import Any, Dict, List, Optional, Union, Literal
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Config,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


class VectorStoreQueryRunConfig(Config):
    """Launchpad/API-settable overrides for a single run -- an ad-hoc retrieval test without
    editing the component's own (YAML/definition-time) config. All fields default to a no-op
    (None = use the component's own static value) so an unconfigured run behaves exactly as
    before. `query_text` is the one field that also makes `upstream_asset_key` unnecessary for
    a single test run: set it alone (with `provider`/`connection_string`/`collection_name`
    already configured on the component) to query without any upstream dependency at all."""

    query_text: Optional[str] = Field(
        default=None,
        description="Run this query instead of (or in addition to, if upstream_asset_key is also set) the component's own query_text/upstream rows.",
    )
    top_k: Optional[int] = Field(default=None, description="Override the component's own top_k for this run only.")
    search_mode: Optional[str] = Field(default=None, description="Override the component's own search_mode ('vector'/'full_text'/'hybrid') for this run only.")
    score_threshold: Optional[float] = Field(default=None, description="Override the component's own score_threshold for this run only.")


def _embed_query_text(text: str, provider: str, model: Optional[str], api_key_env_var: Optional[str]) -> List[float]:
    """Embeds ONE query string with the given provider/model -- same dispatch convention as
    `context_engineering_pipeline`'s `_do_embed_raw` and `resources/context_receipt_store/
    component.py`'s `KnowledgeSearch._embed_query` (single query, not batched, since this runs
    once per asset materialization, not once per upstream row)."""
    if provider == "sentence_transformers":
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError("embed_provider='sentence_transformers' requires: pip install sentence-transformers")
        return SentenceTransformer(model or "all-MiniLM-L6-v2").encode([text], normalize_embeddings=True, show_progress_bar=False)[0].tolist()
    if provider == "openai":
        try:
            import openai
        except ImportError:
            raise ImportError("embed_provider='openai' requires: pip install openai")
        client = openai.OpenAI(api_key=os.environ.get(api_key_env_var or "OPENAI_API_KEY"))
        return client.embeddings.create(model=model or "text-embedding-3-small", input=[text]).data[0].embedding
    if provider == "cohere":
        try:
            import cohere
        except ImportError:
            raise ImportError("embed_provider='cohere' requires: pip install cohere")
        client = cohere.Client(api_key=os.environ.get(api_key_env_var or "COHERE_API_KEY"))
        return client.embed(texts=[text], model=model or "embed-english-v3.0", input_type="search_query").embeddings[0]
    if provider == "litellm":
        try:
            import litellm
        except ImportError:
            raise ImportError("embed_provider='litellm' requires: pip install litellm")
        api_key = os.environ.get(api_key_env_var) if api_key_env_var else None
        return litellm.embedding(model=model or "text-embedding-3-small", input=[text], api_key=api_key).data[0]["embedding"]
    raise ValueError(f"VectorStoreQueryComponent: unsupported embed_provider {provider!r}. Valid: sentence_transformers, openai, cohere, litellm.")


def _infer_embed_model_from_asset(instance, asset_key_str: str) -> Optional[str]:
    """Best-effort: reads `embedding_model` off another asset's LAST materialization metadata
    (the same key context_engineering_pipeline/vector_store_writer promote to top-level
    metadata) -- so a user doesn't have to repeat a model name that asset already recorded.
    Returns None (never raises) if the asset has no materialization yet, or no such key --
    this is a convenience default, not a hard requirement."""
    try:
        from dagster import AssetKey as _AssetKey

        event = instance.get_latest_materialization_event(_AssetKey.from_user_string(asset_key_str))
        if not event or not event.asset_materialization:
            return None
        md = event.asset_materialization.metadata or {}
        value = md.get("embedding_model")
        if value is None:
            return None
        text = str(getattr(value, "value", value) or getattr(value, "text", "") or value)
        return text if text and text != "none" else None
    except Exception:
        return None


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

    # Both shapes set: ambiguous. Pick one.
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


def _build_s3vectors_client(region_name: str) -> Any:
    """Isolated client construction -- tests monkeypatch
    `vector_store_query_component._build_s3vectors_client` to return a fake
    client instead of calling the real (paid, AWS-account-specific) API."""
    import boto3

    return boto3.client("s3vectors", region_name=region_name)


class _DemoS3VectorsClient:
    """Deterministic, zero-cost stand-in for `boto3.client("s3vectors")`'s
    query path. Reads the same local JSON file format
    VectorStoreWriterComponent's demo mode writes, and brute-force
    cosine-ranks against it -- matching the existing faiss branch's own
    local-file persistence convention."""

    def __init__(self, path: str):
        self._path = path

    def query_vectors(self, vectorBucketName: str, indexName: str, topK: int, queryVector: Dict[str, Any], filter: Optional[Dict[str, Any]] = None, returnMetadata: bool = True, returnDistance: bool = True, **_kw) -> Dict[str, Any]:
        import json
        import math

        if not os.path.exists(self._path):
            return {"vectors": []}
        with open(self._path) as f:
            data = json.load(f)
        index = (data.get("indexes") or {}).get(indexName, {"vectors": {}})
        query = queryVector["float32"]
        query_norm = math.sqrt(sum(x * x for x in query)) or 1.0

        scored = []
        for key, entry in index["vectors"].items():
            metadata = entry.get("metadata", {})
            if filter and not all(metadata.get(k) == v for k, v in filter.items()):
                continue
            vec = entry["data"]["float32"]
            dot = sum(a * b for a, b in zip(query, vec))
            vec_norm = math.sqrt(sum(x * x for x in vec)) or 1.0
            cosine_distance = 1.0 - (dot / (query_norm * vec_norm))
            scored.append((cosine_distance, key, metadata))

        scored.sort(key=lambda t: t[0])
        results = []
        for distance, key, metadata in scored[:topK]:
            item = {"key": key}
            if returnMetadata:
                item["metadata"] = metadata
            if returnDistance:
                item["distance"] = distance
            results.append(item)
        return {"vectors": results}

    def get_vectors(self, vectorBucketName: str, indexName: str, keys: List[str], returnData: bool = True, returnMetadata: bool = True, **_kw) -> Dict[str, Any]:
        """track_retrieval_count's read-before-write step -- put_vectors has no partial-update
        API, so a metadata-only change still needs the vector's own embedding fetched first."""
        import json

        if not os.path.exists(self._path):
            return {"vectors": []}
        with open(self._path) as f:
            data = json.load(f)
        index = (data.get("indexes") or {}).get(indexName, {"vectors": {}})
        out = []
        for key in keys:
            entry = index["vectors"].get(key)
            if entry is None:
                continue
            item = {"key": key}
            if returnData:
                item["data"] = entry["data"]
            if returnMetadata:
                item["metadata"] = entry.get("metadata", {})
            out.append(item)
        return {"vectors": out}

    def put_vectors(self, vectorBucketName: str, indexName: str, vectors: List[Dict[str, Any]], **_kw) -> None:
        import json

        data = {"indexes": {}}
        if os.path.exists(self._path):
            with open(self._path) as f:
                data = json.load(f)
        index = data.setdefault("indexes", {}).setdefault(indexName, {"vectors": {}})
        for v in vectors:
            index["vectors"][v["key"]] = {"data": v["data"], "metadata": v.get("metadata", {})}
        with open(self._path, "w") as f:
            json.dump(data, f)


def _extract_result_text(result: Dict[str, Any], provider: str, text_metadata_key: str) -> Optional[str]:
    """chromadb returns document text natively (as 'document'); every other provider here only
    ever returns metadata/payload, so the text has to be read back out of whichever key
    vector_store_writer's own text_column was stored under."""
    if provider == "chromadb":
        return result.get("document")
    container = result.get("payload") if provider == "qdrant" else result.get("metadata")
    return (container or {}).get(text_metadata_key)


def _bm25_scores(query_text: str, texts: List[str]) -> List[float]:
    """Real BM25 (rank_bm25), not a hand-rolled heuristic -- verified against the installed
    package's actual API (BM25Okapi(tokenized_corpus).get_scores(tokenized_query)) before use.
    A candidate with no text at all scores 0 (never favored, never crashes the ranking)."""
    try:
        from rank_bm25 import BM25Okapi
    except ImportError:
        raise ImportError("search_mode='full_text'/'hybrid' requires rank_bm25: pip install rank_bm25")
    tokenized_corpus = [(t or "").lower().split() for t in texts]
    bm25 = BM25Okapi(tokenized_corpus)
    return list(bm25.get_scores(query_text.lower().split()))


def _reciprocal_rank_fusion(rankings: List[List[int]], k: int = 60) -> List[float]:
    """Combines several rankings (each a list of candidate indices, best first) into one fused
    score per candidate -- the standard RRF formula (score = sum(1 / (k + rank))), k=60 being the
    constant from the original paper (Cormack et al., 2009). Scale-independent on purpose: vector
    distance and BM25 score live on totally different, unnormalized scales, and RRF only ever
    looks at each ranking's ORDER, never the raw score values, so there's nothing to normalize."""
    n = max((idx for ranking in rankings for idx in ranking), default=-1) + 1
    fused = [0.0] * n
    for ranking in rankings:
        for rank, idx in enumerate(ranking):
            fused[idx] += 1.0 / (k + rank + 1)
    return fused


def _apply_rerank_model(query_text: str, texts: List[str], model: str, api_key_env_var: str) -> List[float]:
    """litellm.rerank() -- verified against the installed version's real response shape
    (RerankResponse.results: list of {index, relevance_score, document}), same
    litellm-as-universal-gateway convention used throughout this repo. A candidate with no text
    is sent as an empty string (never favored by a real reranker, never crashes the call)."""
    import os
    try:
        import litellm
    except ImportError:
        raise ImportError("rerank_model requires litellm: pip install litellm")
    resp = litellm.rerank(
        model=model, query=query_text, documents=[t or "" for t in texts],
        api_key=os.environ.get(api_key_env_var),
    )
    scores = [0.0] * len(texts)
    for r in resp.results or []:
        scores[r["index"]] = r["relevance_score"]
    return scores


def _extract_result_enabled(result: Dict[str, Any], provider: str) -> Any:
    container = result.get("payload") if provider == "qdrant" else result.get("metadata")
    return (container or {}).get("enabled", True)


def _rerank_and_filter_results(
    results: List[Dict[str, Any]], *, provider: str, query_text: Optional[str], search_mode: str,
    text_metadata_key: str, top_k: int, rerank_model: Optional[str], rerank_api_key_env_var: str,
    score_threshold: Optional[float], exclude_disabled: bool = False,
) -> List[Dict[str, Any]]:
    """Applies exclude_disabled, then search_mode's own re-ranking, then rerank_model, then
    score_threshold, to one query's already-fetched (possibly oversampled) candidate pool --
    shared by every provider branch, since this is all post-processing over `results`, the
    provider's own output shape."""
    if not results:
        return results

    if exclude_disabled:
        results = [r for r in results if _extract_result_enabled(r, provider) is not False]
        if not results:
            return results

    if search_mode != "vector":
        texts = [_extract_result_text(r, provider, text_metadata_key) for r in results]
        vector_rank = list(range(len(results)))  # already provider-sorted by vector distance/score
        bm25_scores = _bm25_scores(query_text, texts)
        bm25_rank = sorted(range(len(results)), key=lambda i: bm25_scores[i], reverse=True)
        if search_mode == "full_text":
            order = bm25_rank
        else:  # hybrid
            fused = _reciprocal_rank_fusion([vector_rank, bm25_rank])
            order = sorted(range(len(results)), key=lambda i: fused[i], reverse=True)
        results = [results[i] for i in order][:top_k]
    else:
        results = results[:top_k]

    if rerank_model:
        texts = [_extract_result_text(r, provider, text_metadata_key) for r in results]
        scores = _apply_rerank_model(query_text, texts, rerank_model, rerank_api_key_env_var)
        order = sorted(range(len(results)), key=lambda i: scores[i], reverse=True)
        results = [{**results[i], "relevance_score": scores[i]} for i in order][:top_k]

    if score_threshold is not None:
        if rerank_model:
            results = [r for r in results if r["relevance_score"] >= score_threshold]
        else:
            results = [
                r for r in results
                if ("distance" not in r or r["distance"] <= score_threshold)
                and ("score" not in r or r["score"] >= score_threshold)
            ]
    return results


def _increment_retrieval_counts(
    results: List[Dict[str, Any]], *, provider: str, demo_mode: bool, connection_string: Optional[str],
    collection_name: str, vector_bucket_name: Optional[str], aws_region: Optional[str],
    expanded_api_key: Optional[str], context: Any,
) -> None:
    """Opt-in write side-effect (track_retrieval_count): bumps each returned hit's own
    'retrieval_count' metadata by 1, persisted back into the vector store via each provider's
    partial-metadata-update API. Best-effort -- a write failure is logged, not raised, since this
    is telemetry on top of an otherwise read-only query, not the query's own result."""
    if not results:
        return
    try:
        if provider == "chromadb":
            import chromadb
            client = chromadb.PersistentClient(path=connection_string or "./chroma_db")
            collection = client.get_collection(name=collection_name)
            ids = [r["id"] for r in results]
            metadatas = [
                {**(r.get("metadata") or {}), "retrieval_count": (r.get("metadata") or {}).get("retrieval_count", 0) + 1}
                for r in results
            ]
            collection.update(ids=ids, metadatas=metadatas)

        elif provider == "pinecone":
            from pinecone import Pinecone
            pc = Pinecone(api_key=expanded_api_key)
            index = pc.Index(collection_name)
            for r in results:
                count = (r.get("metadata") or {}).get("retrieval_count", 0) + 1
                index.update(id=r["id"], set_metadata={"retrieval_count": count})

        elif provider == "qdrant":
            from qdrant_client import QdrantClient
            client = QdrantClient(url=connection_string or "localhost", api_key=expanded_api_key)
            for r in results:
                count = (r.get("payload") or {}).get("retrieval_count", 0) + 1
                client.set_payload(
                    collection_name=collection_name, payload={"retrieval_count": count}, points=[r["id"]],
                )

        elif provider == "faiss":
            import pickle
            index_path = connection_string or f"./{collection_name}.faiss"
            metadata_path = index_path.replace(".faiss", "_metadata.pkl")
            with open(metadata_path, 'rb') as f:
                metadata = pickle.load(f)
            for r in results:
                doc_idx = r["index"]
                if doc_idx < len(metadata):
                    metadata[doc_idx]["retrieval_count"] = metadata[doc_idx].get("retrieval_count", 0) + 1
            with open(metadata_path, 'wb') as f:
                pickle.dump(metadata, f)

        elif provider == "s3vectors":
            if demo_mode:
                demo_path = connection_string or f"./{vector_bucket_name}_{collection_name}_s3vectors_demo.json"
                client = _DemoS3VectorsClient(demo_path)
            else:
                client = _build_s3vectors_client(aws_region)
            for r in results:
                fetched = client.get_vectors(
                    vectorBucketName=vector_bucket_name, indexName=collection_name,
                    keys=[r["key"]], returnData=True, returnMetadata=True,
                )
                vecs = fetched.get("vectors", [])
                if not vecs:
                    continue
                vec = vecs[0]
                metadata = {**vec.get("metadata", {})}
                metadata["retrieval_count"] = metadata.get("retrieval_count", 0) + 1
                client.put_vectors(
                    vectorBucketName=vector_bucket_name, indexName=collection_name,
                    vectors=[{"key": r["key"], "data": vec["data"], "metadata": metadata}],
                )

        elif provider == "lancedb":
            import lancedb
            db = lancedb.connect(connection_string or "./lancedb_data")
            tbl = db.open_table(collection_name)
            if "retrieval_count" not in tbl.schema.names:
                tbl.add_columns({"retrieval_count": "0"})
            for r in results:
                chunk_id = (r.get("metadata") or {}).get("chunk_id")
                if chunk_id is None:
                    continue
                quoted = str(chunk_id).replace("'", "''")
                tbl.update(where=f"chunk_id = '{quoted}'", values_sql={"retrieval_count": "COALESCE(retrieval_count, 0) + 1"})
    except Exception as e:
        context.log.warning(f"track_retrieval_count: failed to write back retrieval_count for provider={provider}: {e}")


class VectorStoreQueryComponent(Component, Model, Resolvable):
    """Component for querying vector stores.

    Accepts query embeddings from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_community_components.VectorStoreQueryComponent
        attributes:
          asset_name: search_results
          provider: chromadb
          collection_name: documents
          top_k: 10
          api_key: ${PINECONE_API_KEY}
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    provider: str = Field(description="Provider: 'pinecone', 'chromadb', 'faiss', 'qdrant', 's3vectors', 'lancedb'")
    collection_name: str = Field(description="Collection/index name")
    query_text: Optional[str] = Field(default=None, description="Direct query text (alternative to embedding)")
    top_k: int = Field(default=10, description="Number of results")
    connection_string: Optional[str] = Field(default=None, description="Connection string")
    api_key: Optional[str] = Field(default=None, description="API key (use ${VAR_NAME} for environment variable)")
    filter_metadata: Optional[str] = Field(default=None, description="JSON filter for metadata")
    include_distances: bool = Field(default=True, description="Include similarity distances")
    embedding_column: Union[str, int] = Field(default="embedding", description="Column name containing query embeddings in the upstream DataFrame")
    query_text_column: Optional[Union[str, int]] = Field(default=None, description="Column with query text to copy into output rows (helps downstream rerankers correlate query+document)")
    search_mode: Literal["vector", "full_text", "hybrid"] = Field(
        default="vector",
        description=(
            "'vector' (default): today's behavior, ranked purely by embedding similarity. "
            "'full_text': fetches top_k * oversample_factor candidates by vector similarity, then "
            "re-ranks them by real BM25 (rank_bm25) relevance to query_text against each "
            "candidate's 'document' text, truncated back to top_k. 'hybrid': same oversampled "
            "candidate pool, combined via Reciprocal Rank Fusion (RRF, k=60 -- the standard "
            "constant from the original RRF paper; a scale-independent way to merge two rankings "
            "without needing to normalize distance and BM25 scores onto the same scale) of the "
            "vector-distance rank and the BM25 rank. None of the 6 providers here has a native "
            "full-text/hybrid search this component verified and could rely on -- this is "
            "deliberately a client-side re-rank over a vector-fetched candidate pool, not a "
            "provider-native inverted-index search; it needs query_text (not just an embedding) "
            "to have any text to score against."
        ),
    )
    oversample_factor: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Only used when search_mode is 'full_text' or 'hybrid': fetches top_k * oversample_factor candidates before re-ranking down to top_k. Ignored for search_mode='vector'.",
    )
    rerank_model: Optional[str] = Field(
        default=None,
        description=(
            "Any litellm-supported rerank model (e.g. Cohere's 'rerank-english-v3.0'). When set, "
            "re-ranks the final candidate set (after search_mode's own ranking) by relevance to "
            "query_text via litellm.rerank(), replacing 'distance' with a 'relevance_score' column "
            "(higher is better, unlike distance) and truncating to top_k. Requires query_text."
        ),
    )
    rerank_api_key_env_var: str = Field(
        default="COHERE_API_KEY",
        description="Only used when rerank_model is set: env var holding the rerank provider's API key.",
    )
    text_metadata_key: str = Field(
        default="text",
        description=(
            "Only used when search_mode is 'full_text'/'hybrid', or rerank_model is set: the "
            "metadata/payload key holding each candidate's text content, for providers that don't "
            "return document text natively (pinecone/qdrant/faiss/s3vectors all only return "
            "metadata/payload -- whatever key vector_store_writer's own text_column was written "
            "under). chromadb returns text natively (as 'document') and ignores this field."
        ),
    )
    score_threshold: Optional[float] = Field(
        default=None,
        description=(
            "Drops hits past this threshold, applied last (after search_mode and rerank_model). "
            "When rerank_model is set: drops hits whose relevance_score is BELOW this value "
            "(higher is better). Otherwise, auto-detected from whichever key the provider itself "
            "returned (a real, pre-existing inconsistency across these 5 providers, not something "
            "this field papers over): chromadb/faiss/s3vectors return 'distance' (lower is better "
            "-- drops hits ABOVE this value); pinecone/qdrant return 'score', a similarity score "
            "for a cosine-configured index (higher is better -- drops hits BELOW this value). "
            "Unset (default): no filtering."
        ),
    )
    exclude_disabled: bool = Field(
        default=False,
        description=(
            "Drop hits whose 'enabled' metadata key is explicitly false -- the write side of this "
            "pairing is vector_store_writer's enabled_column (or its override_enabled run-config "
            "override). A hit with no 'enabled' key at all (vector_store_writer's enabled_column "
            "was never configured) is kept -- opt-in disabling, not a default-everything-off "
            "state. Applied before search_mode's own re-ranking and before truncation to top_k, so "
            "disabled hits don't crowd out enabled ones -- this also means turning it on fetches "
            "top_k * oversample_factor candidates (like search_mode='full_text'/'hybrid' already "
            "does) instead of exactly top_k, to leave room for filtered-out hits. Defaults to "
            "false so an unconfigured query pays no oversampling cost."
        ),
    )
    track_retrieval_count: bool = Field(
        default=False,
        description=(
            "Opt-in write side-effect on an otherwise read-only component: after each query, "
            "increments a 'retrieval_count' metadata field on every returned hit's own vector in "
            "the store (starting from 0 if absent), persisted via each provider's partial-metadata-"
            "update API. Lets vector_store_writer's enabled_column (or any downstream job) surface "
            "which documents are actually being retrieved. Adds one extra write call per hit per "
            "query -- leave false for pure read-only querying."
        ),
    )
    vector_bucket_name: Optional[str] = Field(
        default=None,
        description="S3 Vectors bucket name. Required when provider='s3vectors'. Ignored by every other provider.",
    )
    aws_region: Optional[str] = Field(
        default=None,
        description="AWS region for the s3vectors client. Required when provider='s3vectors'. Ignored by every other provider.",
    )
    demo_mode: bool = Field(
        default=True,
        description=(
            "Only affects provider='s3vectors' -- queries a local JSON-file-backed fake instead "
            "of a real AWS account (no effect on chromadb/pinecone/qdrant/faiss). Set false for "
            "a real S3 Vectors bucket. Point `connection_string` at the same path "
            "VectorStoreWriterComponent's demo mode wrote to, to query what it wrote."
        ),
    )
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )

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
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description=(
            "Include a sample of hits (first preview_rows rows, or a random sample for a large "
            "result set) in run metadata as 'sample_hits', alongside the always-present "
            "queries_count/hits_count/avg_distance/min_distance/max_distance summary stats -- so "
            "retrieval testing (did this change in top_k/rerank/score_threshold improve or worsen "
            "results) doesn't require querying the output table directly."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata. For long DataFrames "
            "(>10x preview_rows), a random sample is used; otherwise head()."
        ),
    )
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Upstream asset key providing a DataFrame with query embeddings (and/or per-row query "
            "text via query_text_column). Optional when query_text is set (as a static field, or "
            "per-run via VectorStoreQueryRunConfig) -- an ad-hoc retrieval test needs no upstream "
            "dependency at all, just a single query string embedded on the fly (see embed_provider)."
        ),
    )
    embed_provider: Optional[str] = Field(
        default=None,
        description=(
            "Embeds query_text/query_text_column with this provider ('sentence_transformers', "
            "'openai', 'cohere', 'litellm') when no embedding_column is present on upstream (or "
            "there is no upstream at all) -- e.g. for an ad-hoc retrieval test. Must match the "
            "provider/model the index was actually built with (context_engineering_pipeline's "
            "embed step, or vector_store_writer's upstream) or the query vector won't be "
            "comparable to what's indexed. Leave unset when upstream already supplies a real "
            "embedding_column (the normal, non-ad-hoc pipeline shape)."
        ),
    )
    embed_model: Optional[str] = Field(
        default=None,
        description="Model name for embed_provider (e.g. 'all-MiniLM-L6-v2', 'text-embedding-3-small'). Each provider has its own sane default if unset.",
    )
    embed_api_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding embed_provider's API key (openai/cohere/litellm only; sentence_transformers needs none).",
    )
    knowledge_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Infers embed_model from this asset's own last materialization metadata (the "
            "`embedding_model` key context_engineering_pipeline/vector_store_writer already "
            "promote to top-level metadata), so an ad-hoc retrieval test doesn't need to repeat a "
            "model name that asset already recorded -- only applied when embed_model is itself "
            "unset. Best-effort: silently skipped if the asset has no materialization yet or no "
            "such key. Does NOT infer provider/connection_string/collection_name -- those aren't "
            "stamped as asset metadata today and must still be set explicitly."
        ),
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' materializes an entire backfill "
            'range in ONE run: the component processes each partition key in the range in turn (filtering '
            'upstream rows by partition_date_column / partition_static_column per key) and concatenates the '
            "results into one DataFrame. 'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's default (one run per partition, "
            'uncapped). Ignored when no partitioning is configured. COST: total paid-API/LLM calls are the same '
            'as running the partitions separately, but note that when no '
            'partition_date_column/partition_static_column matches, every partition key processes ALL upstream '
            "rows, so a range multiplies calls by the number of partitions. 'single_run' requires a "
            'partition-range-aware IO manager (e.g. a DB-table IO manager with partition_expr set); the default '
            'filesystem IO manager cannot persist one output spanning multiple partitions and will raise a clear '
            'error at materialization time. Per-partition output metadata is overwritten by the last partition in '
            'the range.'
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    @model_validator(mode="after")
    def _validate_s3vectors_config(self):
        if self.provider == "s3vectors" and not (self.vector_bucket_name and self.aws_region):
            raise ValueError(
                "VectorStoreQueryComponent: provider='s3vectors' requires both `vector_bucket_name` and `aws_region`."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Vector Store Query', editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        provider = self.provider
        collection_name = self.collection_name
        query_text = self.query_text
        top_k = self.top_k
        connection_string = self.connection_string
        api_key = self.api_key
        filter_metadata = self.filter_metadata
        include_distances = self.include_distances
        embedding_column = self.embedding_column
        query_text_column = self.query_text_column
        search_mode = self.search_mode
        oversample_factor = self.oversample_factor
        rerank_model = self.rerank_model
        rerank_api_key_env_var = self.rerank_api_key_env_var
        text_metadata_key = self.text_metadata_key
        score_threshold = self.score_threshold
        exclude_disabled = self.exclude_disabled
        track_retrieval_count = self.track_retrieval_count
        embed_provider = self.embed_provider
        embed_model = self.embed_model
        embed_api_key_env_var = self.embed_api_key_env_var
        knowledge_asset_key = self.knowledge_asset_key
        if search_mode != "vector" and not query_text and not query_text_column:
            raise ValueError(
                "VectorStoreQueryComponent: search_mode='full_text'/'hybrid' needs query text to "
                "score against -- set query_text (same text for every row) or query_text_column "
                "(per-row query text)."
            )
        if rerank_model and not query_text and not query_text_column:
            raise ValueError("VectorStoreQueryComponent: rerank_model needs query_text or query_text_column to rerank against.")
        vector_bucket_name = self.vector_bucket_name
        aws_region = self.aws_region
        demo_mode = self.demo_mode
        description = self.description or f"Query {provider}/{collection_name}"
        group_name = self.group_name
        upstream_asset_key = self.upstream_asset_key

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        # Infer kinds from component name if not explicitly set
        _comp_name = "vector_store_query"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        from dagster import BackfillPolicy as _BackfillPolicy
        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == 'single_run':
            _backfill_policy_obj = _BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == 'multi_run':
            _backfill_policy_obj = _BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        _ins_kwargs: Dict[str, Any] = {}
        if upstream_asset_key:
            _ins_kwargs["ins"] = {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}

        _asset_deco = asset(retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            description=description,
            partitions_def=partitions_def, backfill_policy=_backfill_policy_obj,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            **_ins_kwargs,
        )
        def _vector_store_query_asset_impl(
            context: AssetExecutionContext, config: VectorStoreQueryRunConfig,
            upstream: Optional[pd.DataFrame] = None, _pk=None,
        ) -> pd.DataFrame:
            # Run-config overrides (None = use the component's own static value). SEPARATE
            # names, not reassignments of query_text/top_k/search_mode/score_threshold
            # themselves -- those are closure-captured from build_defs, and reassigning a
            # closure-captured name anywhere in this function body makes Python treat it as
            # local to the WHOLE function, breaking any earlier read of it (the exact
            # UnboundLocalError class this component's own tests already caught once for
            # top_k/_fetch_k -- see the comment a few lines below).
            _query_text = config.query_text or query_text
            _top_k = config.top_k if config.top_k is not None else top_k
            _search_mode = config.search_mode if config.search_mode is not None else search_mode
            _score_threshold = config.score_threshold if config.score_threshold is not None else score_threshold

            _embed_model = embed_model
            if _embed_model is None and knowledge_asset_key:
                _embed_model = _infer_embed_model_from_asset(context.instance, knowledge_asset_key)

            # Filter to current partition if partitioned
            if _pk is not None and upstream is not None:
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = next((_pk.keys_by_dimension[_d] for _d in (partition_static_dim, "static_dim", "segment") if _d and _d in _pk.keys_by_dimension), "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    _col_dates = pd.to_datetime(upstream[partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                    upstream = upstream[_col_dates == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Query vector store using embeddings from each row of upstream DataFrame."""

            if upstream is None:
                if not _query_text:
                    raise ValueError(
                        "VectorStoreQueryComponent: no upstream_asset_key and no query_text "
                        "(static field or VectorStoreQueryRunConfig.query_text) -- nothing to query with."
                    )
                input_df = pd.DataFrame({embedding_column: [None]})
            else:
                input_df = upstream
                if embedding_column not in input_df.columns and not _query_text:
                    raise ValueError(
                        f"Embedding column '{embedding_column}' not found in upstream DataFrame "
                        f"and no query_text provided. Available: {list(input_df.columns)}"
                    )

            context.log.info(f"Querying {provider} for top {_top_k} results for {len(input_df)} rows")

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    context.log.warning(f"Environment variable not set: {var_name}")

            all_results = []

            for idx, row in input_df.iterrows():
                query_embedding = None
                if embedding_column in input_df.columns:
                    emb = row[embedding_column]
                    if isinstance(emb, list):
                        query_embedding = emb
                    elif hasattr(emb, 'tolist'):
                        query_embedding = emb.tolist()

                _row_query_text = _query_text or (str(row[query_text_column]) if query_text_column and query_text_column in input_df.columns else None)

                # No real embedding on this row (no embedding_column, or an ad-hoc run with no
                # upstream at all) -- embed _row_query_text ourselves with embed_provider,
                # rather than letting chromadb silently fall back to ITS OWN default embedder
                # via query_texts= (a real, reported gap: that fallback embeds with whatever
                # chromadb defaults to, not necessarily the model the index was actually built
                # with, so the query vector may not even be comparable to what's indexed).
                if query_embedding is None and _row_query_text and embed_provider:
                    query_embedding = _embed_query_text(_row_query_text, embed_provider, _embed_model, embed_api_key_env_var)

                if query_embedding is None and provider != "chromadb":
                    raise ValueError(
                        f"VectorStoreQueryComponent: no embedding for this query (no embedding_column "
                        f"on upstream, and either no query_text or no embed_provider set) -- "
                        f"provider={provider!r} requires a real query vector (only chromadb can fall "
                        f"back to its own default embedder via query_texts=)."
                    )

                # full_text/hybrid re-rank a larger candidate pool client-side (see search_mode's
                # own field description for why) -- fetch more than top_k from the provider, then
                # truncate back down after re-ranking. A SEPARATE name, not a reassignment of
                # top_k itself: top_k is read earlier in this same function (the log line above
                # the loop), and reassigning it anywhere in the function body makes Python treat
                # it as local to the whole function, not the closure-captured value from
                # build_defs -- that earlier read would then raise UnboundLocalError (caught by
                # this component's own tests; this is a lesson, not a hypothetical).
                _fetch_k = (_top_k * oversample_factor) if (_search_mode != "vector" or exclude_disabled) else _top_k

                results = []

                if provider == "chromadb":
                    import chromadb
                    client = chromadb.PersistentClient(path=connection_string or "./chroma_db")
                    collection = client.get_collection(name=collection_name)

                    query_results = collection.query(
                        query_embeddings=[query_embedding] if query_embedding else None,
                        # Only chromadb's own default embedder as a last resort -- never reached
                        # when embed_provider produced a real query_embedding above.
                        query_texts=[_row_query_text] if query_embedding is None and _row_query_text else None,
                        n_results=_fetch_k
                    )

                    for i in range(len(query_results['ids'][0])):
                        result = {
                            "query_row_index": idx,
                            "id": query_results['ids'][0][i],
                            "document": query_results['documents'][0][i] if 'documents' in query_results else None,
                            "metadata": query_results['metadatas'][0][i] if 'metadatas' in query_results else {},
                        }
                        if query_text_column and query_text_column in row.index:
                            result["query"] = row[query_text_column]
                        if include_distances and 'distances' in query_results:
                            result["distance"] = query_results['distances'][0][i]
                        results.append(result)

                elif provider == "pinecone":
                    from pinecone import Pinecone
                    pc = Pinecone(api_key=expanded_api_key)
                    index = pc.Index(collection_name)

                    query_response = index.query(vector=query_embedding, top_k=_fetch_k, include_metadata=True)

                    for match in query_response['matches']:
                        results.append({
                            "query_row_index": idx,
                            "id": match['id'],
                            "score": match['score'],
                            "metadata": match.get('metadata', {})
                        })

                elif provider == "qdrant":
                    from qdrant_client import QdrantClient
                    client = QdrantClient(url=connection_string or "localhost", api_key=expanded_api_key)

                    search_results = client.search(
                        collection_name=collection_name,
                        query_vector=query_embedding,
                        limit=_fetch_k
                    )

                    for hit in search_results:
                        results.append({
                            "query_row_index": idx,
                            "id": hit.id,
                            "score": hit.score,
                            "payload": hit.payload
                        })

                elif provider == "faiss":
                    import faiss
                    import pickle
                    import numpy as np

                    index_path = connection_string or f"./{collection_name}.faiss"
                    metadata_path = index_path.replace(".faiss", "_metadata.pkl")

                    faiss_index = faiss.read_index(index_path)
                    query_vector = np.array([query_embedding]).astype('float32')

                    distances, indices = faiss_index.search(query_vector, _fetch_k)

                    with open(metadata_path, 'rb') as f:
                        metadata = pickle.load(f)

                    for i, doc_idx in enumerate(indices[0]):
                        results.append({
                            "query_row_index": idx,
                            "index": int(doc_idx),
                            "distance": float(distances[0][i]),
                            "metadata": metadata[doc_idx] if doc_idx < len(metadata) else {}
                        })

                elif provider == "s3vectors":
                    if demo_mode:
                        demo_path = connection_string or f"./{vector_bucket_name}_{collection_name}_s3vectors_demo.json"
                        client = _DemoS3VectorsClient(demo_path)
                    else:
                        client = _build_s3vectors_client(aws_region)

                    import json as _json
                    query_response = client.query_vectors(
                        vectorBucketName=vector_bucket_name, indexName=collection_name,
                        topK=_fetch_k, queryVector={"float32": [float(x) for x in query_embedding]},
                        filter=_json.loads(filter_metadata) if filter_metadata else None,
                        returnMetadata=True, returnDistance=include_distances,
                    )

                    for v in query_response.get("vectors", []):
                        result = {
                            "query_row_index": idx,
                            "key": v["key"],
                            "metadata": v.get("metadata", {}),
                        }
                        if query_text_column and query_text_column in row.index:
                            result["query"] = row[query_text_column]
                        if include_distances and "distance" in v:
                            result["distance"] = v["distance"]
                        results.append(result)

                elif provider == "lancedb":
                    import json as _json
                    import lancedb

                    db_conn = lancedb.connect(connection_string or "./lancedb_data")
                    tbl = db_conn.open_table(collection_name)
                    search = tbl.search(query_embedding).limit(_fetch_k)
                    if filter_metadata:
                        # Same JSON-filter contract as s3vectors ({"field": value} equality
                        # matches) -- translated to lancedb's own SQL WHERE predicate here,
                        # since lancedb has no separate structured-filter API like the others.
                        _filter_dict = _json.loads(filter_metadata)
                        _clauses = []
                        for _k, _v in _filter_dict.items():
                            _col = str(_k).replace('"', '""')
                            if isinstance(_v, bool):
                                _clauses.append(f'"{_col}" = {str(_v).upper()}')
                            elif isinstance(_v, (int, float)):
                                _clauses.append(f'"{_col}" = {_v}')
                            else:
                                _clauses.append(f""""{_col}" = '{str(_v).replace("'", "''")}'""")
                        search = search.where(" AND ".join(_clauses))
                    hits_df = search.to_pandas()

                    for _, hit in hits_df.iterrows():
                        metadata = {c: hit[c] for c in hits_df.columns if c not in ("vector", "_distance")}
                        result = {
                            "query_row_index": idx,
                            "id": metadata.get("chunk_id"),
                            "metadata": metadata,
                        }
                        if query_text_column and query_text_column in row.index:
                            result["query"] = row[query_text_column]
                        if include_distances and "_distance" in hits_df.columns:
                            result["distance"] = float(hit["_distance"])
                        results.append(result)

                else:
                    raise ValueError(f"Unsupported provider: {provider}")

                results = _rerank_and_filter_results(
                    results, provider=provider, query_text=_row_query_text, search_mode=_search_mode,
                    text_metadata_key=text_metadata_key, top_k=_top_k, rerank_model=rerank_model,
                    rerank_api_key_env_var=rerank_api_key_env_var, score_threshold=_score_threshold,
                    exclude_disabled=exclude_disabled,
                )

                if track_retrieval_count:
                    _increment_retrieval_counts(
                        results, provider=provider, demo_mode=demo_mode,
                        connection_string=connection_string, collection_name=collection_name,
                        vector_bucket_name=vector_bucket_name, aws_region=aws_region,
                        expanded_api_key=expanded_api_key, context=context,
                    )

                all_results.extend(results)

            result_df = pd.DataFrame(all_results)

            context.log.info(f"Found {len(result_df)} total results for {len(input_df)} queries")

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(result_df.dtypes[col]))
                for col in result_df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(result_df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                "queries_count": MetadataValue.int(len(input_df)),
                "hits_count": MetadataValue.int(len(result_df)),
            }
            if "distance" in result_df.columns and len(result_df) > 0:
                _distances = pd.to_numeric(result_df["distance"], errors="coerce").dropna()
                if len(_distances) > 0:
                    _metadata["avg_distance"] = MetadataValue.float(round(float(_distances.mean()), 4))
                    _metadata["min_distance"] = MetadataValue.float(round(float(_distances.min()), 4))
                    _metadata["max_distance"] = MetadataValue.float(round(float(_distances.max()), 4))
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage and upstream is not None:
                try:
                    _upstream_cols = set(upstream.columns)
                    _effective_lineage = {
                        col.name: [col.name] for col in _col_schema.columns
                        if col.name in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[str(out_col)] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=str(ic))
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(result_df) > 0:
                try:
                    _prev = result_df.sample(min(preview_rows, len(result_df))) if len(result_df) > preview_rows * 10 else result_df.head(preview_rows)
                    _metadata["sample_hits"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")

            # Full hit detail as JSON -- a retrieval-test run (query_text/VectorStoreQueryRunConfig)
            # needs the actual text back, not just counts/distance stats. Capped the same way
            # sample_hits already is (preview_rows) to keep this from growing unbounded on a
            # real multi-row upstream query batch.
            try:
                _hits = []
                for _rank, _r in enumerate(all_results[:preview_rows] if include_preview else all_results):
                    _container = _r.get("payload") if provider == "qdrant" else _r.get("metadata")
                    _container = _container or {}
                    _hits.append({
                        "rank": _rank,
                        "score": _r.get("score") if _r.get("score") is not None else _r.get("distance"),
                        "chunk_id": _container.get("chunk_id", _r.get("id") or _r.get("key")),
                        "doc_id": _container.get("source_id", _container.get("doc_id")),
                        "text": _extract_result_text(_r, provider, text_metadata_key),
                        "metadata": _container,
                        "parent_text": _container.get("parent_text"),
                    })
                _metadata["hits"] = MetadataValue.json(_hits)
            except Exception as _e:
                context.log.warning(f"hits metadata emission failed: {_e}")

            context.add_output_metadata(_metadata)

            return result_df

        def _run(context, config, upstream):
            _keys = list(context.partition_keys) if (context.has_partition_key or context.has_partition_key_range) else [None]
            if len(_keys) == 1:
                return _vector_store_query_asset_impl(context, config, upstream, _keys[0])
            context.log.info(f'Processing {len(_keys)} partition keys in one run (backfill range)')
            return pd.concat([_vector_store_query_asset_impl(context, config, upstream, _k) for _k in _keys], ignore_index=True)

        # Dagster infers an asset's inputs from its function SIGNATURE (parameter names), not
        # just the `ins` dict -- a parameter literally named `upstream`, even with a Python
        # default of None, is still treated as an unresolved input needing a producing asset.
        # So when upstream_asset_key is unset, `upstream` can't appear in the signature at all;
        # two separate function definitions (matching this repo's existing optional-upstream
        # components, e.g. tavily_ingestion/google_docs_extractor) rather than one with a
        # default value.
        if upstream_asset_key:
            @_asset_deco
            def vector_store_query_asset(context: AssetExecutionContext, config: VectorStoreQueryRunConfig, upstream: pd.DataFrame) -> pd.DataFrame:
                return _run(context, config, upstream)
        else:
            @_asset_deco
            def vector_store_query_asset(context: AssetExecutionContext, config: VectorStoreQueryRunConfig) -> pd.DataFrame:
                return _run(context, config, None)

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[vector_store_query_asset])


        return Definitions(assets=[vector_store_query_asset], asset_checks=list(_schema_checks))


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
