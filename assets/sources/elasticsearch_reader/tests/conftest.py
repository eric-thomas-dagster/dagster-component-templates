"""Test helpers for ElasticsearchReaderComponent.

Loads component.py directly via importlib so tests don't require the parent package to
be pip-installed. Only the external service (the `elasticsearch` client) is faked;
everything the component itself owns (query construction, search_after pagination,
offset tracking, metadata) runs for real under dg.materialize(). No tests/ directory
existed for this component before this retrofit.

FakeElasticsearch supports exactly the request shapes this component sends: match_all,
a bare multi_match clause (approximated as case-insensitive substring matching across the
named fields -- not real relevance scoring, which isn't the point of these tests), and a
bool/must/filter wrapper with a range {field: {gt: value}} filter -- plus sort +
search_after keyset pagination.
"""
import importlib.util
import pathlib
import sys
import types
from types import ModuleType


def load_component_module() -> ModuleType:
    here = pathlib.Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location("elasticsearch_reader_component", here / "component.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _leaf_matches(doc, clause):
    if "match_all" in clause:
        return True
    if "multi_match" in clause:
        q = clause["multi_match"]["query"].lower()
        fields = clause["multi_match"].get("fields") or ["*"]
        source = doc["_source"]
        haystacks = source.values() if fields == ["*"] else (source.get(f) for f in fields)
        return any(q in str(h).lower() for h in haystacks if h is not None)
    if "range" in clause:
        (field, bounds), = clause["range"].items()
        value = doc["_source"].get(field)
        if value is None:
            return False
        if "gt" in bounds and not value > bounds["gt"]:
            return False
        if "gte" in bounds and not value >= bounds["gte"]:
            return False
        return True
    raise NotImplementedError(f"FakeElasticsearch does not model query clause: {clause}")


def _query_matches(doc, query):
    if "bool" in query:
        must = query["bool"].get("must", [])
        filt = query["bool"].get("filter", [])
        return all(_leaf_matches(doc, c) for c in must) and all(_leaf_matches(doc, c) for c in filt)
    return _leaf_matches(doc, query)


def _sort_key(doc, sort_spec):
    key = []
    for s in sort_spec:
        field = next(iter(s))
        key.append(doc["_id"] if field == "_id" else doc["_source"].get(field))
    return tuple(key)


class FakeElasticsearch:
    """Each newly-constructed instance re-seeds from the class-level `preseed` list (the
    current state of the real index at connection time) -- mirrors the mongodb_reader
    fake's `preseed` convention, since this component also reconnects (constructs a
    fresh Elasticsearch client) on every run. Mutate `FakeElasticsearch.preseed` (or the
    object `install_fake_elasticsearch` returns) between two materialize() calls to
    simulate new documents having been indexed in between."""

    instances = []
    preseed: list = []  # [{"_id": ..., "_source": {...}}, ...]

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.docs = [dict(h, _source=dict(h["_source"])) for h in FakeElasticsearch.preseed]
        self.search_calls = []
        FakeElasticsearch.instances.append(self)

    def search(self, index=None, body=None, size=10, source=None, **kwargs):
        self.search_calls.append({"index": index, "body": body, "size": size})
        body = body or {}
        query = body.get("query", {"match_all": {}})
        sort_spec = body.get("sort")
        search_after = body.get("search_after")

        matched = [d for d in self.docs if _query_matches(d, query)]
        if sort_spec:
            matched.sort(key=lambda d: _sort_key(d, sort_spec))
            if search_after is not None:
                sa = tuple(search_after)
                matched = [d for d in matched if _sort_key(d, sort_spec) > sa]

        page = matched[:size]
        hits = []
        for d in page:
            h = {"_id": d["_id"], "_score": d.get("_score", 1.0), "_source": d["_source"]}
            if sort_spec:
                h["sort"] = list(_sort_key(d, sort_spec))
            hits.append(h)
        return {"hits": {"hits": hits, "total": {"value": len(matched)}}}


def install_fake_elasticsearch(monkeypatch, hits=None):
    """Inject a fake `elasticsearch` module exposing exactly the name the component
    imports (Elasticsearch), preseeded with `hits`. Returns the FakeElasticsearch class
    so a test can read .instances, or set .preseed before a later materialize() call to
    simulate new documents having arrived in the real index."""
    FakeElasticsearch.instances = []
    FakeElasticsearch.preseed = list(hits or [])

    es_mod = types.ModuleType("elasticsearch")
    es_mod.Elasticsearch = FakeElasticsearch
    monkeypatch.setitem(sys.modules, "elasticsearch", es_mod)
    return FakeElasticsearch
