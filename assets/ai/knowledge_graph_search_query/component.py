"""KnowledgeGraphSearchQueryComponent -- a graph-only mirror of
`hybrid_knowledge_search_query`, for testing a knowledge graph built by
`knowledge_graph_builder` from a run without configuring any vector-store settings at all.

One asset per materialization: fuzzy-matches an entity name via an already-registered
`KnowledgeGraphSearchResource`'s `.search_entities()`, then expands its neighborhood via
`.get_neighbors(entity, hops, relation)` -- both reported as run metadata.

Requires a `KnowledgeGraphSearchServerComponent` already registered under
`graph_resource_key` in the same code location (its `resource_key`, default
`knowledge_graph_search`) -- this component only references that resource via Dagster's own
resource-injection (`required_resource_keys` + `getattr(context.resources, resource_key)`),
the same pattern used throughout this repo to reuse an already-built client without
duplicating its connection code.
"""
from typing import Any, Dict, List, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Config,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field

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


class KnowledgeGraphSearchQueryRunConfig(Config):
    """Launchpad/API-settable overrides for a single run -- an ad-hoc graph test without
    editing the component's own (YAML/definition-time) config. All fields default to a no-op
    (None = use the component's own static value)."""

    entity: Optional[str] = Field(default=None, description="Run this entity query instead of the component's own static `entity`. Required (here or statically) for every run.")
    entity_type: Optional[str] = Field(default=None, description="Override the component's own entity_type filter for this run only.")
    hops: Optional[int] = Field(default=None, description="Override the component's own hops for this run only.")
    relation: Optional[str] = Field(default=None, description="Override the component's own relation filter for this run only.")


class KnowledgeGraphSearchQueryComponent(Component, Model, Resolvable):
    """One asset per materialization: `search_entities(entity, type=entity_type)` to fuzzy-match
    a name, then `get_neighbors(resolved_name, hops, relation)` to expand its neighborhood --
    both reported as run metadata. See KnowledgeGraphSearchQueryRunConfig for the Launchpad
    run-config overrides (entity/entity_type/hops/relation)."""

    asset_name: str = Field(description="Output Dagster asset name.")
    graph_resource_key: str = Field(description="Resource key of an already-registered KnowledgeGraphSearchResource (from a KnowledgeGraphSearchServerComponent in the same code location).")
    entity: Optional[str] = Field(default=None, description="Static default entity name/search term. Can be left unset if every run supplies KnowledgeGraphSearchQueryRunConfig.entity instead.")
    entity_type: Optional[str] = Field(default=None, description="Restrict search_entities to this entity type.")
    hops: int = Field(default=1, description="get_neighbors hop count (capped at 3 by the resource).")
    relation: Optional[str] = Field(default=None, description="Restrict get_neighbors to edges with this relation name.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, Any]] = Field(default=None, description="Additional key-value tags to apply to the asset.")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys (no data passed at runtime).")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "deps"), csv=(), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Knowledge Graph Search Query", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        resource_key = self.graph_resource_key
        entity, entity_type, hops, relation = self.entity, self.entity_type, self.hops, self.relation

        tags = dict(self.asset_tags or {})
        tags.setdefault("dagster-community/knowledge-graph-search-query", resource_key)

        @asset(
            key=AssetKey.from_user_string(self.asset_name),
            description=self.description or "Ask-a-question run against a knowledge graph (no vector-store settings required).",
            group_name=self.group_name,
            tags=tags,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            required_resource_keys={resource_key},
        )
        def _asset(context: AssetExecutionContext, config: KnowledgeGraphSearchQueryRunConfig):
            resource = getattr(context.resources, resource_key)
            e = config.entity or entity
            if not e:
                raise ValueError(
                    "knowledge_graph_search_query: no entity -- set it on the component or via "
                    "KnowledgeGraphSearchQueryRunConfig.entity for this run."
                )
            _entity_type = config.entity_type if config.entity_type is not None else entity_type
            _hops = config.hops if config.hops is not None else hops
            _relation = config.relation if config.relation is not None else relation

            matches = resource.search_entities(e, type=_entity_type)
            entities_matched = matches.get("entities") or []
            exact = next((m["name"] for m in entities_matched if m.get("name") == e), None)
            resolved_name = exact or (entities_matched[0]["name"] if entities_matched else e)

            neighbors_result = resource.get_neighbors(resolved_name, hops=_hops, relation=_relation)

            context.add_output_metadata({
                "entities_matched": MetadataValue.json(entities_matched),
                "neighbors": MetadataValue.json(neighbors_result.get("neighbors") or []),
                "neighbor_error": MetadataValue.text(neighbors_result.get("error") or ""),
                "query_entity": MetadataValue.text(e),
                "resolved_entity": MetadataValue.text(resolved_name),
                "hops": MetadataValue.int(_hops),
            })

        return Definitions(assets=[_asset])
