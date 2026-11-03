"""External Asset Catalog Component.

Declares many non-executable external `AssetSpec`s from one YAML table --
for a neighbouring team's assets that live in a different Dagster code
location, so this location's asset graph can show accurate lineage and
ownership for them without owning or materializing them.

Every other component in `external_assets/` declares exactly one external
object per instance (one Snowflake table, one Kafka topic, ...). This one
is the generic, many-rows-per-instance counterpart: a single component
instance is a whole department's or integration's asset catalog.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import BaseModel, Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
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


class ExternalAssetEntry(BaseModel):
    """One externally-owned asset to declare in this location's asset graph."""
    key: Union[str, List[str]] = Field(description="Dagster asset key: a '/' or '.' separated string (e.g. 'finance/gl_balances'), or a list of key segments.")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on. Accepts a comma-separated string or a list.")
    owners: Optional[List[str]] = Field(default=None, description="Owners: an email address, or a team name prefixed with 'team:' (e.g. 'team:finance-eng'). Accepts a comma-separated string or a list.")
    kinds: Optional[List[str]] = Field(default=None, description="Kind tags shown on the asset node (e.g. 'snowflake', 'dbt', 'python'). Accepts a comma-separated string or a list.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Arbitrary string tags shown on the asset.")
    description: Optional[str] = Field(default=None, description="Human-readable description.")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Arbitrary metadata shown on the asset (e.g. a source URL or a contact channel).")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=(), tags=('tags',))


class ExternalAssetCatalogComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declare many external (non-executable) assets from one YAML table.

    Use this when another team's code lives in a different Dagster code
    location, but you want their assets to show up here too -- for lineage
    (`deps` on them from your own assets), ownership, and discoverability --
    without taking on running or observing them. Every `external_assets/*`
    sibling component declares one object per instance (one Snowflake
    table, one Kafka topic, ...); this one declares a whole table of them
    in a single instance, for cases where the thing you need is a registry,
    not a single integration.

    This component makes no external calls and holds no credentials -- it
    only ever builds plain `AssetSpec` objects from the YAML you give it.

    Example:
        ```yaml
        type: dagster_community_components.ExternalAssetCatalogComponent
        attributes:
          assets:
            - key: finance/gl_balances
              owners: ["team:finance-eng"]
              kinds: ["snowflake"]
              tags: {pii: "false"}
              description: "General ledger balances, owned by Finance Engineering."
            - key: marketing/campaign_performance
              deps: ["finance/gl_balances"]
              owners: ["team:marketing-data"]
              kinds: ["dbt", "snowflake"]
              group_name: marketing
        ```
    """
    assets: List[ExternalAssetEntry] = Field(description="Table of external assets to declare.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='External Asset Catalog', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        specs = []
        for entry in self.assets:
            key = dg.AssetKey(entry.key) if isinstance(entry.key, list) else dg.AssetKey.from_user_string(entry.key)
            specs.append(dg.AssetSpec(
                key=key,
                deps=[dg.AssetKey.from_user_string(k) for k in (entry.deps or [])] or None,
                owners=entry.owners or None,
                kinds=set(entry.kinds) if entry.kinds else None,
                tags=entry.tags or None,
                description=entry.description,
                metadata=entry.metadata or None,
                group_name=entry.group_name,
            ))
        return dg.Definitions(assets=specs)
