"""AgentCardWorkspaceComponent — bulk-declare a whole fleet of hosted agents
from one external manifest, instead of one `AgentCardComponent` YAML per
agent.

Same relationship `snowflake_workspace`/`stripe_workspace` have to their
single-asset siblings: one config block (here, a manifest location) emits
MANY assets instead of one. The manifest is a flat JSON array of agent-card
objects in the exact same shape `AgentCardComponent.build_defs()` emits
(`agent_id`, `name`, `description`, `skills`, `invocation`, ...) -- so this
is effectively "run AgentCardComponent in a loop, sourced from one file/URL
instead of N hand-written YAMLs."

Manifest loading intentionally duplicates `_load_external_agent_manifest`
from `assets/ai/agentic_pipeline/component.py` rather than importing it --
this repo's established convention is to copy small helpers locally across
component packages rather than cross-import (see that file's own comments
on `mcp_call`/`tool_use_loop` duplication).

Scope: this component ONLY brings in an external registry's cards. It does
NOT also do sibling-discovery -- that stays exclusively `delegate`'s job,
which already merges both sources. Because `delegate`'s sibling scan reads
`metadata["agent_card"]` off ANY AssetSpec in the folder regardless of which
component produced it, the AssetSpecs this component bulk-emits are picked
up by that scan automatically, with no changes to `delegate` needed.

Example:

    type: dagster_community_components.AgentCardWorkspaceComponent
    attributes:
      manifest_url: https://internal.example.com/agents/manifest.json
      include_tags: [customer-support]
      max_agents: 50
"""
import json
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


def _load_agent_manifest(
    manifest_path: Optional[str], manifest_url: Optional[str], log
) -> List[Dict[str, Any]]:
    """Local duplicate of agentic_pipeline's _load_external_agent_manifest --
    manifest_path (local-file precedence) / manifest_url (urlopen) dual
    source, same as CatalogAgentComponent's loader."""
    if not manifest_path and not manifest_url:
        return []
    try:
        if manifest_path:
            with open(manifest_path) as f:
                data = json.load(f)
        else:
            from urllib.request import urlopen
            with urlopen(manifest_url, timeout=30) as resp:
                data = json.load(resp)
        if isinstance(data, dict):
            data = data.get("agents") or data.get("cards") or []
        return [c for c in data if isinstance(c, dict) and c.get("agent_id")]
    except Exception as e:
        log.warning(f"[agent_card_workspace] failed to load manifest: {e}")
        return []


class AgentCardWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Load a manifest of agent cards (URL or local file) and emit one
    declare-only AssetSpec per entry -- the multi-asset "workspace" sibling
    of AgentCardComponent."""

    manifest_url: Optional[str] = Field(
        default=None, description="URL serving a JSON array (or {\"agents\": [...]}/{\"cards\": [...]}) of agent-card objects."
    )
    manifest_path: Optional[str] = Field(
        default=None, description="Local file with the same shape as manifest_url. Takes precedence over manifest_url if both are set."
    )
    include_ids: Optional[List[str]] = Field(
        default=None, description="If set, only agent_ids in this list are emitted."
    )
    include_tags: Optional[List[str]] = Field(
        default=None, description="If set, only cards with at least one skill tag in this list are emitted."
    )
    include_capabilities: Optional[List[str]] = Field(
        default=None,
        description=(
            "If set, only cards with at least one capability (our own verb taxonomy -- "
            "see AgentCardComponent.capabilities) in this list are emitted."
        ),
    )
    max_agents: int = Field(default=100, description="Defensive cap on how many cards from the manifest are emitted as assets.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name applied to every emitted asset.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners applied to every emitted asset.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags applied to every emitted asset.")
    asset_key_prefix: Optional[List[str]] = Field(default=None, description="Prefix for every emitted asset key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.manifest_path and not self.manifest_url:
            raise ValueError(
                "AgentCardWorkspaceComponent: set manifest_path or manifest_url."
            )

        cards = _load_agent_manifest(self.manifest_path, self.manifest_url, dg.get_dagster_logger())

        if self.include_ids:
            allowed_ids = set(self.include_ids)
            cards = [c for c in cards if c["agent_id"] in allowed_ids]
        if self.include_tags:
            wanted_tags = set(self.include_tags)
            cards = [
                c for c in cards
                if wanted_tags & {t for s in (c.get("skills") or []) for t in (s.get("tags") or [])}
            ]
        if self.include_capabilities:
            wanted_capabilities = set(self.include_capabilities)
            cards = [
                c for c in cards
                if wanted_capabilities & set(c.get("capabilities") or [])
            ]
        cards = cards[: self.max_agents]

        ids = [c["agent_id"] for c in cards]
        if len(set(ids)) != len(ids):
            raise ValueError(f"AgentCardWorkspaceComponent: duplicate agent_id in manifest: {ids}")

        prefix = self.asset_key_prefix or []
        specs = []
        for card in cards:
            specs.append(
                dg.AssetSpec(
                    key=dg.AssetKey([*prefix, card["agent_id"]]),
                    group_name=self.group_name,
                    description=card.get("description"),
                    kinds={"agent"},
                    metadata={
                        "agent_card": card,
                        "dagster.observability_type": "external",
                    },
                    owners=self.owners or [],
                    tags=self.tags or {},
                )
            )
        return dg.Definitions(assets=specs)
