"""AgentCardComponent — declare one hosted agent's capabilities.

Every "agent" an `AgenticPipelineComponent` `delegate` step can dynamically
pick is a HOSTED service, reached over MCP or plain HTTP -- never an
in-process Python class. This component declares one such agent: what it's
called, what it's good for (its `skills`, matched against a step's `task`
by the picker LLM), and how to reach it.

Field names track the real Agent2Agent (A2A) protocol's `AgentCard` schema
(https://docs.cloud.google.com/gemini/data-agents/reference/rest/Shared.Types/AgentCard)
where there's a direct analog -- `name`, `description`, `skills`, `version`,
`default_input_modes`/`default_output_modes` -- so wiring this to a literal
`.well-known/agent.json` later is a rename, not a redesign. `invocation` is
this component's OWN extension (real A2A agents all speak one wire protocol
and don't need a transport choice; this bridges two: MCP and plain HTTP).

Declare-only: this emits a bare `dg.AssetSpec` with no compute function and
no sensor -- same convention as `external_bigquery_table` and the other
`external_*` components. It exists so:
  1. The agent shows up as a node in the Dagster asset graph (lineage,
     Asset Catalog), tagged `kind=agent`.
  2. `AgenticPipelineComponent`'s `delegate` op can discover it as a sibling
     component in the same defs folder (reads `metadata["agent_card"]` off
     this spec) -- or an external process can aggregate many of these specs
     into a manifest for cross-project discovery.

Example (MCP-hosted agent):

    type: dagster_community_components.AgentCardComponent
    attributes:
      agent_id: refund_lookup_agent
      name: "Refund Lookup Agent"
      description: "Looks up refund status and policy exceptions for e-commerce orders."
      skills:
        - id: lookup_refund_status
          name: "Lookup refund status"
          description: "Given an order id, returns refund status + any policy exceptions."
          tags: [refunds, orders, customer-support]
      invocation:
        mcp_server:
          name: refunds-mcp
          type: http
          url: https://internal.example.com/mcp
          headers_env: {Authorization: REFUNDS_MCP_TOKEN}
        tool_name: lookup_refund
        tool_args_template: {order_context: "{prompt}"}

Example (plain-HTTP-hosted agent):

    type: dagster_community_components.AgentCardComponent
    attributes:
      agent_id: sentiment_agent
      name: "Sentiment Agent"
      description: "Scores the sentiment of free-text customer feedback."
      skills:
        - id: score_sentiment
          name: "Score sentiment"
          description: "Returns a sentiment label + confidence for input text."
          tags: [sentiment, nlp, customer-feedback]
      invocation:
        http:
          url: https://internal.example.com/agents/sentiment
          auth_bearer_env_var: SENTIMENT_AGENT_TOKEN
          response_text_path: "$.result.label"
"""
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class AgentCardComponent(dg.Component, dg.Model, dg.Resolvable):
    """Declare one hosted agent (MCP- or HTTP-reachable) and its skills."""

    agent_id: str = Field(description="Stable id -- used by delegate steps' picks and as this asset's key.")
    name: str = Field(description="Human-readable agent name.")
    description: str = Field(description="What this agent does, overall.")
    skills: List[Dict[str, Any]] = Field(
        description=(
            "List of {id, name, description, tags: [...], examples: [...]}. "
            "This is what a delegate step's picker LLM matches a task against -- "
            "write descriptions the way you'd write a route specialist's description."
        ),
        min_length=1,
    )
    version: str = Field(default="1.0.0", description="Agent version, A2A field name kept as-is.")
    default_input_modes: Optional[List[str]] = Field(default=None, description="Default input MIME types.")
    default_output_modes: Optional[List[str]] = Field(default=None, description="Default output MIME types.")

    invocation: Dict[str, Any] = Field(
        description=(
            "Exactly one of:\n"
            "  mcp_server: {name, type: stdio|http|sse|fastmcp, command|url, env|headers|headers_env}"
            " (same shape _call_mcp_tool_async in assets/ai/agentic_pipeline/component.py expects)\n"
            "    + tool_name: <the MCP tool on that server this agent's capability maps to>\n"
            "    + tool_args_template (optional): dict, {prompt} substituted with the delegate step's task\n"
            "  http: {url, url_env_var, method, timeout_seconds, auth_bearer_env_var, headers, headers_env,"
            " payload_template, body_template, response_text_path, ...}"
            " (same shape _call_remote_agent in assets/ai/agentic_pipeline/component.py expects, passed through unchanged)"
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    asset_key_prefix: Optional[List[str]] = Field(default=None, description="Prefix for the emitted asset key.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        has_mcp = bool(self.invocation.get("mcp_server"))
        has_http = bool(self.invocation.get("http"))
        if has_mcp == has_http:
            raise ValueError(
                "AgentCardComponent: set exactly one of invocation.mcp_server or invocation.http "
                f"(agent_id={self.agent_id!r})."
            )
        if has_mcp and not self.invocation.get("tool_name"):
            raise ValueError(
                f"AgentCardComponent: invocation.mcp_server set but invocation.tool_name is missing "
                f"(agent_id={self.agent_id!r}) -- which MCP tool fulfills this agent's capability?"
            )

        agent_card = {
            "agent_id": self.agent_id,
            "name": self.name,
            "description": self.description,
            "skills": self.skills,
            "version": self.version,
            "default_input_modes": self.default_input_modes,
            "default_output_modes": self.default_output_modes,
            "invocation": self.invocation,
        }

        prefix = self.asset_key_prefix or []
        spec = dg.AssetSpec(
            key=dg.AssetKey([*prefix, self.agent_id]),
            group_name=self.group_name,
            description=self.description,
            kinds={"agent"},
            metadata={
                "agent_card": agent_card,
                "dagster.observability_type": "external",
            },
            owners=self.owners or [],
            tags=self.tags or {},
        )
        return dg.Definitions(assets=[spec])
