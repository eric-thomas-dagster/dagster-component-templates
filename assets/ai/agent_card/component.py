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
no sensor. It exists so:
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
    default_input_modes: Optional[List[str]] = Field(
        default=None,
        description=(
            "Default input MIME types (real A2A AgentCard field -- required there, "
            "optional here). delegate drops this card for a step whose input_mode "
            "isn't in this list; leave unset to never be excluded on this basis."
        ),
    )
    default_output_modes: Optional[List[str]] = Field(default=None, description="Default output MIME types (real A2A AgentCard field).")
    capabilities: Optional[List[str]] = Field(
        default=None,
        description=(
            "Our OWN verb taxonomy -- NOT part of the A2A or MCP standard. Freeform "
            "strings describing WHAT KIND of action this agent can perform (e.g. "
            "critique, supervise, triage, translate, summarize, classify, lookup, "
            "extract, generate, code_review), as opposed to `skills.tags` which "
            "describe topic/domain. A delegate step's `required_capabilities` "
            "pre-filters on this, cheaply, before any LLM sees the candidate -- the "
            "lever that keeps delegate usable when hundreds of agents are "
            "registered. See README.md for a recommended starter vocabulary."
        ),
    )

    invocation: Dict[str, Any] = Field(
        description=(
            "Exactly one of:\n"
            "  mcp_server: {name, type: stdio|http|sse|fastmcp, command|url, env|headers|headers_env}"
            " (same shape _call_mcp_tool_async in assets/ai/agentic_pipeline/component.py expects)\n"
            "    + tool_name: <the MCP tool on that server this agent's capability maps to>\n"
            "    + tool_args_template (optional): dict, {prompt} substituted with the delegate step's "
            "task text (an INSTRUCTION, not upstream data -- if the delegate step also has a source:/"
            "prior step feeding it real data, that's {extra.src_text}, a SEPARATE placeholder; forgetting "
            "it means the agent only ever sees the instruction and an LLM-backed agent will still "
            "confidently answer, just ungrounded in any real data)\n"
            "  http: {url, url_env_var, method, timeout_seconds, auth_bearer_env_var, headers, headers_env,"
            " payload_template, body_template, response_text_path, ...}"
            " (same shape _call_remote_agent in assets/ai/agentic_pipeline/component.py expects, passed through unchanged)"
        ),
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    asset_key_prefix: Optional[List[str]] = Field(default=None, description="Prefix for the emitted asset key.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners',), csv=(), tags=('tags',))

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
            "capabilities": self.capabilities,
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
