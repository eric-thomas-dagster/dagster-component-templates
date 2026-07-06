"""Vanta Evidence Response Agent Component.

Reads a DataFrame of Vanta controls (typically from `vanta_controls_ingestion`)
and uses an LLM to draft evidence responses / control narratives per row.

Compliance auditors routinely ask "explain how you meet control X". This
component drafts that narrative programmatically from the control description
(and optional internal-context column, e.g. runbook excerpts, tool inventories).
The output is a first draft — a human reviewer should still sign it off.

Row-wise pattern mirrors `langchain_chain_asset`, but with a fixed prompt
scaffold tuned for compliance framing.
"""

from typing import Any, Dict, List, Optional, Union

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext, AssetIn, AssetKey
from pydantic import Field


DEFAULT_SYSTEM_PROMPT = (
    "You are a compliance analyst drafting evidence responses for a SOC 2 / "
    "ISO 27001 audit. Given a control id and description, write a concise, "
    "auditor-ready narrative (3-6 sentences) explaining how the organization "
    "satisfies the control. Use plain, professional English. Avoid marketing "
    "language. If internal context is provided, ground the narrative in it "
    "and do NOT invent specifics that aren't in the context."
)


class VantaEvidenceResponseAgentComponent(dg.Component, dg.Model, dg.Resolvable):
    """Draft evidence responses for Vanta controls with an LLM.

    Reads an upstream DataFrame of Vanta controls, produces a draft response
    per row, and writes it into `response_column`.

    Example:

        ```yaml
        type: dagster_community_components.VantaEvidenceResponseAgentComponent
        attributes:
          asset_name: vanta_control_narratives
          upstream_asset_key: vanta_soc2_controls
          llm_provider: openai
          model: gpt-4o
          api_key_env_var: OPENAI_API_KEY
          control_id_column: control_id
          control_description_column: description
          response_column: draft_response
        ```
    """

    asset_name: str = Field(description="Dagster asset name")

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame of Vanta controls",
    )

    # --- LLM configuration --------------------------------------------------

    llm_provider: str = Field(
        default="openai",
        description="LLM provider: 'openai' or 'anthropic'",
    )

    model: str = Field(
        default="gpt-4o",
        description="Model name (provider-specific), e.g. 'gpt-4o', 'claude-3-5-sonnet-20241022'",
    )

    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var containing the LLM provider API key",
    )

    temperature: float = Field(default=0.2, description="Sampling temperature (low = more deterministic)")

    max_tokens: int = Field(default=800, description="Max tokens per response")

    system_prompt: str = Field(
        default=DEFAULT_SYSTEM_PROMPT,
        description="System prompt setting the compliance-analyst persona.",
    )

    # --- DataFrame column mapping ------------------------------------------

    control_id_column: Union[str, int] = Field(
        default="control_id",
        description="Column holding the control identifier",
    )

    control_description_column: Union[str, int] = Field(
        default="description",
        description="Column holding the control description text",
    )

    context_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Optional column holding internal context (runbooks, tool inventories) to ground the narrative.",
    )

    response_column: Union[str, int] = Field(
        default="draft_response",
        description="Column name to write the draft response into",
    )

    # --- Processing knobs ---------------------------------------------------

    max_rows: Optional[int] = Field(
        default=None,
        description="Cap the number of rows processed (useful for testing / cost control)",
    )

    batch_log_every: int = Field(
        default=10,
        description="Log progress every N rows",
    )

    # --- Standard asset metadata --------------------------------------------

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(default="vanta", description="Asset group")

    owners: Optional[List[str]] = Field(default=None, description="Asset owners")

    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags")

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['ai', 'vanta']",
    )

    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream keys")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        _kinds = list(self.kinds or []) or ["ai", "vanta"]
        _all_tags = dict(self.asset_tags or {})
        for k in _kinds:
            _all_tags[f"dagster/kind/{k}"] = ""

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            description=_self.description or f"LLM-drafted Vanta control narratives ({_self.llm_provider}/{_self.model})",
            group_name=_self.group_name,
            owners=_self.owners or [],
            tags=_all_tags,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(_self.upstream_asset_key))},
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def vanta_evidence_response_agent_asset(
            context: AssetExecutionContext, upstream: pd.DataFrame
        ) -> pd.DataFrame:
            import os

            df = upstream.copy()
            if _self.max_rows:
                df = df.head(_self.max_rows)

            api_key = os.environ.get(_self.api_key_env_var)
            if not api_key:
                raise ValueError(
                    f"Env var {_self.api_key_env_var!r} is not set. "
                    f"Export it before running this asset."
                )

            provider = _self.llm_provider.lower()
            if provider == "openai":
                from openai import OpenAI
                client = OpenAI(api_key=api_key)

                def _call(system: str, user: str) -> str:
                    resp = client.chat.completions.create(
                        model=_self.model,
                        temperature=_self.temperature,
                        max_tokens=_self.max_tokens,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                    )
                    return resp.choices[0].message.content or ""

            elif provider == "anthropic":
                import anthropic
                client = anthropic.Anthropic(api_key=api_key)

                def _call(system: str, user: str) -> str:
                    resp = client.messages.create(
                        model=_self.model,
                        max_tokens=_self.max_tokens,
                        temperature=_self.temperature,
                        system=system,
                        messages=[{"role": "user", "content": user}],
                    )
                    return resp.content[0].text if resp.content else ""

            else:
                raise ValueError(
                    f"Unsupported llm_provider: {provider!r}. Use 'openai' or 'anthropic'."
                )

            # Validate columns
            for col in (_self.control_id_column, _self.control_description_column):
                if col not in df.columns:
                    raise ValueError(
                        f"Column {col!r} not found in upstream DataFrame. "
                        f"Available: {list(df.columns)}"
                    )
            if _self.context_column and _self.context_column not in df.columns:
                context.log.warning(
                    f"context_column={_self.context_column!r} not in upstream columns; ignoring."
                )

            context.log.info(
                f"Drafting evidence responses over {len(df)} controls "
                f"({provider}/{_self.model})"
            )

            responses: List[Optional[str]] = []
            for i, row in enumerate(df.itertuples(index=False)):
                row_dict = row._asdict()
                cid = row_dict.get(_self.control_id_column, "")
                cdesc = row_dict.get(_self.control_description_column, "")
                cctx = (
                    row_dict.get(_self.context_column, "")
                    if _self.context_column and _self.context_column in df.columns
                    else ""
                )

                user_prompt_parts = [
                    f"Control ID: {cid}",
                    f"Control description:\n{cdesc}",
                ]
                if cctx:
                    user_prompt_parts.append(f"Internal context:\n{cctx}")
                user_prompt_parts.append(
                    "Draft the evidence response (3-6 sentences)."
                )
                user_prompt = "\n\n".join(user_prompt_parts)

                try:
                    response = _call(_self.system_prompt, user_prompt)
                except Exception as e:
                    context.log.warning(f"Row {i} (control {cid!r}) failed: {e}")
                    response = None
                responses.append(response)

                if (i + 1) % _self.batch_log_every == 0:
                    context.log.info(f"Processed {i + 1}/{len(df)} controls")

            df[_self.response_column] = responses
            context.log.info(f"Drafted {sum(1 for r in responses if r)} / {len(df)} responses")

            return df

        return dg.Definitions(assets=[vanta_evidence_response_agent_asset])
