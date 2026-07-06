"""Gong Call Summary Asset Component.

Reads a DataFrame of Gong calls (typically produced by
``GongCallsIngestionComponent``) and uses an OpenAI-compatible LLM to summarize
each call transcript into a short summary + bulleted action items. Emits an
enriched DataFrame with two new columns.

Row-wise pattern (one API call per row) mirrors ``langchain_chain_asset`` and
``anthropic_llm`` — favors simplicity over batching. For heavy volume switch to
a batched wrapper.
"""

import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field

SYSTEM_PROMPT = (
    "You are an assistant that reads sales call transcripts and produces a "
    "concise structured summary. Respond ONLY with a JSON object with exactly "
    "two keys: 'summary' (a 2-4 sentence paragraph) and 'action_items' (an "
    "array of short imperative strings, each a concrete follow-up). Do not "
    "include any prose outside the JSON."
)


class GongCallSummaryAssetComponent(Component, Model, Resolvable):
    """Summarize each call transcript into a short summary + action items.

    Example:

        ```yaml
        type: dagster_component_templates.GongCallSummaryAssetComponent
        attributes:
          asset_name: gong_call_summaries
          upstream_asset_key: gong_calls
          llm_provider: openai
          model: gpt-4o-mini
          api_key_env_var: OPENAI_API_KEY
          transcript_column: transcript
        ```
    """

    asset_name: str = Field(description="Name of the asset to create")

    upstream_asset_key: str = Field(
        description="Upstream asset key providing a DataFrame with a transcript column",
    )

    llm_provider: str = Field(
        default="openai",
        description="LLM provider. Currently supports 'openai' and OpenAI-compatible endpoints via base_url.",
    )

    model: str = Field(
        default="gpt-4o-mini",
        description="Model identifier passed to the provider",
    )

    api_key_env_var: str = Field(
        default="OPENAI_API_KEY",
        description="Env var holding the LLM provider API key",
    )

    base_url: Optional[str] = Field(
        default=None,
        description="Override the OpenAI-compatible base URL (e.g. for Azure OpenAI, LiteLLM, local vLLM)",
    )

    transcript_column: str = Field(
        default="transcript",
        description="Name of the column containing the flattened transcript text",
    )

    summary_column: str = Field(
        default="call_summary",
        description="Name of the output column to write the summary text into",
    )

    action_items_column: str = Field(
        default="action_items",
        description="Name of the output column to write the action items list into",
    )

    max_tokens: int = Field(
        default=500,
        description="Maximum tokens per completion",
    )

    temperature: float = Field(
        default=0.2,
        description="Sampling temperature (lower = more deterministic)",
    )

    skip_empty: bool = Field(
        default=True,
        description="If true, rows where the transcript column is empty/NaN are skipped (empty outputs).",
    )

    description: Optional[str] = Field(default=None, description="Asset description")

    group_name: Optional[str] = Field(
        default="gong",
        description="Asset group for organization",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog. Defaults to ['gong', 'openai', 'python'].",
    )

    include_preview_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata",
    )

    preview_rows: int = Field(
        default=10,
        ge=1,
        le=200,
        description="Rows to include in the preview metadata",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime)",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        llm_provider = self.llm_provider
        model = self.model
        api_key_env_var = self.api_key_env_var
        base_url = self.base_url
        transcript_column = self.transcript_column
        summary_column = self.summary_column
        action_items_column = self.action_items_column
        max_tokens = self.max_tokens
        temperature = self.temperature
        skip_empty = self.skip_empty
        description = self.description or f"LLM summaries + action items using {model}"
        group_name = self.group_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

        _kinds = list(self.kinds or ["gong", llm_provider, "python"])
        _all_tags = dict(self.asset_tags or {})
        for _k in _kinds:
            _all_tags[f"dagster/kind/{_k}"] = ""

        owners = self.owners or []

        @asset(
            key=AssetKey.from_user_string(asset_name),
            description=description,
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def gong_call_summary_asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                raise ValueError(f"Env var {api_key_env_var!r} is not set")

            if llm_provider != "openai":
                raise ValueError(
                    f"llm_provider={llm_provider!r} not supported. "
                    "Use 'openai' (or an OpenAI-compatible endpoint via base_url)."
                )

            from openai import OpenAI
            client = OpenAI(api_key=api_key, base_url=base_url) if base_url else OpenAI(api_key=api_key)

            if transcript_column not in upstream.columns:
                raise ValueError(
                    f"Column {transcript_column!r} not found in upstream DataFrame. "
                    f"Available columns: {list(upstream.columns)}"
                )

            df = upstream.copy()
            summaries: List[str] = []
            action_items: List[List[str]] = []

            for idx, row in df.iterrows():
                transcript = row.get(transcript_column)
                if skip_empty and (not transcript or (isinstance(transcript, float) and pd.isna(transcript))):
                    summaries.append("")
                    action_items.append([])
                    continue

                resp = client.chat.completions.create(
                    model=model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": str(transcript)},
                    ],
                )
                content = resp.choices[0].message.content or "{}"
                parsed = json.loads(content)
                summaries.append(str(parsed.get("summary", "")))
                items = parsed.get("action_items") or []
                action_items.append([str(i) for i in items] if isinstance(items, list) else [])

                if (len(summaries)) % 10 == 0:
                    context.log.info(f"Summarized {len(summaries)}/{len(df)} calls")

            df[summary_column] = summaries
            df[action_items_column] = action_items

            metadata: Dict[str, Any] = {
                "row_count": MetadataValue.int(len(df)),
                "model": MetadataValue.text(model),
                "provider": MetadataValue.text(llm_provider),
                "summarized_rows": MetadataValue.int(sum(1 for s in summaries if s)),
            }
            if include_preview and len(df) > 0:
                _cols = [c for c in [transcript_column, summary_column, action_items_column] if c in df.columns]
                _prev = df[_cols].head(preview_rows).copy()
                if transcript_column in _prev.columns:
                    _prev[transcript_column] = _prev[transcript_column].astype(str).str.slice(0, 200) + "…"
                metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
            return Output(value=df, metadata=metadata)

        return Definitions(assets=[gong_call_summary_asset])
