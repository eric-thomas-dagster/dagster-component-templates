"""Anthropic Message Batches — submit component.

Submits rows of a DataFrame as an async Anthropic Message Batch (the
~50%-cheaper, non-latency-sensitive endpoint — NOT the regular synchronous
messages.create endpoint). Content-addressed idempotency: retrying the same
materialization with unchanged prompts reattaches to the existing live batch
instead of resubmitting; changed prompts cancel the stale batch (best-effort)
and submit fresh.

Two modes:
  - wait_for_completion=False (default): submit/reattach, write a small
    manifest (batch_id, processing_status, request_count), return
    immediately. The real results are fetched later by the sibling
    anthropic_batch_results asset once anthropic_batch_status_sensor
    confirms processing_status == "ended".
  - wait_for_completion=True: poll to "ended" in this same run, then parse
    and return the full results DataFrame inline.
"""
import hashlib
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Union

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
    Resolvable,
    asset,
)
from pydantic import ConfigDict, Field

# Anthropic requires custom_id to match this pattern on every batch request.
_CUSTOM_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


def _build_custom_id(raw_id: Any) -> str:
    cid = str(raw_id)
    if not _CUSTOM_ID_PATTERN.match(cid):
        raise ValueError(
            f"anthropic_batch_submit: custom_id {cid!r} (derived from id_column) does not "
            "match Anthropic's required pattern ^[a-zA-Z0-9_-]{1,64}$ — fix the id_column "
            "values (e.g. strip special characters) or unset id_column to use the row's "
            "positional index as the custom_id instead."
        )
    return cid


def _build_batch_requests(
    pairs: List[tuple],
    model: str,
    max_tokens: int,
    temperature: float,
    system_prompt: Optional[str],
) -> List[Dict[str, Any]]:
    """Plain dicts matching the documented Request/MessageCreateParamsNonStreaming
    shape — safer across anthropic SDK versions than the typed constructors."""
    requests = []
    for cid, text in pairs:
        params: Dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": text}],
        }
        if system_prompt:
            params["system"] = system_prompt
        requests.append({"custom_id": cid, "params": params})
    return requests


def _parse_anthropic_batch_results(client: Any, batch_id: str) -> pd.DataFrame:
    """Iterate client.messages.batches.results(batch_id) (SDK helper — handles
    the streaming JSONL parsing) into a flat DataFrame."""
    rows = []
    for entry in client.messages.batches.results(batch_id):
        custom_id = entry.custom_id
        result_type = entry.result.type
        raw_output = None
        error = None
        if result_type == "succeeded":
            blocks = getattr(entry.result.message, "content", []) or []
            texts = [b.text for b in blocks if getattr(b, "type", None) == "text"]
            raw_output = "\n".join(texts)
        elif result_type == "errored":
            err = getattr(entry.result, "error", None)
            err_obj = getattr(err, "error", err)
            err_type = getattr(err_obj, "type", None)
            err_message = getattr(err_obj, "message", str(err_obj))
            error = f"{err_type}: {err_message}" if err_type else str(err_message)
        else:  # canceled / expired
            error = f"request {result_type}"
        rows.append(
            {
                "custom_id": custom_id,
                "raw_output": raw_output,
                "error": error,
                "result_type": result_type,
            }
        )
    return pd.DataFrame(rows, columns=["custom_id", "raw_output", "error", "result_type"])


class AnthropicBatchSubmitComponent(Component, Model, Resolvable):
    """Submit a DataFrame's prompts as an Anthropic Message Batch.

    Example:
        ```yaml
        type: dagster_component_templates.AnthropicBatchSubmitComponent
        attributes:
          asset_name: support_ticket_batch
          upstream_asset_key: raw_support_tickets
          prompt_column: body
          id_column: ticket_id
          model: claude-haiku-4-5-20251001
          system_prompt: "Classify the sentiment as positive, neutral, or negative."
          max_tokens: 50
          wait_for_completion: false
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame of prompts")
    prompt_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column containing the raw text to send as the user message. Set this OR prompt_template, not both.",
    )
    prompt_template: Optional[str] = Field(
        default=None,
        description='Format-string template using row values, e.g. "Summarize: {body}". Set this OR prompt_column, not both.',
    )
    id_column: Optional[Union[str, int]] = Field(
        default=None,
        description=(
            "Column to use as each row's custom_id (must match Anthropic's "
            "^[a-zA-Z0-9_-]{1,64}$ pattern — a non-matching value raises a clear error "
            "rather than being silently mangled). If unset, the row's positional index "
            "(stringified) is used."
        ),
    )
    model_id: str = Field(
        alias="model",
        default="claude-haiku-4-5-20251001",
        description="Anthropic model id for each batch request",
    )
    system_prompt: Optional[str] = Field(default=None, description="System prompt applied to every request in the batch")
    max_tokens: int = Field(default=1000, description="Maximum tokens per completion")
    temperature: float = Field(default=0.0, description="Sampling temperature")
    wait_for_completion: bool = Field(
        default=False,
        description=(
            "If True, poll in-process until the batch ends and return the full parsed "
            "results DataFrame. If False (default), submit/reattach and return only a "
            "small manifest (batch_id, processing_status, request_count) — the real "
            "results are fetched later by anthropic_batch_results once the status "
            "sensor confirms completion."
        ),
    )
    poll_interval_seconds: int = Field(default=30, description="Seconds between polls. Only used when wait_for_completion=True.")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for the batch to end before raising. Only used when wait_for_completion=True.")
    api_key_env_var: str = Field(default="ANTHROPIC_API_KEY", description="Env var holding the Anthropic API key")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['anthropic', 'python']. Auto-inferred from component name if not set.",
    )
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        prompt_column = self.prompt_column
        prompt_template = self.prompt_template
        id_column = self.id_column
        model = self.model_id
        system_prompt = self.system_prompt
        max_tokens = self.max_tokens
        temperature = self.temperature
        wait_for_completion = self.wait_for_completion
        poll_interval_seconds = self.poll_interval_seconds
        timeout_seconds = self.timeout_seconds
        api_key_env_var = self.api_key_env_var
        group_name = self.group_name

        if prompt_column is not None and prompt_template:
            raise ValueError("anthropic_batch_submit: set prompt_column or prompt_template, not both.")
        if prompt_column is None and not prompt_template:
            raise ValueError("anthropic_batch_submit: set prompt_column or prompt_template.")

        # Infer kinds from component name if not explicitly set (boilerplate
        # shared with assets/ai/litellm_batch_completion/component.py).
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
                _inferred_kinds = ["anthropic"]

        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        owners = self.owners or []

        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy

            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @asset(
            retry_policy=_retry_policy,
            key=AssetKey.from_user_string(asset_name),
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            owners=owners,
            tags=_all_tags,
            group_name=group_name,
            description=self.description,
        )
        def _asset(context: AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            # Defensive Output/MaterializeResult unwrap.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: when an unpartitioned asset
            # consumes a partitioned upstream, Dagster's IO manager loads
            # ALL partitions as a dict; concat to a single DataFrame first.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            try:
                import anthropic
            except ImportError:
                raise ImportError("pip install anthropic")

            df = upstream.reset_index(drop=True)

            # Build (custom_id, prompt_text) pairs.
            pairs = []
            for idx, row in df.iterrows():
                row_dict = row.to_dict()
                if prompt_template:
                    text = prompt_template.format(**row_dict)
                else:
                    text = str(row_dict.get(prompt_column, ""))
                raw_id = row_dict.get(id_column, idx) if id_column is not None else idx
                cid = _build_custom_id(raw_id)
                pairs.append((cid, text))

            prompts_hash = hashlib.sha256(
                json.dumps([[cid, text] for cid, text in pairs], sort_keys=True).encode()
            ).hexdigest()

            client = anthropic.Anthropic(api_key=os.environ[api_key_env_var])

            # Retry-reattach check: read back this asset's OWN prior
            # materialization metadata (Anthropic's batches.create has no
            # metadata param at all, unlike OpenAI's — so the idempotency
            # key lives entirely on the Dagster side).
            prior_batch_id = None
            prior_hash = None
            try:
                event = context.instance.get_latest_materialization_event(context.asset_key)
                if event is not None and event.asset_materialization is not None:
                    md = event.asset_materialization.metadata or {}
                    if "batch_id" in md:
                        prior_batch_id = md["batch_id"].text
                    if "prompts_hash" in md:
                        prior_hash = md["prompts_hash"].text
            except Exception as e:
                context.log.warning(f"Could not read prior materialization metadata: {e}")

            batch = None
            if prior_batch_id and prior_hash == prompts_hash:
                context.log.info(
                    f"Prompts unchanged since last materialization (hash={prompts_hash[:12]}); "
                    f"reattaching to existing batch {prior_batch_id} instead of resubmitting."
                )
                try:
                    batch = client.messages.batches.retrieve(prior_batch_id)
                except Exception as e:
                    context.log.warning(
                        f"Failed to retrieve prior batch {prior_batch_id} ({e}); submitting fresh instead."
                    )
                    batch = None
            elif prior_batch_id and prior_hash is not None and prior_hash != prompts_hash:
                context.log.info(
                    f"Prompts changed since last materialization (prior batch {prior_batch_id}); "
                    "canceling the stale batch (best-effort) and submitting fresh."
                )
                try:
                    client.messages.batches.cancel(prior_batch_id)
                except Exception as e:
                    context.log.warning(f"Failed to cancel stale batch {prior_batch_id}: {e}")

            if batch is None:
                context.log.info(f"Submitting new batch of {len(pairs)} requests to model={model}")
                batch = client.messages.batches.create(
                    requests=_build_batch_requests(pairs, model, max_tokens, temperature, system_prompt)
                )

            batch_id = batch.id
            processing_status = batch.processing_status

            if wait_for_completion:
                deadline = time.time() + timeout_seconds
                last_status = None
                last_counts = None
                while processing_status != "ended":
                    if time.time() > deadline:
                        raise Exception(
                            f"anthropic_batch_submit: batch {batch_id} did not reach 'ended' "
                            f"within {timeout_seconds}s (last status={processing_status})"
                        )
                    time.sleep(poll_interval_seconds)
                    batch = client.messages.batches.retrieve(batch_id)
                    processing_status = batch.processing_status
                    counts = batch.request_counts
                    counts_snapshot = (
                        counts.processing, counts.succeeded, counts.errored,
                        counts.canceled, counts.expired,
                    )
                    if processing_status != last_status or counts_snapshot != last_counts:
                        context.log.info(
                            f"batch {batch_id} status={processing_status} counts={counts_snapshot}"
                        )
                        last_status = processing_status
                        last_counts = counts_snapshot

                result_df = _parse_anthropic_batch_results(client, batch_id)
                context.add_output_metadata(
                    {
                        "batch_id": MetadataValue.text(batch_id),
                        "prompts_hash": MetadataValue.text(prompts_hash),
                        "processing_status": MetadataValue.text(processing_status),
                        "request_count": MetadataValue.int(len(pairs)),
                    }
                )
                return result_df

            # Deferred mode: small manifest only, no polling loop.
            manifest_df = pd.DataFrame(
                [{"batch_id": batch_id, "processing_status": processing_status, "request_count": len(pairs)}]
            )
            context.add_output_metadata(
                {
                    "batch_id": MetadataValue.text(batch_id),
                    "prompts_hash": MetadataValue.text(prompts_hash),
                    "processing_status": MetadataValue.text(processing_status),
                    "request_count": MetadataValue.int(len(pairs)),
                }
            )
            return manifest_df

        return Definitions(assets=[_asset])
