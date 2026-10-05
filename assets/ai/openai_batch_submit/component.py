"""OpenaiBatchSubmitComponent — submit a DataFrame of prompts to OpenAI's Batch API.

This is a real integration with OpenAI's async Batch API (POST /v1/batches,
~50% cheaper than the synchronous chat completions endpoint, turnaround up
to `completion_window`) -- NOT a synchronous call in a thread pool like
litellm_batch_completion / litellm_embedding_batch elsewhere in this repo.

Design: submit-then-poll-or-defer, with content-addressed idempotency.

- A deterministic `prompts_hash` (sha256 over the sorted (custom_id, prompt)
  pairs) is stamped onto the batch at submission time and read back from
  the asset's own prior materialization metadata on every run. If the hash
  matches a previously-submitted batch, this run REATTACHES to that batch
  (calls `batches.retrieve`, never resubmits) instead of creating a
  duplicate paid batch -- this makes retries and redundant manual
  re-materializations idempotent, which a bare run_id key would not give
  you (a run_id changes every time; content does not).
- If the hash differs from the prior one, the prior batch is stale (the
  upstream prompts changed): it's best-effort cancelled and a fresh batch
  is submitted.
- `wait_for_completion=False` (default) returns immediately with a small
  manifest (batch_id/status/request_count) -- no worker sits blocked
  polling. The paired `openai_batch_status_sensor` + `openai_batch_results`
  components pick up the result once OpenAI finishes the batch, which can
  take up to the full `completion_window`.
- `wait_for_completion=True` instead blocks, polls to a terminal state, and
  inlines the parsed output DataFrame directly -- useful for small batches,
  tests, or backfills where you really do want "materialize and get
  everything back right now."
"""
import hashlib
import io
import json
import os
import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import dagster as dg
from pydantic import ConfigDict, Field

_TERMINAL_STATUSES = {"completed", "failed", "expired", "cancelled"}


def _build_openai_client(api_key: str) -> Any:
    """The one place an OpenAI client gets constructed -- isolated so tests
    can monkeypatch `openai_batch_submit_component._build_openai_client` to
    return a fake client instead of ever touching the real (paid) API."""
    from openai import OpenAI

    return OpenAI(api_key=api_key)


def _resolve_column(col: Optional[Union[str, int]], columns: List[str]) -> Optional[str]:
    """Union[str, int] column refs: an int is a positional index into the
    upstream DataFrame's columns; a str is used as-is."""
    if col is None:
        return None
    if isinstance(col, int):
        return columns[col]
    return col


def _build_prompt_pairs(
    df: pd.DataFrame,
    prompt_column: Optional[str],
    prompt_template: Optional[str],
    id_column: Optional[str],
) -> List[List[str]]:
    """Returns a list of [custom_id, prompt_text] pairs (list, not tuple, so
    it round-trips through json.dumps identically for hashing)."""
    pairs: List[List[str]] = []
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        if prompt_template:
            text = prompt_template.format(**row_dict)
        else:
            text = str(row_dict.get(prompt_column, ""))
        if id_column:
            custom_id = str(row_dict.get(id_column, idx))
        else:
            custom_id = str(idx)
        pairs.append([custom_id, text])
    return pairs


def _compute_prompts_hash(pairs: List[List[str]]) -> str:
    payload = json.dumps(pairs, sort_keys=True).encode()
    return hashlib.sha256(payload).hexdigest()


def _build_jsonl_bytes(
    pairs: List[List[str]],
    model: str,
    system_prompt: Optional[str],
    max_tokens: int,
    temperature: float,
) -> bytes:
    lines = []
    for custom_id, prompt_text in pairs:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt_text})
        body = {
            "custom_id": custom_id,
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
        }
        lines.append(json.dumps(body))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _parse_openai_batch_output(client: Any, output_file_id: str) -> pd.DataFrame:
    """Download + parse a completed batch's output file into a DataFrame
    with columns: custom_id, raw_output, error."""
    text = client.files.content(output_file_id).text
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        custom_id = obj.get("custom_id")
        resp = obj.get("response") or {}
        err = obj.get("error")
        if err:
            rows.append({"custom_id": custom_id, "raw_output": None, "error": json.dumps(err)})
            continue
        try:
            content = resp["body"]["choices"][0]["message"]["content"]
            rows.append({"custom_id": custom_id, "raw_output": content, "error": None})
        except (KeyError, IndexError, TypeError) as e:
            rows.append({"custom_id": custom_id, "raw_output": None, "error": f"malformed response body: {e}"})
    return pd.DataFrame(rows, columns=["custom_id", "raw_output", "error"])


class OpenaiBatchSubmitComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a DataFrame of prompts to OpenAI's async Batch API.

    Example:
        ```yaml
        type: dagster_component_templates.OpenaiBatchSubmitComponent
        attributes:
          asset_name: support_reply_batch
          upstream_asset_key: support_tickets
          prompt_column: body
          id_column: ticket_id
          model: gpt-4o-mini
          system_prompt: "Draft a helpful, concise reply to this support ticket."
          wait_for_completion: false
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame of prompts.")

    prompt_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column holding the raw text to send as the user message. Mutually exclusive with prompt_template.",
    )
    prompt_template: Optional[str] = Field(
        default=None,
        description='Format-string template using row values, e.g. "Summarize: {body}". Rendered per row via str.format(**row_dict). Mutually exclusive with prompt_column.',
    )
    id_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column to use as each row's custom_id in the batch request. If unset, the row's positional index (as a string) is used.",
    )

    model_id: str = Field(
        alias="model",
        default="gpt-4o-mini",
        description="OpenAI model id used in each batch request's body.",
    )
    system_prompt: Optional[str] = Field(default=None, description="System message prepended to each request.")
    max_tokens: int = Field(default=1000, description="Maximum tokens per completion.")
    temperature: float = Field(default=0.0, description="Sampling temperature.")
    completion_window: str = Field(
        default="24h",
        description="OpenAI batch completion window. Currently '24h' is the only value OpenAI accepts, but this stays a field for forward-compat.",
    )

    wait_for_completion: bool = Field(
        default=False,
        description=(
            "If True, poll synchronously until the batch reaches a terminal state and return the "
            "fully parsed output DataFrame directly (blocking, 'give me everything now' mode). If "
            "False (default), return immediately after submit/reattach with a small manifest "
            "(batch_id/status/request_count) -- no worker blocks waiting; the paired "
            "openai_batch_status_sensor + openai_batch_results components fetch results later."
        ),
    )
    poll_interval_seconds: int = Field(default=30, description="Seconds between polls. Only used when wait_for_completion=True.")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for a terminal state before raising. Only used when wait_for_completion=True.")

    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Env var holding the OpenAI API key.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'support', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['openai', 'llm']. Defaults to ['openai', 'llm'] if not set.",
    )
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",
    )
    retry_policy_delay_seconds: Optional[int] = Field(default=None, description="Seconds between retries (default 1).")
    retry_policy_backoff: str = Field(default="exponential", description="Backoff strategy: 'linear' or 'exponential'.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.prompt_column and not self.prompt_template:
            raise ValueError("OpenaiBatchSubmitComponent: set prompt_column or prompt_template.")
        if self.prompt_column and self.prompt_template:
            raise ValueError("OpenaiBatchSubmitComponent: set prompt_column OR prompt_template, not both.")

        asset_name = self.asset_name
        upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key)
        prompt_column = self.prompt_column
        prompt_template = self.prompt_template
        id_column = self.id_column
        model = self.model_id
        system_prompt = self.system_prompt
        max_tokens = self.max_tokens
        temperature = self.temperature
        completion_window = self.completion_window
        wait_for_completion = self.wait_for_completion
        poll_interval_seconds = self.poll_interval_seconds
        timeout_seconds = self.timeout_seconds
        api_key_env_var = self.api_key_env_var

        _kinds = set(self.kinds or ["openai", "llm"])

        retry_policy = None
        if self.retry_policy_max_retries is not None:
            retry_policy = dg.RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=dg.Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or f"OpenAI Batch API submission over {self.upstream_asset_key} ({model}).",
            group_name=self.group_name,
            kinds=_kinds,
            tags=self.asset_tags or None,
            owners=self.owners or None,
            ins={"upstream": dg.AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
        )
        def _asset(context: dg.AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            # Defensive Output/MaterializeResult unwrap, same rationale as
            # every other DataFrame-consuming component in this repo.
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            # partition bridge dict-concat: an unpartitioned asset consuming
            # a partitioned upstream gets a dict of {partition: DataFrame}
            # from the IO manager; concat before any DataFrame ops.
            if isinstance(upstream, dict):
                _frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(_frames, ignore_index=True) if _frames else pd.DataFrame()

            df = upstream.copy().reset_index(drop=True)
            columns = list(df.columns)
            prompt_col_name = _resolve_column(prompt_column, columns)
            id_col_name = _resolve_column(id_column, columns)

            pairs = _build_prompt_pairs(df, prompt_col_name, prompt_template, id_col_name)
            prompts_hash = _compute_prompts_hash(pairs)

            if not pairs:
                context.log.info("OpenaiBatchSubmitComponent: zero rows, nothing to submit.")
                manifest = pd.DataFrame([{"batch_id": None, "status": "skipped_empty", "request_count": 0}])
                context.add_output_metadata({
                    "batch_id": dg.MetadataValue.text(""),
                    "prompts_hash": dg.MetadataValue.text(prompts_hash),
                    "status": dg.MetadataValue.text("skipped_empty"),
                    "request_count": dg.MetadataValue.int(0),
                })
                return manifest

            # Retry-reattach check: read this asset's own prior materialization
            # metadata to decide whether to reuse an in-flight/completed batch
            # instead of submitting a duplicate.
            prior_batch_id = None
            prior_hash = None
            event = context.instance.get_latest_materialization_event(context.asset_key)
            if event and event.asset_materialization and event.asset_materialization.metadata:
                md = event.asset_materialization.metadata
                prior_batch_id = md.get("batch_id").text if md.get("batch_id") else None
                prior_hash = md.get("prompts_hash").text if md.get("prompts_hash") else None

            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                raise ValueError(f"{api_key_env_var} not set. Get a key at https://platform.openai.com/api-keys")
            client = _build_openai_client(api_key)

            batch = None
            if prior_batch_id and prior_hash == prompts_hash:
                context.log.info(f"Reattaching to existing batch {prior_batch_id} (prompts_hash unchanged).")
                batch = client.batches.retrieve(prior_batch_id)
            else:
                if prior_batch_id and prior_hash != prompts_hash:
                    context.log.info(
                        f"Prompts changed since last run (prior batch {prior_batch_id}); "
                        f"cancelling stale batch and submitting fresh."
                    )
                    try:
                        client.batches.cancel(prior_batch_id)
                    except Exception as e:
                        context.log.warning(f"Best-effort cancel of stale batch {prior_batch_id} failed (likely already terminal): {e}")

                jsonl_bytes = _build_jsonl_bytes(pairs, model, system_prompt, max_tokens, temperature)
                file_obj = client.files.create(file=io.BytesIO(jsonl_bytes), purpose="batch")
                batch = client.batches.create(
                    input_file_id=file_obj.id,
                    endpoint="/v1/chat/completions",
                    completion_window=completion_window,
                    metadata={"prompts_hash": prompts_hash},
                )
                context.log.info(f"Submitted new batch {batch.id} ({len(pairs)} requests).")

            batch_id = batch.id
            status = batch.status

            if wait_for_completion:
                deadline = time.time() + timeout_seconds
                last_status = status
                while status not in _TERMINAL_STATUSES:
                    if time.time() >= deadline:
                        raise Exception(
                            f"Batch {batch_id} did not reach a terminal state within {timeout_seconds}s "
                            f"(last status={status})."
                        )
                    time.sleep(poll_interval_seconds)
                    batch = client.batches.retrieve(batch_id)
                    status = batch.status
                    if status != last_status:
                        context.log.info(f"batch {batch_id} status: {last_status} -> {status}")
                        last_status = status

                if status != "completed":
                    raise Exception(f"Batch {batch_id} ended in terminal state {status!r} (not 'completed').")

                result_df = _parse_openai_batch_output(client, batch.output_file_id)
                context.add_output_metadata({
                    "batch_id": dg.MetadataValue.text(batch_id),
                    "prompts_hash": dg.MetadataValue.text(prompts_hash),
                    "status": dg.MetadataValue.text(status),
                    "request_count": dg.MetadataValue.int(len(pairs)),
                })
                return result_df

            manifest = pd.DataFrame([{"batch_id": batch_id, "status": status, "request_count": len(pairs)}])
            context.add_output_metadata({
                "batch_id": dg.MetadataValue.text(batch_id),
                "prompts_hash": dg.MetadataValue.text(prompts_hash),
                "status": dg.MetadataValue.text(status),
                "request_count": dg.MetadataValue.int(len(pairs)),
            })
            return manifest

        return dg.Definitions(assets=[_asset])
