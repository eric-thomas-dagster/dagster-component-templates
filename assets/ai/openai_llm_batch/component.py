"""OpenaiLlmBatchComponent — one component, submit + sensor + results.

This replaces what used to be three separate components (openai_batch_submit
/ openai_batch_status_sensor / openai_batch_results) that required manually
cross-wiring watch_asset_key / results_asset_key / job_name across three YAML
blocks. A single `build_defs()` can return assets AND sensors together, so
this component derives all of that wiring internally from one `asset_name` --
you configure it once and the sensor "just appears," correctly wired.

Real integration with OpenAI's async Batch API (POST /v1/batches, ~50%
cheaper than synchronous chat completions) -- NOT a synchronous call in a
thread pool like litellm_batch_completion elsewhere in this repo.

Two modes:
  - wait_for_completion=False (default): emits two assets --
    `{asset_name}__submit` (submits/reattaches, returns a small manifest) and
    `{asset_name}` (the real output -- parses results once the batch is
    done). A sensor named `{asset_name}__batch_status_sensor` polls the
    submit asset's live batch status and materializes `{asset_name}`
    directly via `RunRequest(asset_selection=...)` once terminal -- no
    worker sits blocked waiting, and no job_name/companion job is needed.
  - wait_for_completion=True: a single asset named `asset_name` that
    submits, blocks polling to a terminal state, and returns the fully
    parsed results DataFrame directly. No sensor is created in this mode --
    there's nothing to wait on by the time the asset returns.

Idempotency: a deterministic prompts_hash (sha256 over the sorted
(custom_id, prompt) pairs) is stamped into the submit asset's materialization
metadata and read back on every run. A retry/redundant re-run with
unchanged prompts reattaches to the existing batch instead of submitting
(and paying for) a duplicate. A changed hash best-effort cancels the stale
batch before submitting fresh.
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
    can monkeypatch `openai_llm_batch_component._build_openai_client` to
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


def _rows_from_output_file(client: Any, output_file_id: Optional[str]) -> List[Dict[str, Any]]:
    """Successful + per-row-errored requests from the batch's output file."""
    if not output_file_id:
        return []
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
        status_code = resp.get("status_code")
        if err or status_code != 200:
            err_text = json.dumps(err) if err else f"non-200 status_code: {status_code}"
            rows.append({"custom_id": custom_id, "raw_output": None, "error": err_text})
            continue
        try:
            content = resp["body"]["choices"][0]["message"]["content"]
            rows.append({"custom_id": custom_id, "raw_output": content, "error": None})
        except (KeyError, IndexError, TypeError) as e:
            rows.append({"custom_id": custom_id, "raw_output": None, "error": f"malformed response body: {e}"})
    return rows


def _rows_from_error_file(client: Any, error_file_id: Optional[str]) -> List[Dict[str, Any]]:
    """Request-level failures (e.g. malformed input lines) from the batch's
    separate error file -- distinct from per-row API errors already captured
    in the output file."""
    if not error_file_id:
        return []
    text = client.files.content(error_file_id).text
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        custom_id = obj.get("custom_id")
        err = obj.get("error")
        rows.append({"custom_id": custom_id, "raw_output": None, "error": json.dumps(err) if err else "unknown error"})
    return rows


def _resolve_dotted_class(dotted: str) -> Any:
    """`module.path:ClassName` (or `module.path.ClassName` as a fallback) ->
    the class object. Mirrors smart_retry's dotted-path resolution."""
    import importlib

    if ":" in dotted:
        module_path, cls_name = dotted.rsplit(":", 1)
    else:
        module_path, cls_name = dotted.rsplit(".", 1)
    mod = importlib.import_module(module_path.strip())
    cls = getattr(mod, cls_name.strip(), None)
    if cls is None:
        raise ValueError(f"output_schema {dotted!r}: {cls_name!r} not found in {module_path!r}.")
    return cls


def _apply_output_schema(df: pd.DataFrame, output_schema: Optional[str]) -> pd.DataFrame:
    """Shared by both the inline-blocking path and the standalone results
    asset. A row whose raw_output fails JSON parsing or Pydantic validation
    is marked invalid_output=True with the raw text preserved -- never
    fails the whole asset."""
    if not output_schema:
        df = df.copy()
        df["invalid_output"] = False
        return df

    model_cls = _resolve_dotted_class(output_schema)
    invalid_flags: List[bool] = []
    parsed_rows: List[Dict[str, Any]] = []
    for raw, err in zip(df["raw_output"], df["error"]):
        if err is not None or raw is None:
            invalid_flags.append(False)
            parsed_rows.append({})
            continue
        try:
            instance = model_cls.model_validate_json(raw)
            parsed_rows.append(instance.model_dump())
            invalid_flags.append(False)
        except Exception:
            invalid_flags.append(True)
            parsed_rows.append({})
    df = df.copy()
    df["invalid_output"] = invalid_flags
    parsed_df = pd.DataFrame(parsed_rows, index=df.index)
    return pd.concat([df, parsed_df], axis=1)


def _parse_completed_batch(client: Any, batch: Any, output_schema: Optional[str]) -> pd.DataFrame:
    rows = _rows_from_output_file(client, getattr(batch, "output_file_id", None))
    rows += _rows_from_error_file(client, getattr(batch, "error_file_id", None))
    df = pd.DataFrame(rows, columns=["custom_id", "raw_output", "error"])
    return _apply_output_schema(df, output_schema)


class _OpenaiLlmBatchResultsConfig(dg.Config):
    """Per-run override for batch_id, wired via run_config by the
    auto-created status sensor. Falls back to None (manual runs must set
    batch_id some other way, or just re-run the submit asset)."""

    batch_id: Optional[str] = None


class OpenaiLlmBatchComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a DataFrame of prompts to OpenAI's async Batch API, with the
    submit->poll->parse lifecycle bundled into one component.

    Example:
        ```yaml
        type: dagster_component_templates.OpenaiLlmBatchComponent
        attributes:
          asset_name: support_reply_results
          upstream_asset_key: support_tickets
          prompt_column: body
          id_column: ticket_id
          model: gpt-4o-mini
          system_prompt: "Draft a helpful, concise reply to this support ticket."
          wait_for_completion: false
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(
        description=(
            "Name of the results asset -- the one downstream assets should depend on. When "
            "wait_for_completion=False, a companion '{asset_name}__submit' asset and a "
            "'{asset_name}__batch_status_sensor' sensor are also created automatically; you "
            "never need to name or wire those yourself."
        ),
    )
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

    output_schema: Optional[str] = Field(
        default=None,
        description=(
            "Dotted path to a Pydantic model for typed output, e.g. 'my_project.schemas:SupportReply' "
            "(module.path:ClassName). Each row's raw_output is parsed as JSON and validated against "
            "this model; on success the model's fields are merged in as extra columns. On ANY failure "
            "(bad JSON, validation error) the row is marked invalid_output=True with raw_output kept "
            "as-is -- it never fails the asset."
        ),
    )

    wait_for_completion: bool = Field(
        default=False,
        description=(
            "If True, a single asset (named asset_name) submits, polls synchronously to a terminal "
            "state, and returns the fully parsed results DataFrame directly -- blocking, no sensor "
            "created. If False (default), submit/reattach returns immediately with a small manifest "
            "under '{asset_name}__submit', and the auto-created '{asset_name}__batch_status_sensor' "
            "materializes '{asset_name}' once OpenAI finishes the batch -- no worker blocks waiting."
        ),
    )
    poll_interval_seconds: int = Field(default=30, description="Seconds between polls. Used when wait_for_completion=True (blocking) or by the status sensor's own live-status check.")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for a terminal state before raising. Only used when wait_for_completion=True.")
    sensor_minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between the auto-created status sensor's evaluations. Only used when wait_for_completion=False.")
    sensor_default_status: str = Field(default="running", description="'running' or 'stopped' -- initial status of the auto-created sensor. Only used when wait_for_completion=False.")

    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Env var holding the OpenAI API key.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the assets, e.g. {'domain': 'support', 'tier': 'gold'}",
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
            raise ValueError("OpenaiLlmBatchComponent: set prompt_column or prompt_template.")
        if self.prompt_column and self.prompt_template:
            raise ValueError("OpenaiLlmBatchComponent: set prompt_column OR prompt_template, not both.")

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
        output_schema = self.output_schema
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

        def _submit_or_reattach(context: dg.AssetExecutionContext, upstream: Any):
            """Shared by both modes: load upstream, build pairs, hash,
            retry-reattach-or-submit. Returns (client, batch, pairs)."""
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
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
                return None, None, pairs, prompts_hash

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

            return client, batch, pairs, prompts_hash

        if self.wait_for_completion:
            @dg.asset(
                key=dg.AssetKey.from_user_string(asset_name),
                description=self.description or f"OpenAI Batch API run over {self.upstream_asset_key} ({model}) — blocking.",
                group_name=self.group_name,
                kinds=_kinds,
                tags=self.asset_tags or None,
                owners=self.owners or None,
                ins={"upstream": dg.AssetIn(key=upstream_key)},
                retry_policy=retry_policy,
            )
            def _blocking_asset(context: dg.AssetExecutionContext, upstream: Any) -> pd.DataFrame:
                client, batch, pairs, prompts_hash = _submit_or_reattach(context, upstream)
                if not pairs:
                    context.log.info("OpenaiLlmBatchComponent: zero rows, nothing to submit.")
                    context.add_output_metadata({
                        "batch_id": dg.MetadataValue.text(""),
                        "status": dg.MetadataValue.text("skipped_empty"),
                        "request_count": dg.MetadataValue.int(0),
                    })
                    return pd.DataFrame(columns=["custom_id", "raw_output", "error", "invalid_output"])

                batch_id = batch.id
                status = batch.status
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

                result_df = _parse_completed_batch(client, batch, output_schema)
                context.add_output_metadata({
                    "batch_id": dg.MetadataValue.text(batch_id),
                    "prompts_hash": dg.MetadataValue.text(prompts_hash),
                    "status": dg.MetadataValue.text(status),
                    "request_count": dg.MetadataValue.int(len(pairs)),
                    "row_count": dg.MetadataValue.int(len(result_df)),
                })
                return result_df

            return dg.Definitions(assets=[_blocking_asset])

        # Deferred mode: submit asset + results asset + auto-wired sensor.
        submit_key_str = f"{asset_name}__submit"
        submit_key = dg.AssetKey.from_user_string(submit_key_str)
        results_key = dg.AssetKey.from_user_string(asset_name)
        sensor_name = f"{asset_name}__batch_status_sensor"
        results_op_name = results_key.to_python_identifier()

        @dg.asset(
            key=submit_key,
            description=f"OpenAI Batch API submission manifest over {self.upstream_asset_key} ({model}). See {asset_name} for parsed results.",
            group_name=self.group_name,
            kinds=_kinds,
            tags=self.asset_tags or None,
            owners=self.owners or None,
            ins={"upstream": dg.AssetIn(key=upstream_key)},
            retry_policy=retry_policy,
        )
        def _submit_asset(context: dg.AssetExecutionContext, upstream: Any) -> pd.DataFrame:
            client, batch, pairs, prompts_hash = _submit_or_reattach(context, upstream)
            if not pairs:
                context.log.info("OpenaiLlmBatchComponent: zero rows, nothing to submit.")
                context.add_output_metadata({
                    "batch_id": dg.MetadataValue.text(""),
                    "prompts_hash": dg.MetadataValue.text(prompts_hash),
                    "status": dg.MetadataValue.text("skipped_empty"),
                    "request_count": dg.MetadataValue.int(0),
                })
                return pd.DataFrame([{"batch_id": None, "status": "skipped_empty", "request_count": 0}])

            context.add_output_metadata({
                "batch_id": dg.MetadataValue.text(batch.id),
                "prompts_hash": dg.MetadataValue.text(prompts_hash),
                "status": dg.MetadataValue.text(batch.status),
                "request_count": dg.MetadataValue.int(len(pairs)),
            })
            return pd.DataFrame([{"batch_id": batch.id, "status": batch.status, "request_count": len(pairs)}])

        @dg.asset(
            key=results_key,
            description=self.description or f"Parsed results of an OpenAI Batch API run (see {submit_key_str}).",
            group_name=self.group_name,
            kinds=_kinds,
            tags=self.asset_tags or None,
            owners=self.owners or None,
        )
        def _results_asset(context: dg.AssetExecutionContext, config: _OpenaiLlmBatchResultsConfig) -> pd.DataFrame:
            batch_id = config.batch_id
            if not batch_id:
                raise ValueError(
                    "OpenaiLlmBatchComponent results asset: no batch_id in run_config. This asset is "
                    f"meant to be materialized by {sensor_name} once the batch completes, not run "
                    "directly without a batch_id."
                )
            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                raise ValueError(f"{api_key_env_var} not set. Get a key at https://platform.openai.com/api-keys")
            client = _build_openai_client(api_key)

            batch = client.batches.retrieve(batch_id)
            if batch.status != "completed":
                raise Exception(
                    f"Batch {batch_id} is not completed (status={batch.status!r}). This asset should "
                    f"only run once {sensor_name} confirms completion."
                )

            df = _parse_completed_batch(client, batch, output_schema)
            row_count = len(df)
            errored_count = int(df["error"].notna().sum())
            succeeded_count = row_count - errored_count
            invalid_output_count = int(df["invalid_output"].sum())
            context.add_output_metadata({
                "row_count": dg.MetadataValue.int(row_count),
                "succeeded_count": dg.MetadataValue.int(succeeded_count),
                "errored_count": dg.MetadataValue.int(errored_count),
                "invalid_output_count": dg.MetadataValue.int(invalid_output_count),
            })
            return df

        default_status = (
            dg.DefaultSensorStatus.RUNNING if self.sensor_default_status == "running"
            else dg.DefaultSensorStatus.STOPPED
        )

        @dg.sensor(
            name=sensor_name,
            asset_selection=[results_key],
            minimum_interval_seconds=self.sensor_minimum_interval_seconds,
            default_status=default_status,
        )
        def _status_sensor(context: dg.SensorEvaluationContext):
            event = context.instance.get_latest_materialization_event(submit_key)
            if not event or not event.asset_materialization:
                return dg.SensorResult(skip_reason=f"No materialization found for {submit_key_str}.")

            md = event.asset_materialization.metadata
            batch_id_mv = md.get("batch_id")
            if not batch_id_mv or not batch_id_mv.text:
                return dg.SensorResult(skip_reason=f"Latest materialization of {submit_key_str} has no batch_id.")
            batch_id = batch_id_mv.text

            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                return dg.SensorResult(skip_reason=f"{api_key_env_var} not set.")
            client = _build_openai_client(api_key)
            try:
                batch = client.batches.retrieve(batch_id)
            except Exception as e:
                return dg.SensorResult(skip_reason=f"batches.retrieve({batch_id!r}) failed: {e}")

            status = batch.status
            if status not in _TERMINAL_STATUSES:
                return dg.SensorResult(skip_reason=f"Batch {batch_id} status={status!r} (not terminal yet).")

            fingerprint = f"{batch_id}|{status}"
            if fingerprint == (context.cursor or ""):
                return dg.SensorResult(skip_reason=f"Already processed {fingerprint}.")

            return dg.SensorResult(
                run_requests=[
                    dg.RunRequest(
                        run_key=fingerprint,
                        asset_selection=[results_key],
                        run_config={"ops": {results_op_name: {"config": {"batch_id": batch_id}}}},
                    )
                ],
                cursor=fingerprint,
            )

        return dg.Definitions(assets=[_submit_asset, _results_asset], sensors=[_status_sensor])
