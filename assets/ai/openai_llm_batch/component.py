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

Crash recovery: if a prior run's process died between batches.create()
succeeding (a paid call) and this asset's materialization recording that
batch's id, the next run scans OpenAI's own recent-batches list for one
already tagged with the current prompts_hash before assuming none exists --
see `_find_orphaned_batch`. This is the same recovery path Apache Airflow's
`LLMBatchOperator` documents for OpenAI specifically (its PR notes Anthropic
offers no equivalent lookup; same is true here -- see
AnthropicLlmBatchComponent's `on_orphaned_intent` field for how that gap is
handled there instead).
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


def _ingest_warehouse_query(source_config: dict, context) -> "pd.DataFrame":
    """Execute SQL via a Dagster resource that exposes .get_engine() (SQLAlchemy)
    OR .get_connection() (DB-API), or a bare SQLAlchemy engine built from
    `database_url_env_var` when no Dagster resource is registered. Same
    helper, same contract, as every other dual-ingestion component in this
    repo (e.g. automl_asset, logistic_regression_model, churn_prediction)."""
    sql = source_config["sql"]
    resource_key = source_config.get("resource_key")
    if resource_key:
        resource = getattr(context.resources, resource_key)
        if hasattr(resource, "get_engine"):
            return pd.read_sql(sql, resource.get_engine())
        if hasattr(resource, "get_connection"):
            with resource.get_connection() as conn:
                return pd.read_sql(sql, conn)
        if hasattr(resource, "get_client"):
            # "get_client" means something different per vendor -- there's no
            # universal calling convention, so dispatch on the CLIENT's own
            # shape rather than assume one. Verified against the real APIs,
            # not guessed:
            client = resource.get_client()
            if hasattr(client, "query"):
                # BigQuery (google.cloud.bigquery.Client): .query(sql) returns
                # a QueryJob; .to_dataframe() blocks until done and returns a
                # pandas DataFrame directly -- no .result() call needed first.
                job = client.query(sql)
                if hasattr(job, "to_dataframe"):
                    return job.to_dataframe()
            if hasattr(client, "execute_query"):
                # Redshift Data API (dagster_aws RedshiftClient):
                # execute_query(sql, fetch_results=True) returns bare
                # List[Tuple] with NO column names attached -- a
                # RealDictCursor factory is required to get dict rows a
                # DataFrame can use with correct column names.
                try:
                    from psycopg2.extras import RealDictCursor
                    rows = client.execute_query(sql, fetch_results=True, cursor_factory=RealDictCursor)
                except ImportError:
                    rows = client.execute_query(sql, fetch_results=True)
                return pd.DataFrame([dict(r) for r in (rows or [])])
            raise ValueError(
                f"resource {resource_key!r}'s get_client() returned {type(client).__name__}, "
                "which this helper doesn't know how to query (no .query()/.to_dataframe() "
                "or .execute_query() method found). Add a dispatch branch for it."
            )
        raise ValueError(
            f"resource {resource_key!r} must expose .get_engine() (SQLAlchemy), "
            f".get_connection() (DBAPI), or .get_client() (vendor client); got {type(resource).__name__}"
        )
    env_var = source_config.get("database_url_env_var")
    if env_var:
        from sqlalchemy import create_engine
        url = os.environ.get(env_var, "")
        if not url:
            raise ValueError(f"database_url_env_var {env_var!r} is unset")
        return pd.read_sql(sql, create_engine(url))
    raise ValueError("source requires 'resource_key' OR 'database_url_env_var'")


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


def _find_orphaned_batch(client: Any, prompts_hash: str, lookback: int = 20) -> Optional[Any]:
    """Scan the N most recent batches for one whose own OpenAI-side metadata
    already carries this exact prompts_hash -- recovers a batch that was
    submitted (and paid for) on a prior run whose process then crashed before
    this asset's materialization recorded its batch_id. Best-effort: any
    failure here just means we fall through to a normal fresh submit."""
    try:
        resp = client.batches.list(limit=lookback)
        for b in resp.data:
            if (getattr(b, "metadata", None) or {}).get("prompts_hash") == prompts_hash:
                return b
    except Exception:
        return None
    return None


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



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization



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


class OpenaiLlmBatchComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a DataFrame of prompts to OpenAI's async Batch API, with the
    submit->poll->parse lifecycle bundled into one component.

    Example:
        ```yaml
        type: dagster_community_components.OpenaiLlmBatchComponent
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
    upstream_asset_key: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Upstream asset key providing a DataFrame of prompts. Mutually exclusive with `source` -- set exactly one.",
    )
    source: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Pull rows directly via SQL instead of from an upstream asset: "
            "{kind: warehouse_query, resource_key: <registered resource> OR database_url_env_var: <env var>, sql: <query>}. "
            "Mutually exclusive with `upstream_asset_key` -- set exactly one."
        ),
    )

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

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', 'dynamic', or None for unpartitioned. A partitioned run submits one batch of only the rows in its partition.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b'.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides the flat partition fields.",
    )
    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to submit to the current date partition (time partitions).",
    )
    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the rows to submit to the current static/dynamic partition key.",
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds'), csv=(), tags=('asset_tags',))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.prompt_column and not self.prompt_template:
            raise ValueError("OpenaiLlmBatchComponent: set prompt_column or prompt_template.")
        if self.prompt_column and self.prompt_template:
            raise ValueError("OpenaiLlmBatchComponent: set prompt_column OR prompt_template, not both.")
        if bool(self.upstream_asset_key) == bool(self.source):
            raise ValueError("OpenaiLlmBatchComponent: set exactly one of `upstream_asset_key` or `source`.")

        asset_name = self.asset_name
        _validate_batch_partitioning(self.partition_type, self.partition_date_column, self.partition_static_column)
        partitions_def = _build_partitions_def(self.partition_type, self.partition_start, self.partition_values, self.dynamic_partition_name, self.partition_dimensions,
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key) if self.upstream_asset_key else None
        source_cfg = self.source
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

            _partition_key = _batch_partition_key(context)
            df = _filter_df_to_partition(upstream, context, partition_date_column, partition_static_column).copy().reset_index(drop=True)
            columns = list(df.columns)
            prompt_col_name = _resolve_column(prompt_column, columns)
            id_col_name = _resolve_column(id_column, columns)

            pairs = _build_prompt_pairs(df, prompt_col_name, prompt_template, id_col_name)
            prompts_hash = _compute_prompts_hash(pairs)

            if not pairs:
                return None, None, pairs, prompts_hash

            prior_batch_id = None
            prior_hash = None
            md = _latest_materialization_metadata(context.instance, context.asset_key, _partition_key)
            if md:
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

                # Crash-recovery: a process crash between batches.create()
                # succeeding (a paid call) and this asset's compute function
                # returning (which is when Dagster actually records
                # add_output_metadata's batch_id) would otherwise be invisible
                # to prior_batch_id above -- the next run would submit a
                # second, duplicate, paid batch. OpenAI's own batch metadata
                # is the recovery path: scan recent batches for one already
                # tagged with this exact prompts_hash before creating a new
                # one. (Anthropic's batch API has no metadata param on create
                # and no equivalent lookup -- see AnthropicLlmBatchComponent's
                # on_orphaned_intent field for how that's handled instead.)
                orphaned = _find_orphaned_batch(client, prompts_hash)
                if orphaned is not None:
                    context.log.info(
                        f"Found existing batch {orphaned.id} already tagged with this "
                        f"prompts_hash but never recorded in this asset's own metadata "
                        f"-- likely a crash between a prior run's submit and its "
                        f"materialization recording. Reattaching instead of submitting "
                        f"a duplicate."
                    )
                    batch = orphaned
                else:
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
            _blocking_kwargs: Dict[str, Any] = dict(
                key=dg.AssetKey.from_user_string(asset_name),
                description=self.description or f"OpenAI Batch API run ({model}) — blocking.",
                group_name=self.group_name,
                kinds=_kinds,
                tags=self.asset_tags or None,
                owners=self.owners or None,
                retry_policy=retry_policy,
                partitions_def=partitions_def,
            )
            if upstream_key:
                _blocking_kwargs["ins"] = {"upstream": dg.AssetIn(key=upstream_key)}
            if source_cfg and source_cfg.get("resource_key"):
                _blocking_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

            @dg.asset(**_blocking_kwargs)
            def _blocking_asset(context: dg.AssetExecutionContext, **kwargs) -> pd.DataFrame:
                upstream = kwargs.get("upstream")
                if upstream is None:
                    upstream = _ingest_warehouse_query(source_cfg, context)
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

        _submit_kwargs: Dict[str, Any] = dict(
            key=submit_key,
            description=f"OpenAI Batch API submission manifest ({model}). See {asset_name} for parsed results.",
            group_name=self.group_name,
            kinds=_kinds,
            tags=self.asset_tags or None,
            owners=self.owners or None,
            retry_policy=retry_policy,
            partitions_def=partitions_def,
        )
        if upstream_key:
            _submit_kwargs["ins"] = {"upstream": dg.AssetIn(key=upstream_key)}
        if source_cfg and source_cfg.get("resource_key"):
            _submit_kwargs["required_resource_keys"] = {source_cfg["resource_key"]}

        @dg.asset(**_submit_kwargs)
        def _submit_asset(context: dg.AssetExecutionContext, **kwargs) -> pd.DataFrame:
            upstream = kwargs.get("upstream")
            if upstream is None:
                upstream = _ingest_warehouse_query(source_cfg, context)
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

        @dg.asset(retry_policy=retry_policy, partitions_def=partitions_def,
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

        def _partitioned_status(context: dg.SensorEvaluationContext):
            """One results run per partition whose submitted batch has finished. The cursor remembers, per partition, the
            batch it already handled, so finished partitions cost no API call on later ticks."""
            import json

            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                return dg.SensorResult(skip_reason=f"{api_key_env_var} not set.")
            client = _build_openai_client(api_key)
            cursor = json.loads(context.cursor) if context.cursor else {}
            run_requests, notes = [], []
            for pk in sorted(context.instance.get_materialized_partitions(submit_key)):
                md = _latest_materialization_metadata(context.instance, submit_key, pk)
                batch_id_mv = md.get("batch_id")
                if not batch_id_mv or not batch_id_mv.text:
                    continue
                batch_id = batch_id_mv.text
                if str(cursor.get(pk, "")).startswith(batch_id + "|"):
                    continue
                try:
                    batch = client.batches.retrieve(batch_id)
                except Exception as e:
                    notes.append(f"{pk}: batches.retrieve({batch_id!r}) failed: {e}")
                    continue
                status = batch.status
                if not (status in _TERMINAL_STATUSES):
                    notes.append(f"{pk}: batch {batch_id} status={status!r} (not finished yet)")
                    continue
                run_requests.append(dg.RunRequest(
                    run_key=f"{pk}|{batch_id}|{status}",
                    asset_selection=[results_key],
                    partition_key=pk,
                    run_config={"ops": {results_op_name: {"config": {"batch_id": batch_id}}}},
                ))
                cursor[pk] = f"{batch_id}|{status}"
            if not run_requests:
                return dg.SensorResult(skip_reason="; ".join(notes) or f"No finished batches to process for {submit_key_str}.")
            return dg.SensorResult(run_requests=run_requests, cursor=json.dumps(cursor))

        @dg.sensor(
            name=sensor_name,
            asset_selection=[results_key],
            minimum_interval_seconds=self.sensor_minimum_interval_seconds,
            default_status=default_status,
        )
        def _status_sensor(context: dg.SensorEvaluationContext):
            if partitions_def is not None:
                return _partitioned_status(context)
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


# BEGIN batch partition helpers (inlined per component; keep copies identical, see tests/test_partition_llm_batch.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from dagster import (  # noqa: E402,F401
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _filter_df_to_partition(df, context, partition_date_column, partition_static_column):
    """Keep the rows that belong to the run's partition(s). A no-op for an unpartitioned run or a non-DataFrame input.

    Time partitions use `context.partition_time_window` (which spans the whole range of a backfill run) against
    `partition_date_column`; static/dynamic/multi partitions match `partition_static_column` against
    `context.partition_keys`. Without the relevant column the input is returned unchanged."""
    import pandas as pd

    if not hasattr(df, "columns") or not (context.has_partition_key or context.has_partition_key_range):
        return df
    if partition_date_column and partition_date_column in df.columns:
        try:
            window = context.partition_time_window
        except Exception:
            return df  # static/dynamic/multi partition: no time window
        dates = pd.to_datetime(df[partition_date_column], errors="coerce", utc=True)
        return df[(dates >= pd.Timestamp(window.start)) & (dates < pd.Timestamp(window.end))]
    if partition_static_column and partition_static_column in df.columns:
        keys = {str(k) for k in context.partition_keys}
        return df[df[partition_static_column].astype(str).isin(keys)]
    return df


def _validate_batch_partitioning(partition_type, date_column, static_column):
    """Without a column to slice by, every partition would submit the whole dataset: a duplicate paid batch each time."""
    if not partition_type:
        return
    if partition_type == "multi":
        raise ValueError("partition_type='multi' is not supported by the batch components; use a time or static partition.")
    if partition_type in ("daily", "weekly", "monthly", "hourly"):
        if not date_column:
            raise ValueError(f"partition_type={partition_type!r} needs partition_date_column: each partition submits only its own rows.")
    elif not static_column:
        raise ValueError(f"partition_type={partition_type!r} needs partition_static_column: each partition submits only its own rows.")


def _batch_partition_key(context):
    """The partition key of a partitioned run, or None. A batch run handles exactly one partition: its batch id, prompts hash
    and cancel/reattach decisions are all kept per partition."""
    if not (context.has_partition_key or context.has_partition_key_range):
        return None
    keys = list(context.partition_keys)
    if len(keys) != 1:
        raise ValueError(f"a batch run handles one partition at a time, got {len(keys)}; backfill with one run per partition (the default).")
    return str(keys[0])


def _latest_materialization_metadata(instance, asset_key, partition_key):
    """Metadata of the latest materialization of `asset_key`: of that partition when partitioned, else overall."""
    if partition_key is None:
        event = instance.get_latest_materialization_event(asset_key)
        materialization = event.asset_materialization if event else None
    else:
        records = instance.fetch_materializations(dg.AssetRecordsFilter(asset_key=asset_key, asset_partitions=[partition_key]), limit=1).records
        materialization = records[0].asset_materialization if records else None
    return materialization.metadata if materialization is not None and materialization.metadata else {}


def _latest_observation(instance, asset_key, partition_key):
    """The latest observation record of `asset_key` (of that partition when partitioned), or None."""
    flt = asset_key if partition_key is None else dg.AssetRecordsFilter(asset_key=asset_key, asset_partitions=[partition_key])
    records = instance.fetch_observations(flt, limit=1).records
    return records[0] if records else None
# END batch partition helpers
