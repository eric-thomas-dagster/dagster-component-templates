"""Databricks Genie Query Component.

Natural-language → SQL → result via Databricks AI/BI Genie spaces.

Genie's Conversation API takes a question, generates SQL against the
attached genie space, executes it, and returns rows. This component
wraps the start-conversation → poll-message → fetch-attachment-result
flow into a single asset that materializes a DataFrame.

Two shapes:
  - **Single question** (`question` is set, no upstream): one Genie call
    per materialization. Returns a DataFrame.
  - **Per-row** (`upstream_asset_key` + `question_column` set): for each
    row in the upstream DataFrame, call Genie with that row's question.
    Appends `sql`, `result`, and `genie_message_id` columns.

Polling: Genie message-status goes through PENDING → EXECUTING → COMPLETED.
We poll every `poll_interval_seconds` until completed/failed/timeout.

Auth: Databricks PAT or OAuth bearer token, read from `host_env_var`
(e.g. `https://my-workspace.cloud.databricks.com`) + `token_env_var`.
"""
from typing import Any, Dict, List, Optional, Union
import dagster as dg
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


class DatabricksGenieQueryComponent(Component, Model, Resolvable):
    """NL→SQL→result via Databricks Genie spaces.

    Single-question example:

        ```yaml
        type: dagster_component_templates.DatabricksGenieQueryComponent
        attributes:
          asset_name: weekly_revenue
          host_env_var: DATABRICKS_HOST
          token_env_var: DATABRICKS_TOKEN
          space_id: 01ef2a4b8c9d4e7fa1b2c3d4e5f60718
          question: "What was the total revenue last week, broken down by region?"
        ```

    Per-row example:

        ```yaml
        type: dagster_component_templates.DatabricksGenieQueryComponent
        attributes:
          asset_name: q_and_a_results
          upstream_asset_key: customer_questions
          question_column: question_text
          host_env_var: DATABRICKS_HOST
          token_env_var: DATABRICKS_TOKEN
          space_id: 01ef2a4b8c9d4e7fa1b2c3d4e5f60718
        ```
    """

    model_config = ConfigDict(populate_by_name=True)

    asset_name: str = Field(description="Output Dagster asset name.")
    host_env_var: str = Field(
        default="DATABRICKS_HOST",
        description="Env var with the workspace URL (`https://<workspace>.cloud.databricks.com`).",
    )
    token_env_var: str = Field(
        default="DATABRICKS_TOKEN",
        description="Env var with a Databricks PAT or OAuth bearer token.",
    )
    space_id: str = Field(description="Genie space id (the UUID).")

    # Mode A: single question.
    question: Optional[str] = Field(
        default=None,
        description="Single NL question. Mutually exclusive with `upstream_asset_key`. Supports {partition_key} / {run_id} substitution.",
    )

    # Mode B: per-row.
    upstream_asset_key: Optional[str] = Field(
        default=None,
        description="Upstream DataFrame asset. For each row, the value in `question_column` is sent to Genie.",
    )
    question_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column on the upstream DataFrame that contains the NL question (mode B).",
    )

    poll_interval_seconds: float = Field(default=2.0, gt=0.0, description="Seconds between status polls.")
    timeout_seconds: int = Field(default=300, ge=1, description="Total wait timeout per question.")
    request_timeout_seconds: int = Field(default=60, ge=1, description="HTTP request timeout.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['databricks', 'genie', 'sql'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only deps.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic'.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[str] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        if self.question and self.upstream_asset_key:
            raise ValueError("set either `question` (single-shot) or `upstream_asset_key` (per-row), not both.")
        if not self.question and not self.upstream_asset_key:
            raise ValueError("set either `question` or `upstream_asset_key`.")
        if self.upstream_asset_key and not self.question_column:
            raise ValueError("`upstream_asset_key` requires `question_column`.")

        space_id = self.space_id
        host_env_var = self.host_env_var
        token_env_var = self.token_env_var
        question = self.question
        upstream_asset_key = self.upstream_asset_key
        question_column = self.question_column
        poll_interval_seconds = self.poll_interval_seconds
        timeout_seconds = self.timeout_seconds
        request_timeout_seconds = self.request_timeout_seconds
        group_name = self.group_name
        description = self.description
        owners = self.owners or []

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        _inferred_kinds = list(self.kinds or ["databricks", "genie", "sql"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from datetime import timedelta
            from dagster import FreshnessPolicy
            _lag = timedelta(minutes=int(self.freshness_max_lag_minutes))
            _freshness_policy = (
                FreshnessPolicy.cron(deadline_cron=self.freshness_cron, lower_bound_delta=_lag)
                if self.freshness_cron
                else FreshnessPolicy.time_window(fail_window=_lag)
            )

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        if upstream_asset_key:
            # ---------- mode B: per-row ----------
            @asset(
                key=AssetKey.from_user_string(asset_name),
                ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
                partitions_def=partitions_def,
                group_name=group_name,
                description=description,
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                retry_policy=_retry_policy,
                deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            )
            def _genie_rowwise_asset(context: AssetExecutionContext, upstream):
                import pandas as pd  # noqa: F401
                df = upstream.copy()
                if question_column not in df.columns:
                    raise ValueError(f"question_column {question_column!r} not in upstream columns: {list(df.columns)}")
                sqls = []
                results = []
                message_ids = []
                for _, row in df.iterrows():
                    q = str(row[question_column])
                    log_prefix = f"[genie row q={q[:60]!r}]"
                    context.log.info(log_prefix)
                    sql, rows, mid = _ask_genie(
                        log=context.log,
                        host_env_var=host_env_var,
                        token_env_var=token_env_var,
                        space_id=space_id,
                        question=q,
                        poll_interval_seconds=poll_interval_seconds,
                        timeout_seconds=timeout_seconds,
                        request_timeout_seconds=request_timeout_seconds,
                    )
                    sqls.append(sql)
                    results.append(rows)
                    message_ids.append(mid)
                df["sql"] = sqls
                df["result"] = results
                df["genie_message_id"] = message_ids

                context.add_output_metadata({
                    "rows_processed": MetadataValue.int(len(df)),
                    "space_id": MetadataValue.text(space_id),
                })
                return df

            return Definitions(assets=[_genie_rowwise_asset])

        # ---------- mode A: single question ----------
        @asset(
            key=AssetKey.from_user_string(asset_name),
            partitions_def=partitions_def,
            group_name=group_name,
            description=description,
            owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _genie_single_asset(context: AssetExecutionContext):
            import pandas as pd
            substitutions = {"run_id": context.run_id}
            if context.has_partition_key:
                pk = context.partition_key
                substitutions["partition_key"] = str(pk)
            q = _substitute(question, substitutions)
            context.log.info(f"[genie] q={q!r}")
            sql, rows, mid = _ask_genie(
                log=context.log,
                host_env_var=host_env_var,
                token_env_var=token_env_var,
                space_id=space_id,
                question=q,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
                request_timeout_seconds=request_timeout_seconds,
            )

            df = pd.DataFrame(rows)
            context.add_output_metadata({
                "question": MetadataValue.text(q),
                "sql": MetadataValue.md(f"```sql\n{sql}\n```" if sql else "_(no SQL produced)_"),
                "row_count": MetadataValue.int(len(df)),
                "space_id": MetadataValue.text(space_id),
                "genie_message_id": MetadataValue.text(mid or ""),
            })
            return df

        return Definitions(assets=[_genie_single_asset])


def _substitute(s: Optional[str], substitutions: Dict[str, Any]) -> str:
    if not s or "{" not in s:
        return s or ""
    out = s
    out = out.replace("{run_id}", str(substitutions.get("run_id", "")))
    out = out.replace("{partition_key}", str(substitutions.get("partition_key", "")))
    return out


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start'")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            return DynamicPartitionsDefinition(name=spec.get("dynamic_partition_name") or spec.get("name"))
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _axis(d) for d in partition_dimensions})
    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _vals = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _vals = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _vals: raise ValueError("static requires values.")
        return StaticPartitionsDefinition(_vals)
    if partition_type == "dynamic":
        if not dynamic_partition_name: raise ValueError("dynamic requires dynamic_partition_name.")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _ask_genie(
    log,
    host_env_var: str,
    token_env_var: str,
    space_id: str,
    question: str,
    poll_interval_seconds: float,
    timeout_seconds: int,
    request_timeout_seconds: int,
):
    """Start a Genie conversation, wait for the message, return (sql, rows, message_id)."""
    import os
    import time
    import requests

    host = os.environ.get(host_env_var, "").rstrip("/")
    if not host:
        raise RuntimeError(f"{host_env_var!r} not set.")
    token = os.environ.get(token_env_var)
    if not token:
        raise RuntimeError(f"{token_env_var!r} not set.")
    auth = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    base = f"{host}/api/2.0/genie/spaces/{space_id}"

    # 1. Start a new conversation.
    start = requests.post(
        f"{base}/start-conversation",
        json={"content": question},
        headers=auth,
        timeout=request_timeout_seconds,
    )
    if start.status_code >= 400:
        raise RuntimeError(f"Genie start-conversation failed: HTTP {start.status_code} {start.text[:500]!r}")
    body = start.json()
    conv_id = body.get("conversation_id") or body.get("conversation", {}).get("id")
    msg_id = (body.get("message") or {}).get("id") or body.get("message_id")
    if not conv_id or not msg_id:
        raise RuntimeError(f"Genie start-conversation returned unexpected shape: {body!r}")

    # 2. Poll message status.
    msg_url = f"{base}/conversations/{conv_id}/messages/{msg_id}"
    deadline = time.time() + timeout_seconds
    status = "UNKNOWN"
    mb: Dict[str, Any] = {}
    while time.time() < deadline:
        time.sleep(poll_interval_seconds)
        m = requests.get(msg_url, headers=auth, timeout=request_timeout_seconds)
        if m.status_code >= 400:
            raise RuntimeError(f"Genie message poll failed: HTTP {m.status_code} {m.text[:500]!r}")
        mb = m.json()
        status = (mb.get("status") or "").upper()
        log.info(f"[genie] message status={status}")
        if status == "COMPLETED":
            break
        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Genie message ended in status={status}: {mb!r}")
    else:
        raise TimeoutError(f"Genie message did not complete within {timeout_seconds}s (last status: {status!r}).")

    # 3. Extract the SQL + result. Genie messages have `attachments`, each
    #    with `query` (containing query_text) and optionally `attachment_id`
    #    pointing at a query-result we can fetch separately.
    attachments = mb.get("attachments") or []
    sql_text = None
    rows = []
    for att in attachments:
        q = att.get("query") or {}
        if q.get("query_text"):
            sql_text = q["query_text"]
        att_id = att.get("attachment_id")
        if att_id:
            r = requests.get(
                f"{msg_url}/attachments/{att_id}/query-result",
                headers=auth,
                timeout=request_timeout_seconds,
            )
            if r.status_code < 400:
                payload = r.json()
                # Result shape: { statement_response: { result: { data_array: [...], schema: { columns: [{name, ...}] } } } }
                sr = (payload.get("statement_response") or {}).get("result") or {}
                cols = [c.get("name") for c in (sr.get("schema") or {}).get("columns", [])]
                data = sr.get("data_array") or []
                rows = [dict(zip(cols, r)) for r in data]
    return sql_text or "", rows, msg_id
