"""SlackReplyComponent -- deterministically posts a chat agent's answer back into the Slack
thread it's answering. A model is never trusted to remember to call a "post" tool -- if it
forgets, or the run fails outright, the human sees nothing. This component is the opposite of
that: a plain downstream asset for the success path, PLUS a run-failure sensor that fires
whenever any failed run carries the same `slack/channel`/`slack/thread_ts` tags
`slack_agent_request_sensor` sets, so a failed run still gets a reply (a short error + a run
link) instead of silence.

Both halves read channel/thread straight off the RUN's own tags (`context.run.tags` for the
success-path asset, `context.dagster_run.tags` for the failure sensor) -- the same identifiers
`slack_agent_request_sensor` set, never re-derived or guessed.
"""
import re
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

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

_REDACTED = "[REDACTED]"
_REDACT_PATTERNS = [
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), lambda m: f"{m.group(1)} {_REDACTED}"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{16,}"), lambda m: _REDACTED),
    (re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"), lambda m: _REDACTED),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), lambda m: _REDACTED),
]


def _redact(text: str) -> str:
    s = str(text)
    for pattern, repl in _REDACT_PATTERNS:
        s = pattern.sub(repl, s)
    return s


def _get_slack_client(bot_token_env_var: str):
    """Same shape as slack_run_request_sensor's / slack_chat_tools' own `_get_slack_client`."""
    import os
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("slack_reply requires slack_sdk: pip install 'slack_sdk>=3.0'") from e

    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(f"Slack bot token env var {bot_token_env_var!r} not set.")
    return WebClient(token=token)


def _extract_answer(value: Any, answer_field: Optional[str]) -> str:
    """The agent components' own documented output shape is {final_answer, iterations,
    tool_calls_made, stopped_reason, ...} -- default to `final_answer`; answer_field overrides
    for a differently-shaped upstream."""
    if answer_field:
        if isinstance(value, dict):
            return str(value.get(answer_field, ""))
        return str(getattr(value, answer_field, ""))
    if isinstance(value, dict):
        if "final_answer" in value:
            return str(value.get("final_answer") or "_(empty)_")
        return str(value)
    return str(value)


def _split_or_truncate(text: str, max_chars: int, mode: str) -> List[str]:
    text = text or "_(empty)_"
    if len(text) <= max_chars:
        return [text]
    if mode == "truncate":
        return [text[: max(0, max_chars - 20)].rstrip() + "… (truncated)"]
    parts = [text[i: i + max_chars] for i in range(0, len(text), max_chars)]
    return parts or [text]


def _run_url(webserver_base_url: Optional[str], run_id: str) -> Optional[str]:
    if not webserver_base_url:
        return None
    return f"{webserver_base_url.rstrip('/')}/runs/{run_id}"


class SlackReplyComponent(dg.Component, dg.Model, dg.Resolvable):
    """Posts a chat agent's answer into the Slack thread named by the run's own
    `slack/channel`/`slack/thread_ts` tags. Emits TWO things: `{asset_name}` (downstream of
    `upstream_asset_key`, the success path) and `{asset_name}_failure_watcher` (a run-status
    sensor that posts a short error instead, for ANY failed run carrying those same tags --
    not just one where this asset itself ran)."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: Union[str, List[str]] = Field(description="The chat agent asset to depend on and read the answer from (slash-delimited). Exactly one -- a list of more than one is a validation error.")

    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's 'xoxb-...' token. Needs chat:write.")
    answer_field: Optional[str] = Field(
        default=None,
        description="Which key of the upstream asset's output dict holds the answer text. Defaults to 'final_answer' (the agent components' own documented output shape) when the upstream value is a dict; used verbatim (str()'d) otherwise.",
    )
    max_chars_per_message: int = Field(default=3000, description="Split or truncate (see on_long_answer) an answer longer than this many characters.")
    on_long_answer: Literal["split", "truncate"] = Field(default="split", description="'split' (default): post as multiple sequential thread replies, each <= max_chars_per_message. 'truncate': post one reply, cut short with an ellipsis.")
    include_run_link: bool = Field(default=True, description="Append a link to the Dagster run (requires webserver_base_url) to the last reply posted.")
    run_link_label: str = Field(default="View run", description="Link text for include_run_link.")
    webserver_base_url: Optional[str] = Field(default=None, description="Base URL of the Dagster webserver (e.g. 'http://localhost:3000' or your Dagster+ deployment URL) -- used to build the run link. Leave unset to omit the link entirely.")

    monitor_failures: bool = Field(default=True, description="Also emit {asset_name}_failure_watcher, a run-status sensor that posts a short error reply for ANY failed run carrying slack/channel + slack/thread_ts tags -- so a crashed run is never silent in the channel. Set false to handle failure notification some other way.")
    error_message_template: str = Field(default="⚠️ Something went wrong answering this: {error}", description="Template for the failure-path reply. Placeholder: {error}.")

    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys, beyond upstream_asset_key -- no value loaded.")
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners -- team names ('team:analytics') or email addresses.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Additional key-value tags applied to the asset in the Dagster catalog.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("deps", "owners", "kinds"), csv=(), tags=("asset_tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Slack Reply", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = dg.AssetKey(self.asset_name.split("/"))
        upstream_key = dg.AssetKey(self.upstream_asset_key.split("/"))
        bot_token_env_var = self.slack_bot_token_env_var
        answer_field = self.answer_field
        max_chars = self.max_chars_per_message
        on_long_answer = self.on_long_answer
        include_run_link = self.include_run_link
        run_link_label = self.run_link_label
        webserver_base_url = self.webserver_base_url
        error_message_template = self.error_message_template

        @dg.asset(
            key=asset_key,
            ins={"agent_output": dg.AssetIn(key=upstream_key)},
            deps=[dg.AssetKey(k.split("/")) for k in (self.deps or [])],
            group_name=self.group_name,
            description=self.description,
            owners=self.owners or [],
            tags=self.asset_tags or {},
            kinds=frozenset(self.kinds) if self.kinds else frozenset(),
        )
        def _reply(context: dg.AssetExecutionContext, agent_output: Any):
            tags = context.run.tags or {}
            channel, thread_ts = tags.get("slack/channel"), tags.get("slack/thread_ts")
            if not channel or not thread_ts:
                raise dg.Failure(
                    "slack_reply requires 'slack/channel' and 'slack/thread_ts' run tags "
                    "(normally set by slack_agent_request_sensor) -- this asset should only "
                    "ever be materialized as part of that chain, not run standalone."
                )
            text = _redact(_extract_answer(agent_output, answer_field))
            parts = _split_or_truncate(text, max_chars, on_long_answer)
            link = _run_url(webserver_base_url, context.run_id) if include_run_link else None
            if link:
                parts[-1] = f"{parts[-1]}\n\n<{link}|{run_link_label}>"
            client = _get_slack_client(bot_token_env_var)
            for part in parts:
                resp = client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=part)
                if not resp.get("ok"):
                    raise dg.Failure(f"Slack chat_postMessage failed: {resp.get('error')}")
            return dg.Output(
                {"posted_parts": len(parts), "channel": channel, "thread_ts": thread_ts},
                metadata={"posted_parts": dg.MetadataValue.int(len(parts)), "channel": dg.MetadataValue.text(channel)},
            )

        defs = dg.Definitions(assets=[_reply])

        if self.monitor_failures:
            failure_sensor_name = f"{self.asset_name.replace('/', '_')}_failure_watcher"

            @dg.run_status_sensor(
                name=failure_sensor_name,
                run_status=dg.DagsterRunStatus.FAILURE,
                monitor_all_code_locations=True,
                default_status=dg.DefaultSensorStatus.RUNNING,
            )
            def _failure_watcher(sensor_context: dg.RunStatusSensorContext):
                run_tags = sensor_context.dagster_run.tags or {}
                channel, thread_ts = run_tags.get("slack/channel"), run_tags.get("slack/thread_ts")
                if not channel or not thread_ts:
                    return  # not a chat-agent run -- nothing to do, same tag-based identity check as everywhere else here
                error = _redact(getattr(sensor_context.dagster_event, "message", None) or "the run failed with no further detail.")
                link = _run_url(webserver_base_url, sensor_context.dagster_run.run_id) if include_run_link else None
                text = error_message_template.format(error=error)
                if link:
                    text = f"{text}\n<{link}|{run_link_label}>"
                try:
                    client = _get_slack_client(bot_token_env_var)
                    client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=text)
                except Exception as e:  # noqa: BLE001 -- a failed failure-notification must not crash the sensor loop
                    sensor_context.log.warning(f"slack_reply failure_watcher: could not post error reply: {e}")

            defs = dg.Definitions.merge(defs, dg.Definitions(sensors=[_failure_watcher]))

        return defs
