"""SlackAgentRequestSensorComponent.

A free-form chat agent, in the style of `slack_run_request_sensor` but without phrase
matching: this sensor polls a Slack channel and launches a chat-agent asset (typically an
`anthropic_agent`/`openai_agent`/`litellm_agent` asset, plus a downstream `slack_reply` asset)
for a message that @-mentions the bot, is in a DM, or is a reply in a thread the bot already
answered -- the agent itself decides what to do with it (answer a question, call a read tool,
call `dagster_workflow_tools`' `launch_job`, ...).

## Why there's no owners check here, unlike slack_run_request_sensor

`slack_run_request_sensor` launches a SPECIFIC, pre-declared job/asset directly, so it has to
gate who can trigger it. This sensor only launches the AGENT -- a read-only conversation by
default. The actual safety-critical decision ("can this person launch THIS job") belongs to
whatever WRITE tool the agent calls, not to "can this person talk to the bot at all" (that's
Slack's own channel-membership model). `dagster_workflow_tools`' `launch_job` is where that
owners check actually happens, using the SAME `slack/user` tag this sensor sets -- see its
README. This sensor's only authorization-adjacent decision is `trigger_mode` (who the bot even
listens to), which is about chattiness, not security.

## Identifiers only in tags -- never the message text

Same rule as `slack_run_request_sensor`: `slack/channel`, `slack/thread_ts`, `slack/message_ts`,
`slack/user` are the only things this sensor puts in a run tag. The message text itself is never
tagged -- the agent reads it live via `slack_chat_tools`' `get_thread()`, using those same tag
values, right when it actually needs it.

## Detecting a follow-up reply needs a second kind of poll

`conversations.history` (used to find NEW root messages) does not return thread replies --
confirmed against Slack's own API reference, not assumed; replies live only behind
`conversations.replies`. So `follow_up_threads` tracks each thread this sensor has already
launched a run for (thread_ts -> the last reply timestamp seen in it) in its own cursor state,
and polls `conversations.replies(channel, ts=thread_ts, oldest=last_seen)` for each one every
tick -- Slack's own `oldest` on that endpoint has the same exclusive-by-default semantics as
`conversations.history`'s, so this naturally returns only genuine NEW replies, never the thread
root again. This is N extra Slack API calls per tick (one per tracked thread, bounded by
`known_threads_limit`) -- real cost, not free, see "What this does NOT do".
"""
import json
import time
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field

_MAX_LAUNCHED_RUN_KEYS = 1000  # bounds the persisted run_key dedup set -- see _launch's own check.


def _now_slack_ts() -> str:
    """Slack timestamps are decimal-seconds strings ('1700000000.123456'); this is a synthetic
    cursor seed, not a real message ts."""
    return f"{time.time():.6f}"


def _get_slack_client(bot_token_env_var: str):
    """Same shape as slack_run_request_sensor's own `_get_slack_client` -- inlined fresh copy,
    not a shared import, per this repo's convention."""
    import os
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("slack_agent_request_sensor requires slack_sdk: pip install 'slack_sdk>=3.0'") from e

    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(
            f"Slack bot token env var {bot_token_env_var!r} not set. Create a bot at "
            f"https://api.slack.com/apps with the `channels:history` (or `groups:history`) + "
            f"`chat:write` + `reactions:write` + `users:read.email` scopes, install it to your "
            f"workspace, invite it to the target channel, and export the `xoxb-...` token."
        )
    return WebClient(token=token)


def _list_new_messages(client, channel_id: str, oldest_ts: str, limit: int, max_pages: int) -> List[Dict[str, Any]]:
    """New ROOT messages only (conversations.history never returns thread replies -- see this
    module's docstring). Same shape as slack_run_request_sensor's own `_list_new_messages`."""
    out: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    while pages < max(max_pages, 1):
        resp = client.conversations_history(channel=channel_id, oldest=oldest_ts, limit=min(max(limit, 1), 200), cursor=cursor)
        if not resp.get("ok"):
            raise RuntimeError(f"Slack conversations_history failed: {resp.get('error')}")
        out.extend(resp.get("messages", []) or [])
        cursor = (resp.get("response_metadata") or {}).get("next_cursor") or None
        pages += 1
        if not cursor:
            break
    return out


def _list_new_replies(client, channel_id: str, thread_ts: str, oldest_ts: str, limit: int) -> List[Dict[str, Any]]:
    """New replies in one thread since `oldest_ts` -- `conversations.replies`' own `oldest` is
    exclusive-by-default, same semantics as `conversations.history`'s (confirmed against Slack's
    own API reference), so setting it to the last reply ts already seen (or the thread root's own
    ts, the first time) naturally excludes anything already processed, parent included."""
    resp = client.conversations_replies(channel=channel_id, ts=thread_ts, oldest=oldest_ts, limit=min(max(limit, 1), 200))
    if not resp.get("ok"):
        raise RuntimeError(f"Slack conversations_replies failed: {resp.get('error')}")
    return resp.get("messages", []) or []


def _resolve_user_email(client, user_id: str) -> Optional[str]:
    """`users.info` -> profile.email. Needs `users:read.email`; any failure degrades to None."""
    try:
        resp = client.users_info(user=user_id)
        if not resp.get("ok"):
            return None
        return ((resp.get("user") or {}).get("profile") or {}).get("email")
    except Exception:  # noqa: BLE001
        return None


def _discover_bot_user_id(client) -> Optional[str]:
    """`auth.test` -> this bot's own Slack user id, so mention-detection (`<@BOT_ID>` in a
    message's text) and self-exclusion both work without the user having to look it up by hand."""
    try:
        resp = client.auth_test()
        return resp.get("user_id") if resp.get("ok") else None
    except Exception:  # noqa: BLE001
        return None


# ── Sibling-asset discovery + selection DSL resolution -- same shape as
# slack_run_request_sensor's own copy (inlined fresh per DCC's no-shared-helper convention). ──
def _discover_sibling_assets(context):
    keys: List[str] = []
    sibling_defs: Optional[dg.Definitions] = None
    try:
        parent_path = context.path.parent if hasattr(context, "path") and hasattr(context.path, "parent") else None
        if parent_path:
            sibling_defs = context.build_defs(parent_path)
            if sibling_defs and sibling_defs.assets:
                for assets_def in sibling_defs.assets:
                    for key in assets_def.keys:
                        keys.append(key.to_user_string())
    except Exception as _exc:
        import logging
        logging.getLogger(__name__).warning(f"Could not discover sibling assets ({type(_exc).__name__}: {_exc})")
    return keys, sibling_defs


def _resolve_selection(selection: Union[str, List[str]], discovered_keys: List[str], sibling_defs: Optional[dg.Definitions]) -> List[str]:
    import fnmatch

    if isinstance(selection, list):
        return selection
    if not discovered_keys:
        return []
    if selection == "*":
        return list(discovered_keys)
    if sibling_defs is not None:
        try:
            graph = sibling_defs.resolve_asset_graph()
            matched = dg.AssetSelection.from_string(selection).resolve(graph)
            if matched:
                return sorted(k.to_user_string() for k in matched)
        except Exception as _exc:
            import logging
            logging.getLogger(__name__).warning(f"Could not resolve the asset selection ({type(_exc).__name__}: {_exc})")
    return [k for k in discovered_keys if fnmatch.fnmatch(k, selection)]


class SlackAgentRequestSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Polls a Slack channel and launches a chat-agent asset for a mention, a DM, or a reply in
    an already-answered thread. See this module's docstring for the full design rationale."""

    sensor_name: str = Field(description="Unique sensor name.")
    slack_channel_id: str = Field(description="Channel ID to poll (e.g. 'C0123ABCDEF' or a DM's 'D0123ABCDEF') -- NOT a '#channel-name'; conversations.history requires the real id.")
    asset_selection: Union[str, List[str]] = Field(
        description=(
            "The agent asset (and anything downstream, e.g. a slack_reply asset) to materialize "
            "when a message matches: explicit key list, the Dagster selection DSL (tag:/group:/"
            "kind:/boolean composition), '*', or a bare fnmatch glob -- resolved against sibling "
            "assets in the same defs folder, same as slack_run_request_sensor's own "
            "asset_selection trigger."
        ),
    )

    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's 'xoxb-...' token. Needs channels:history (or groups:history) + chat:write + reactions:write + users:read.email scopes.")
    bot_user_id: Optional[str] = Field(default=None, description="This bot's own Slack user id, for mention-detection ('<@BOT_ID>' in a message) and self-exclusion. Auto-discovered via auth.test if unset -- set explicitly only to skip that one extra API call per tick.")

    trigger_mode: Literal["mention", "dm", "all"] = Field(
        default="mention",
        description=(
            "'mention' (default): only a message that @-mentions the bot starts a NEW "
            "conversation (a reply in an already-tracked thread still matches regardless of "
            "mode -- see follow_up_threads). 'dm' and 'all' both match every new root message "
            "unconditionally and are functionally identical; 'dm' exists as a separate name "
            "purely so a DM channel's config reads as a DM, not as 'answer literally "
            "everything' by accident. Never use 'all'/'dm' on a busy shared channel -- the bot "
            "would answer every single message in it."
        ),
    )
    follow_up_threads: bool = Field(
        default=True,
        description=(
            "Also launch a run for a NEW REPLY in a thread this sensor has already launched a "
            "run for -- not gated by trigger_mode, so once a thread is tracked, every new reply "
            "in it triggers another run, not just ones that re-mention the bot. Costs one extra "
            "conversations.replies call per tracked thread per tick (see known_threads_limit) -- "
            "set false for a channel with many concurrent long-lived threads."
        ),
    )
    known_threads_limit: int = Field(default=200, description="Most-recently-active threads to keep polling for follow-ups. Bounds both cursor size and the extra conversations.replies calls follow_up_threads costs per tick.")

    resolve_requester_email: bool = Field(default=True, description="Resolve the message sender's Slack user id to an email address (needs users:read.email) before setting the slack/user tag -- most owners: checks downstream (e.g. dagster_workflow_tools' launch_job) use emails, not Slack user ids. When false, slack/user carries the raw Slack user id instead.")

    messages_per_poll: int = Field(default=200, description="Root messages to request per Slack page (Slack's own recommended max is 200).")
    max_pages_per_poll: int = Field(default=3, description="Follow at most this many pagination cursors per poll, for both new-root-message and (per-thread) new-reply polling.")
    process_backlog_on_first_run: bool = Field(default=False, description="If false (default), the first-ever tick seeds the cursor to 'now' and processes nothing -- activating this sensor never answers a channel's past history.")

    post_acknowledgement: bool = Field(default=True, description="React to a matched message immediately (acknowledgement_reaction) so the sender sees it was received before the agent run even starts.")
    acknowledgement_reaction: str = Field(default="eyes", description="Reaction name for post_acknowledgement (one of Slack's built-in emoji names, e.g. 'eyes', 'thinking_face').")
    placeholder_text: Optional[str] = Field(
        default=None,
        description=(
            "If set, post this text as a new thread reply right after acknowledging a matched "
            "message, and tag the run slack/placeholder_ts with that reply's own ts. "
            "slack_reply then updates THIS message with the final answer (chat.update) instead "
            "of posting a new one, and its failure_watcher updates it with the error -- so a "
            "20-30s wait shows 'Looking into it…' (or whatever text you set) instead of nothing. "
            "Needs chat:write (already required). Leave unset (default) to skip this -- "
            "slack_reply just posts new replies, as before. Checked against Slack's own docs, "
            "not assumed: the newer agents.sessions.setStatus DOES work in regular channel "
            "threads (not just the AI-assistant pane the older assistant.threads.setStatus is "
            "scoped to), but it has no free-text status (an enum: active/processing/suspended/"
            "closed) and needs the Agents feature enabled on the workspace (feature_disabled "
            "error otherwise) -- this placeholder message is what actually shows free text like "
            "'Looking into it…' in an ordinary channel, everywhere, with no feature flag."
        ),
    )
    set_isolation_disabled_tag: bool = Field(
        default=True,
        description=(
            "Tag every RunRequest dagster/isolation: disabled, so the run starts in-process on "
            "the code location server instead of spinning up an isolated container (which can "
            "take up to 3 minutes on Dagster+ -- far too slow for chat). Needs non_isolated_runs "
            "enabled on the deployment, and a sensible max_concurrent_non_isolated_runs (default "
            "1 on Dagster+ -- a second concurrent question falls back to a slow isolated run, "
            "not a queue, if you don't raise it). See the README's 'Isolation' section -- "
            "Dagster's own docs are inconsistent about whether this is on by default."
        ),
    )
    use_in_process_executor: bool = Field(
        default=True,
        description=(
            "Tag every RunRequest with run_config={'execution': {'config': {'in_process': {}}}}, "
            "so the agent asset and its downstream slack_reply asset run as plain function "
            "calls in one step-process instead of each spawning its own multiprocess-executor "
            "subprocess -- real subprocess spawn time otherwise adds up on top of "
            "set_isolation_disabled_tag's own container-startup savings. Requires the code "
            "location that owns these assets not to set a non-default executor itself (e.g. "
            "k8s_job_executor/celery_executor/docker_executor) -- if it does, this run_config "
            "key is rejected by that executor's own config schema and the run fails with a "
            "RunConfigValidationInvalid error. Detected at sensor-evaluation time (via "
            "context.repository_def), not just when a run starts: every tick raises a clear "
            "error naming the conflicting executor_def, instead of launching a run doomed to "
            "fail. Set false to leave execution untouched."
        ),
    )

    minimum_interval_seconds: int = Field(default=10, description="Minimum seconds between sensor evaluations. Chat latency is roughly this plus the run-queue dequeue time plus the LLM call -- keep this low.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED' -- initial status of the sensor.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Slack Agent Request Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.default_status.upper() not in ("RUNNING", "STOPPED"):
            raise ValueError(f"SlackAgentRequestSensorComponent {self.sensor_name!r}: default_status must be 'RUNNING' or 'STOPPED'.")

        discovered_keys, sibling_defs = _discover_sibling_assets(context)
        keys = _resolve_selection(self.asset_selection, discovered_keys, sibling_defs)
        if not keys:
            raise ValueError(
                f"SlackAgentRequestSensorComponent {self.sensor_name!r}: asset_selection "
                f"{self.asset_selection!r} matched no assets (discovered {len(discovered_keys)} "
                f"sibling asset(s))."
            )

        sensor_name = self.sensor_name
        channel_id = self.slack_channel_id
        bot_token_env_var = self.slack_bot_token_env_var
        configured_bot_user_id = self.bot_user_id
        trigger_mode = self.trigger_mode
        follow_up_threads = self.follow_up_threads
        known_threads_limit = max(1, self.known_threads_limit)
        resolve_requester_email = self.resolve_requester_email
        messages_per_poll = self.messages_per_poll
        max_pages_per_poll = self.max_pages_per_poll
        process_backlog_on_first_run = self.process_backlog_on_first_run
        post_acknowledgement = self.post_acknowledgement
        acknowledgement_reaction = self.acknowledgement_reaction
        placeholder_text = self.placeholder_text
        set_isolation_disabled_tag = self.set_isolation_disabled_tag
        use_in_process_executor = self.use_in_process_executor
        default_status = dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED
        sensor_asset_selection = [dg.AssetKey(k.split("/")) for k in sorted(set(keys))]

        def sensor_fn(context: dg.SensorEvaluationContext):
            if use_in_process_executor:
                repo = getattr(context, "repository_def", None)
                if repo is not None:
                    job = repo.get_implicit_job_def_for_assets(sensor_asset_selection)
                    if job is not None and job.has_specified_executor:
                        raise ValueError(
                            f"SlackAgentRequestSensorComponent {sensor_name!r}: use_in_process_executor=true, "
                            f"but the code location pins executor_def={job.executor_def.name!r} on the job for "
                            f"this asset selection -- in_process run_config is rejected by that executor's own "
                            f"config schema, and every run this sensor launches would fail at launch with "
                            f"RunConfigValidationInvalid. Set use_in_process_executor: false, or remove the "
                            f"pinned executor so the default multi_or_in_process_executor (which accepts "
                            f"in_process config) is used instead."
                        )

            try:
                client = _get_slack_client(bot_token_env_var)
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Slack auth failed: {e}")

            raw_cursor = context.cursor
            if not raw_cursor:
                if not process_backlog_on_first_run:
                    return dg.SensorResult(
                        cursor=json.dumps({"last_ts": _now_slack_ts(), "threads": {}}),
                        skip_reason="First tick: seeding cursor to now (process_backlog_on_first_run=false) -- no channel history processed.",
                    )
                state = {"last_ts": "0", "threads": {}}
            else:
                try:
                    state = json.loads(raw_cursor)
                except Exception:  # noqa: BLE001 -- a corrupt cursor restarts clean rather than crashing the sensor forever
                    state = {"last_ts": "0", "threads": {}}
            last_ts = state.get("last_ts", "0")
            launched_run_keys: List[str] = list(state.get("launched_run_keys") or [])
            launched_set = set(launched_run_keys)
            threads: Dict[str, str] = dict(state.get("threads") or {})

            bot_id = configured_bot_user_id or _discover_bot_user_id(client)

            try:
                root_messages = _list_new_messages(client, channel_id, last_ts, messages_per_poll, max_pages_per_poll)
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Slack conversations_history failed: {e}")

            new_last_ts = max([m["ts"] for m in root_messages] + [last_ts], key=float) if root_messages else last_ts
            root_candidates = sorted(
                (m for m in root_messages if m.get("user") and not m.get("subtype") and not m.get("bot_id") and m.get("user") != bot_id),
                key=lambda m: float(m["ts"]),
            )

            reply_candidates: List[Dict[str, Any]] = []
            notes: List[str] = []
            if follow_up_threads:
                for thread_ts in sorted(threads.keys(), key=lambda t: threads[t], reverse=True)[:known_threads_limit]:
                    try:
                        replies = _list_new_replies(client, channel_id, thread_ts, threads[thread_ts], messages_per_poll)
                    except Exception as e:  # noqa: BLE001 -- one bad thread must never block the others
                        notes.append(f"thread {thread_ts}: conversations_replies failed: {e}")
                        continue
                    last_seen_for_thread = threads[thread_ts]
                    for m in replies:
                        if not m.get("user") or m.get("subtype") or m.get("bot_id") or m.get("user") == bot_id:
                            continue
                        if m["ts"] == thread_ts or float(m["ts"]) <= float(last_seen_for_thread):
                            # conversations.replies always returns the thread's own parent
                            # message as part of the page, whatever `oldest` is set to --
                            # confirmed against real Slack behavior, not just its docs, which
                            # don't carve out this exception explicitly. Re-filter here rather
                            # than trust `oldest` alone, or the parent (and any reply already
                            # processed) gets treated as a brand new message forever.
                            continue
                        reply_candidates.append(m)
                    if replies:
                        threads[thread_ts] = max([m["ts"] for m in replies] + [threads[thread_ts]], key=float)

            run_requests: List[dg.RunRequest] = []
            email_cache: Dict[str, Optional[str]] = {}

            def _launch(m: Dict[str, Any]):
                run_key = f"{sensor_name}:{m['ts']}"
                if run_key in launched_set:
                    # Already launched (this tick or an earlier one) -- Dagster's own
                    # run_key dedup would silently drop a second RunRequest for it anyway,
                    # but only AFTER this function's side effects (placeholder post,
                    # acknowledgement reaction) already ran. Checking here, before any of
                    # that, is what actually stops a message from being re-launched
                    # (and re-acknowledged) every tick.
                    return
                launched_set.add(run_key)
                launched_run_keys.append(run_key)
                sender_id = m["user"]
                thread_ts = m.get("thread_ts") or m["ts"]
                threads[thread_ts] = max(threads.get(thread_ts, thread_ts), m["ts"], key=float)
                if sender_id not in email_cache:
                    email_cache[sender_id] = _resolve_user_email(client, sender_id) if resolve_requester_email else None
                identity = email_cache[sender_id] or sender_id
                tags = {
                    "slack/channel": channel_id, "slack/thread_ts": thread_ts,
                    "slack/message_ts": m["ts"], "slack/user": identity,
                }
                if set_isolation_disabled_tag:
                    tags["dagster/isolation"] = "disabled"
                if placeholder_text:
                    try:
                        resp = client.chat_postMessage(channel=channel_id, thread_ts=thread_ts, text=placeholder_text)
                        if resp.get("ok") and resp.get("ts"):
                            tags["slack/placeholder_ts"] = resp["ts"]
                    except Exception as e:  # noqa: BLE001 -- a failed placeholder post must never block the launch
                        notes.append(f"message {m['ts']}: placeholder post failed: {e}")
                run_config = {"execution": {"config": {"in_process": {}}}} if use_in_process_executor else {}
                run_requests.append(dg.RunRequest(
                    run_key=run_key, asset_selection=sensor_asset_selection, tags=tags, run_config=run_config,
                ))
                if post_acknowledgement:
                    try:
                        client.reactions_add(channel=channel_id, timestamp=m["ts"], name=acknowledgement_reaction)
                    except Exception as e:  # noqa: BLE001 -- a failed courtesy reaction must never block the launch
                        notes.append(f"message {m['ts']}: reaction post failed: {e}")

            for m in root_candidates:
                text = m.get("text") or ""
                is_mention = bool(bot_id) and f"<@{bot_id}>" in text
                if trigger_mode in ("dm", "all") or is_mention:
                    _launch(m)
                # else: unmatched -- never tracked in `threads`, so it's correctly ineligible
                # for follow_up_threads later (nothing has engaged with it yet).

            for m in reply_candidates:
                _launch(m)

            threads = dict(sorted(threads.items(), key=lambda kv: kv[1], reverse=True)[:known_threads_limit])
            new_cursor = json.dumps({
                "last_ts": new_last_ts, "threads": threads,
                "launched_run_keys": launched_run_keys[-_MAX_LAUNCHED_RUN_KEYS:],
            })

            for n in notes:
                context.log.info(n)

            if not run_requests:
                return dg.SensorResult(cursor=new_cursor, skip_reason="; ".join(notes) or "No matching messages this poll.")
            return dg.SensorResult(run_requests=run_requests, cursor=new_cursor)

        sensor = dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=default_status,
            asset_selection=sensor_asset_selection,
        )(sensor_fn)

        return dg.Definitions(sensors=[sensor])
