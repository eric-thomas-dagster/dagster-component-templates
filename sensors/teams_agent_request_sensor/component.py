"""TeamsAgentRequestSensorComponent.

A free-form chat agent for Microsoft Teams, in the style of `teams_run_request_sensor` but
without phrase matching: this sensor polls a Teams channel and launches a chat-agent asset
(typically an `anthropic_agent`/`openai_agent`/`litellm_agent` asset, plus a downstream
`teams_reply` asset) for a message that @-mentions the bot, or is a reply in a thread the bot
already answered -- the agent itself decides what to do with it (answer a question, call a
read tool, call `dagster_workflow_tools`' `launch_job`, ...). Teams analog of
`slack_agent_request_sensor` -- no shared code (per this repo's convention), but the same
design.

## Why there's no owners check here, unlike teams_run_request_sensor

`teams_run_request_sensor` launches a SPECIFIC, pre-declared job/asset directly, so it has to
gate who can trigger it. This sensor only launches the AGENT -- a read-only conversation by
default. The actual safety-critical decision ("can this person launch THIS job") belongs to
whatever WRITE tool the agent calls (`dagster_workflow_tools`' `launch_job`, using the SAME
`teams/user` tag this sensor sets), not to "can this person talk to the bot at all" (that's
Teams' own channel-membership model). This sensor's only authorization-adjacent decision is
`trigger_mode` (who the bot even listens to), which is about chattiness, not security.

## Identifiers only in tags -- never the message text

Same rule as `slack_agent_request_sensor`: `teams/team_id`, `teams/channel_id`,
`teams/thread_id`, `teams/message_id`, `teams/user` are the only things this sensor puts in a
run tag. The message text itself is never tagged -- the agent reads it live via
`teams_chat_tools`' `get_thread()`, using those same tag values, right when it actually needs
it.

## No delta query for replies -- a real, different cost from Slack

Confirmed against Microsoft's own `chatmessage-list-replies` API reference: unlike Slack's
`conversations.replies(oldest=...)`, Graph's replies endpoint supports NO `$filter` on
`createdDateTime`/`lastModifiedDateTime` and no delta token -- only `$top` (hard-capped at 50)
and `@odata.nextLink` pagination. So `follow_up_threads` must re-fetch and diff ALL replies in
a tracked thread every tick (client-side, by `createdDateTime > last_seen`), not an
incremental fetch the way Slack's sensor gets for free. `known_threads_limit` and
`messages_per_poll`/`max_pages_per_poll` bound the real cost this adds -- see README.

## Mentions are structured, not text-parsed

Unlike Slack's `<@BOT_ID>` substring check, a Teams `chatMessage.mentions[]` entry carries a
`mentioned.application.id` when a bot/app is @-mentioned (confirmed against Graph's own
`chatMessageMentionedIdentitySet` schema) -- this sensor compares that id against
`bot_application_id` (defaulting to the same `teams_client_id_env_var` value already used for
OAuth, since there is no Teams equivalent of Slack's `auth.test` self-lookup under a delegated
token representing a human, not the bot identity).
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import dagster as dg
from pydantic import Field

_GRAPH = "https://graph.microsoft.com/v1.0"
_MAX_LAUNCHED_RUN_KEYS = 1000  # bounds the persisted run_key dedup set -- see _launch's own check.


# ── Inline fs helper: local + cloud (s3://, gs://, abfs://) via fsspec. Kept inline per
# DCC's self-contained convention -- same shape as teams_run_request_sensor's/
# teams_approval_gate's own copy, not a shared import. Only used to persist a rotated OAuth
# refresh token. ──
class _StateFS:
    def __init__(self, root: str):
        if "://" in root:
            import fsspec
            fs, rt = fsspec.core.url_to_fs(root)
            self.fs, self.root, self.is_uri = fs, rt.rstrip("/"), True
        else:
            self.fs, self.root, self.is_uri = None, str(Path(root).expanduser().resolve()), False

    def path(self, *parts: str) -> str:
        pieces = [self.root, *[p.strip("/") for p in parts if p]]
        return "/".join(pieces) if self.is_uri else str(Path(*pieces))

    def exists(self, p: str) -> bool:
        return bool(self.fs.exists(p)) if self.is_uri else Path(p).exists()

    def read_json(self, p: str) -> Any:
        import json as _json
        if self.is_uri:
            with self.fs.open(p, "r") as f:
                return _json.loads(f.read())
        return _json.loads(Path(p).read_text())

    def write_json(self, p: str, obj: Any) -> None:
        import json as _json
        body = _json.dumps(obj, indent=2, default=str)
        if self.is_uri:
            try:
                self.fs.makedirs("/".join(p.split("/")[:-1]), exist_ok=True)
            except Exception:
                pass
            with self.fs.open(p, "w") as f:
                f.write(body)
        else:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            Path(p).write_text(body)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _oauth_state_path(fs: "_StateFS") -> str:
    return fs.path(".teams_oauth_state.json")


def _get_teams_bearer(fs: "_StateFS", tenant_id_env: str, client_id_env: str, client_secret_env: str, refresh_token_env: str) -> str:
    """OAuth2 refresh-token grant against Azure AD -- a DELEGATED token, not app-only. Same
    shape as teams_run_request_sensor's own copy -- see that component's README ("Auth") for
    why app-only has no working path here. Azure AD can rotate the refresh token on any call;
    the rotated value is persisted to `<oauth_state_dir>/.teams_oauth_state.json` and
    preferred over `refresh_token_env` on every later call."""
    import requests
    tenant_id = os.environ.get(tenant_id_env)
    client_id = os.environ.get(client_id_env)
    client_secret = os.environ.get(client_secret_env)
    seed_refresh_token = os.environ.get(refresh_token_env)
    if not (tenant_id and client_id and client_secret and seed_refresh_token):
        raise RuntimeError(
            f"Teams auth env vars not all set: "
            f"{tenant_id_env}={'✓' if tenant_id else '✗'}, "
            f"{client_id_env}={'✓' if client_id else '✗'}, "
            f"{client_secret_env}={'✓' if client_secret else '✗'}, "
            f"{refresh_token_env}={'✓' if seed_refresh_token else '✗'}. "
            f"Register an app at https://portal.azure.com -> Entra ID -> App registrations -> "
            f"New, and run the one-time interactive consent for the dedicated bot account "
            f"(see this component's README, 'Auth')."
        )
    refresh_token = seed_refresh_token
    state_path = _oauth_state_path(fs)
    if fs.exists(state_path):
        try:
            saved = fs.read_json(state_path)
            if saved.get("refresh_token"):
                refresh_token = saved["refresh_token"]
        except Exception:  # noqa: BLE001 -- a corrupt state file falls back to the seed token, not a crash
            pass

    r = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default offline_access",
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    new_refresh_token = body.get("refresh_token")
    if new_refresh_token and new_refresh_token != refresh_token:
        fs.write_json(state_path, {"refresh_token": new_refresh_token, "rotated_at": _now_iso()})
    return body["access_token"]


def _list_channel_messages(bearer: str, team_id: str, channel_id: str, top: int, max_pages: int) -> List[Dict[str, Any]]:
    """Same shape as teams_run_request_sensor's own `_list_channel_messages` -- no delta query
    exists for channel messages (confirmed against Graph's own reference); only `$top`
    (hard-capped at 50) and `@odata.nextLink` pagination."""
    import requests
    url = f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages"
    params: Optional[Dict[str, Any]] = {"$top": min(max(top, 1), 50)}
    out: List[Dict[str, Any]] = []
    pages = 0
    while url and pages < max(max_pages, 1):
        r = requests.get(url, headers={"Authorization": f"Bearer {bearer}"}, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        out.extend(body.get("value", []) or [])
        url = body.get("@odata.nextLink")
        params = None  # nextLink already embeds the query string
        pages += 1
    return out


def _get_teams_replies(bearer: str, team_id: str, channel_id: str, message_id: str, top: int, max_pages: int) -> List[Dict[str, Any]]:
    """Lists ALL current replies to one message -- Graph's replies endpoint has no delta/
    incremental query (confirmed against Microsoft's own `chatmessage-list-replies`
    reference), so unlike Slack's `conversations.replies(oldest=...)`, every tick re-fetches
    the full list and the caller must diff client-side by `createdDateTime`."""
    import requests
    url = f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies"
    params: Optional[Dict[str, Any]] = {"$top": min(max(top, 1), 50)}
    out: List[Dict[str, Any]] = []
    pages = 0
    while url and pages < max(max_pages, 1):
        r = requests.get(url, headers={"Authorization": f"Bearer {bearer}"}, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        out.extend(body.get("value", []) or [])
        url = body.get("@odata.nextLink")
        params = None
        pages += 1
    return out


def _resolve_user_email(bearer: str, aad_id: str) -> Optional[str]:
    """`from.user` on a chatMessage carries only an Azure AD object id + display name, never
    an email (confirmed against Graph's own chatMessage schema) -- same shape as
    teams_run_request_sensor's own `_resolve_user_email`. Needs delegated `User.Read.All`;
    any failure degrades to None rather than raising."""
    import requests
    try:
        r = requests.get(
            f"{_GRAPH}/users/{aad_id}",
            headers={"Authorization": f"Bearer {bearer}"},
            params={"$select": "mail,userPrincipalName"},
            timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        return body.get("mail") or body.get("userPrincipalName")
    except Exception:  # noqa: BLE001 -- see docstring
        return None


def _set_teams_reaction(bearer: str, team_id: str, channel_id: str, message_id: str, reaction_type: str, *, parent_id: Optional[str] = None) -> None:
    """Adds a reaction to a channel message or reply. Confirmed against Microsoft's own
    `chatmessage-setreaction` reference: delegated `ChannelMessage.Send` is sufficient (the
    SAME scope this sensor already provisions for replies) -- no Bot Framework, no extra
    consent. `reaction_type` is any supported Unicode emoji character (or a legacy name like
    'like'/'angry'/'sad'/'laugh'/'heart'/'surprised'). Pass `parent_id` (the thread root's own
    id) when reacting to a REPLY rather than a root message."""
    import requests
    base = f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages"
    url = f"{base}/{parent_id}/replies/{message_id}/setReaction" if parent_id else f"{base}/{message_id}/setReaction"
    r = requests.post(
        url, headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"reactionType": reaction_type}, timeout=30,
    )
    r.raise_for_status()


def _to_html(text: str) -> str:
    """Teams message bodies are HTML (contentType: 'html') -- same minimal escape as
    teams_reply's own `_to_html`: & < > escaped, newlines to <br>."""
    escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return escaped.replace("\n", "<br>")


def _post_teams_reply(bearer: str, team_id: str, channel_id: str, thread_id: str, html_body: str) -> Dict[str, Any]:
    """Posts a reply and returns the created chatMessage -- same shape as teams_reply's own
    `_post_teams_reply`, needed here (not just there) so placeholder_text's reply id can be
    tagged onto the RunRequest before the run even starts."""
    import requests
    r = requests.post(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{thread_id}/replies",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"body": {"contentType": "html", "content": html_body}},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _is_bot_mentioned(message: Dict[str, Any], bot_application_id: Optional[str]) -> bool:
    if not bot_application_id:
        return False
    for m in message.get("mentions") or []:
        app = (m.get("mentioned") or {}).get("application") or {}
        if app.get("id") == bot_application_id:
            return True
    return False


# ── Sibling-asset discovery + selection DSL resolution -- same shape as
# teams_run_request_sensor's / slack_agent_request_sensor's own copies (inlined fresh per
# DCC's no-shared-helper convention). ──
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


class TeamsAgentRequestSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Polls a Teams channel and launches a chat-agent asset for a mention, or a reply in an
    already-answered thread. See this module's docstring for the full design rationale."""

    sensor_name: str = Field(description="Unique sensor name.")
    teams_team_id: str = Field(description="Teams team id (Teams client -> team -> ... -> Get link).")
    teams_channel_id: str = Field(description="Teams channel id inside the team (e.g. '19:xxx@thread.tacv2').")
    asset_selection: Union[str, List[str]] = Field(
        description=(
            "The agent asset (and anything downstream, e.g. a teams_reply asset) to "
            "materialize when a message matches: explicit key list, the Dagster selection "
            "DSL (tag:/group:/kind:/boolean composition), '*', or a bare fnmatch glob -- "
            "resolved against sibling assets in the same defs folder, same as "
            "teams_run_request_sensor's own asset_selection trigger."
        ),
    )
    oauth_state_dir: str = Field(
        description=(
            "Local path or fsspec URI (s3://, gs://, abfs://) dedicated to this sensor's "
            "'.teams_oauth_state.json' -- holds a rotated OAuth refresh token, if Microsoft "
            "ever issues one. Without a durable place to persist it, the next rotation breaks "
            "this sensor's auth entirely."
        ),
    )

    teams_tenant_id_env_var: str = Field(default="TEAMS_TENANT_ID", description="Env var holding the Azure AD tenant id.")
    teams_client_id_env_var: str = Field(default="TEAMS_CLIENT_ID", description="Env var holding the Azure AD app registration client id. Also the default source for bot_application_id when that field is left unset.")
    teams_client_secret_env_var: str = Field(default="TEAMS_CLIENT_SECRET", description="Env var holding the Azure AD app registration client secret.")
    teams_refresh_token_env_var: str = Field(
        default="TEAMS_REFRESH_TOKEN",
        description="Env var holding the INITIAL OAuth refresh token (one-time interactive consent for the dedicated bot account -- see README, 'Auth'). Only the seed; a later rotation is persisted to oauth_state_dir instead.",
    )
    bot_application_id: Optional[str] = Field(
        default=None,
        description=(
            "This bot's own Azure AD application (client) id, for mention-detection -- a "
            "message's mentions[] carries mentioned.application.id when a bot/app is "
            "@-mentioned (confirmed against Graph's own chatMessageMentionedIdentitySet "
            "schema). Defaults to reading teams_client_id_env_var's own value if unset, since "
            "there is no Teams equivalent of Slack's auth.test self-lookup under a delegated "
            "token (it represents the consenting HUMAN, not the bot identity) -- set this "
            "explicitly only if that assumption ever proves wrong for your tenant."
        ),
    )

    trigger_mode: Literal["mention", "all"] = Field(
        default="mention",
        description=(
            "'mention' (default): only a message that @-mentions the bot starts a NEW "
            "conversation (a reply in an already-tracked thread still matches regardless of "
            "mode -- see follow_up_threads). 'all' matches every new root message "
            "unconditionally -- never use this on a busy shared channel. Unlike "
            "slack_agent_request_sensor, there is no separate 'dm' mode: this sensor only "
            "polls team CHANNELS, never 1:1 personal chats, the same scope "
            "teams_run_request_sensor already has."
        ),
    )
    follow_up_threads: bool = Field(
        default=True,
        description=(
            "Also launch a run for a NEW REPLY in a thread this sensor has already launched a "
            "run for -- not gated by trigger_mode. Unlike Slack, Graph's replies endpoint has "
            "no delta/incremental query (confirmed against Microsoft's own API reference), so "
            "this re-fetches and diffs ALL replies in each tracked thread every tick -- real, "
            "higher cost than the Slack version. Set false for a channel with many concurrent "
            "long-lived threads."
        ),
    )
    known_threads_limit: int = Field(default=200, description="Most-recently-active threads to keep polling for follow-ups. Bounds both cursor size and the extra replies-list calls follow_up_threads costs per tick.")

    resolve_requester_email: bool = Field(default=True, description="Resolve the message sender's Azure AD object id to an email address (needs delegated User.Read.All) before setting the teams/user tag -- most owners: checks downstream (e.g. dagster_workflow_tools' launch_job) use emails, not Azure AD object ids. When false, teams/user carries the raw Azure AD object id instead.")

    messages_per_poll: int = Field(default=50, description="Messages/replies to request per Graph page (Graph's own hard cap is 50).")
    max_pages_per_poll: int = Field(default=3, description="Follow at most this many '@odata.nextLink' pages per poll, for both new-root-message and (per-thread) replies polling.")
    process_backlog_on_first_run: bool = Field(default=False, description="If false (default), the first-ever tick seeds the cursor to 'now' and processes nothing -- activating this sensor never answers a channel's past history.")

    post_acknowledgement: bool = Field(default=True, description="React to a matched message immediately (acknowledgement_reaction) so the sender sees it was received before the agent run even starts. Needs delegated ChannelMessage.Send (the same scope already provisioned for replies).")
    acknowledgement_reaction: str = Field(default="👀", description="Reaction for post_acknowledgement -- any Unicode emoji character, or a legacy name ('like', 'heart', 'laugh', 'surprised', 'sad', 'angry').")
    placeholder_text: Optional[str] = Field(
        default=None,
        description=(
            "If set, post this text as a new reply right after acknowledging a matched message, "
            "and tag the run teams/placeholder_id with that reply's own id. teams_reply then "
            "edits THIS reply with the final answer (PATCH .../replies/{id}, delegated "
            "ChannelMessage.ReadWrite -- a DIFFERENT scope than ChannelMessage.Send, which only "
            "covers posting) instead of posting a new one, and its failure_watcher edits it "
            "with the error. Leave unset (default) to skip this -- teams_reply just posts new "
            "replies, as before."
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
            "not a queue, if you don't raise it)."
        ),
    )
    use_in_process_executor: bool = Field(
        default=True,
        description=(
            "Tag every RunRequest with run_config={'execution': {'config': {'in_process': {}}}}, "
            "so the agent asset and its downstream teams_reply asset run as plain function "
            "calls in one step-process instead of each spawning its own multiprocess-executor "
            "subprocess. Requires the code location that owns these assets not to set a "
            "non-default executor itself -- if it does, this run_config key is rejected by "
            "that executor's own config schema and the run fails with a "
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
        return ComponentFormConfig(label="Teams Agent Request Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.default_status.upper() not in ("RUNNING", "STOPPED"):
            raise ValueError(f"TeamsAgentRequestSensorComponent {self.sensor_name!r}: default_status must be 'RUNNING' or 'STOPPED'.")

        discovered_keys, sibling_defs = _discover_sibling_assets(context)
        keys = _resolve_selection(self.asset_selection, discovered_keys, sibling_defs)
        if not keys:
            raise ValueError(
                f"TeamsAgentRequestSensorComponent {self.sensor_name!r}: asset_selection "
                f"{self.asset_selection!r} matched no assets (discovered {len(discovered_keys)} "
                f"sibling asset(s))."
            )

        sensor_name = self.sensor_name
        team_id = self.teams_team_id
        channel_id = self.teams_channel_id
        tenant_env, client_env, secret_env, refresh_env = (
            self.teams_tenant_id_env_var, self.teams_client_id_env_var,
            self.teams_client_secret_env_var, self.teams_refresh_token_env_var,
        )
        oauth_state_dir = self.oauth_state_dir
        configured_bot_application_id = self.bot_application_id
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
        epoch = "1970-01-01T00:00:00.0000000Z"

        def sensor_fn(context: dg.SensorEvaluationContext):
            if use_in_process_executor:
                repo = getattr(context, "repository_def", None)
                if repo is not None:
                    job = repo.get_implicit_job_def_for_assets(sensor_asset_selection)
                    if job is not None and job.has_specified_executor:
                        raise ValueError(
                            f"TeamsAgentRequestSensorComponent {sensor_name!r}: use_in_process_executor=true, "
                            f"but the code location pins executor_def={job.executor_def.name!r} on the job for "
                            f"this asset selection -- in_process run_config is rejected by that executor's own "
                            f"config schema, and every run this sensor launches would fail at launch with "
                            f"RunConfigValidationInvalid. Set use_in_process_executor: false, or remove the "
                            f"pinned executor so the default multi_or_in_process_executor (which accepts "
                            f"in_process config) is used instead."
                        )

            fs = _StateFS(oauth_state_dir)
            try:
                bearer = _get_teams_bearer(fs, tenant_env, client_env, secret_env, refresh_env)
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Teams auth failed: {e}")

            raw_cursor = context.cursor
            if not raw_cursor:
                if not process_backlog_on_first_run:
                    return dg.SensorResult(
                        cursor=json.dumps({"last_ts": _now_iso(), "threads": {}}),
                        skip_reason="First tick: seeding cursor to now (process_backlog_on_first_run=false) -- no channel history processed.",
                    )
                state = {"last_ts": epoch, "threads": {}}
            else:
                try:
                    state = json.loads(raw_cursor)
                except Exception:  # noqa: BLE001 -- a corrupt cursor restarts clean rather than crashing the sensor forever
                    state = {"last_ts": epoch, "threads": {}}
            last_ts = state.get("last_ts", epoch)
            threads: Dict[str, str] = dict(state.get("threads") or {})
            launched_run_keys: List[str] = list(state.get("launched_run_keys") or [])
            launched_set = set(launched_run_keys)

            bot_application_id = configured_bot_application_id or os.environ.get(client_env)

            try:
                messages = _list_channel_messages(bearer, team_id, channel_id, messages_per_poll, max_pages_per_poll)
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Teams list-messages failed: {e}")

            new_last_ts = max([m.get("createdDateTime", "") for m in messages] + [last_ts]) if messages else last_ts
            root_candidates = sorted(
                (m for m in messages if m.get("createdDateTime", "") > last_ts and (m.get("from") or {}).get("user")),
                key=lambda m: m["createdDateTime"],
            )

            reply_candidates: List[Tuple[str, Dict[str, Any]]] = []
            notes: List[str] = []
            if follow_up_threads:
                for thread_id in sorted(threads.keys(), key=lambda t: threads[t], reverse=True)[:known_threads_limit]:
                    try:
                        replies = _get_teams_replies(bearer, team_id, channel_id, thread_id, messages_per_poll, max_pages_per_poll)
                    except Exception as e:  # noqa: BLE001 -- one bad thread must never block the others
                        notes.append(f"thread {thread_id}: replies fetch failed: {e}")
                        continue
                    last_seen = threads[thread_id]
                    for r in replies:
                        if r.get("createdDateTime", "") > last_seen and (r.get("from") or {}).get("user"):
                            reply_candidates.append((thread_id, r))
                    if replies:
                        threads[thread_id] = max([r.get("createdDateTime", "") for r in replies] + [last_seen])

            run_requests: List[dg.RunRequest] = []
            email_cache: Dict[str, Optional[str]] = {}

            def _launch(m: Dict[str, Any], *, thread_id: str, parent_id_for_reaction: Optional[str]):
                run_key = f"{sensor_name}:{m['id']}"
                if run_key in launched_set:
                    # Already launched (this tick or an earlier one) -- Dagster's own
                    # run_key dedup would silently drop a second RunRequest for it anyway,
                    # but only AFTER this function's side effects (placeholder post,
                    # acknowledgement reaction) already ran. Checking here, before any of
                    # that, is what actually stops a message from being re-launched (and
                    # re-acknowledged) every tick -- same fix as slack_agent_request_sensor's
                    # own launched_run_keys, applied here as defense-in-depth even though
                    # Teams' own replies endpoint has no known equivalent to the Slack bug
                    # that motivated it (Graph's replies list never returns the thread root).
                    return
                launched_set.add(run_key)
                launched_run_keys.append(run_key)
                sender = (m.get("from") or {}).get("user") or {}
                sender_id = sender.get("id") or ""
                threads[thread_id] = max(threads.get(thread_id, m.get("createdDateTime", "")), m.get("createdDateTime", ""))
                if sender_id not in email_cache:
                    email_cache[sender_id] = _resolve_user_email(bearer, sender_id) if resolve_requester_email else None
                identity = email_cache[sender_id] or sender_id
                tags = {
                    "teams/team_id": team_id, "teams/channel_id": channel_id, "teams/thread_id": thread_id,
                    "teams/message_id": m["id"], "teams/user": identity,
                }
                if set_isolation_disabled_tag:
                    tags["dagster/isolation"] = "disabled"
                if placeholder_text:
                    try:
                        resp = _post_teams_reply(bearer, team_id, channel_id, thread_id, _to_html(placeholder_text))
                        if resp.get("id"):
                            tags["teams/placeholder_id"] = resp["id"]
                    except Exception as e:  # noqa: BLE001 -- a failed placeholder post must never block the launch
                        notes.append(f"message {m['id']}: placeholder post failed: {e}")
                run_config = {"execution": {"config": {"in_process": {}}}} if use_in_process_executor else {}
                run_requests.append(dg.RunRequest(
                    run_key=run_key, asset_selection=sensor_asset_selection, tags=tags, run_config=run_config,
                ))
                if post_acknowledgement:
                    try:
                        _set_teams_reaction(bearer, team_id, channel_id, m["id"], acknowledgement_reaction, parent_id=parent_id_for_reaction)
                    except Exception as e:  # noqa: BLE001 -- a failed courtesy reaction must never block the launch
                        notes.append(f"message {m['id']}: reaction post failed: {e}")

            for m in root_candidates:
                if trigger_mode == "all" or _is_bot_mentioned(m, bot_application_id):
                    _launch(m, thread_id=m["id"], parent_id_for_reaction=None)
                # else: unmatched -- never tracked in `threads`, so it's correctly ineligible
                # for follow_up_threads later (nothing has engaged with it yet).

            for thread_id, m in reply_candidates:
                _launch(m, thread_id=thread_id, parent_id_for_reaction=thread_id)

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
