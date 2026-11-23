"""SlackRunRequestSensorComponent.

Slack analog of `teams_run_request_sensor` -- replaces the person who reads a Slack
channel for "can you run X" messages and manually launches the job. No shared code with
the Teams version (per this repo's no-shared-helper-files convention), but the same
design decisions: deterministic phrase matching (not an LLM) and a requester must already
be a listed owner of the target before anything launches.

## Why authorization is load-bearing, not optional

Anyone can post to a Slack channel. Without a real check, this component would let any
channel member launch production jobs by typing a sentence. Every match is checked
against the TARGET'S OWN `owners` catalog metadata before a RunRequest is ever created:

- An `asset_selection` trigger's owners are read LIVE from the matched asset(s)' own
  `owners` field (the same `owners:` every asset in this repo already declares -- team
  names or email addresses).
- A `job_name` trigger has no asset metadata to check, so it requires an explicit
  `owners:` list on the trigger itself instead -- there is no implicit "anyone can run
  this" default for either shape.

## Command matching is deterministic, not an LLM

Each trigger lists plain-text `phrases`. A message matches a trigger when any of its
phrases appears (case-insensitively by default) anywhere in the message text. No
intent-classification model is used on purpose -- a sentence that silently launches a job
either matches a configured phrase exactly or it doesn't.

## Auth -- a bot token, much simpler than the Teams version

Unlike Microsoft Graph (where sending/reading a channel message has no working app-only
permission and needs a one-time delegated OAuth consent -- see `teams_run_request_sensor`
and `teams_approval_gate`), Slack's own bot-token model just works: install an app to the
workspace, grant it scopes, export its `xoxb-...` token. No refresh dance, nothing to
persist between polls.

**One-time setup:**
1. Create a Slack app at https://api.slack.com/apps (or reuse the one `slack_approval_gate`
   / `slack_message_send` already uses in this workspace).
2. Under OAuth & Permissions, add Bot Token Scopes: `channels:history` (public channel) or
   `groups:history` (private channel), `chat:write` (unless `post_acknowledgement: false`),
   and `users:read.email` (unless `resolve_requester_email: false`).
3. Install the app to the workspace and invite the bot to the target channel (a bot token
   can only read conversations it's a member of).
4. Export `export SLACK_BOT_TOKEN=xoxb-...` (or your own configured env var name).

## What this does NOT do

- **Verify `team:`-prefixed owners.** An asset's `owners:` may list `"team:analytics"` --
  there is no Slack-side group-membership check here. Only an exact email or literal
  Slack user id (`U0123ABCD`) in an owners list can ever authorize a chat-triggered run;
  a `team:`-only owners list means no one can trigger that target from chat (fails closed).
- **Replay channel history.** `process_backlog_on_first_run: false` (the default) seeds
  the cursor to "now" the first time this sensor ever ticks.
- **React to thread replies or bot/system messages.** `conversations.history` returns
  channel root messages only (thread replies -- including this sensor's own
  acknowledgement replies -- are excluded by default), and any message carrying a
  `subtype` or a `bot_id` is skipped, so this sensor can't re-trigger on its own posts.
"""
import os
import random
import re
import time
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


def _now_slack_ts() -> str:
    """Slack timestamps are decimal-seconds strings ('1700000000.123456');
    this is a synthetic cursor seed, not a real message ts."""
    return f"{time.time():.6f}"


def _get_slack_client(bot_token_env_var: str):
    """Deferred import so anything can load the component; only fails at
    actual runtime if the SDK isn't installed. Same shape as
    slack_approval_gate's own `_get_slack_client` -- inlined fresh copy,
    not a shared import, per this repo's convention."""
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError(
            "slack_run_request_sensor requires slack_sdk: pip install 'slack_sdk>=3.0'"
        ) from e

    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(
            f"Slack bot token env var {bot_token_env_var!r} not set. "
            f"Create a bot at https://api.slack.com/apps with the "
            f"`channels:history` (or `groups:history`) + `chat:write` + "
            f"`users:read.email` scopes, install it to your workspace, invite it "
            f"to the target channel, and export the `xoxb-...` token."
        )
    return WebClient(token=token)


def _list_new_messages(client, channel_id: str, oldest_ts: str, limit: int, max_pages: int) -> List[Dict[str, Any]]:
    """Messages strictly after `oldest_ts` (Slack's own `oldest` filter is exclusive by
    default, confirmed against Slack's own API reference -- unlike Microsoft Graph's
    channel-messages endpoint, which has no server-side timestamp filter at all).
    Paginates via `response_metadata.next_cursor` up to `max_pages`."""
    out: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    pages = 0
    while pages < max(max_pages, 1):
        resp = client.conversations_history(
            channel=channel_id, oldest=oldest_ts, limit=min(max(limit, 1), 200),
            cursor=cursor,
        )
        if not resp.get("ok"):
            raise RuntimeError(f"Slack conversations_history failed: {resp.get('error')}")
        out.extend(resp.get("messages", []) or [])
        cursor = (resp.get("response_metadata") or {}).get("next_cursor") or None
        pages += 1
        if not cursor:
            break
    return out


def _resolve_user_email(client, user_id: str) -> Optional[str]:
    """`users.info` -> profile.email. Needs `users:read.email`; any failure (missing
    scope, user not found, ...) degrades to None rather than raising, so a missing scope
    just skips email-matching instead of crashing the whole sensor tick."""
    try:
        resp = client.users_info(user=user_id)
        if not resp.get("ok"):
            return None
        return ((resp.get("user") or {}).get("profile") or {}).get("email")
    except Exception:  # noqa: BLE001 -- see docstring
        return None


def _post_slack_reply(client, channel_id: str, thread_ts: str, text: str) -> None:
    resp = client.chat_postMessage(channel=channel_id, thread_ts=thread_ts, text=text)
    if not resp.get("ok"):
        raise RuntimeError(f"Slack chat_postMessage failed: {resp.get('error')}")


# ── Dagster UI link recognition -- so "can someone run this: <link>" works as well as a
# typed phrase. Same regexes as teams_run_request_sensor's own copy (inlined fresh, not
# shared) -- extracted directly from dagster-webserver's own real route/link-building
# source, not guessed: an asset's link is `/assets/<key-path-segments, each
# encodeURIComponent'd>` (assetDetailsPathForKey.tsx, no location prefix -- the global
# asset catalog); a job's link is `/locations/<repoPath>/jobs/<jobName>` or the legacy
# `/pipelines/<jobName>` (workspacePath.ts's workspacePipelinePath). The excluded-char set
# also stops at '|' and '>', which is what makes this work against Slack's own link
# markup too: a pasted URL is auto-wrapped as '<https://.../assets/nightly/etl|label>'.
_ASSET_LINK_RE = re.compile(r"/assets/([^\s?\"'<>|)]+)")
_JOB_LINK_RE = re.compile(r"/locations/[^/\s]+/(?:jobs|pipelines)/([^/\s?\"'<>|)]+)")


def _extract_dagster_link_targets(text: str) -> List["tuple[str, str]"]:
    """Returns [(\"asset\", \"nightly/etl\"), (\"job\", \"nightly_etl_job\"), ...] for every
    Dagster UI asset/job link found in `text`, in the order they appear."""
    from urllib.parse import unquote

    out: List["tuple[str, str]"] = []
    for m in _ASSET_LINK_RE.finditer(text or ""):
        segs = [unquote(s) for s in m.group(1).split("/") if s]
        if segs:
            out.append(("asset", "/".join(segs)))
    for m in _JOB_LINK_RE.finditer(text or ""):
        out.append(("job", unquote(m.group(1))))
    return out


def _render_template(template: Union[str, List[str]], **kwargs: Any) -> str:
    """A list picks one entry at random each call, so a reply doesn't feel copy-pasted;
    a bare string renders every time, unchanged."""
    tpl = random.choice(template) if isinstance(template, list) else template
    return tpl.format(**kwargs)


# ── Sibling-asset discovery + selection DSL resolution -- same shape as
# asset_to_job_trigger_sensor's / teams_run_request_sensor's own copies (inlined fresh per
# DCC's no-shared-helper convention, not imported). ──
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
        logging.getLogger(__name__).warning(f"Could not discover sibling assets; asset_selection triggers will match nothing ({type(_exc).__name__}: {_exc})")
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
            logging.getLogger(__name__).warning(f"Could not resolve the asset selection; falling back to key matching ({type(_exc).__name__}: {_exc})")
    return [k for k in discovered_keys if fnmatch.fnmatch(k, selection)]


def _owners_by_key(sibling_defs: Optional[dg.Definitions]) -> Dict[str, List[str]]:
    """{asset key string: that asset's own `owners` catalog metadata} -- the live source
    of truth an asset_selection trigger is authorized against."""
    owners_map: Dict[str, List[str]] = {}
    if sibling_defs is None:
        return owners_map
    try:
        for assets_def in sibling_defs.assets:
            for spec in assets_def.specs:
                owners_map[spec.key.to_user_string()] = list(spec.owners or [])
    except Exception:  # noqa: BLE001 -- a malformed sibling def just yields no owners, not a crash
        pass
    return owners_map


class TriggerSpec(dg.Model, dg.Resolvable):
    """One phrase-to-target mapping this sensor recognizes in a channel message."""

    phrases: List[str] = Field(
        description=(
            "Case-insensitive substrings (unless case_sensitive_phrases=true) that identify "
            "this trigger in a message's text, e.g. ['run the nightly etl', 'kick off nightly']. "
            "The first trigger (in list order) with a phrase appearing anywhere in the message "
            "wins -- list more specific triggers first if phrases could overlap."
        )
    )
    asset_selection: Optional[Union[str, List[str]]] = Field(
        default=None,
        description=(
            "Materialize this asset selection when matched: explicit key list, the Dagster "
            "selection DSL (tag:/group:/kind:/boolean composition), '*', or a bare fnmatch "
            "glob -- resolved against sibling assets in the same defs folder. Owners are read "
            "LIVE from the matched assets' own `owners` catalog metadata; do not also set "
            "`owners` on this trigger."
        ),
    )
    job_name: Optional[str] = Field(
        default=None,
        description=(
            "Run this named job when matched, instead of an asset_selection -- for job-shaped "
            "targets with no asset metadata of their own. Requires `owners` below: a bare job "
            "has no Dagster metadata to authorize against."
        ),
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description=(
            "Email addresses (or literal Slack user ids, e.g. 'U0123ABCD') allowed to trigger "
            "`job_name` from chat. Required when job_name is set -- there is no implicit "
            "default. Ignored for asset_selection triggers, whose owners come from the matched "
            "assets' own catalog metadata instead."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_shapes(cls, data):
        if not isinstance(data, dict):
            return data
        out = dict(data)
        for k in ("phrases", "owners"):
            v = out.get(k)
            if isinstance(v, str):
                out[k] = [v]
        return out

    @model_validator(mode="after")
    def _check_shape(self):
        if bool(self.asset_selection) == bool(self.job_name):
            raise ValueError(f"TriggerSpec {self.phrases!r}: set exactly one of asset_selection or job_name.")
        if self.job_name and not self.owners:
            raise ValueError(
                f"TriggerSpec {self.phrases!r}: job_name requires a non-empty owners list "
                f"(no asset metadata exists to authorize a bare job against)."
            )
        return self


class SlackRunRequestSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Polls a Slack channel for chat-requested job runs and launches them for authorized
    owners only. See this module's docstring for the full design rationale."""

    sensor_name: str = Field(description="Unique sensor name.")
    slack_channel_id: str = Field(
        description=(
            "Channel ID to poll (e.g. 'C0123ABCDEF') -- NOT a '#channel-name'; "
            "conversations.history requires the real id. Find it via the channel's "
            "'View channel details' panel in Slack."
        ),
    )
    triggers: List[TriggerSpec] = Field(description="Phrase-to-target mappings this sensor recognizes. At least one required.")

    slack_bot_token_env_var: str = Field(
        default="SLACK_BOT_TOKEN",
        description="Env var holding the bot's 'xoxb-...' token. Needs channels:history (or groups:history) + chat:write + users:read.email scopes -- see this file's module docstring, 'Auth'.",
    )

    require_owner_authorization: bool = Field(
        default=True,
        description=(
            "Require the message sender to be a listed owner of the matched target before "
            "launching it. THIS IS THE SAFETY CHECK THIS COMPONENT EXISTS TO ENFORCE -- setting "
            "it to false means ANY channel member can launch ANY configured trigger's target. "
            "Only disable it for a channel where every member is already trusted with every "
            "configured target."
        ),
    )
    resolve_requester_email: bool = Field(
        default=True,
        description=(
            "Resolve the message sender's Slack user id to an email address via users.info "
            "(needs users:read.email) before checking owners -- most owners: lists use emails, "
            "not Slack user ids. When false, only a literal Slack user id in an owners list can "
            "ever match."
        ),
    )
    require_all_owners: bool = Field(
        default=False,
        description=(
            "For an asset_selection trigger matching MULTIPLE assets with different owners: "
            "false (default) authorizes the requester if they own ANY matched asset; true "
            "requires them to own EVERY matched asset. Ignored for job_name triggers."
        ),
    )
    case_sensitive_phrases: bool = Field(default=False, description="Match trigger phrases case-sensitively. Default: case-insensitive.")
    match_dagster_links: bool = Field(
        default=True,
        description=(
            "Also match a message that contains a link straight to the target asset or job in "
            "the Dagster UI (e.g. pasted via 'Copy link'), not just a typed phrase -- same "
            "owners-authorization gate either way. Recognizes the real dagster-webserver URL "
            "shapes: an asset's '/assets/<key-path>' (no location prefix needed) and a job's "
            "'/locations/<repoPath>/jobs/<name>' (or legacy '/pipelines/<name>'). Matching is by "
            "URL PATH SHAPE only -- it does not verify the link points at this Dagster instance, "
            "so a same-named target on a different deployment would also match; the owners gate "
            "still applies regardless."
        ),
    )

    messages_per_poll: int = Field(default=200, description="Messages to request per Slack page (Slack's own recommended max is 200).")
    max_pages_per_poll: int = Field(default=3, description="Follow at most this many pagination cursors per poll -- bounds how far back a single tick can catch up after an outage.")
    process_backlog_on_first_run: bool = Field(
        default=False,
        description="If false (default), the first-ever tick seeds the cursor to 'now' and processes nothing -- activating this sensor never replays a channel's past run requests. Set true only if that replay is actually wanted.",
    )

    post_acknowledgement: bool = Field(default=True, description="Reply in-thread when a match is authorized and launched, and (per on_unauthorized) when it's rejected. Needs chat:write.")
    on_unauthorized: Literal["skip", "reply"] = Field(default="reply", description="'reply' (default) posts unauthorized_message_template as a threaded reply; 'skip' just logs it. Ignored when post_acknowledgement is false.")
    ack_message_template: Union[str, List[str]] = Field(
        default="✅ Got it — launching *{target}* for you.",
        description="Threaded reply posted on an authorized match. Placeholders: {target}, {requester}, {phrase}. Give a list instead of a single string to have one chosen at random each time, so replies don't feel copy-pasted.",
    )
    unauthorized_message_template: Union[str, List[str]] = Field(
        default="⚠️ Sorry, you're not listed as an owner of *{target}*, so I can't launch it from chat. Ask an owner to run it, or ask to be added as one.",
        description="Threaded reply posted on an unauthorized match (on_unauthorized: reply). Placeholders: {target}, {requester}, {phrase}. Also accepts a list for a randomly-chosen reply, same as ack_message_template.",
    )

    minimum_interval_seconds: int = Field(default=30, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED' -- initial status of the sensor.")
    skip_in_branch_deployments: bool = Field(
        default=True,
        description=(
            "Skip every tick while running inside a Dagster+ branch deployment "
            "(DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=1). A branch deployment runs its own copy of every sensor "
            "alongside the full deployment's -- without this, a branch deployment double-acknowledges the "
            "same message and double-launches the matched job/asset selection."
        ),
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Slack Run Request Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.triggers:
            raise ValueError(f"SlackRunRequestSensorComponent {self.sensor_name!r}: triggers must be non-empty.")
        if self.default_status.upper() not in ("RUNNING", "STOPPED"):
            raise ValueError(f"SlackRunRequestSensorComponent {self.sensor_name!r}: default_status must be 'RUNNING' or 'STOPPED'.")

        discovered_keys, sibling_defs = _discover_sibling_assets(context)
        owners_by_key = _owners_by_key(sibling_defs)

        resolved_triggers: List[Dict[str, Any]] = []
        all_asset_keys: List[str] = []
        for t in self.triggers:
            phrases = t.phrases if self.case_sensitive_phrases else [p.lower() for p in t.phrases]
            if t.asset_selection is not None:
                keys = _resolve_selection(t.asset_selection, discovered_keys, sibling_defs)
                if not keys:
                    raise ValueError(
                        f"SlackRunRequestSensorComponent {self.sensor_name!r}: trigger {t.phrases!r}'s "
                        f"asset_selection {t.asset_selection!r} matched no assets (discovered "
                        f"{len(discovered_keys)} sibling asset(s))."
                    )
                all_asset_keys.extend(keys)
                resolved_triggers.append({
                    "phrases": phrases,
                    "mode": "asset",
                    "keys": keys,
                    "owner_sets": [set(owners_by_key.get(k, [])) for k in keys],
                    "label": t.asset_selection if isinstance(t.asset_selection, str) else "/".join(keys),
                })
            else:
                resolved_triggers.append({
                    "phrases": phrases,
                    "mode": "job",
                    "job_name": t.job_name,
                    "owner_sets": [set(t.owners or [])],
                    "label": t.job_name,
                })

        sensor_name = self.sensor_name
        channel_id = self.slack_channel_id
        bot_token_env_var = self.slack_bot_token_env_var
        require_owner_authorization = self.require_owner_authorization
        resolve_requester_email = self.resolve_requester_email
        require_all_owners = self.require_all_owners
        case_sensitive = self.case_sensitive_phrases
        match_dagster_links = self.match_dagster_links
        messages_per_poll = self.messages_per_poll
        max_pages_per_poll = self.max_pages_per_poll
        process_backlog_on_first_run = self.process_backlog_on_first_run
        post_acknowledgement = self.post_acknowledgement
        on_unauthorized = self.on_unauthorized
        ack_template = self.ack_message_template
        unauthorized_template = self.unauthorized_message_template
        skip_in_branch_deployments = self.skip_in_branch_deployments
        default_status = dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED
        sensor_asset_selection = [dg.AssetKey(k.split("/")) for k in sorted(set(all_asset_keys))] or None

        def _match_trigger_or_link(text_lower: str, link_text: str) -> "tuple[Optional[Dict[str, Any]], str]":
            for tr in resolved_triggers:
                if any(p in text_lower for p in tr["phrases"]):
                    return tr, next(p for p in tr["phrases"] if p in text_lower)
            if match_dagster_links:
                targets = _extract_dagster_link_targets(link_text)
                if targets:
                    for tr in resolved_triggers:
                        for kind, ident in targets:
                            if (tr["mode"] == "asset" and kind == "asset" and ident in tr["keys"]) or (
                                tr["mode"] == "job" and kind == "job" and ident == tr["job_name"]
                            ):
                                return tr, f"(linked: {ident})"
            return None, ""

        def _is_authorized(tr: Dict[str, Any], sender_id: str, client, email_cache: Dict[str, Optional[str]]) -> bool:
            identity = {sender_id.lower()} if sender_id else set()
            if resolve_requester_email and sender_id:
                if sender_id not in email_cache:
                    email_cache[sender_id] = _resolve_user_email(client, sender_id)
                email = email_cache[sender_id]
                if email:
                    identity.add(email.lower())
            owner_sets = tr["owner_sets"]
            checks = (identity & {o.lower() for o in s} for s in owner_sets)
            return all(checks) if require_all_owners else any(checks)

        def sensor_fn(context: dg.SensorEvaluationContext):
            if skip_in_branch_deployments and os.environ.get("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1":
                return dg.SkipReason("branch deployment -- skipping (skip_in_branch_deployments=True)")
            try:
                client = _get_slack_client(bot_token_env_var)
            except Exception as e:  # noqa: BLE001 -- surfaced as a skip, not a sensor crash
                return dg.SkipReason(f"Slack auth failed: {e}")

            if not context.cursor:
                if not process_backlog_on_first_run:
                    return dg.SensorResult(cursor=_now_slack_ts(), skip_reason="First tick: seeding cursor to now (process_backlog_on_first_run=false) -- no channel history processed.")
                last_ts = "0"
            else:
                last_ts = context.cursor

            try:
                messages = _list_new_messages(client, channel_id, last_ts, messages_per_poll, max_pages_per_poll)
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Slack conversations_history failed: {e}")

            if not messages:
                return dg.SensorResult(cursor=last_ts, skip_reason="No new channel messages since last poll.")

            candidates = sorted(
                (m for m in messages if m.get("user") and not m.get("subtype") and not m.get("bot_id")),
                key=lambda m: float(m["ts"]),
            )
            new_cursor = max([m["ts"] for m in messages] + [last_ts], key=float)

            if not candidates:
                return dg.SensorResult(cursor=new_cursor, skip_reason="No new human-authored channel messages since last poll.")

            run_requests: List[dg.RunRequest] = []
            notes: List[str] = []
            email_cache: Dict[str, Optional[str]] = {}

            for m in candidates:
                text = m.get("text") or ""
                haystack = text if case_sensitive else text.lower()
                tr, phrase = _match_trigger_or_link(haystack, text)
                if tr is None:
                    continue

                sender_id = m.get("user") or ""

                if require_owner_authorization and not _is_authorized(tr, sender_id, client, email_cache):
                    notes.append(f"message {m.get('ts')}: sender {sender_id or '?'} is not an authorized owner of {tr['label']!r} -- skipped.")
                    if post_acknowledgement and on_unauthorized == "reply":
                        try:
                            _post_slack_reply(client, channel_id, m["ts"], _render_template(
                                unauthorized_template, target=tr["label"], requester=email_cache.get(sender_id) or sender_id, phrase=phrase,
                            ))
                        except Exception as e:  # noqa: BLE001 -- a failed courtesy reply must never block the gate
                            notes.append(f"message {m.get('ts')}: unauthorized-reply post failed: {e}")
                    continue

                run_key = f"{sensor_name}:{m.get('ts')}"
                if tr["mode"] == "job":
                    run_requests.append(dg.RunRequest(run_key=run_key, job_name=tr["job_name"]))
                else:
                    run_requests.append(dg.RunRequest(run_key=run_key, asset_selection=[dg.AssetKey(k.split("/")) for k in tr["keys"]]))

                if post_acknowledgement:
                    try:
                        _post_slack_reply(client, channel_id, m["ts"], _render_template(
                            ack_template, target=tr["label"], requester=email_cache.get(sender_id) or sender_id, phrase=phrase,
                        ))
                    except Exception as e:  # noqa: BLE001 -- a failed courtesy reply must never block the launch
                        notes.append(f"message {m.get('ts')}: ack-reply post failed: {e}")

            for n in notes:
                context.log.info(n)

            if not run_requests:
                return dg.SensorResult(cursor=new_cursor, skip_reason="; ".join(notes) or "No authorized matches this poll.")
            return dg.SensorResult(run_requests=run_requests, cursor=new_cursor)

        sensor = dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=default_status,
            asset_selection=sensor_asset_selection,
        )(sensor_fn)

        return dg.Definitions(sensors=[sensor])
