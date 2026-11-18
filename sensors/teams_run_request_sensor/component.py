"""TeamsRunRequestSensorComponent.

Replaces the person who reads a Teams channel for "can you run X" messages and manually
launches the job -- this sensor polls the channel itself and launches the matching
RunRequest, but only for a requester who is already an authorized owner of the target.

## Why authorization is load-bearing, not optional

Anyone can post to a Teams channel. Without a real check, this component would let any
channel member launch production jobs by typing a sentence. So every match is checked
against the TARGET'S OWN `owners` catalog metadata (the same `owners:` field every asset
in this repo already declares -- team names or email addresses) before a RunRequest is
ever created. A `job_name` target has no Dagster asset metadata to check, so it requires
an explicit `owners:` list on the trigger itself instead -- there is no implicit "anyone
can run this" default for either shape. See `require_owner_authorization` to understand
exactly what turning it off means before doing so.

## Command matching is deterministic, not an LLM

Each `TriggerSpec` lists plain-text `phrases` ("run the nightly etl", "kick off nightly").
A message matches a trigger when any of its phrases appears (case-insensitively by
default) anywhere in the message text. No intent-classification model is used on purpose
-- a sentence that silently launches a job either matches a configured phrase exactly or
it doesn't; an LLM "close enough" match would make the set of things a channel member can
trigger impossible to audit from the YAML alone.

## Auth -- a DELEGATED token, not app-only (same real constraint as `teams_approval_gate`)

Confirmed against Microsoft Graph's own docs: posting a channel message
(`POST /teams/{id}/channels/{id}/messages`) has no working application (app-only)
permission at all (only `Teamwork.Migrate.All`, scoped to migration tooling).
`ChannelMessage.Send` and `ChannelMessage.Read.All` as APPLICATION permissions also hit a
separate "Protected API" approval process Microsoft gates beyond normal tenant-admin
consent. The delegated versions of both need only ordinary admin consent, so -- like
`teams_approval_gate` -- this component authenticates as a dedicated "requests bot" Teams
account via a one-time interactive consent, then refreshes that token on every poll.

**One-time setup:**
1. Register an app in Entra ID (Azure AD) with **delegated** permissions `ChannelMessage.Read.All`
   (or the resource-specific `ChannelMessage.Read.Group`), `offline_access`, and -- unless
   `post_acknowledgement: false` -- `ChannelMessage.Send`. Add `User.Read.All` too unless
   `resolve_requester_email: false` (needed to turn a message sender's Azure AD object id
   into the email address your `owners:` lists actually use).
2. Create a client secret for it.
3. Run an interactive OAuth consent ONCE, signed in as the dedicated bot account (a normal
   Teams user added to the target team/channel) -- e.g. via MSAL's device-code flow -- to
   capture the first refresh token.
4. Export `TEAMS_TENANT_ID` / `TEAMS_CLIENT_ID` / `TEAMS_CLIENT_SECRET` / `TEAMS_REFRESH_TOKEN`
   (or your own configured env var names).

Azure AD can rotate the refresh token on any refresh call -- `teams_refresh_token_env_var`
is only the INITIAL seed; once Microsoft returns a different one, it's persisted to
`<oauth_state_dir>/.teams_oauth_state.json` and preferred over the env var from then on.

## What this does NOT do

- **Verify `team:`-prefixed owners.** An asset's `owners:` may list `"team:analytics"` --
  there is no way to check Teams channel membership against a Dagster team name without a
  separate Azure AD group-membership lookup this component doesn't make. Only exact email
  / Azure AD object id owner entries can ever authorize a chat-triggered run; a
  `team:`-only owners list means NO ONE can trigger that target from chat (fails closed).
- **Replay channel history.** `process_backlog_on_first_run: false` (the default) seeds the
  cursor to "now" the first time this sensor ever ticks, so activating it doesn't suddenly
  launch every job anyone has ever asked for in that channel's history.
- **Catch more than one page of genuinely new messages per poll.** Microsoft Graph's
  channel-messages endpoint has no delta/incremental query and no `$filter` on
  `createdDateTime` (confirmed against Microsoft's own API reference) -- each poll re-lists
  the `messages_per_poll` most recent messages (max 50, Graph's own hard cap) and filters
  client-side by `createdDateTime > cursor`. Keep `minimum_interval_seconds` short enough
  that a channel never accumulates more than `messages_per_poll * max_pages_per_poll` new
  messages between polls, or genuinely new requests can be missed.
"""
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

_GRAPH = "https://graph.microsoft.com/v1.0"


# ── Inline fs helper: local + cloud (s3://, gs://, abfs://) via fsspec. Kept inline per
# DCC's self-contained convention -- same shape as teams_approval_gate's own copy, not a
# shared import. Only used to persist a rotated OAuth refresh token. ──
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
        import json
        if self.is_uri:
            with self.fs.open(p, "r") as f:
                return json.loads(f.read())
        return json.loads(Path(p).read_text())

    def write_json(self, p: str, obj: Any) -> None:
        import json
        body = json.dumps(obj, indent=2, default=str)
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
    """OAuth2 refresh-token grant against Azure AD -- a DELEGATED token, not app-only.
    See this module's docstring ("Auth") for why. Azure AD can rotate the refresh token on
    any call; the rotated value is persisted to `<oauth_state_dir>/.teams_oauth_state.json`
    and preferred over `refresh_token_env` on every later call."""
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
    """Lists the most recent messages in a channel -- the only read shape Graph supports here
    (confirmed: no delta query exists for channel messages, and `$filter`/`$orderby` on
    createdDateTime/lastModifiedDateTime aren't supported; only `$top` (Graph's own hard cap
    is 50) and pagination via `@odata.nextLink`)."""
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


def _resolve_user_email(bearer: str, aad_id: str) -> Optional[str]:
    """`from.user` on a chatMessage carries only an Azure AD object id + display name, never
    an email (confirmed against Graph's own chatMessage schema) -- this is the extra call
    that turns it into the email address an `owners:` list actually uses. Needs delegated
    `User.Read.All`; any failure (permission not granted, user not found, ...) degrades to
    None rather than raising, so a missing permission skips email-matching instead of
    crashing the whole sensor tick."""
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


def _post_teams_reply(bearer: str, team_id: str, channel_id: str, message_id: str, html_body: str) -> None:
    import requests
    r = requests.post(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"body": {"contentType": "html", "content": html_body}},
        timeout=30,
    )
    r.raise_for_status()


def _strip_html(html: str) -> str:
    return re.sub(r"<[^>]+>", " ", html or "").strip()


# ── Sibling-asset discovery + selection DSL resolution -- same shape as
# asset_to_job_trigger_sensor's own copy (inlined fresh per DCC's no-shared-helper
# convention, not imported). ──
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
    """{asset key string: that asset's own `owners` catalog metadata} -- this is the live
    source of truth an asset_selection trigger is authorized against, read straight off each
    matched asset's AssetSpec, never duplicated into this component's own config."""
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
            "targets with no asset metadata of their own (e.g. a job wrapping a bare op). "
            "Requires `owners` below: a bare job has no Dagster metadata to authorize against."
        ),
    )
    owners: Optional[List[str]] = Field(
        default=None,
        description=(
            "Email addresses (or literal Azure AD object ids) allowed to trigger `job_name` "
            "from chat. Required when job_name is set -- there is no implicit default. Ignored "
            "for asset_selection triggers, whose owners come from the matched assets' own "
            "catalog metadata instead."
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


class TeamsRunRequestSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """Polls a Teams channel for chat-requested job runs and launches them for authorized
    owners only. See this module's docstring for the full design rationale."""

    sensor_name: str = Field(description="Unique sensor name.")
    teams_team_id: str = Field(description="Teams team id (Teams client -> team -> ... -> Get link).")
    teams_channel_id: str = Field(description="Teams channel id inside the team (e.g. '19:xxx@thread.tacv2').")
    triggers: List[TriggerSpec] = Field(description="Phrase-to-target mappings this sensor recognizes. At least one required.")
    oauth_state_dir: str = Field(
        description=(
            "Local path or fsspec URI (s3://, gs://, abfs://) dedicated to this sensor's "
            "'.teams_oauth_state.json' -- holds a rotated OAuth refresh token, if Microsoft "
            "ever issues one. Without a durable place to persist it, the next rotation breaks "
            "this sensor's auth entirely."
        ),
    )

    teams_tenant_id_env_var: str = Field(default="TEAMS_TENANT_ID", description="Env var holding the Azure AD tenant id.")
    teams_client_id_env_var: str = Field(default="TEAMS_CLIENT_ID", description="Env var holding the Azure AD app registration client id.")
    teams_client_secret_env_var: str = Field(default="TEAMS_CLIENT_SECRET", description="Env var holding the Azure AD app registration client secret.")
    teams_refresh_token_env_var: str = Field(
        default="TEAMS_REFRESH_TOKEN",
        description="Env var holding the INITIAL OAuth refresh token (one-time interactive consent for the dedicated bot account -- see this file's module docstring, 'Auth'). Only the seed; a later rotation is persisted to oauth_state_dir instead.",
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
            "Resolve the message sender's Azure AD object id to an email address via "
            "'GET /users/{id}' (needs delegated User.Read.All) before checking owners -- most "
            "owners: lists use emails, not Azure AD object ids. When false, only a literal "
            "Azure AD object id in an owners list can ever match."
        ),
    )
    require_all_owners: bool = Field(
        default=False,
        description=(
            "For an asset_selection trigger matching MULTIPLE assets with different owners: "
            "false (default) authorizes the requester if they own ANY matched asset; true "
            "requires them to own EVERY matched asset. Ignored for job_name triggers (one "
            "owners list, not per-asset)."
        ),
    )
    case_sensitive_phrases: bool = Field(default=False, description="Match trigger phrases case-sensitively. Default: case-insensitive.")

    messages_per_poll: int = Field(default=50, description="Messages to request per Graph page (Graph's own hard cap is 50).")
    max_pages_per_poll: int = Field(default=3, description="Follow at most this many '@odata.nextLink' pages per poll -- bounds how far back a single tick can catch up after an outage.")
    process_backlog_on_first_run: bool = Field(
        default=False,
        description="If false (default), the first-ever tick seeds the cursor to 'now' and processes nothing -- activating this sensor never replays a channel's past run requests. Set true only if that replay is actually wanted.",
    )

    post_acknowledgement: bool = Field(default=True, description="Reply in-thread when a match is authorized and launched, and (per on_unauthorized) when it's rejected. Needs delegated ChannelMessage.Send.")
    on_unauthorized: Literal["skip", "reply"] = Field(default="reply", description="'reply' (default) posts unauthorized_message_template as a thread reply; 'skip' just logs it. Ignored when post_acknowledgement is false.")
    ack_message_template: str = Field(
        default="✅ Got it — launching <b>{target}</b> for you.",
        description="HTML reply posted on an authorized match. Placeholders: {target}, {requester}, {phrase}.",
    )
    unauthorized_message_template: str = Field(
        default="⚠️ Sorry, you're not listed as an owner of <b>{target}</b>, so I can't launch it from chat. Ask an owner to run it, or ask to be added as one.",
        description="HTML reply posted on an unauthorized match (on_unauthorized: reply). Placeholders: {target}, {requester}, {phrase}.",
    )

    minimum_interval_seconds: int = Field(default=30, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED' -- initial status of the sensor.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Teams Run Request Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.triggers:
            raise ValueError(f"TeamsRunRequestSensorComponent {self.sensor_name!r}: triggers must be non-empty.")
        if self.default_status.upper() not in ("RUNNING", "STOPPED"):
            raise ValueError(f"TeamsRunRequestSensorComponent {self.sensor_name!r}: default_status must be 'RUNNING' or 'STOPPED'.")

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
                        f"TeamsRunRequestSensorComponent {self.sensor_name!r}: trigger {t.phrases!r}'s "
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
        team_id = self.teams_team_id
        channel_id = self.teams_channel_id
        tenant_env, client_env, secret_env, refresh_env = (
            self.teams_tenant_id_env_var, self.teams_client_id_env_var,
            self.teams_client_secret_env_var, self.teams_refresh_token_env_var,
        )
        oauth_state_dir = self.oauth_state_dir
        require_owner_authorization = self.require_owner_authorization
        resolve_requester_email = self.resolve_requester_email
        require_all_owners = self.require_all_owners
        case_sensitive = self.case_sensitive_phrases
        messages_per_poll = self.messages_per_poll
        max_pages_per_poll = self.max_pages_per_poll
        process_backlog_on_first_run = self.process_backlog_on_first_run
        post_acknowledgement = self.post_acknowledgement
        on_unauthorized = self.on_unauthorized
        ack_template = self.ack_message_template
        unauthorized_template = self.unauthorized_message_template
        default_status = dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED
        sensor_asset_selection = [dg.AssetKey(k.split("/")) for k in sorted(set(all_asset_keys))] or None

        def _match_trigger(text_lower: str) -> Optional[Dict[str, Any]]:
            for tr in resolved_triggers:
                if any(p in text_lower for p in tr["phrases"]):
                    return tr
            return None

        def _is_authorized(tr: Dict[str, Any], sender_id: str, bearer: str, email_cache: Dict[str, Optional[str]]) -> bool:
            identity = {sender_id.lower()} if sender_id else set()
            if resolve_requester_email and sender_id:
                if sender_id not in email_cache:
                    email_cache[sender_id] = _resolve_user_email(bearer, sender_id)
                email = email_cache[sender_id]
                if email:
                    identity.add(email.lower())
            owner_sets = tr["owner_sets"]
            checks = (identity & {o.lower() for o in s} for s in owner_sets)
            return all(checks) if require_all_owners else any(checks)

        def sensor_fn(context: dg.SensorEvaluationContext):
            fs = _StateFS(oauth_state_dir)
            try:
                bearer = _get_teams_bearer(fs, tenant_env, client_env, secret_env, refresh_env)
            except Exception as e:  # noqa: BLE001 -- surfaced as a skip, not a sensor crash
                return dg.SkipReason(f"Teams auth failed: {e}")

            if not context.cursor:
                if not process_backlog_on_first_run:
                    return dg.SensorResult(cursor=_now_iso(), skip_reason="First tick: seeding cursor to now (process_backlog_on_first_run=false) -- no channel history processed.")
                last_ts = "1970-01-01T00:00:00.0000000Z"
            else:
                last_ts = context.cursor

            try:
                messages = _list_channel_messages(bearer, team_id, channel_id, messages_per_poll, max_pages_per_poll)
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Teams list-messages failed: {e}")

            if not messages:
                return dg.SkipReason("No channel messages returned.")

            new_cursor = max([m.get("createdDateTime", "") for m in messages] + [last_ts])
            candidates = sorted(
                (m for m in messages if m.get("createdDateTime", "") > last_ts and (m.get("from") or {}).get("user")),
                key=lambda m: m["createdDateTime"],
            )
            if not candidates:
                return dg.SensorResult(cursor=new_cursor, skip_reason="No new channel messages since last poll.")

            run_requests: List[dg.RunRequest] = []
            notes: List[str] = []
            email_cache: Dict[str, Optional[str]] = {}

            for m in candidates:
                text = _strip_html((m.get("body") or {}).get("content") or "")
                haystack = text if case_sensitive else text.lower()
                tr = _match_trigger(haystack)
                if tr is None:
                    continue

                sender = (m.get("from") or {}).get("user") or {}
                sender_id = sender.get("id") or ""
                phrase = next((p for p in tr["phrases"] if p in haystack), "")

                if require_owner_authorization and not _is_authorized(tr, sender_id, bearer, email_cache):
                    notes.append(f"message {m.get('id')}: sender {sender_id or '?'} is not an authorized owner of {tr['label']!r} -- skipped.")
                    if post_acknowledgement and on_unauthorized == "reply":
                        try:
                            _post_teams_reply(bearer, team_id, channel_id, m["id"], unauthorized_template.format(
                                target=tr["label"], requester=email_cache.get(sender_id) or sender_id, phrase=phrase,
                            ))
                        except Exception as e:  # noqa: BLE001 -- a failed courtesy reply must never block the gate
                            notes.append(f"message {m.get('id')}: unauthorized-reply post failed: {e}")
                    continue

                run_key = f"{sensor_name}:{m.get('id')}"
                if tr["mode"] == "job":
                    run_requests.append(dg.RunRequest(run_key=run_key, job_name=tr["job_name"]))
                else:
                    run_requests.append(dg.RunRequest(run_key=run_key, asset_selection=[dg.AssetKey(k.split("/")) for k in tr["keys"]]))

                if post_acknowledgement:
                    try:
                        _post_teams_reply(bearer, team_id, channel_id, m["id"], ack_template.format(
                            target=tr["label"], requester=email_cache.get(sender_id) or sender_id, phrase=phrase,
                        ))
                    except Exception as e:  # noqa: BLE001 -- a failed courtesy reply must never block the launch
                        notes.append(f"message {m.get('id')}: ack-reply post failed: {e}")

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
