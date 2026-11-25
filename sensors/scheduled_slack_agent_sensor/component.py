"""ScheduledSlackAgentSensorComponent.

A scheduled agent run that announces itself in Slack. Every `period_hours` it posts
`announce_text` to a channel, then starts the agent run tagged `slack/channel`,
`slack/thread_ts` (the ts of the message it just posted) and `slack/message_ts`, so a downstream
`slack_reply` asset answers in that thread.

This is the scheduled counterpart of `slack_agent_request_sensor`: same run tags, but the trigger
is the clock instead of a person's message.

## Timing

The sensor keeps `last_started_ts` in its cursor. A tick inside the period skips and says how long
is left. On the very first tick the period starts at "now": the first run is
`first_run_delay_hours` later (default: one full period), so activating the sensor or deploying
never fires a run immediately. Set `first_run_delay_hours: 0` to fire on the first tick.

## Post before launch, record only after Slack accepts

The announcement is posted first, because the run needs its ts. If Slack rejects the post, no run
is started and the cursor does not move, so the next tick retries. Once the post succeeded the
cursor is advanced in the same tick as the RunRequest, and the run_key is derived from the posted ts,
so a run is never started twice for one announcement.
"""
import json
import os
import time
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
from pydantic import Field, model_validator


# BEGIN announcement notifier (inlined per component; keep copies identical -- see
# tests/test_announcement_notifier_drift.py). Posts one announcement to either Slack or Microsoft
# Teams and returns the thread-identifying tags the matching reply asset (slack_reply / teams_reply)
# already reads -- platform dispatch only, no new auth: Slack uses slack_sdk's WebClient (unchanged
# from this component's own prior behavior); Teams uses the same delegated-OAuth2 refresh-token
# Graph bearer flow teams_agent_request_sensor/teams_approval_gate already use, minus refresh-token-
# rotation persistence (an announcement-only sensor has no existing state directory to put it in --
# a rotated token is simply re-derived from the seed on the next call, same as teams_approval_gate's
# own behavior when no approval_dir is configured).
class AnnouncementPostError(Exception):
    pass


def _get_slack_client_for_announcement(bot_token_env_var: str):
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("requires slack_sdk: pip install 'slack_sdk>=3.0'") from e
    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(f"Slack bot token env var {bot_token_env_var!r} not set.")
    return WebClient(token=token)


def _get_teams_bearer_for_announcement(tenant_id_env: str, client_id_env: str, client_secret_env: str, refresh_token_env: str) -> str:
    import requests
    tenant_id = os.environ.get(tenant_id_env)
    client_id = os.environ.get(client_id_env)
    client_secret = os.environ.get(client_secret_env)
    refresh_token = os.environ.get(refresh_token_env)
    if not (tenant_id and client_id and client_secret and refresh_token):
        raise RuntimeError(
            f"Teams auth env vars not all set: {tenant_id_env}={'present' if tenant_id else 'missing'}, "
            f"{client_id_env}={'present' if client_id else 'missing'}, "
            f"{client_secret_env}={'present' if client_secret else 'missing'}, "
            f"{refresh_token_env}={'present' if refresh_token else 'missing'}."
        )
    r = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "client_id": client_id, "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default offline_access",
            "grant_type": "refresh_token", "refresh_token": refresh_token,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _post_teams_channel_message_for_announcement(bearer: str, team_id: str, channel_id: str, html_body: str) -> Dict[str, Any]:
    import requests
    r = requests.post(
        f"https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"body": {"contentType": "html", "content": html_body}},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _announcement_text_to_html(text: str) -> str:
    escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return escaped.replace("\n", "<br>")


def post_announcement(
    platform: str, text: str, *,
    slack_channel: Optional[str] = None, slack_bot_token_env_var: str = "SLACK_BOT_TOKEN",
    teams_team_id: Optional[str] = None, teams_channel_id: Optional[str] = None,
    teams_tenant_id_env_var: str = "TEAMS_TENANT_ID", teams_client_id_env_var: str = "TEAMS_CLIENT_ID",
    teams_client_secret_env_var: str = "TEAMS_CLIENT_SECRET", teams_refresh_token_env_var: str = "TEAMS_REFRESH_TOKEN",
) -> Dict[str, str]:
    """Posts one announcement via the given platform's client and returns the thread-identifying
    tags the matching reply asset (slack_reply / teams_reply) reads. Raises AnnouncementPostError
    with a platform-specific message on any failure."""
    if platform == "teams":
        if not (teams_team_id and teams_channel_id):
            raise AnnouncementPostError("platform='teams' requires teams_team_id and teams_channel_id.")
        try:
            bearer = _get_teams_bearer_for_announcement(
                teams_tenant_id_env_var, teams_client_id_env_var, teams_client_secret_env_var, teams_refresh_token_env_var,
            )
            posted = _post_teams_channel_message_for_announcement(bearer, teams_team_id, teams_channel_id, _announcement_text_to_html(text))
        except Exception as e:  # noqa: BLE001
            raise AnnouncementPostError(f"Teams post failed: {e}") from e
        return {"teams/team_id": teams_team_id, "teams/channel_id": teams_channel_id, "teams/thread_id": posted["id"]}

    if not slack_channel:
        raise AnnouncementPostError("platform='slack' requires slack_channel.")
    try:
        client = _get_slack_client_for_announcement(slack_bot_token_env_var)
        posted = client.chat_postMessage(channel=slack_channel, text=text)
    except Exception as e:  # noqa: BLE001
        raise AnnouncementPostError(f"Slack post failed: {e}") from e
    if not posted.get("ok"):
        raise AnnouncementPostError(f"Slack post failed: {posted.get('error')}")
    return {"slack/channel": posted["channel"], "slack/thread_ts": posted["ts"]}
# END announcement notifier


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


def _humanize(seconds: float) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    parts = [f"{d}d" if d else "", f"{h}h" if h else "", f"{m}m" if m or not (d or h) else ""]
    return " ".join(p for p in parts if p)


class ScheduledSlackAgentSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    sensor_name: str = Field(description="Unique sensor name.")
    platform: Literal["slack", "teams"] = Field(
        default="slack",
        description=(
            "Chat platform to announce each scheduled run on. 'teams' posts via Microsoft Graph "
            "instead of Slack and tags the run teams/team_id + teams/channel_id + teams/thread_id "
            "instead of slack/channel + slack/thread_ts + slack/message_ts, so teams_reply answers "
            "in it instead of slack_reply."
        ),
    )
    slack_channel_id: Optional[str] = Field(default=None, description="platform='slack' (required then). Channel ID to announce in (e.g. 'C0123ABCDEF') -- the real id, not '#name'.")
    teams_team_id: Optional[str] = Field(default=None, description="platform='teams' (required then). Teams team id (Teams client -> team -> ... -> Get link).")
    teams_channel_id: Optional[str] = Field(default=None, description="platform='teams' (required then). Teams channel id inside the team (e.g. '19:xxx@thread.tacv2').")
    teams_tenant_id_env_var: str = Field(default="TEAMS_TENANT_ID", description="platform='teams'. Env var holding the Azure AD tenant id.")
    teams_client_id_env_var: str = Field(default="TEAMS_CLIENT_ID", description="platform='teams'. Env var holding the Azure AD app registration client id.")
    teams_client_secret_env_var: str = Field(default="TEAMS_CLIENT_SECRET", description="platform='teams'. Env var holding the Azure AD app registration client secret.")
    teams_refresh_token_env_var: str = Field(default="TEAMS_REFRESH_TOKEN", description="platform='teams'. Env var holding the OAuth refresh token.")
    asset_selection: Union[str, List[str]] = Field(
        description=(
            "The agent asset (and anything downstream, e.g. a slack_reply asset) to run each period: explicit key "
            "list, the Dagster selection DSL, '*', or a bare fnmatch glob, resolved against sibling assets."
        ),
    )
    announce_text: str = Field(
        default=":bar_chart: Scheduled agent run started",
        description="Text of the top-level Slack message posted when a run starts. The agent's reply lands in this message's thread.",
    )
    period_hours: float = Field(default=168.0, description="Hours between runs (168 = weekly).")
    first_run_delay_hours: Optional[float] = Field(
        default=None,
        description=(
            "Hours after the first tick before the first run. Unset = one full period, so the period starts at "
            "'now' and a deploy never fires a run right away. 0 = fire on the first tick."
        ),
    )
    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's 'xoxb-...' token (needs chat:write; invite the bot to the channel).")
    extra_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional run tags, e.g. {'team': 'data-eng'}. slack/* tags are set by the sensor and win on a clash.")
    set_isolation_disabled_tag: bool = Field(default=False, description="Tag each run dagster/isolation: disabled (run in-process on the code server). Off by default: a scheduled run is not latency-sensitive.")
    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="STOPPED", description="'RUNNING' or 'STOPPED' -- initial status of the sensor.")
    skip_in_branch_deployments: bool = Field(
        default=True,
        description="Skip every tick inside a Dagster+ branch deployment (DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=1), which would otherwise announce and run a second copy.",
    )

    @model_validator(mode="after")
    def _validate_platform_fields(self):
        if self.platform == "teams":
            if not (self.teams_team_id and self.teams_channel_id):
                raise ValueError("ScheduledSlackAgentSensorComponent: platform='teams' requires teams_team_id and teams_channel_id.")
        elif not self.slack_channel_id:
            raise ValueError("ScheduledSlackAgentSensorComponent: platform='slack' requires slack_channel_id.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Scheduled Slack Agent Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.default_status.upper() not in ("RUNNING", "STOPPED"):
            raise ValueError(f"ScheduledSlackAgentSensorComponent {self.sensor_name!r}: default_status must be 'RUNNING' or 'STOPPED'.")
        if self.period_hours <= 0:
            raise ValueError(f"ScheduledSlackAgentSensorComponent {self.sensor_name!r}: period_hours must be > 0.")
        if self.first_run_delay_hours is not None and self.first_run_delay_hours < 0:
            raise ValueError(f"ScheduledSlackAgentSensorComponent {self.sensor_name!r}: first_run_delay_hours must be >= 0.")

        discovered_keys, sibling_defs = _discover_sibling_assets(context)
        keys = _resolve_selection(self.asset_selection, discovered_keys, sibling_defs)
        if not keys:
            raise ValueError(
                f"ScheduledSlackAgentSensorComponent {self.sensor_name!r}: asset_selection {self.asset_selection!r} "
                f"matched no assets (discovered {len(discovered_keys)} sibling asset(s))."
            )

        sensor_name = self.sensor_name
        platform = self.platform
        channel_id = self.slack_channel_id
        teams_team_id = self.teams_team_id
        teams_channel_id = self.teams_channel_id
        teams_tenant_id_env_var = self.teams_tenant_id_env_var
        teams_client_id_env_var = self.teams_client_id_env_var
        teams_client_secret_env_var = self.teams_client_secret_env_var
        teams_refresh_token_env_var = self.teams_refresh_token_env_var
        announce_text = self.announce_text
        period_seconds = self.period_hours * 3600.0
        first_delay_seconds = (self.first_run_delay_hours if self.first_run_delay_hours is not None else self.period_hours) * 3600.0
        bot_token_env_var = self.slack_bot_token_env_var
        extra_tags = dict(self.extra_tags or {})
        set_isolation_disabled_tag = self.set_isolation_disabled_tag
        skip_in_branch_deployments = self.skip_in_branch_deployments
        default_status = dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED
        sensor_asset_selection = [dg.AssetKey(k.split("/")) for k in sorted(set(keys))]

        def sensor_fn(context: dg.SensorEvaluationContext):
            if skip_in_branch_deployments and os.environ.get("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1":
                return dg.SkipReason("branch deployment -- skipping (skip_in_branch_deployments=True)")

            now = time.time()
            state: Dict[str, Any] = {}
            if context.cursor:
                try:
                    state = json.loads(context.cursor)
                except ValueError:
                    state = {}
            # `due_at` is when the next run may start. Stored (not derived) so changing period_hours
            # or first_run_delay_hours never re-fires a run that already happened.
            if "due_at" not in state:
                due_at = now + first_delay_seconds
                state = {"due_at": due_at, "last_ts": None}
                if first_delay_seconds > 0:
                    return dg.SensorResult(
                        cursor=json.dumps(state),
                        skip_reason=f"First tick: period starts now, first run in {_humanize(first_delay_seconds)}.",
                    )
            due_at = float(state["due_at"])
            if now < due_at:
                return dg.SkipReason(f"Within the period: next run in {_humanize(due_at - now)}.")

            try:
                thread_tags = post_announcement(
                    platform, announce_text,
                    slack_channel=channel_id, slack_bot_token_env_var=bot_token_env_var,
                    teams_team_id=teams_team_id, teams_channel_id=teams_channel_id,
                    teams_tenant_id_env_var=teams_tenant_id_env_var, teams_client_id_env_var=teams_client_id_env_var,
                    teams_client_secret_env_var=teams_client_secret_env_var, teams_refresh_token_env_var=teams_refresh_token_env_var,
                )
            except AnnouncementPostError as e:
                return dg.SkipReason(f"Announcement not posted, will retry next tick: {e}")

            ts = thread_tags.get("slack/thread_ts") or thread_tags.get("teams/thread_id")
            tags = dict(extra_tags)
            tags.update(thread_tags)
            if platform == "slack":
                tags["slack/message_ts"] = ts
            if set_isolation_disabled_tag:
                tags["dagster/isolation"] = "disabled"
            new_state = {"due_at": now + period_seconds, "last_ts": ts}
            return dg.SensorResult(
                run_requests=[dg.RunRequest(run_key=f"{sensor_name}:{ts}", asset_selection=sensor_asset_selection, tags=tags)],
                cursor=json.dumps(new_state),
            )

        sensor = dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=default_status,
            asset_selection=sensor_asset_selection,
        )(sensor_fn)
        return dg.Definitions(sensors=[sensor])
