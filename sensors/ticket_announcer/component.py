"""TicketAnnouncerComponent.

Posts each new ticket an agent filed (GitHub Issues with a given label, and/or Linear issues in a given team) to
Slack with a short excerpt, once. Dedup is a plain set of "source:number" keys held in this sensor's own
Dagster-native cursor -- no file at all.
"""
import json
import os
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field, model_validator


def _gh_list_agent_issues(repo: str, label: str, *, token: str, api_base_url: str) -> List[Dict[str, Any]]:
    """Open issues (never pull requests) carrying `label`, with their bodies."""
    import requests
    resp = requests.get(
        f"{api_base_url}/repos/{repo}/issues",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"},
        params={"state": "open", "labels": label, "per_page": 50},
        timeout=30,
    )
    resp.raise_for_status()
    return [
        {"number": i["number"], "title": i["title"], "url": i["html_url"], "body": i.get("body") or ""}
        for i in resp.json()
        if "pull_request" not in i
    ]


def _linear_list_agent_issues(team_id: str, marker: str, *, api_key: str, base_url: str = "https://api.linear.app/graphql") -> List[Dict[str, Any]]:
    """Open Linear tickets in `team_id` whose description contains `marker` (an agent-filed ticket), with their bodies."""
    import requests
    query = (
        "query($t:ID!,$m:String!){ issues(first:50, filter:{ team:{id:{eq:$t}}, description:{contains:$m}, "
        'state:{type:{nin:["completed","canceled"]}} }){ nodes{ identifier title url description } } }'
    )
    resp = requests.post(base_url, json={"query": query, "variables": {"t": team_id, "m": marker}}, headers={"Authorization": api_key, "Content-Type": "application/json"}, timeout=30)
    data = resp.json()
    if resp.status_code >= 400 or data.get("errors"):
        raise RuntimeError(f"Linear request failed: {str(data.get('errors') or data)[:300]}")
    return [
        {"number": i["identifier"], "title": i["title"], "url": i["url"], "body": i.get("description") or ""}
        for i in data["data"]["issues"]["nodes"]
    ]


def _excerpt(body: str, limit: int) -> str:
    """The analysis section of a ticket body, or its start, as a short Slack quote."""
    text = body
    low = body.lower()
    if "analysis" in low:
        text = body[low.index("analysis"):].split("\n", 1)[-1]
        cut = text.lower().find("suggested change")
        text = text[:cut] if cut > 0 else text
    text = " ".join(text.split())
    return (text[:limit] + "…") if len(text) > limit else text


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


class TicketAnnouncerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Posts each new ticket an agent filed (GitHub Issues and/or Linear) to Slack with an excerpt, once."""

    sensor_name: str = Field(default="ticket_announcer", description="Unique sensor name.")
    platform: Literal["slack", "teams"] = Field(default="slack", description="Chat platform to announce new tickets in. 'teams' posts via Microsoft Graph instead of Slack.")
    slack_channel: Optional[str] = Field(default=None, description="platform='slack' (required then). Channel to announce new tickets in (either `C123ABC456` id or `#channel-name`).")
    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="platform='slack'. Env var holding the bot's `xoxb-...` token. Needs `chat:write`.")
    teams_team_id: Optional[str] = Field(default=None, description="platform='teams' (required then). Teams team id (Teams client -> team -> ... -> Get link).")
    teams_channel_id: Optional[str] = Field(default=None, description="platform='teams' (required then). Teams channel id inside the team (e.g. '19:xxx@thread.tacv2').")
    teams_tenant_id_env_var: str = Field(default="TEAMS_TENANT_ID", description="platform='teams'. Env var holding the Azure AD tenant id.")
    teams_client_id_env_var: str = Field(default="TEAMS_CLIENT_ID", description="platform='teams'. Env var holding the Azure AD app registration client id.")
    teams_client_secret_env_var: str = Field(default="TEAMS_CLIENT_SECRET", description="platform='teams'. Env var holding the Azure AD app registration client secret.")
    teams_refresh_token_env_var: str = Field(default="TEAMS_REFRESH_TOKEN", description="platform='teams'. Env var holding the OAuth refresh token.")
    excerpt_chars: int = Field(default=320, description="Max characters of the ticket body quoted in the announcement.")

    repo: Optional[str] = Field(default=None, description="GitHub repository to watch for agent-filed issues, 'owner/repo'. Leave unset to skip GitHub.")
    github_token_env_var: str = Field(default="GITHUB_TOKEN", description="Env var holding a GitHub token with read access to issues.")
    github_issue_label: str = Field(default="engineering-agent", description="Only issues carrying this label are announced.")

    linear_team_id: Optional[str] = Field(default=None, description="Linear team UUID to watch for agent-filed tickets. Leave unset to skip Linear.")
    linear_api_key_env_var: str = Field(default="LINEAR_API_KEY", description="Env var holding a Linear API key.")
    linear_marker: str = Field(default="Filed by an engineering agent.", description="Only Linear issues whose description contains this exact marker text are announced.")

    minimum_interval_seconds: int = Field(default=60, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="RUNNING", description="'RUNNING' or 'STOPPED'.")

    @model_validator(mode="after")
    def _validate_at_least_one_source(self):
        if not self.repo and not self.linear_team_id:
            raise ValueError("TicketAnnouncerComponent: set at least one of repo (GitHub) or linear_team_id (Linear).")
        return self

    @model_validator(mode="after")
    def _validate_platform_fields(self):
        if self.platform == "teams":
            if not (self.teams_team_id and self.teams_channel_id):
                raise ValueError("TicketAnnouncerComponent: platform='teams' requires teams_team_id and teams_channel_id.")
        elif not self.slack_channel:
            raise ValueError("TicketAnnouncerComponent: platform='slack' requires slack_channel.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Ticket Announcer", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        platform = self.platform
        slack_channel = self.slack_channel
        slack_bot_token_env_var = self.slack_bot_token_env_var
        teams_team_id = self.teams_team_id
        teams_channel_id = self.teams_channel_id
        teams_tenant_id_env_var = self.teams_tenant_id_env_var
        teams_client_id_env_var = self.teams_client_id_env_var
        teams_client_secret_env_var = self.teams_client_secret_env_var
        teams_refresh_token_env_var = self.teams_refresh_token_env_var
        excerpt_chars = self.excerpt_chars
        repo = self.repo
        github_token_env_var = self.github_token_env_var
        github_issue_label = self.github_issue_label
        linear_team_id = self.linear_team_id
        linear_api_key_env_var = self.linear_api_key_env_var
        linear_marker = self.linear_marker

        def sensor_fn(context: dg.SensorEvaluationContext, **_resources):
            announced = set(json.loads(context.cursor)) if context.cursor else set()
            found: List["tuple[str, Dict[str, Any]]"] = []
            problems: List[str] = []

            if repo:
                token = os.environ.get(github_token_env_var, "")
                if not token:
                    problems.append(f"{github_token_env_var} not set")
                else:
                    try:
                        found += [("GitHub", i) for i in _gh_list_agent_issues(repo, github_issue_label, token=token, api_base_url="https://api.github.com")]
                    except Exception as e:  # noqa: BLE001
                        problems.append(f"GitHub error: {e}")

            if linear_team_id:
                api_key = os.environ.get(linear_api_key_env_var, "")
                if not api_key:
                    problems.append(f"{linear_api_key_env_var} not set")
                else:
                    try:
                        found += [("Linear", i) for i in _linear_list_agent_issues(linear_team_id, linear_marker, api_key=api_key)]
                    except Exception as e:  # noqa: BLE001
                        problems.append(f"Linear error: {e}")

            new_ones = [(source, issue) for source, issue in found if f"{source}:{issue['number']}" not in announced]
            if not new_ones:
                return dg.SensorResult(skip_reason="; ".join(problems) or "no new tickets", cursor=json.dumps(sorted(announced)))

            newly_announced: List[str] = []
            for source, issue in new_ones:
                key = f"{source}:{issue['number']}"
                text = f":memo: {source} ticket <{issue['url']}|{issue['number']} {issue['title']}> filed by an engineering agent.\n> {_excerpt(issue['body'], excerpt_chars)}"
                try:
                    post_announcement(
                        platform, text,
                        slack_channel=slack_channel, slack_bot_token_env_var=slack_bot_token_env_var,
                        teams_team_id=teams_team_id, teams_channel_id=teams_channel_id,
                        teams_tenant_id_env_var=teams_tenant_id_env_var, teams_client_id_env_var=teams_client_id_env_var,
                        teams_client_secret_env_var=teams_client_secret_env_var, teams_refresh_token_env_var=teams_refresh_token_env_var,
                    )
                except AnnouncementPostError as e:
                    problems.append(f"post failed for {key}: {e}")
                    continue
                announced.add(key)
                newly_announced.append(key)

            summary = "; ".join([f"announced {k}" for k in newly_announced] + problems) or "nothing new"
            return dg.SensorResult(skip_reason=summary, cursor=json.dumps(sorted(announced)))

        sensor_def = dg.SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED,
        )
        return dg.Definitions(sensors=[sensor_def])
