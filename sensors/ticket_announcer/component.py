"""TicketAnnouncerComponent.

Posts each new ticket an agent filed (GitHub Issues with a given label, and/or Linear issues in a given team) to
Slack with a short excerpt, once. Dedup is a plain set of "source:number" keys held in this sensor's own
Dagster-native cursor -- no file at all.
"""
import json
import os
from typing import Any, Dict, List, Optional

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


def _get_slack_client(bot_token_env_var: str):
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("requires slack_sdk: pip install 'slack_sdk>=3.0'") from e
    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(f"Slack bot token env var {bot_token_env_var!r} not set.")
    return WebClient(token=token)


class TicketAnnouncerComponent(dg.Component, dg.Model, dg.Resolvable):
    """Posts each new ticket an agent filed (GitHub Issues and/or Linear) to Slack with an excerpt, once."""

    sensor_name: str = Field(default="ticket_announcer", description="Unique sensor name.")
    slack_channel: str = Field(description="Channel to announce new tickets in (either `C123ABC456` id or `#channel-name`).")
    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's `xoxb-...` token. Needs `chat:write`.")
    excerpt_chars: int = Field(default=320, description="Max characters of the ticket body quoted in the Slack message.")

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

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Ticket Announcer", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        slack_channel = self.slack_channel
        slack_bot_token_env_var = self.slack_bot_token_env_var
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

            client = _get_slack_client(slack_bot_token_env_var)
            newly_announced: List[str] = []
            for source, issue in new_ones:
                key = f"{source}:{issue['number']}"
                text = f":memo: {source} ticket <{issue['url']}|{issue['number']} {issue['title']}> filed by an engineering agent.\n> {_excerpt(issue['body'], excerpt_chars)}"
                try:
                    client.chat_postMessage(channel=slack_channel, text=text)
                except Exception as e:  # noqa: BLE001
                    problems.append(f"Slack post failed for {key}: {e}")
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
