"""TeamsApprovalGateComponent — Microsoft Teams HITL approval gate.

Slack analog for MS Teams. Composes with `HumanApprovalGateComponent`
via the shared `approval_dir` — Teams side handles posting + polling +
quorum; the existing gate consumes the resulting token unchanged. Same
file convention, same token format, same downstream sensor.

Emits one asset (`{asset_name}_posted`) + one sensor
(`{asset_name}_watcher`). Every partition that hasn't been tokened yet
is polled every `poll_interval_seconds`.

## Auth (Azure AD app registration)

Teams needs an Azure AD app registration with **application permissions**
(client-credentials flow, tenant admin consent required):

    ChannelMessage.Read.All        (poll reactions / replies)
    ChannelMessage.Send            (post approval message)

Register at https://portal.azure.com → Azure AD → App registrations →
New. Grant application permissions + admin consent. Copy tenant_id,
client_id, and generate a client_secret.

## Approval mode: reactions vs replies

Teams reactions are limited to 6 built-in emoji (like / heart / laugh /
surprised / sad / angry) — no custom emoji, unlike Slack. This
component supports both:

- **`mode: reactions`** (default) — approvers react with the configured
  emoji (default: `like` = approve, `angry` = reject). Fast, no typing.
- **`mode: reply`** — approvers post a thread reply containing the
  configured keyword (`APPROVE` / `REJECT`). More flexible (approver
  can add context in the same message) but requires typing.

Both modes read votes via MS Graph API and write the standard JSON
token when quorum is reached.
"""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg


# ── Inline fs helper: local + cloud (s3://, gs://, abfs://) via fsspec ──
# Kept inline per DCC's self-contained convention.
class _ApprovalFS:
    """Uniform read/write over `approval_dir`. Plain paths use pathlib
    (no fsspec dep); URIs (`s3://`, `gs://`, `abfs://`) route through
    fsspec + the appropriate driver."""
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
    def mkdir(self, subdir: str = "") -> None:
        target = self.path(subdir) if subdir else self.root
        if self.is_uri:
            try: self.fs.makedirs(target, exist_ok=True)
            except Exception: pass
        else:
            Path(target).mkdir(parents=True, exist_ok=True)
    def read_json(self, p: str) -> Any:
        if self.is_uri:
            with self.fs.open(p, "r") as f: return json.loads(f.read())
        return json.loads(Path(p).read_text())
    def write_json(self, p: str, obj: Any) -> None:
        body = json.dumps(obj, indent=2, default=str)
        if self.is_uri:
            try: self.fs.makedirs("/".join(p.split("/")[:-1]), exist_ok=True)
            except Exception: pass
            with self.fs.open(p, "w") as f: f.write(body)
        else:
            Path(p).parent.mkdir(parents=True, exist_ok=True)
            Path(p).write_text(body)
    def glob(self, pattern: str) -> List[str]:
        if self.is_uri:
            proto = self.fs.protocol if isinstance(self.fs.protocol, str) else self.fs.protocol[0]
            return [f"{proto}://{m}" for m in self.fs.glob(self.path(pattern))]
        return [str(p) for p in Path(self.root).glob(pattern)]
from dagster import (
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    HourlyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    WeeklyPartitionsDefinition,
)
from pydantic import Field


# ── Shared helpers (mirror of SlackApprovalGate's helpers) ────────────


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
):
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _safe_partition_key(raw: str) -> str:
    return raw.replace("/", "_").replace("\\", "_")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_upstream_text(upstream: Any) -> str:
    if isinstance(upstream, dict):
        for k in ("text", "content", "value"):
            v = upstream.get(k)
            if isinstance(v, str):
                return v
        return json.dumps(upstream, default=str)[:4000]
    if isinstance(upstream, str):
        return upstream
    return str(upstream)


# ── MS Graph helpers ──────────────────────────────────────────────────

_TEAMS_STATE_SUFFIX = ".teams_state.json"
_GRAPH = "https://graph.microsoft.com/v1.0"


def _get_teams_bearer(tenant_id_env: str, client_id_env: str, client_secret_env: str) -> str:
    """Client-credentials OAuth against Azure AD. Returns bearer token.

    Requires an app registration with application permissions
    (ChannelMessage.Read.All + ChannelMessage.Send) + tenant admin consent.
    """
    import requests
    tenant_id = os.environ.get(tenant_id_env)
    client_id = os.environ.get(client_id_env)
    client_secret = os.environ.get(client_secret_env)
    if not (tenant_id and client_id and client_secret):
        raise RuntimeError(
            f"Teams auth env vars not all set: "
            f"{tenant_id_env}={'✓' if tenant_id else '✗'}, "
            f"{client_id_env}={'✓' if client_id else '✗'}, "
            f"{client_secret_env}={'✓' if client_secret else '✗'}. "
            f"Register an app at https://portal.azure.com → Azure AD → "
            f"App registrations → New."
        )
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    r = requests.post(
        url,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def _post_teams_message(
    bearer: str, team_id: str, channel_id: str, html_body: str,
) -> Dict[str, Any]:
    """POST a channel message via Graph. Returns {message_id, ...}."""
    import requests
    r = requests.post(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"body": {"contentType": "html", "content": html_body}},
        timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    return {"message_id": body["id"], "web_url": body.get("webUrl", "")}


def _get_teams_message(bearer: str, team_id: str, channel_id: str, message_id: str) -> Dict[str, Any]:
    import requests
    r = requests.get(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}",
        headers={"Authorization": f"Bearer {bearer}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _get_teams_replies(bearer: str, team_id: str, channel_id: str, message_id: str) -> List[Dict[str, Any]]:
    import requests
    r = requests.get(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
        headers={"Authorization": f"Bearer {bearer}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("value", []) or []


def _post_teams_reply(
    bearer: str, team_id: str, channel_id: str, message_id: str, html_body: str,
) -> None:
    import requests
    r = requests.post(
        f"{_GRAPH}/teams/{team_id}/channels/{channel_id}/messages/{message_id}/replies",
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        json={"body": {"contentType": "html", "content": html_body}},
        timeout=30,
    )
    r.raise_for_status()


def _classify_reaction_votes(
    message: Dict[str, Any],
    approve_reaction: str,
    reject_reaction: str,
    allowlist: List[str],
) -> Dict[str, List[str]]:
    """Reactions live in message['reactions']: [{reactionType, user: {user: {id}}}]."""
    allowset = set(allowlist)
    approvers: List[str] = []
    rejecters: List[str] = []
    for r in message.get("reactions") or []:
        rtype = r.get("reactionType") or ""
        user = ((r.get("user") or {}).get("user") or {}).get("id") or ""
        if not user or user not in allowset:
            continue
        if rtype == approve_reaction:
            approvers.append(user)
        elif rtype == reject_reaction:
            rejecters.append(user)
    return {"approve": sorted(set(approvers)), "reject": sorted(set(rejecters))}


def _classify_reply_votes(
    replies: List[Dict[str, Any]],
    approve_keyword: str,
    reject_keyword: str,
    allowlist: List[str],
) -> Dict[str, List[str]]:
    """Reply text lookup. `body.content` is HTML — strip tags naively for
    keyword match. First keyword wins per user (approve beats reject if
    both appear in the same reply — unlikely but explicit)."""
    import re as _re
    allowset = set(allowlist)
    approvers: List[str] = []
    rejecters: List[str] = []
    ap = approve_keyword.upper()
    rj = reject_keyword.upper()
    for r in replies:
        from_ = ((r.get("from") or {}).get("user") or {}).get("id") or ""
        if not from_ or from_ not in allowset:
            continue
        html = ((r.get("body") or {}).get("content") or "")
        text = _re.sub(r"<[^>]+>", " ", html).upper()
        if ap in text and rj in text:
            # Ambiguous — approve wins by convention. Log via caller if needed.
            approvers.append(from_)
        elif ap in text:
            approvers.append(from_)
        elif rj in text:
            rejecters.append(from_)
    return {"approve": sorted(set(approvers)), "reject": sorted(set(rejecters))}


# ── Component ─────────────────────────────────────────────────────────


class TeamsApprovalGateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Microsoft Teams HITL approval gate. Slack analog — same asset +
    sensor shape, same token format, composes via shared approval_dir
    with HumanApprovalGateComponent.
    """

    asset_name: str = Field(
        description="Base name. Emits `{asset_name}_posted` asset + `{asset_name}_watcher` sensor.",
    )
    upstream_asset_key: str = Field(
        description="Upstream asset whose text is posted to Teams for approval.",
    )
    approval_dir: str = Field(
        description=(
            "Absolute path to the shared approval-token directory. Matches "
            "HumanApprovalGateComponent's `approval_dir` so the existing "
            "downstream gate + sensor pipeline just works."
        ),
    )

    # Teams / MS Graph config
    teams_tenant_id_env_var: str = Field(
        default="TEAMS_TENANT_ID",
        description="Env var holding the Azure AD tenant id.",
    )
    teams_client_id_env_var: str = Field(
        default="TEAMS_CLIENT_ID",
        description="Env var holding the Azure AD app registration client id.",
    )
    teams_client_secret_env_var: str = Field(
        default="TEAMS_CLIENT_SECRET",
        description="Env var holding the Azure AD app registration client secret.",
    )
    teams_team_id: str = Field(
        description="Teams team id (from Teams client → team → three-dot menu → Get link).",
    )
    teams_channel_id: str = Field(
        description="Teams channel id inside the team (e.g. `19:xxx@thread.tacv2`).",
    )

    # Approval mode
    mode: str = Field(
        default="reactions",
        description=(
            "'reactions' (default; approvers react with the configured emoji — "
            "Teams limits to 6 built-ins: like, heart, laugh, surprised, sad, "
            "angry) OR 'reply' (approvers post a thread reply containing the "
            "configured keyword — more flexible, allows context)."
        ),
    )
    approve_reaction: str = Field(
        default="like",
        description="Reaction type for approval. One of: like, heart, laugh, surprised, sad, angry.",
    )
    reject_reaction: str = Field(
        default="angry",
        description="Reaction type for rejection.",
    )
    approve_reply_keyword: str = Field(
        default="APPROVE",
        description="Keyword (case-insensitive) that marks a reply as an approval vote (mode='reply').",
    )
    reject_reply_keyword: str = Field(
        default="REJECT",
        description="Keyword (case-insensitive) that marks a reply as a rejection vote.",
    )
    message_template: Optional[str] = Field(
        default=None,
        description=(
            "Optional custom message template (HTML). Placeholders: "
            "`{upstream_text}`, `{approve}` (reaction name or keyword), "
            "`{reject}`, `{required_approvers}`, `{n_allowlisted}`, "
            "`{partition_key}`, `{mode}`."
        ),
    )

    # Quorum + timeout
    required_approvers: int = Field(
        default=1,
        description="How many allowlisted approvers must vote-approve before the token is written.",
    )
    approver_allowlist: List[str] = Field(
        description=(
            "Azure AD object IDs (GUIDs) of users allowed to vote. Reactions/"
            "replies from anyone else are ignored. Find via Azure Portal → "
            "Azure AD → Users → {user} → Object ID."
        ),
    )
    timeout_hours: Optional[float] = Field(
        default=None,
        description="Optional. If quorum isn't reached within this many hours, applies `on_timeout` policy.",
    )
    on_timeout: str = Field(
        default="escalate",
        description="`escalate` (default) | `reject` | `approve`",
    )
    escalate_teams_user: Optional[str] = Field(
        default=None,
        description="Azure AD object ID pinged when timeout escalates. Only used when `on_timeout: escalate`.",
    )

    poll_interval_seconds: int = Field(
        default=60,
        description=(
            "Sensor cadence — how often to poll Teams for new votes. Default "
            "60s (Teams API is rate-limited; 30s is fine for a handful of "
            "partitions but pump higher at scale)."
        ),
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None)
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds. Default: ['human', 'approval', 'teams'].",
    )
    owners: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    description: Optional[str] = Field(default=None)

    # Partitioning
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Teams Approval Gate", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        posted_asset_name = f"{asset_name}_posted"
        upstream_asset_key = self.upstream_asset_key
        approval_dir = self.approval_dir

        tenant_env = self.teams_tenant_id_env_var
        client_id_env = self.teams_client_id_env_var
        client_secret_env = self.teams_client_secret_env_var
        team_id = self.teams_team_id
        channel_id = self.teams_channel_id

        mode = self.mode
        if mode not in ("reactions", "reply"):
            raise ValueError(f"mode must be 'reactions' or 'reply'; got {mode!r}")
        approve_reaction = self.approve_reaction
        reject_reaction = self.reject_reaction
        approve_keyword = self.approve_reply_keyword
        reject_keyword = self.reject_reply_keyword
        approve_label = approve_reaction if mode == "reactions" else approve_keyword
        reject_label = reject_reaction if mode == "reactions" else reject_keyword

        message_template = self.message_template or (
            "<b>Approval needed:</b><br/>"
            "<pre>{upstream_text}</pre>"
            "<br/>Vote by {vote_hint}. "
            "Requires {required_approvers} of {n_allowlisted} allowlisted approvers."
        )
        vote_hint = (
            f"reacting with :{approve_label}: (approve) or :{reject_label}: (reject)"
            if mode == "reactions"
            else f"replying with `{approve_label}` (approve) or `{reject_label}` (reject)"
        )

        required_approvers = self.required_approvers
        approver_allowlist = list(self.approver_allowlist)
        if required_approvers > len(approver_allowlist):
            raise ValueError(
                f"required_approvers={required_approvers} > "
                f"len(approver_allowlist)={len(approver_allowlist)}. Impossible quorum."
            )
        timeout_hours = self.timeout_hours
        on_timeout = self.on_timeout
        if on_timeout not in ("escalate", "reject", "approve"):
            raise ValueError(f"on_timeout must be escalate|reject|approve; got {on_timeout!r}")
        escalate_teams_user = self.escalate_teams_user
        poll_interval_seconds = self.poll_interval_seconds

        kinds = self.kinds or ["human", "approval", "teams"]
        tag_map = dict(self.tags or {})
        for k in kinds:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        # ── Asset: post to Teams + record state in a sidecar file ──────
        # Sidecar (per-partition) file: `<approval_dir>/<safe_key>.teams_state.json`

        @dg.asset(
            key=dg.AssetKey.from_user_string(posted_asset_name),
            description=self.description or f"Teams post seeking approval for {upstream_asset_key}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            partitions_def=partitions_def,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))},
        )
        def _posted_asset(context: dg.AssetExecutionContext, upstream):
            raw_key = context.partition_key if context.has_partition_key else "default"
            safe_key = _safe_partition_key(raw_key)
            fs = _ApprovalFS(approval_dir)
            fs.mkdir()
            sidecar_path = fs.path(f"{safe_key}{_TEAMS_STATE_SUFFIX}")

            if fs.exists(sidecar_path):
                state = fs.read_json(sidecar_path)
                context.log.info(
                    f"[teams_approval] partition {raw_key!r} already posted "
                    f"(id={state.get('message_id')}); reusing existing message."
                )
                return state

            bearer = _get_teams_bearer(tenant_env, client_id_env, client_secret_env)
            upstream_text = _extract_upstream_text(upstream)[:2000]
            html = message_template.format(
                upstream_text=upstream_text.replace("<", "&lt;").replace(">", "&gt;"),
                approve=approve_label,
                reject=reject_label,
                required_approvers=required_approvers,
                n_allowlisted=len(approver_allowlist),
                partition_key=raw_key,
                mode=mode,
                vote_hint=vote_hint,
            )
            posted = _post_teams_message(bearer, team_id, channel_id, html)
            state = {
                "message_id": posted["message_id"],
                "web_url": posted.get("web_url", ""),
                "team_id": team_id,
                "channel_id": channel_id,
                "partition_key": raw_key,
                "safe_partition_key": safe_key,
                "posted_at": _now_iso(),
                "required_approvers": required_approvers,
                "approver_allowlist": approver_allowlist,
                "mode": mode,
                "approve_label": approve_label,
                "reject_label": reject_label,
                "timeout_hours": timeout_hours,
                "on_timeout": on_timeout,
                "escalate_teams_user": escalate_teams_user,
                "tenant_env": tenant_env,
                "client_id_env": client_id_env,
                "client_secret_env": client_secret_env,
                "approval_dir": fs.root,
                "escalated_at": None,
            }
            fs.write_json(sidecar_path, state)
            context.log.info(
                f"[teams_approval] posted to team={team_id} channel={channel_id} for "
                f"partition {raw_key!r} (id={posted['message_id']})"
            )
            context.add_output_metadata({
                "teams_team_id": team_id,
                "teams_channel_id": channel_id,
                "teams_message_id": posted["message_id"],
                "teams_web_url": posted.get("web_url", ""),
                "sidecar_path": sidecar_path,
                "partition_key": raw_key,
                "required_approvers": required_approvers,
                "mode": mode,
                "allowlisted": ",".join(approver_allowlist),
            })
            return state

        # ── Sensor: poll reactions/replies + write token on quorum ─────

        sensor_name = f"{asset_name}_watcher"

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=poll_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _watcher(context: dg.SensorEvaluationContext):
            fs = _ApprovalFS(approval_dir)
            if not fs.is_uri and not Path(fs.root).exists():
                return dg.SensorResult(skip_reason=f"approval_dir does not exist yet: {fs.root}")

            sidecars = fs.glob(f"*{_TEAMS_STATE_SUFFIX}")
            if not sidecars:
                return dg.SensorResult(skip_reason="no partitions posted yet")

            actions: List[str] = []
            for sidecar in sidecars:
                try:
                    state = fs.read_json(sidecar)
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"skipping malformed sidecar {sidecar}: {e}")
                    continue

                safe_key = state["safe_partition_key"]
                token_path = fs.path(f"{safe_key}.json")
                if fs.exists(token_path):
                    continue  # already resolved

                try:
                    bearer = _get_teams_bearer(
                        state["tenant_env"], state["client_id_env"], state["client_secret_env"],
                    )
                    if state["mode"] == "reactions":
                        message = _get_teams_message(
                            bearer, state["team_id"], state["channel_id"], state["message_id"],
                        )
                        votes = _classify_reaction_votes(
                            message, state["approve_label"], state["reject_label"],
                            state["approver_allowlist"],
                        )
                    else:  # reply mode
                        replies = _get_teams_replies(
                            bearer, state["team_id"], state["channel_id"], state["message_id"],
                        )
                        votes = _classify_reply_votes(
                            replies, state["approve_label"], state["reject_label"],
                            state["approver_allowlist"],
                        )
                except Exception as e:  # noqa: BLE001
                    context.log.error(
                        f"Teams fetch failed for {safe_key}: {e}. Skipping this tick."
                    )
                    continue

                required = state["required_approvers"]
                token_body: Optional[Dict[str, Any]] = None

                if len(votes["approve"]) >= required:
                    token_body = {
                        "approved": True,
                        "approver": ",".join(votes["approve"]),
                        "reason": f"Teams quorum reached: {len(votes['approve'])}/{required} approved",
                        "timestamp": _now_iso(),
                        "source": "teams_approval_gate",
                        "teams_message_id": state["message_id"],
                        "via": "graph.microsoft.com",
                    }
                elif len(votes["reject"]) >= required:
                    token_body = {
                        "approved": False,
                        "approver": ",".join(votes["reject"]),
                        "reason": f"Teams rejection quorum reached: {len(votes['reject'])}/{required} rejected",
                        "timestamp": _now_iso(),
                        "source": "teams_approval_gate",
                        "teams_message_id": state["message_id"],
                        "via": "graph.microsoft.com",
                    }
                else:
                    timeout = state.get("timeout_hours")
                    if timeout is not None:
                        posted = datetime.fromisoformat(state["posted_at"])
                        if datetime.now(timezone.utc) - posted > timedelta(hours=timeout):
                            on_to = state["on_timeout"]
                            if on_to == "reject":
                                token_body = {
                                    "approved": False,
                                    "approver": "timeout",
                                    "reason": f"Timed out after {timeout}h with no quorum; auto-rejected.",
                                    "timestamp": _now_iso(),
                                    "source": "teams_approval_gate",
                                }
                            elif on_to == "approve":
                                token_body = {
                                    "approved": True,
                                    "approver": "timeout",
                                    "reason": f"Timed out after {timeout}h with no quorum; auto-approved (per policy).",
                                    "timestamp": _now_iso(),
                                    "source": "teams_approval_gate",
                                }
                            elif (on_to == "escalate"
                                  and state.get("escalate_teams_user")
                                  and not state.get("escalated_at")):
                                # Post an escalation reply ONCE, keep waiting.
                                try:
                                    mention_html = (
                                        f"<at id=\"0\">{state['escalate_teams_user']}</at>"
                                    )
                                    escalation_body = (
                                        f"{mention_html} — approval on this partition "
                                        f"(<code>{state['partition_key']}</code>) has been "
                                        f"pending for {timeout}h. Please review."
                                    )
                                    _post_teams_reply(
                                        bearer, state["team_id"], state["channel_id"],
                                        state["message_id"], escalation_body,
                                    )
                                    state["escalated_at"] = _now_iso()
                                    fs.write_json(sidecar, state)
                                    actions.append(f"{safe_key}: escalated")
                                except Exception as e:  # noqa: BLE001
                                    context.log.warning(f"escalation post failed for {safe_key}: {e}")

                if token_body is not None:
                    fs.write_json(token_path, token_body)
                    actions.append(
                        f"{safe_key}: wrote token ({'approved' if token_body['approved'] else 'rejected'})"
                    )

            if not actions:
                return dg.SensorResult(skip_reason=f"polled {len(sidecars)} partition(s); no quorum changes")
            return dg.SensorResult(skip_reason="; ".join(actions))

        return dg.Definitions(assets=[_posted_asset], sensors=[_watcher])
