"""SlackApprovalGateComponent.

Human-in-the-loop approval via Slack reactions. Composes with the
existing `HumanApprovalGateComponent` — Slack side handles the posting
+ reaction polling + quorum accounting; when quorum is reached, this
component writes the same JSON token file `HumanApprovalGateComponent`
already reads. Everything downstream of the gate is unchanged.

## Why reactions polling (not interactive buttons)

Interactive buttons + slash commands require a public webhook Slack can
POST to. That doesn't work in Dagster+ Serverless (containers are
short-lived, no fixed URL). Reactions polling is pure outbound HTTPS to
Slack's API — works everywhere Dagster runs. Trade-off: 30-60s
approval-detection latency instead of instant. Fine for humans.

## Emits (per YAML instance)

1. **`{asset_name}_posted` asset** (partitioned). Materialization posts
   the report to Slack, seeds it with approve/reject emoji reactions
   (so voters see the buttons), and stores `message_ts + channel_id + ...`
   in metadata for the sensor.
2. **`{asset_name}_watcher` sensor** (default_status: RUNNING). Polls
   Slack every `poll_interval_seconds` for each posted-but-not-tokened
   partition. Counts allowlisted reactions; on quorum, writes the same
   JSON token `HumanApprovalGateComponent` reads.

Pair with a `HumanApprovalGateComponent` (`upstream_asset_key:
{same_report_asset_key}` + `approval_dir: {same_dir}`) to complete the
gate — Slack handles the human side, the existing gate handles the
downstream asset-check + blocking.
"""

import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


# ── Partition helper (matches HumanApprovalGateComponent shape) ───────

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
    """Same sanitization as HumanApprovalGateComponent — composite keys
    like `dagster-io/dagster#30000` land as single-segment filenames."""
    return raw.replace("/", "_").replace("\\", "_")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_upstream_text(upstream: Any) -> str:
    """Prefer `text` / `content` / str-coerce."""
    if isinstance(upstream, dict):
        for k in ("text", "content", "value"):
            v = upstream.get(k)
            if isinstance(v, str):
                return v
        return json.dumps(upstream, default=str)[:4000]
    if isinstance(upstream, str):
        return upstream
    return str(upstream)


# ── Slack helpers ─────────────────────────────────────────────────────

_SLACK_STATE_SUFFIX = ".slack_state.json"


def _get_slack_client(bot_token_env_var: str):
    """Deferred import so anything can load the component; only fails at
    actual runtime if the SDK isn't installed."""
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError(
            "slack_approval_gate requires slack_sdk: pip install 'slack_sdk>=3.0'"
        ) from e

    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(
            f"Slack bot token env var {bot_token_env_var!r} not set. "
            f"Create a bot at https://api.slack.com/apps with the "
            f"`chat:write` + `reactions:read` + `reactions:write` scopes, "
            f"install to your workspace, and export the `xoxb-...` token."
        )
    return WebClient(token=token)


def _post_slack_message(
    client, channel: str, text: str, approve_emoji: str, reject_emoji: str, ping_users: List[str],
) -> Dict[str, Any]:
    """Post + seed with approve/reject reactions so voters see the buttons."""
    pings = " ".join(f"<@{u}>" for u in ping_users) + " " if ping_users else ""
    body = f"{pings}{text}"
    resp = client.chat_postMessage(channel=channel, text=body)
    if not resp.get("ok"):
        raise RuntimeError(f"Slack chat_postMessage failed: {resp.get('error')}")
    message_ts = resp["ts"]
    channel_id = resp["channel"]

    # Seed with approve + reject reactions so voters see the buttons
    for emoji in (approve_emoji, reject_emoji):
        try:
            client.reactions_add(channel=channel_id, timestamp=message_ts, name=emoji)
        except Exception:  # noqa: BLE001
            # Emoji may not exist in the workspace; not fatal.
            pass

    return {"message_ts": message_ts, "channel_id": channel_id}


def _fetch_reactions(client, channel_id: str, message_ts: str) -> Dict[str, List[str]]:
    """Return {emoji_name: [user_id, ...]} for the message."""
    resp = client.reactions_get(channel=channel_id, timestamp=message_ts)
    if not resp.get("ok"):
        raise RuntimeError(f"Slack reactions_get failed: {resp.get('error')}")
    message = resp.get("message", {})
    reactions = message.get("reactions", []) or []
    return {r["name"]: r.get("users", []) for r in reactions}


def _classify_votes(
    reactions: Dict[str, List[str]],
    approve_emoji: str,
    reject_emoji: str,
    allowlist: List[str],
    bot_user_id: Optional[str],
) -> Dict[str, List[str]]:
    """Filter reactions to allowlisted users. Bot's own seed reactions
    don't count as votes."""
    allowset = set(allowlist)
    exclude = {bot_user_id} if bot_user_id else set()
    approvers = [u for u in reactions.get(approve_emoji, []) if u in allowset and u not in exclude]
    rejecters = [u for u in reactions.get(reject_emoji, []) if u in allowset and u not in exclude]
    return {"approve": approvers, "reject": rejecters}


# ── Component ─────────────────────────────────────────────────────────


class SlackApprovalGateComponent(dg.Component, dg.Model, dg.Resolvable):
    """Slack-native HITL approval gate. Composes with
    `HumanApprovalGateComponent` — Slack side handles posting + reaction
    polling + quorum; the existing gate consumes the resulting token
    unchanged.

    Emits one asset (`{asset_name}_posted`) + one sensor
    (`{asset_name}_watcher`). Every partition that hasn't been tokened
    yet is polled every `poll_interval_seconds`.
    """

    asset_name: str = Field(
        description="Base name. Emits `{asset_name}_posted` asset + `{asset_name}_watcher` sensor."
    )
    upstream_asset_key: str = Field(
        description=(
            "Upstream asset whose text is posted to Slack for approval. "
            "String (not AssetKey object); multi-part keys use slash notation."
        )
    )
    approval_dir: str = Field(
        description=(
            "Absolute path to the shared approval-token directory. When "
            "quorum is reached, writes `<safe_partition_key>.json` here — "
            "matches `HumanApprovalGateComponent`'s `approval_dir` so the "
            "existing gate + sensor pipeline just works."
        )
    )

    # Slack config
    slack_channel: str = Field(
        description="Channel to post to (either `C123ABC456` id or `#channel-name`)."
    )
    slack_bot_token_env_var: str = Field(
        default="SLACK_BOT_TOKEN",
        description=(
            "Env var holding the bot's `xoxb-...` token. Bot needs the "
            "`chat:write` + `reactions:read` + `reactions:write` scopes."
        )
    )
    approve_emoji: str = Field(
        default="white_check_mark",
        description="Emoji name (no colons) approvers react with. Default `white_check_mark` (✅)."
    )
    reject_emoji: str = Field(
        default="x",
        description="Emoji name (no colons) for explicit rejection. Default `x` (❌)."
    )
    ping_users_on_post: Optional[List[str]] = Field(
        default=None,
        description="Optional Slack user IDs (`U123...`) to @-mention in the initial post so they get a notification."
    )
    message_template: Optional[str] = Field(
        default=None,
        description=(
            "Optional custom message template. Placeholders: "
            "`{upstream_text}` (full upstream text, truncated to 2000 chars), "
            "`{approve_emoji}`, `{reject_emoji}`, `{required_approvers}`, "
            "`{n_allowlisted}`, `{partition_key}`. Default: "
            "`*Approval needed:* {upstream_text} — react :{approve_emoji}: to approve, :{reject_emoji}: to reject. Requires {required_approvers} of {n_allowlisted} allowlisted approvers.`"
        )
    )

    # Quorum + timeout
    required_approvers: int = Field(
        default=1,
        description="How many allowlisted approvers must react with `approve_emoji` before the token is written."
    )
    approver_allowlist: List[str] = Field(
        description="Slack user IDs (`U123...`) allowed to vote. Reactions from anyone else are ignored."
    )
    timeout_hours: Optional[float] = Field(
        default=None,
        description="Optional. If quorum isn't reached within this many hours, applies `on_timeout` policy."
    )
    on_timeout: str = Field(
        default="escalate",
        description="`escalate` (default — ping escalate_slack_user, keep waiting) | `reject` (write approved=false token) | `approve` (write approved=true — use with caution)."
    )
    escalate_slack_user: Optional[str] = Field(
        default=None,
        description="Slack user ID pinged when timeout escalates. Only used when `on_timeout: escalate`."
    )

    poll_interval_seconds: int = Field(
        default=30,
        description="Sensor cadence — how often to poll Slack for new reactions. 30s default; lower for demos, higher (60-120s) at scale to stay under Slack rate limits."
    )

    # Catalog / governance
    group_name: Optional[str] = Field(default=None, description="Asset group.")
    kinds: Optional[List[str]] = Field(
        default=None, description="Asset kinds. Default: ['human', 'approval', 'slack']."
    )
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Extra tags.")
    description: Optional[str] = Field(default=None, description="Asset description.")

    # Partitioning — must match upstream asset's partitions_def.
    partition_type: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)

    @classmethod
    def get_form_config(cls):
        """UI-editable via the Dagster / Dagster+ Components tab."""
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Slack Approval Gate", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        posted_asset_name = f"{asset_name}_posted"
        upstream_asset_key = self.upstream_asset_key
        approval_dir = self.approval_dir
        slack_channel = self.slack_channel
        bot_token_env_var = self.slack_bot_token_env_var
        approve_emoji = self.approve_emoji
        reject_emoji = self.reject_emoji
        ping_users = list(self.ping_users_on_post or [])
        message_template = self.message_template or (
            "*Approval needed:* {upstream_text}\n\n"
            "React :{approve_emoji}: to approve, :{reject_emoji}: to reject. "
            "Requires {required_approvers} of {n_allowlisted} allowlisted approvers."
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
        escalate_slack_user = self.escalate_slack_user
        poll_interval_seconds = self.poll_interval_seconds

        kinds = self.kinds or ["human", "approval", "slack"]
        tag_map = dict(self.tags or {})
        for k in kinds:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
        )

        # ── Asset: post to Slack + record state in a sidecar file ──────
        # Sidecar (per-partition) file: `<approval_dir>/<safe_key><suffix>` —
        # holds message_ts, channel_id, posted_at, escalated_at. Existing
        # gate's downstream sensor picks up the actual token file
        # `<approval_dir>/<safe_key>.json` written by our watcher sensor.

        @dg.asset(
            key=dg.AssetKey.from_user_string(posted_asset_name),
            description=self.description or f"Slack post seeking approval for {upstream_asset_key}",
            group_name=self.group_name,
            owners=self.owners or [],
            tags=tag_map,
            partitions_def=partitions_def,
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(upstream_asset_key))},
        )
        def _posted_asset(context: dg.AssetExecutionContext, upstream):
            raw_key = context.partition_key if context.has_partition_key else "default"
            safe_key = _safe_partition_key(raw_key)
            approval_root = Path(approval_dir).expanduser().resolve()
            approval_root.mkdir(parents=True, exist_ok=True)
            sidecar = approval_root / f"{safe_key}{_SLACK_STATE_SUFFIX}"

            if sidecar.exists():
                # Already posted for this partition — re-materializing is a no-op
                # (safe idempotent re-run; existing message stays valid).
                state = json.loads(sidecar.read_text())
                context.log.info(
                    f"[slack_approval] partition {raw_key!r} already posted "
                    f"(ts={state.get('message_ts')}); reusing existing message."
                )
                return state

            client = _get_slack_client(bot_token_env_var)

            # Look up bot user id so we can exclude the bot's own seed
            # reactions from vote counting.
            bot_user_id: Optional[str] = None
            try:
                auth = client.auth_test()
                bot_user_id = auth.get("user_id")
            except Exception:  # noqa: BLE001
                pass

            upstream_text = _extract_upstream_text(upstream)[:2000]
            body = message_template.format(
                upstream_text=upstream_text,
                approve_emoji=approve_emoji,
                reject_emoji=reject_emoji,
                required_approvers=required_approvers,
                n_allowlisted=len(approver_allowlist),
                partition_key=raw_key,
            )

            posted = _post_slack_message(
                client, slack_channel, body, approve_emoji, reject_emoji, ping_users,
            )
            state = {
                "message_ts": posted["message_ts"],
                "channel_id": posted["channel_id"],
                "partition_key": raw_key,
                "safe_partition_key": safe_key,
                "bot_user_id": bot_user_id,
                "posted_at": _now_iso(),
                "required_approvers": required_approvers,
                "approver_allowlist": approver_allowlist,
                "approve_emoji": approve_emoji,
                "reject_emoji": reject_emoji,
                "timeout_hours": timeout_hours,
                "on_timeout": on_timeout,
                "escalate_slack_user": escalate_slack_user,
                "bot_token_env_var": bot_token_env_var,
                "approval_dir": str(approval_root),
                "escalated_at": None,
            }
            sidecar.write_text(json.dumps(state, indent=2))
            context.log.info(
                f"[slack_approval] posted to {slack_channel} for partition "
                f"{raw_key!r} (ts={posted['message_ts']})"
            )
            context.add_output_metadata({
                "slack_channel": slack_channel,
                "slack_message_ts": posted["message_ts"],
                "sidecar_path": str(sidecar),
                "partition_key": raw_key,
                "required_approvers": required_approvers,
                "allowlisted": ",".join(approver_allowlist),
            })
            return state

        # ── Sensor: poll reactions + write token on quorum ─────────────

        sensor_name = f"{asset_name}_watcher"

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=poll_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _watcher(context: dg.SensorEvaluationContext):
            approval_root = Path(approval_dir).expanduser().resolve()
            if not approval_root.exists():
                return dg.SensorResult(skip_reason=f"approval_dir does not exist yet: {approval_root}")

            # For each sidecar (per partition), check if token exists.
            # If not, poll Slack + evaluate quorum.
            sidecars = list(approval_root.glob(f"*{_SLACK_STATE_SUFFIX}"))
            if not sidecars:
                return dg.SensorResult(skip_reason="no partitions posted yet")

            actions: List[str] = []  # log summary
            for sidecar in sidecars:
                try:
                    state = json.loads(sidecar.read_text())
                except Exception as e:  # noqa: BLE001
                    context.log.warning(f"skipping malformed sidecar {sidecar}: {e}")
                    continue

                safe_key = state["safe_partition_key"]
                token_path = approval_root / f"{safe_key}.json"
                if token_path.exists():
                    continue  # already resolved

                try:
                    client = _get_slack_client(state["bot_token_env_var"])
                    reactions = _fetch_reactions(
                        client, state["channel_id"], state["message_ts"],
                    )
                except Exception as e:  # noqa: BLE001
                    context.log.error(
                        f"Slack fetch failed for {safe_key}: {e}. Skipping this tick."
                    )
                    continue

                votes = _classify_votes(
                    reactions,
                    state["approve_emoji"],
                    state["reject_emoji"],
                    state["approver_allowlist"],
                    state.get("bot_user_id"),
                )
                required = state["required_approvers"]

                token_body: Optional[Dict[str, Any]] = None
                if len(votes["approve"]) >= required:
                    token_body = {
                        "approved": True,
                        "approver": ",".join(votes["approve"]),
                        "reason": f"Slack quorum reached: {len(votes['approve'])}/{required} approved",
                        "timestamp": _now_iso(),
                        "source": "slack_approval_gate",
                        "slack_message_ts": state["message_ts"],
                    }
                elif len(votes["reject"]) >= required:
                    token_body = {
                        "approved": False,
                        "approver": ",".join(votes["reject"]),
                        "reason": f"Slack rejection quorum reached: {len(votes['reject'])}/{required} rejected",
                        "timestamp": _now_iso(),
                        "source": "slack_approval_gate",
                        "slack_message_ts": state["message_ts"],
                    }
                else:
                    # Check timeout
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
                                    "source": "slack_approval_gate",
                                }
                            elif on_to == "approve":
                                token_body = {
                                    "approved": True,
                                    "approver": "timeout",
                                    "reason": f"Timed out after {timeout}h with no quorum; auto-approved (per policy).",
                                    "timestamp": _now_iso(),
                                    "source": "slack_approval_gate",
                                }
                            elif on_to == "escalate" and state.get("escalate_slack_user") and not state.get("escalated_at"):
                                # Send an escalation ping ONCE, keep waiting.
                                try:
                                    client.chat_postMessage(
                                        channel=state["channel_id"],
                                        thread_ts=state["message_ts"],
                                        text=(
                                            f"<@{state['escalate_slack_user']}> — approval on this "
                                            f"partition (`{state['partition_key']}`) has been "
                                            f"pending for {timeout}h. Please review."
                                        ),
                                    )
                                    state["escalated_at"] = _now_iso()
                                    sidecar.write_text(json.dumps(state, indent=2))
                                    actions.append(f"{safe_key}: escalated")
                                except Exception as e:  # noqa: BLE001
                                    context.log.warning(f"escalation post failed for {safe_key}: {e}")

                if token_body is not None:
                    token_path.write_text(json.dumps(token_body, indent=2))
                    actions.append(f"{safe_key}: wrote token ({'approved' if token_body['approved'] else 'rejected'})")

            if not actions:
                return dg.SensorResult(skip_reason=f"polled {len(sidecars)} partition(s); no quorum changes")
            return dg.SensorResult(skip_reason="; ".join(actions))

        return dg.Definitions(assets=[_posted_asset], sensors=[_watcher])
