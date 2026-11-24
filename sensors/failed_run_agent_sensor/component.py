"""FailedRunAgentSensorComponent.

On a failed run (optionally filtered by code location, job name or run tags), launches an agent asset selection
with the failed run id on the run's tags, posts an announcement to Slack, and puts the run's Slack thread on the
launched run's tags so `slack_reply` answers in it.

Cooldown per (job, error signature), same file-claim rate-limiter design `RunFailureRemediationComponent` already
uses (append-only, one small file per event, under `<evidence_dir>/_state/ratelimit/`) -- this is the one place in
this engineering-agents component family that genuinely needs a file: it bounds a RATE across many distinct,
never-repeating run ids, which Dagster's own `RunRequest(run_key=...)` dedup (one run per distinct key) can't
express on its own.
"""
import hashlib
import json
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


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


# Same append-only-one-file-per-event design as RunFailureRemediationComponent's own _RateLimiter -- see
# assets/ai/run_failure_remediation/component.py -- ported onto a plain local-path file store here instead of that
# component's own cloud-capable `_Store` abstraction, since rate-limit state is small and local/NFS-shared disk is
# the common case for this use. Not byte-identical to that file (different store abstraction), so not drift-tested
# against it; this is the only component in this repo using this exact plain-local-path version.
_RL_DIR = ("_state", "ratelimit")
_RL_LOCKS: Dict[str, threading.Lock] = {}
_RL_LOCKS_GUARD = threading.Lock()


def _approval_key(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", s)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:  # noqa: BLE001
        return None


class _RateLimiterFS:
    """Plain local-path file store: exclusive-create (os.O_CREAT|O_EXCL), list, read, remove. Not cloud-URI
    capable (unlike this repo's _ApprovalFS) -- exclusive-create needs a real filesystem's atomicity guarantee."""

    def __init__(self, root: str):
        self.root = str(Path(root).expanduser().resolve())

    def _p(self, *parts: str) -> Path:
        return Path(self.root, *parts)

    def mkdir(self) -> None:
        self._p(*_RL_DIR).mkdir(parents=True, exist_ok=True)

    def create_exclusive(self, parts, body: str) -> bool:
        p = self._p(*parts)
        p.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as f:
            f.write(body)
        return True

    def list_names(self, *parts: str) -> List[str]:
        d = self._p(*parts)
        if not d.exists():
            return []
        return [p.name for p in d.iterdir() if p.is_file()]

    def read_json(self, *parts: str) -> Any:
        return json.loads(self._p(*parts).read_text())

    def remove(self, *parts: str) -> None:
        try:
            self._p(*parts).unlink()
        except FileNotFoundError:
            pass


class _RateLimiter:
    def __init__(self, fs: _RateLimiterFS):
        self.fs = fs
        with _RL_LOCKS_GUARD:
            self._lock = _RL_LOCKS.setdefault(fs.root, threading.Lock())

    @staticmethod
    def _ms(t: datetime) -> int:
        return int(t.timestamp() * 1000)

    def _append(self, kind: str, run_id: str, now: datetime, payload: Dict[str, Any]) -> str:
        ms = self._ms(now)
        while True:  # same ms + same run + same kind is only ever a retry of ourselves: bump the ms, never overwrite
            name = f"{ms:013d}-{_approval_key(run_id)}-{kind}.json"
            if self.fs.create_exclusive(_RL_DIR + (name,), json.dumps({"run_id": run_id, "kind": kind, **payload}, default=str)):
                return name
            ms += 1

    def _withdraw(self, *names: Optional[str]) -> None:
        for n in names:
            if n:
                self.fs.remove(*_RL_DIR, n)

    def _events(self) -> List["tuple[int, str, str]"]:
        out = []
        for n in self.fs.list_names(*_RL_DIR):
            m = re.match(r"^(\d{13})-.+-(triage|cooldown|suppressed)\.json$", n)
            if m:
                out.append((int(m.group(1)), m.group(2), n))
        return sorted(out, key=lambda e: e[2])

    def _cooldown(self, name: str) -> Optional[Dict[str, Any]]:
        d = self.fs.read_json(*_RL_DIR, name)
        return d if isinstance(d, dict) else None

    def _compact(self, evs, now: datetime, cooldown_minutes: float) -> None:
        now_ms = self._ms(now)
        for ms, kind, n in evs:
            # Kept for 24h (not just the 1h max_per_hour needs) so max_per_day's rolling-24h
            # count has history to look back over too -- a strict superset of what max_per_hour
            # alone would need, at the cost of a few extra small JSON files for a day.
            if kind == "triage" and ms <= now_ms - 86_400_000:
                self._withdraw(n)
            elif kind == "suppressed" and ms <= now_ms - int(max(cooldown_minutes, 60.0) * 60_000):
                self._withdraw(n)
            elif kind == "cooldown":
                d = self._cooldown(n)
                until = _parse_ts((d or {}).get("until"))
                if d is not None and (until is None or until <= now):
                    self._withdraw(n)

    def _active_cooldowns(self, evs, key: str, now: datetime):
        out = []
        for _, kind, n in evs:
            if kind != "cooldown":
                continue
            d = self._cooldown(n)
            until = _parse_ts((d or {}).get("until"))
            if d and d.get("key") == key and until and until > now:
                out.append((n, d))
        return out

    def admit(
        self, key: str, run_id: str, now: datetime, max_per_hour: int, cooldown_minutes: float,
        max_per_day: Optional[int] = None,
    ) -> "tuple[str, Optional[str]]":
        """Claim one triage slot for `run_id`. Returns ("ok", None) | ("cooldown", until_iso) | ("rate_limited", None).

        `max_per_hour`/`max_per_day` are both GLOBAL counts across every key (every signature),
        not per-key -- two independent rolling-window ceilings on top of the per-key cooldown."""
        with self._lock:
            evs = self._events()
            self._compact(evs, now, cooldown_minutes)
            evs = self._events()
            live = self._active_cooldowns(evs, key, now)
            if live:
                self._append("suppressed", run_id, now, {"key": key})
                return "cooldown", str(live[0][1].get("until"))
            lo_hour = self._ms(now) - 3_600_000
            lo_day = self._ms(now) - 86_400_000
            if sum(1 for ms, kind, _ in evs if kind == "triage" and ms > lo_hour) >= int(max_per_hour):
                return "rate_limited", None
            if max_per_day and sum(1 for ms, kind, _ in evs if kind == "triage" and ms > lo_day) >= int(max_per_day):
                return "rate_limited", None
            t = self._append("triage", run_id, now, {"key": key})
            c = None
            if cooldown_minutes > 0:
                c = self._append("cooldown", run_id, now, {"key": key, "until": _iso(now + timedelta(minutes=float(cooldown_minutes)))})
            evs = self._events()
            earlier = [(n, d) for n, d in self._active_cooldowns(evs, key, now) if c and n < c]
            if earlier:
                self._withdraw(t, c)
                self._append("suppressed", run_id, now, {"key": key})
                return "cooldown", str(earlier[0][1].get("until"))
            window = [n for ms, kind, n in evs if kind == "triage" and ms > lo_hour]
            if window.index(t) >= int(max_per_hour):
                self._withdraw(t, c)
                return "rate_limited", None
            day_window = [n for ms, kind, n in evs if kind == "triage" and ms > lo_day]
            if max_per_day and day_window.index(t) >= int(max_per_day):
                self._withdraw(t, c)
                return "rate_limited", None
            return "ok", None


def _strip_dynamic_suffix(step_key: str) -> str:
    """`process_chunk[4]` and `process_chunk[9]` are the same mapped step failing on different
    dynamic-output indices -- strip the trailing bracketed index before hashing into the
    signature, so N parallel failures of the same underlying step collapse to one signature
    instead of N distinct ones that each look brand-new to the cooldown."""
    return re.sub(r"\[[^\]]*\]$", "", step_key)


def error_signature(location: str, job_name: str, error_class: str, message: str, step_keys: List[str]) -> str:
    """Stable id for 'the same failure': location + job + root error class + the message with
    ids/numbers normalized + failed steps (dynamic-mapping suffixes stripped -- see
    _strip_dynamic_suffix)."""
    first = (message or "").strip().splitlines()[0] if (message or "").strip() else ""
    norm = re.sub(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", "<uuid>", first)
    norm = re.sub(r"0x[0-9a-fA-F]+", "<hex>", norm)
    norm = re.sub(r"\d+", "<n>", norm)[:200]
    normalized_steps = sorted({_strip_dynamic_suffix(k) for k in step_keys})
    blob = json.dumps([location or "", job_name, error_class, norm, normalized_steps], ensure_ascii=False)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]
# END failure-cooldown rate limiter


def _extract_failure_signature(context, location: Optional[str], job_name: str) -> "tuple[str, List[str]]":
    """The failed step keys + a stable error signature, from the run's own STEP_FAILURE events (falling back to
    the generic failure_event when there are none -- e.g. a run-level failure with no step ever starting)."""
    events = context.get_step_failure_events()
    step_keys = [e.step_key for e in events if getattr(e, "step_key", None)]
    error_class, message = "", ""
    if events:
        err = getattr(events[0].event_specific_data, "error", None)
        root = err
        while root is not None and getattr(root, "cause", None) is not None:
            root = root.cause
        if root is not None:
            error_class = getattr(root, "cls_name", "") or ""
            message = getattr(root, "message", "") or ""
    else:
        fe = getattr(context, "failure_event", None)
        message = (getattr(fe, "message", None) or "") if fe is not None else ""
    return error_signature(location or "", job_name, error_class, message, step_keys), step_keys


def _run_history_admit(
    instance, sensor_name: str, sig: str, now: datetime, max_per_hour: Optional[int],
    max_per_day: Optional[int], cooldown_minutes: float,
) -> "tuple[str, Optional[str]]":
    """Same verdict shape as _RateLimiter.admit, but reads and writes no files at all: cooldown and
    rate-limit state are both derived purely from runs this sensor itself already launched, found by
    querying past RunRequest tags (eng/error_signature for cooldown, eng/sensor_name for the two
    GLOBAL rate ceilings -- mirroring _RateLimiter.admit's own global-not-per-key counting). Used
    when evidence_dir is unset: no state directory, no shared disk requirement, survives a restart
    for free since it's just a query against Dagster's own run storage."""
    import dagster as dg

    if cooldown_minutes > 0:
        same_sig = instance.get_run_records(
            filters=dg.RunsFilter(tags={"eng/error_signature": sig}, created_after=now - timedelta(minutes=cooldown_minutes)),
        )
        if same_sig:
            newest = max(r.create_timestamp for r in same_sig)
            return "cooldown", _iso(newest + timedelta(minutes=cooldown_minutes))
    if max_per_hour:
        cnt = len(instance.get_run_records(
            filters=dg.RunsFilter(tags={"eng/sensor_name": sensor_name}, created_after=now - timedelta(hours=1)),
        ))
        if cnt >= int(max_per_hour):
            return "rate_limited", None
    if max_per_day:
        cnt = len(instance.get_run_records(
            filters=dg.RunsFilter(tags={"eng/sensor_name": sensor_name}, created_after=now - timedelta(hours=24)),
        ))
        if cnt >= int(max_per_day):
            return "rate_limited", None
    return "ok", None


def _already_triaged_retry_chain_root(instance, root_run_id: str) -> bool:
    """True when some earlier run in this retry chain was already triaged (tagged
    eng/retry_chain_root=root_run_id on the launched RunRequest) -- a direct lineage check via
    Dagster's own dagster/root_run_id tag, independent of signature matching entirely, so it still
    works even if cooldown_minutes/cooldown_hours is 0 or the retry's error message happens to differ
    slightly from the original's."""
    import dagster as dg

    return bool(instance.get_run_records(filters=dg.RunsFilter(tags={"eng/retry_chain_root": root_run_id}), limit=1))


def _get_slack_client(bot_token_env_var: str):
    try:
        from slack_sdk import WebClient
    except ImportError as e:
        raise ImportError("requires slack_sdk: pip install 'slack_sdk>=3.0'") from e
    token = os.environ.get(bot_token_env_var)
    if not token:
        raise RuntimeError(f"Slack bot token env var {bot_token_env_var!r} not set.")
    return WebClient(token=token)


class FailedRunAgentSensorComponent(dg.Component, dg.Model, dg.Resolvable):
    """On a failed run, launches `asset_keys` with the failed run id on the launched run's tags, and announces it
    in Slack -- the thread stays open for `slack_reply` to answer in. Rate-limited per (job, error signature)."""

    sensor_name: str = Field(default="failed_run_agent_sensor", description="Unique sensor name.")
    asset_keys: List[str] = Field(description="Agent asset(s) to launch on a failure (e.g. a bug-fixer asset + its Slack-reply asset).")

    code_location_names: Optional[List[str]] = Field(default=None, description="Only trigger for runs from one of these code locations. Leave unset to monitor every code location.")
    job_names: Optional[List[str]] = Field(default=None, description="Only trigger for these job names. Leave unset to monitor every job.")
    required_tags: Optional[Dict[str, Any]] = Field(default=None, description="Only trigger when the failed run carries ALL of these tags (key=value).")

    slack_channel: str = Field(description="Channel to announce a failed run in (either `C123ABC456` id or `#channel-name`).")
    slack_bot_token_env_var: str = Field(default="SLACK_BOT_TOKEN", description="Env var holding the bot's `xoxb-...` token. Needs `chat:write`.")
    message_template: Optional[str] = Field(
        default=None,
        description="Optional custom message template. Placeholders: {run_id} (short, 8 chars), {job_name}, {url}. Default: ':beetle: Run <{url}|{run_id}> of `{job_name}` failed. Looking into the cause.'",
    )
    dagster_url: Optional[str] = Field(default=None, description="Base Dagster(+) URL (e.g. 'https://org.dagster.cloud/prod') used to build the run link in the Slack message. Leave unset to link just the run id with no URL.")

    evidence_dir: Optional[str] = Field(
        default=None,
        description=(
            "Directory the cooldown rate-limiter's small per-event files live under (local or "
            "NFS-shared path -- not a cloud URI; exclusive-create needs a real filesystem). Leave "
            "unset to track cooldown/rate-limit state purely from past run history instead -- "
            "queried by the eng/error_signature and eng/sensor_name tags this sensor puts on every "
            "run it launches -- with no state file at all, no shared-disk requirement, and nothing "
            "lost on a restart. Set this only for the more precise sliding-window counting the "
            "file-based limiter gives (it has a finer-grained global rate ceiling than the "
            "once-per-tick run-history query does)."
        ),
    )
    max_triage_per_hour: int = Field(default=10, description="Max failures triaged (Slack post + run launch) GLOBALLY (across every signature) per rolling hour.")
    max_runs_per_day: Optional[int] = Field(
        default=None,
        description=(
            "Max failures triaged GLOBALLY (across every signature) per rolling 24 hours -- an "
            "independent ceiling on top of max_triage_per_hour, for a deployment where many distinct "
            "failure signatures could each individually stay under the hourly cap but still add up to "
            "an unreasonable number of agent runs over a day. Leave unset for no daily cap."
        ),
    )
    cooldown_minutes: float = Field(default=60.0, description="After triaging one occurrence of a (job, error signature), suppress further triage of the SAME signature for this many minutes. Ignored when cooldown_hours is set.")
    cooldown_hours: Optional[float] = Field(
        default=None,
        description="Same as cooldown_minutes, expressed in hours -- set this instead when the natural unit is days (e.g. 168 for a week of flapping). Takes precedence over cooldown_minutes when set.",
    )
    use_retry_rule: bool = Field(
        default=True,
        description=(
            "A failed run that is itself a retry of an earlier run (carries Dagster's own "
            "dagster/root_run_id tag) is skipped if that retry chain's root was already triaged; an "
            "untriaged retry becomes the triage point for its whole chain instead, so later retries of "
            "the same underlying failure are suppressed too. This is a direct lineage check (not "
            "signature matching), so it still works even with cooldown set to 0."
        ),
    )

    minimum_interval_seconds: int = Field(default=30, description="Minimum seconds between sensor evaluations.")
    default_status: str = Field(default="RUNNING", description="'RUNNING' or 'STOPPED'.")
    skip_in_branch_deployments: bool = Field(
        default=True,
        description=(
            "Skip every tick while running inside a Dagster+ branch deployment "
            "(DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=1). A branch deployment runs its own copy of every sensor "
            "alongside the full deployment's -- without this, a branch deployment double-announces the same "
            "failed run in Slack and double-launches a fixer run."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("asset_keys", "code_location_names", "job_names"), csv=(), tags=("required_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Failed Run Agent Sensor", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        targets = [dg.AssetKey.from_user_string(k) for k in self.asset_keys]
        job = dg.define_asset_job(f"{self.sensor_name}_job", selection=targets)

        code_location_names = set(self.code_location_names) if self.code_location_names else None
        job_names = set(self.job_names) if self.job_names else None
        required_tags = dict(self.required_tags or {})
        slack_channel = self.slack_channel
        slack_bot_token_env_var = self.slack_bot_token_env_var
        message_template = self.message_template or ":beetle: Run <{url}|{run_id}> of `{job_name}` failed. Looking into the cause."
        dagster_url = (self.dagster_url or "").rstrip("/")
        evidence_dir = self.evidence_dir
        max_triage_per_hour = self.max_triage_per_hour
        max_runs_per_day = self.max_runs_per_day
        cooldown_minutes = self.cooldown_hours * 60.0 if self.cooldown_hours is not None else self.cooldown_minutes
        use_retry_rule = self.use_retry_rule
        sensor_name = self.sensor_name
        skip_in_branch_deployments = self.skip_in_branch_deployments

        def failure_fn(context: dg.RunFailureSensorContext):
            if skip_in_branch_deployments and os.environ.get("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT") == "1":
                return dg.SkipReason("branch deployment -- skipping (skip_in_branch_deployments=True)")
            run = context.dagster_run
            origin = run.remote_job_origin
            location = origin.location_name if origin else None
            if code_location_names and location not in code_location_names:
                return dg.SkipReason(f"run {run.run_id[:8]} is not in a monitored code location ({location!r}).")
            if job_names and run.job_name not in job_names:
                return dg.SkipReason(f"run {run.run_id[:8]}'s job {run.job_name!r} is not monitored.")
            if required_tags and not all((run.tags or {}).get(k) == v for k, v in required_tags.items()):
                return dg.SkipReason(f"run {run.run_id[:8]} does not carry all required_tags.")

            root_run_id = (run.tags or {}).get("dagster/root_run_id")
            chain_root = root_run_id or run.run_id
            if use_retry_rule and root_run_id and _already_triaged_retry_chain_root(context.instance, chain_root):
                return dg.SkipReason(
                    f"retry: run {run.run_id[:8]} is a retry (dagster/root_run_id={chain_root[:8]}) "
                    "of a run already triaged."
                )

            sig, step_keys = _extract_failure_signature(context, location, run.job_name)
            now = datetime.now(timezone.utc)
            if evidence_dir:
                ckey = f"{run.job_name}|{sig}"
                fs = _RateLimiterFS(evidence_dir)
                fs.mkdir()
                limiter = _RateLimiter(fs)
                verdict, until = limiter.admit(ckey, run.run_id, now, max_triage_per_hour, cooldown_minutes, max_per_day=max_runs_per_day)
            else:
                verdict, until = _run_history_admit(context.instance, sensor_name, sig, now, max_triage_per_hour, max_runs_per_day, cooldown_minutes)
            if verdict == "cooldown":
                cooldown_h = cooldown_minutes / 60.0
                return dg.SkipReason(f"Same failure analyzed in the last {cooldown_h:g}h ({sig}) -- job {run.job_name!r}, suppressed until {until}.")
            if verdict == "rate_limited":
                cap_desc = f"max_triage_per_hour={max_triage_per_hour}" + (f", max_runs_per_day={max_runs_per_day}" if max_runs_per_day else "")
                return dg.SkipReason(f"rate limit reached ({cap_desc}) for job {run.job_name!r} / signature {sig}.")

            run_url = f"{dagster_url}/runs/{run.run_id}" if dagster_url else run.run_id
            text = message_template.format(run_id=run.run_id[:8], job_name=run.job_name, url=run_url)
            client = _get_slack_client(slack_bot_token_env_var)
            try:
                posted = client.chat_postMessage(channel=slack_channel, text=text)
            except Exception as e:  # noqa: BLE001
                return dg.SkipReason(f"Slack post failed: {e}")

            tags = {
                "eng/failed_run_id": run.run_id, "eng/error_signature": sig, "eng/sensor_name": sensor_name,
                "slack/channel": posted["channel"], "slack/thread_ts": posted["ts"],
            }
            if use_retry_rule:
                tags["eng/retry_chain_root"] = chain_root
            return dg.RunRequest(run_key=run.run_id, tags=tags)

        sensor_def = dg.run_failure_sensor(
            name=self.sensor_name,
            monitor_all_code_locations=True,
            request_job=job,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=dg.DefaultSensorStatus.RUNNING if self.default_status.upper() == "RUNNING" else dg.DefaultSensorStatus.STOPPED,
        )(failure_fn)

        return dg.Definitions(sensors=[sensor_def])
