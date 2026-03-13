"""AutoSys Asset Component.

Connects to a CA/Broadcom AutoSys enterprise job scheduler (REST API v12+ or CLI
fallback) and creates one Dagster asset per discovered AutoSys job. Box jobs are
expanded into sub-DAGs with condition-based dependency edges. File Watcher jobs are
represented as external AssetSpecs (source assets).

Uses StateBackedComponent so the AutoSys API/CLI is called once at prepare time
(write_state_to_path) and cached on disk. build_defs_from_state builds the full
asset graph from the cached job list with zero network calls, keeping code-server
reloads fast.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""
from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


# ── Condition parser ──────────────────────────────────────────────────────────

def _parse_conditions(condition_str: str) -> list[str]:
    """Parse AutoSys condition syntax and return upstream job names to depend on.

    Rules:
    - s(JobName)  → success dependency  → included
    - d(JobName)  → done dependency     → included (treat done as a dep)
    - f(JobName)  → failure dependency  → excluded (noted in metadata only)
    - exitcode()  → ignored
    - value()     → ignored
    - & / AND     → all branches collected (union)
    - | / OR      → all branches collected (conservative union)
    - Nested parens are handled by the regex scan across the whole string.
    """
    if not condition_str:
        return []
    # Match s(...) or d(...) — include; f(...) — skip
    deps: list[str] = []
    for m in re.finditer(r'([sdf])\(([^)]+)\)', condition_str):
        qualifier, job_name = m.group(1), m.group(2).strip()
        if qualifier in ("s", "d"):
            deps.append(job_name)
    return deps


def _sanitize(name: str) -> str:
    """Convert an AutoSys job name to a valid Dagster asset name."""
    # Replace characters that are invalid in Dagster asset names
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()


# ── REST / CLI helpers ────────────────────────────────────────────────────────

def _rest_headers(
    username_env_var: Optional[str],
    password_env_var: Optional[str],
    token_env_var: Optional[str],
) -> dict:
    headers: dict = {"Content-Type": "application/json", "Accept": "application/json"}
    if token_env_var and os.environ.get(token_env_var):
        headers["X-AUTH-TOKEN"] = os.environ[token_env_var]
    elif username_env_var and password_env_var:
        user = os.environ.get(username_env_var, "")
        pwd = os.environ.get(password_env_var, "")
        b64 = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        headers["Authorization"] = f"Basic {b64}"
    return headers


def _rest_base_url(host: str, port: int, use_ssl: bool) -> str:
    scheme = "https" if use_ssl else "http"
    return f"{scheme}://{host}:{port}/AEWS/api"


def _fetch_jobs_rest(
    host: str,
    port: int,
    use_ssl: bool,
    verify_ssl: bool,
    username_env_var: Optional[str],
    password_env_var: Optional[str],
    token_env_var: Optional[str],
    job_filter: str,
) -> list[dict]:
    """Fetch job list from AutoSys REST API (v12+)."""
    import requests  # optional dep — only needed at state-write time

    base = _rest_base_url(host, port, use_ssl)
    headers = _rest_headers(username_env_var, password_env_var, token_env_var)
    url = f"{base}/job"
    params = {"filter": f"job_name=={job_filter}"}
    resp = requests.get(url, headers=headers, params=params, timeout=30, verify=verify_ssl)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("jobs", data.get("data", []))


def _fetch_box_children_rest(
    host: str,
    port: int,
    use_ssl: bool,
    verify_ssl: bool,
    username_env_var: Optional[str],
    password_env_var: Optional[str],
    token_env_var: Optional[str],
    box_name: str,
) -> list[dict]:
    """Fetch child jobs of a BOX via REST."""
    import requests

    base = _rest_base_url(host, port, use_ssl)
    headers = _rest_headers(username_env_var, password_env_var, token_env_var)
    url = f"{base}/job"
    params = {"filter": f"boxname=={box_name}"}
    resp = requests.get(url, headers=headers, params=params, timeout=30, verify=verify_ssl)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("jobs", data.get("data", []))


def _parse_jil_output(jil_text: str) -> list[dict]:
    """Parse autorep -q JIL output into a list of job dicts.

    JIL blocks look like:
        /* ------------- ETL_EXTRACT ------------- */
        insert_job: ETL_EXTRACT   job_type: CMD
        box_name: ETL_DAILY_BOX
        command: /opt/etl/extract.sh
        machine: etl-server-01
        condition: s(ETL_EXTRACT_PREP)
        description: "Extract from source"
        ...
    """
    jobs: list[dict] = []
    current: dict = {}

    for line in jil_text.splitlines():
        line = line.strip()
        if not line or line.startswith("/*"):
            if current.get("name"):
                jobs.append(current)
                current = {}
            continue

        # Key-value pairs separated by a colon
        if ":" in line:
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip().strip('"')

            if key == "insert_job":
                # New job block — flush previous
                if current.get("name"):
                    jobs.append(current)
                # Parse "insert_job: NAME   job_type: TYPE"
                parts = value.split()
                current = {
                    "name": parts[0] if parts else value,
                    "type": "CMD",
                    "machine": None,
                    "condition": None,
                    "description": None,
                    "box_name": None,
                    "children": [],
                    "command": None,
                }
                # job_type may be inline on the same line
                if "job_type" in line:
                    m = re.search(r'job_type:\s*(\S+)', line)
                    if m:
                        current["type"] = m.group(1).upper()
            elif key == "job_type":
                current["type"] = value.upper()
            elif key == "box_name":
                current["box_name"] = value or None
            elif key == "command":
                current["command"] = value or None
            elif key == "machine":
                current["machine"] = value or None
            elif key == "condition":
                current["condition"] = value or None
            elif key == "description":
                current["description"] = value or None

    if current.get("name"):
        jobs.append(current)

    return jobs


def _fetch_jobs_cli(job_filter: str, autosys_instance: Optional[str]) -> list[dict]:
    """Fetch jobs using autorep CLI subprocess."""
    env = os.environ.copy()
    if autosys_instance:
        env["AUTOSERV"] = autosys_instance

    result = subprocess.run(
        ["autorep", "-J", job_filter, "-q"],
        capture_output=True, text=True, env=env, timeout=120,
    )
    if result.returncode not in (0, 1):  # autorep returns 1 for "no jobs found"
        raise RuntimeError(
            f"autorep exited {result.returncode}: {result.stderr.strip()}"
        )
    return _parse_jil_output(result.stdout)


def _fetch_box_children_cli(
    box_name: str, autosys_instance: Optional[str]
) -> list[str]:
    """Return child job names for a BOX job via CLI."""
    env = os.environ.copy()
    if autosys_instance:
        env["AUTOSERV"] = autosys_instance
    result = subprocess.run(
        ["autorep", "-J", box_name, "-q"],
        capture_output=True, text=True, env=env, timeout=60,
    )
    jobs = _parse_jil_output(result.stdout)
    return [j["name"] for j in jobs if j.get("box_name") == box_name]


# ── Runtime helpers: trigger + poll ──────────────────────────────────────────

_TERMINAL_SUCCESS = {"SU"}
_TERMINAL_FAILURE = {"FA", "TE", "OH", "OI"}
_RUNNING_STATUSES = {"RU", "ST", "AC", "QW", "WA", "RE"}

_STATUS_MESSAGES: dict[str, str] = {
    "SU": "SUCCESS",
    "FA": "FAILURE",
    "TE": "TERMINATED",
    "OH": (
        "Job is ON_HOLD in AutoSys — release it with: "
        "sendevent -E JOB_OFF_HOLD -J {name}"
    ),
    "OI": (
        "Job is ON_ICE — remove with: "
        "sendevent -E JOB_OFF_ICE -J {name}"
    ),
    "RU": "RUNNING",
    "ST": "STARTING",
    "AC": "ACTIVATED",
    "QW": "QUE_WAIT",
    "WA": "WAITING",
    "RE": "RESTART",
    "IN": "INACTIVE",
    "SK": "SKIPPED",
}


def _trigger_job_rest(
    job_name: str,
    host: str,
    port: int,
    use_ssl: bool,
    verify_ssl: bool,
    username_env_var: Optional[str],
    password_env_var: Optional[str],
    token_env_var: Optional[str],
    force_start: bool,
) -> None:
    import requests

    base = _rest_base_url(host, port, use_ssl)
    headers = _rest_headers(username_env_var, password_env_var, token_env_var)
    event = "force-start-job" if force_start else "start-job"
    url = f"{base}/event/{event}"
    resp = requests.post(url, headers=headers, json={"job": job_name}, timeout=30, verify=verify_ssl)
    resp.raise_for_status()


def _get_status_rest(
    job_name: str,
    host: str,
    port: int,
    use_ssl: bool,
    verify_ssl: bool,
    username_env_var: Optional[str],
    password_env_var: Optional[str],
    token_env_var: Optional[str],
) -> str:
    import requests

    base = _rest_base_url(host, port, use_ssl)
    headers = _rest_headers(username_env_var, password_env_var, token_env_var)
    url = f"{base}/job/{job_name}"
    resp = requests.get(url, headers=headers, timeout=30, verify=verify_ssl)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list) and data:
        data = data[0]
    return str(data.get("status", "UN")).upper()


def _trigger_job_cli(
    job_name: str, force_start: bool, autosys_instance: Optional[str]
) -> None:
    env = os.environ.copy()
    if autosys_instance:
        env["AUTOSERV"] = autosys_instance
    event = "FORCE_STARTJOB" if force_start else "STARTJOB"
    result = subprocess.run(
        ["sendevent", "-E", event, "-J", job_name],
        capture_output=True, text=True, env=env, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"sendevent failed (rc={result.returncode}): {result.stderr.strip()}"
        )


def _get_status_cli(
    job_name: str, autosys_instance: Optional[str]
) -> str:
    env = os.environ.copy()
    if autosys_instance:
        env["AUTOSERV"] = autosys_instance
    result = subprocess.run(
        ["autorep", "-J", job_name, "-s"],
        capture_output=True, text=True, env=env, timeout=30,
    )
    # autorep -s output: columns Job Name | Last Start | Last End | Status/Exit Code
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[0] == job_name:
            return parts[-1].upper()
    return "UN"


def _run_and_poll(
    context: dg.AssetExecutionContext,
    job_name: str,
    machine: Optional[str],
    use_rest: bool,
    host: str,
    port: int,
    use_ssl: bool,
    verify_ssl: bool,
    username_env_var: Optional[str],
    password_env_var: Optional[str],
    token_env_var: Optional[str],
    autosys_instance: Optional[str],
    force_start: bool,
    poll_interval_seconds: int,
    job_timeout_seconds: int,
) -> dg.MaterializeResult:
    run_mode = "rest" if use_rest else "cli"
    context.log.info(
        f"Triggering AutoSys job '{job_name}' via {run_mode} "
        f"({'FORCE_' if force_start else ''}STARTJOB)"
    )

    if use_rest:
        _trigger_job_rest(
            job_name, host, port, use_ssl, verify_ssl,
            username_env_var, password_env_var, token_env_var, force_start,
        )
    else:
        _trigger_job_cli(job_name, force_start, autosys_instance)

    elapsed = 0
    status = "UN"
    while elapsed < job_timeout_seconds:
        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

        if use_rest:
            status = _get_status_rest(
                job_name, host, port, use_ssl, verify_ssl,
                username_env_var, password_env_var, token_env_var,
            )
        else:
            status = _get_status_cli(job_name, autosys_instance)

        context.log.info(
            f"Job '{job_name}' status: {status} "
            f"({_STATUS_MESSAGES.get(status, status)}) — {elapsed}s elapsed"
        )

        if status in _TERMINAL_SUCCESS:
            break
        if status in _TERMINAL_FAILURE:
            msg = _STATUS_MESSAGES.get(status, status).format(name=job_name)
            raise Exception(f"AutoSys job '{job_name}' ended with status {status}: {msg}")
        if status not in _RUNNING_STATUSES:
            context.log.warning(f"Unexpected status '{status}' — continuing to poll")

    if status not in _TERMINAL_SUCCESS:
        raise Exception(
            f"AutoSys job '{job_name}' did not complete within {job_timeout_seconds}s. "
            f"Last status: {status}"
        )

    return dg.MaterializeResult(metadata={
        "autosys/job_name": dg.MetadataValue.text(job_name),
        "autosys/final_status": dg.MetadataValue.text(status),
        "autosys/machine": dg.MetadataValue.text(machine or "any"),
        "autosys/run_mode": dg.MetadataValue.text(run_mode),
        "autosys/force_start": dg.MetadataValue.bool(force_start),
    })


# ── Override helpers ──────────────────────────────────────────────────────────

def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    return dg.AssetSpec(
        key=dg.AssetKey.from_user_string(ov["key"]) if "key" in ov else base.key,
        description=ov.get("description", base.description),
        group_name=ov.get("group_name", base.group_name),
        metadata={**(base.metadata or {}), **(ov.get("metadata") or {})},
        tags={**(base.tags or {}), **(ov.get("tags") or {})},
        kinds=set(ov["kinds"]) if "kinds" in ov else base.kinds,
        deps=list(base.deps or []) + extra_deps,
    )


def _apply_job_overrides(
    default_spec: dg.AssetSpec,
    job_name: str,
    overrides: Optional[dict],
) -> list[dg.AssetSpec]:
    """Apply assets_by_job_name overrides. Returns list (1 usually, >1 if one job → multiple assets)."""
    if not overrides or job_name not in overrides:
        return [default_spec]
    ov = overrides[job_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


# ── Defs builder ──────────────────────────────────────────────────────────────

def _build_autosys_defs(
    jobs: list[dict],
    api_mode: str,
    host: str,
    port: int,
    use_ssl: bool,
    verify_ssl: bool,
    username_env_var: Optional[str],
    password_env_var: Optional[str],
    token_env_var: Optional[str],
    use_cli: bool,
    autosys_instance: Optional[str],
    group_name: str,
    key_prefix: Optional[str],
    expand_box_jobs: bool,
    box_completion_asset: bool,
    map_conditions: bool,
    force_start: bool,
    poll_interval_seconds: int,
    job_timeout_seconds: int,
    assets_by_job_name: Optional[dict] = None,
) -> dg.Definitions:
    """Build Definitions from a cached list of AutoSys job dicts (no network calls)."""
    if not jobs:
        return dg.Definitions()

    use_rest = (api_mode == "rest") and not use_cli

    def _asset_key(job_name: str) -> dg.AssetKey:
        parts = [key_prefix, _sanitize(job_name)] if key_prefix else [_sanitize(job_name)]
        return dg.AssetKey([p for p in parts if p])

    # Index jobs by name for fast lookup
    job_index = {j["name"]: j for j in jobs}

    assets: list = []

    # Track which jobs have been emitted as assets to avoid duplicates
    emitted: set[str] = set()

    # ── File Watcher jobs → external AssetSpec (source assets) ──
    fw_jobs = [j for j in jobs if j.get("type", "").upper() == "FW"]
    for job in fw_jobs:
        default_spec = dg.AssetSpec(
            key=_asset_key(job["name"]),
            description=job.get("description") or f"AutoSys File Watcher job: {job['name']}",
            group_name=group_name,
            kinds={"file_watcher", "autosys"},
            metadata={
                "autosys/job_name": dg.MetadataValue.text(job["name"]),
                "autosys/job_type": dg.MetadataValue.text("FW"),
                "autosys/machine": dg.MetadataValue.text(job.get("machine") or "any"),
                "autosys/box": dg.MetadataValue.text(job.get("box_name") or ""),
            },
        )
        expanded = _apply_job_overrides(default_spec, job["name"], assets_by_job_name)
        assets.extend(expanded)
        emitted.add(job["name"])

    # ── BOX jobs ──
    box_jobs = [j for j in jobs if j.get("type", "").upper() == "BOX"]

    for box_job in box_jobs:
        box_name = box_job["name"]
        children: list[str] = box_job.get("children") or []

        if expand_box_jobs and children:
            # Create individual assets for each child job
            child_asset_keys: list[dg.AssetKey] = []

            for child_name in children:
                child_job = job_index.get(child_name)
                if child_job is None:
                    # Child not in discovered set — create a minimal stub
                    child_job = {
                        "name": child_name,
                        "type": "CMD",
                        "machine": None,
                        "condition": None,
                        "description": f"AutoSys CMD job: {child_name}",
                        "box_name": box_name,
                        "children": [],
                        "command": None,
                    }

                child_key = _asset_key(child_name)
                child_asset_keys.append(child_key)

                if child_name in emitted:
                    continue
                emitted.add(child_name)

                if child_job.get("type", "").upper() == "FW":
                    continue  # already handled above

                # Resolve condition-based deps
                condition_str = child_job.get("condition") or ""
                dep_job_names = _parse_conditions(condition_str) if map_conditions else []
                dep_keys = [_asset_key(d) for d in dep_job_names]

                # Capture loop vars for closure
                _child_name = child_name
                _child_job = child_job
                _dep_keys = dep_keys
                _child_key = child_key

                @dg.asset(
                    name=_sanitize(_child_name),
                    key_prefix=[key_prefix] if key_prefix else [],
                    group_name=group_name,
                    deps=_dep_keys,
                    description=_child_job.get("description") or f"AutoSys CMD job: {_child_name}",
                    metadata={
                        "autosys/job_name": dg.MetadataValue.text(_child_name),
                        "autosys/job_type": dg.MetadataValue.text(_child_job.get("type", "CMD")),
                        "autosys/machine": dg.MetadataValue.text(_child_job.get("machine") or "any"),
                        "autosys/command": dg.MetadataValue.text(_child_job.get("command") or ""),
                        "autosys/box": dg.MetadataValue.text(_child_job.get("box_name") or ""),
                        "autosys/condition": dg.MetadataValue.text(_child_job.get("condition") or ""),
                    },
                    kinds={"autosys"},
                )
                def _child_asset_fn(
                    context: dg.AssetExecutionContext,
                    _jname=_child_name,
                    _jmachine=_child_job.get("machine"),
                ) -> dg.MaterializeResult:
                    return _run_and_poll(
                        context=context,
                        job_name=_jname,
                        machine=_jmachine,
                        use_rest=use_rest,
                        host=host, port=port,
                        use_ssl=use_ssl, verify_ssl=verify_ssl,
                        username_env_var=username_env_var,
                        password_env_var=password_env_var,
                        token_env_var=token_env_var,
                        autosys_instance=autosys_instance,
                        force_start=force_start,
                        poll_interval_seconds=poll_interval_seconds,
                        job_timeout_seconds=job_timeout_seconds,
                    )

                assets.append(_child_asset_fn)

            # ── Box completion gate asset ──
            if box_completion_asset and child_asset_keys:
                _box_name = box_name
                _child_keys = child_asset_keys

                @dg.asset(
                    name=f"{_sanitize(_box_name)}_complete",
                    key_prefix=[key_prefix] if key_prefix else [],
                    group_name=group_name,
                    deps=_child_keys,
                    description=f"AutoSys Box complete: {_box_name}",
                    metadata={
                        "autosys/job_name": dg.MetadataValue.text(_box_name),
                        "autosys/job_type": dg.MetadataValue.text("BOX"),
                        "autosys/children": dg.MetadataValue.json(children),
                    },
                    kinds={"autosys"},
                )
                def _box_complete_fn(
                    context: dg.AssetExecutionContext,
                    _bname=_box_name,
                    _kids=children,
                ) -> dg.MaterializeResult:
                    context.log.info(f"Box {_bname} — all {len(_kids)} children succeeded")
                    return dg.MaterializeResult(metadata={
                        "autosys/box": dg.MetadataValue.text(_bname),
                        "autosys/children": dg.MetadataValue.json(_kids),
                        "autosys/child_count": dg.MetadataValue.int(len(_kids)),
                    })

                assets.append(_box_complete_fn)
                emitted.add(f"{box_name}_complete")

        else:
            # Treat BOX as a single opaque asset
            if box_name in emitted:
                continue
            emitted.add(box_name)

            condition_str = box_job.get("condition") or ""
            dep_job_names = _parse_conditions(condition_str) if map_conditions else []
            dep_keys = [_asset_key(d) for d in dep_job_names]

            _box_name = box_name
            _box_job = box_job

            @dg.asset(
                name=_sanitize(_box_name),
                key_prefix=[key_prefix] if key_prefix else [],
                group_name=group_name,
                deps=dep_keys,
                description=_box_job.get("description") or f"AutoSys BOX job: {_box_name}",
                metadata={
                    "autosys/job_name": dg.MetadataValue.text(_box_name),
                    "autosys/job_type": dg.MetadataValue.text("BOX"),
                    "autosys/condition": dg.MetadataValue.text(_box_job.get("condition") or ""),
                    "autosys/children": dg.MetadataValue.json(_box_job.get("children") or []),
                },
                kinds={"autosys"},
            )
            def _box_asset_fn(
                context: dg.AssetExecutionContext,
                _jname=_box_name,
            ) -> dg.MaterializeResult:
                return _run_and_poll(
                    context=context,
                    job_name=_jname,
                    machine=None,
                    use_rest=use_rest,
                    host=host, port=port,
                    use_ssl=use_ssl, verify_ssl=verify_ssl,
                    username_env_var=username_env_var,
                    password_env_var=password_env_var,
                    token_env_var=token_env_var,
                    autosys_instance=autosys_instance,
                    force_start=force_start,
                    poll_interval_seconds=poll_interval_seconds,
                    job_timeout_seconds=job_timeout_seconds,
                )

            assets.append(_box_asset_fn)

    # ── Standalone CMD jobs (not in any box, not already emitted) ──
    for job in jobs:
        job_name = job["name"]
        job_type = job.get("type", "CMD").upper()

        if job_name in emitted or job_type in ("BOX", "FW"):
            continue
        emitted.add(job_name)

        condition_str = job.get("condition") or ""
        dep_job_names = _parse_conditions(condition_str) if map_conditions else []
        dep_keys = [_asset_key(d) for d in dep_job_names]

        _job_name = job_name
        _job = job

        _default_spec = dg.AssetSpec(
            key=_asset_key(_job_name),
            description=_job.get("description") or f"AutoSys {job_type} job: {_job_name}",
            group_name=group_name,
            kinds={"autosys"},
            deps=dep_keys,
            metadata={
                "autosys/job_name": dg.MetadataValue.text(_job_name),
                "autosys/job_type": dg.MetadataValue.text(job_type),
                "autosys/machine": dg.MetadataValue.text(_job.get("machine") or "any"),
                "autosys/command": dg.MetadataValue.text(_job.get("command") or ""),
                "autosys/box": dg.MetadataValue.text(_job.get("box_name") or ""),
                "autosys/condition": dg.MetadataValue.text(_job.get("condition") or ""),
            },
        )
        _expanded = _apply_job_overrides(_default_spec, _job_name, assets_by_job_name)

        if len(_expanded) == 1:
            _spec = _expanded[0]

            @dg.asset(
                key=_spec.key,
                group_name=_spec.group_name,
                deps=list(_spec.deps or []),
                description=_spec.description,
                metadata=dict(_spec.metadata or {}),
                tags=dict(_spec.tags or {}),
                kinds=set(_spec.kinds or set()),
            )
            def _cmd_asset_fn(
                context: dg.AssetExecutionContext,
                _jname=_job_name,
                _jmachine=_job.get("machine"),
            ) -> dg.MaterializeResult:
                return _run_and_poll(
                    context=context,
                    job_name=_jname,
                    machine=_jmachine,
                    use_rest=use_rest,
                    host=host, port=port,
                    use_ssl=use_ssl, verify_ssl=verify_ssl,
                    username_env_var=username_env_var,
                    password_env_var=password_env_var,
                    token_env_var=token_env_var,
                    autosys_instance=autosys_instance,
                    force_start=force_start,
                    poll_interval_seconds=poll_interval_seconds,
                    job_timeout_seconds=job_timeout_seconds,
                )

            assets.append(_cmd_asset_fn)
        else:
            # One AutoSys job → multiple Dagster assets via multi_asset
            _safe_name = _sanitize(_job_name)
            _multi_specs = _expanded
            _multi_jname = _job_name
            _multi_jmachine = _job.get("machine")

            @dg.multi_asset(
                name=f"autosys_{_safe_name}_multi",
                specs=_multi_specs,
            )
            def _cmd_multi_asset_fn(
                context: dg.AssetExecutionContext,
                _jname=_multi_jname,
                _jmachine=_multi_jmachine,
                _specs=_multi_specs,
            ):
                result = _run_and_poll(
                    context=context,
                    job_name=_jname,
                    machine=_jmachine,
                    use_rest=use_rest,
                    host=host, port=port,
                    use_ssl=use_ssl, verify_ssl=verify_ssl,
                    username_env_var=username_env_var,
                    password_env_var=password_env_var,
                    token_env_var=token_env_var,
                    autosys_instance=autosys_instance,
                    force_start=force_start,
                    poll_interval_seconds=poll_interval_seconds,
                    job_timeout_seconds=job_timeout_seconds,
                )
                for spec in _specs:
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata=result.metadata or {},
                    )

            assets.append(_cmd_multi_asset_fn)

    return dg.Definitions(assets=assets)


# ── Component ─────────────────────────────────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class AutoSysAssetComponent(StateBackedComponent, dg.Resolvable):
        """AutoSys enterprise job scheduler component.

        Connects to CA/Broadcom AutoSys (REST API v12+ or CLI fallback) and creates
        one Dagster asset per discovered job. Box jobs are expanded into sub-DAGs
        with condition-based dependency edges. File Watcher jobs are represented as
        external source assets.

        Uses StateBackedComponent to cache the job catalogue from AutoSys at
        prepare time (write_state_to_path). build_defs_from_state reconstructs the
        full asset graph from the cached JSON — zero network calls on hot reloads.

        Populate / refresh the cache with:
          dagster dev                         (automatic in local dev)
          dg utils refresh-defs-state         (CI/CD or image build)

        Example:
            ```yaml
            type: dagster_component_templates.AutoSysAssetComponent
            attributes:
              host: autosys.internal.company.com
              port: 9443
              username_env_var: AUTOSYS_USER
              password_env_var: AUTOSYS_PASSWORD
              job_filter: "FINANCE_ETL_*"
              expand_box_jobs: true
              box_completion_asset: true
              map_conditions: true
              group_name: autosys_finance
            ```
        """

        # ── Connection — REST API (v12+) ──────────────────────────────────
        host: str = dg.Field(description="AutoSys server hostname")
        port: int = dg.Field(default=9443, description="AutoSys REST API port (default 9443)")
        username_env_var: Optional[str] = dg.Field(
            default=None,
            description="Env var containing the Basic-auth username",
        )
        password_env_var: Optional[str] = dg.Field(
            default=None,
            description="Env var containing the Basic-auth password",
        )
        token_env_var: Optional[str] = dg.Field(
            default=None,
            description="Env var containing the X-AUTH-TOKEN header value",
        )
        use_ssl: bool = dg.Field(default=True, description="Use HTTPS for REST calls")
        verify_ssl: bool = dg.Field(
            default=True,
            description="Verify SSL certificate (set False for self-signed certs in dev)",
        )

        # ── Connection — CLI fallback ─────────────────────────────────────
        use_cli: bool = dg.Field(
            default=False,
            description="Force CLI mode (autorep/sendevent) even when REST is configured",
        )
        autosys_instance: Optional[str] = dg.Field(
            default=None,
            description="AutoSys instance name — sets the AUTOSERV env var for CLI calls",
        )

        # ── Discovery filters ─────────────────────────────────────────────
        job_filter: str = dg.Field(
            default="%",
            description="AutoSys wildcard job filter, e.g. 'ETL_*' or 'FINANCE_*'",
        )
        exclude_jobs: Optional[list[str]] = dg.Field(
            default=None,
            description="Job names to exclude from the asset graph",
        )
        include_job_types: list[str] = dg.Field(
            default_factory=lambda: ["CMD", "BOX", "FW"],
            description="AutoSys job types to include (CMD, BOX, FW)",
        )
        machine_filter: Optional[str] = dg.Field(
            default=None,
            description="Only include jobs that run on this machine",
        )

        # ── Box job behaviour ─────────────────────────────────────────────
        expand_box_jobs: bool = dg.Field(
            default=True,
            description=(
                "Create individual assets for each child job inside a Box, "
                "forming a sub-DAG. When False the Box is a single opaque asset."
            ),
        )
        box_completion_asset: bool = dg.Field(
            default=True,
            description=(
                "Create an additional '{box}_complete' gate asset that depends on "
                "all children — signals the full Box has succeeded."
            ),
        )
        map_conditions: bool = dg.Field(
            default=True,
            description=(
                "Parse the AutoSys condition field and map s()/d() predicates "
                "to Dagster asset dependencies. f() predicates are stored as "
                "metadata only."
            ),
        )

        # ── Execution ─────────────────────────────────────────────────────
        group_name: str = dg.Field(default="autosys", description="Dagster asset group name")
        key_prefix: Optional[str] = dg.Field(
            default=None,
            description="Optional asset key prefix for all generated assets",
        )
        force_start: bool = dg.Field(
            default=False,
            description=(
                "Use FORCE_STARTJOB (ignores AutoSys conditions and calendar) "
                "instead of STARTJOB"
            ),
        )
        poll_interval_seconds: int = dg.Field(
            default=30,
            description="Seconds between status polls while a job is running",
        )
        job_timeout_seconds: int = dg.Field(
            default=7200,
            description="Maximum seconds to wait for a job to complete before raising",
        )

        # ── Per-job overrides ──────────────────────────────────────────────
        assets_by_job_name: Optional[dict] = dg.Field(
            default=None,
            description=(
                "Override AssetSpec for specific AutoSys job names. Keys are AutoSys job "
                "names; values are dicts (or lists of dicts for one-job→multiple-assets) "
                "with optional fields: key, description, group_name, metadata, tags, kinds, deps."
            ),
        )

        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"AutoSysAssetComponent[{self.host}:{self.job_filter}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Discover AutoSys jobs (REST first, CLI fallback) and cache to disk."""
            jobs: list[dict] = []
            api_mode = "cli"

            if not self.use_cli:
                try:
                    raw_jobs = _fetch_jobs_rest(
                        host=self.host,
                        port=self.port,
                        use_ssl=self.use_ssl,
                        verify_ssl=self.verify_ssl,
                        username_env_var=self.username_env_var,
                        password_env_var=self.password_env_var,
                        token_env_var=self.token_env_var,
                        job_filter=self.job_filter,
                    )
                    api_mode = "rest"

                    for raw in raw_jobs:
                        job: dict = {
                            "name": raw.get("jobName", raw.get("job_name", raw.get("name", ""))),
                            "type": raw.get("jobType", raw.get("job_type", "CMD")).upper(),
                            "machine": raw.get("machine"),
                            "condition": raw.get("condition"),
                            "description": raw.get("description"),
                            "box_name": raw.get("boxName", raw.get("box_name")),
                            "children": [],
                            "command": raw.get("command"),
                        }
                        if job["type"] == "BOX":
                            children_raw = _fetch_box_children_rest(
                                host=self.host,
                                port=self.port,
                                use_ssl=self.use_ssl,
                                verify_ssl=self.verify_ssl,
                                username_env_var=self.username_env_var,
                                password_env_var=self.password_env_var,
                                token_env_var=self.token_env_var,
                                box_name=job["name"],
                            )
                            job["children"] = [
                                c.get("jobName", c.get("job_name", c.get("name", "")))
                                for c in children_raw
                            ]
                        jobs.append(job)

                except Exception as exc:
                    # REST failed — fall back to CLI
                    api_mode = "cli"
                    jobs = []
                    import warnings
                    warnings.warn(
                        f"AutoSys REST API failed ({exc}); falling back to CLI. "
                        "Ensure autorep is on PATH and AUTOSERV is set."
                    )

            if api_mode == "cli" or self.use_cli:
                raw_cli = _fetch_jobs_cli(self.job_filter, self.autosys_instance)
                # Attach children to BOX jobs
                box_to_children: dict[str, list[str]] = {}
                for j in raw_cli:
                    parent = j.get("box_name")
                    if parent:
                        box_to_children.setdefault(parent, []).append(j["name"])
                for j in raw_cli:
                    if j.get("type", "").upper() == "BOX":
                        j["children"] = box_to_children.get(j["name"], [])
                jobs = raw_cli
                api_mode = "cli"

            # Apply filters
            exclude = set(self.exclude_jobs or [])
            include_types = {t.upper() for t in (self.include_job_types or ["CMD", "BOX", "FW"])}

            filtered: list[dict] = []
            for j in jobs:
                if j["name"] in exclude:
                    continue
                if j.get("type", "CMD").upper() not in include_types:
                    continue
                if self.machine_filter and j.get("machine") and j["machine"] != self.machine_filter:
                    continue
                filtered.append(j)

            state_path.write_text(json.dumps({
                "jobs": filtered,
                "api_mode": api_mode,
                "discovery_filter": self.job_filter,
            }, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build the AutoSys asset graph from cached job JSON — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore
                        "AutoSysAssetComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            payload = json.loads(state_path.read_text())
            jobs: list[dict] = payload.get("jobs", payload) if isinstance(payload, dict) else payload
            api_mode: str = payload.get("api_mode", "rest") if isinstance(payload, dict) else "rest"

            return _build_autosys_defs(
                jobs=jobs,
                api_mode=api_mode,
                host=self.host,
                port=self.port,
                use_ssl=self.use_ssl,
                verify_ssl=self.verify_ssl,
                username_env_var=self.username_env_var,
                password_env_var=self.password_env_var,
                token_env_var=self.token_env_var,
                use_cli=self.use_cli,
                autosys_instance=self.autosys_instance,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                expand_box_jobs=self.expand_box_jobs,
                box_completion_asset=self.box_completion_asset,
                map_conditions=self.map_conditions,
                force_start=self.force_start,
                poll_interval_seconds=self.poll_interval_seconds,
                job_timeout_seconds=self.job_timeout_seconds,
                assets_by_job_name=self.assets_by_job_name,
            )

else:
    # Fallback: StateBackedComponent not available in this dagster version.
    # Calls AutoSys on every build_defs (live discovery on every code-server reload).
    class AutoSysAssetComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """AutoSys enterprise job scheduler component (fallback — no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """

        host: str = dg.Field(description="AutoSys server hostname")
        port: int = dg.Field(default=9443)
        username_env_var: Optional[str] = dg.Field(default=None)
        password_env_var: Optional[str] = dg.Field(default=None)
        token_env_var: Optional[str] = dg.Field(default=None)
        use_ssl: bool = dg.Field(default=True)
        verify_ssl: bool = dg.Field(default=True)
        use_cli: bool = dg.Field(default=False)
        autosys_instance: Optional[str] = dg.Field(default=None)
        job_filter: str = dg.Field(default="%")
        exclude_jobs: Optional[list[str]] = dg.Field(default=None)
        include_job_types: list[str] = dg.Field(default_factory=lambda: ["CMD", "BOX", "FW"])
        machine_filter: Optional[str] = dg.Field(default=None)
        expand_box_jobs: bool = dg.Field(default=True)
        box_completion_asset: bool = dg.Field(default=True)
        map_conditions: bool = dg.Field(default=True)
        group_name: str = dg.Field(default="autosys")
        key_prefix: Optional[str] = dg.Field(default=None)
        force_start: bool = dg.Field(default=False)
        poll_interval_seconds: int = dg.Field(default=30)
        job_timeout_seconds: int = dg.Field(default=7200)
        assets_by_job_name: Optional[dict] = dg.Field(default=None)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            import tempfile
            from pathlib import Path as _Path

            # Re-use the same discovery logic via a temp file
            tmp = _Path(tempfile.mktemp(suffix=".json"))
            # Build a minimal shim that has write_state_to_path
            shim = _AutoSysDiscoveryShim(
                host=self.host, port=self.port,
                use_ssl=self.use_ssl, verify_ssl=self.verify_ssl,
                username_env_var=self.username_env_var,
                password_env_var=self.password_env_var,
                token_env_var=self.token_env_var,
                use_cli=self.use_cli,
                autosys_instance=self.autosys_instance,
                job_filter=self.job_filter,
                exclude_jobs=self.exclude_jobs,
                include_job_types=self.include_job_types,
                machine_filter=self.machine_filter,
            )
            shim.discover(tmp)
            payload = json.loads(tmp.read_text())
            jobs = payload.get("jobs", [])
            api_mode = payload.get("api_mode", "cli")
            tmp.unlink(missing_ok=True)

            return _build_autosys_defs(
                jobs=jobs, api_mode=api_mode,
                host=self.host, port=self.port,
                use_ssl=self.use_ssl, verify_ssl=self.verify_ssl,
                username_env_var=self.username_env_var,
                password_env_var=self.password_env_var,
                token_env_var=self.token_env_var,
                use_cli=self.use_cli,
                autosys_instance=self.autosys_instance,
                group_name=self.group_name, key_prefix=self.key_prefix,
                expand_box_jobs=self.expand_box_jobs,
                box_completion_asset=self.box_completion_asset,
                map_conditions=self.map_conditions,
                force_start=self.force_start,
                poll_interval_seconds=self.poll_interval_seconds,
                job_timeout_seconds=self.job_timeout_seconds,
                assets_by_job_name=self.assets_by_job_name,
            )


class _AutoSysDiscoveryShim:
    """Thin shim to reuse the discovery logic in the non-state-backed fallback."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def discover(self, state_path: Path) -> None:
        jobs: list[dict] = []
        api_mode = "cli"

        if not getattr(self, "use_cli", False):
            try:
                raw_jobs = _fetch_jobs_rest(
                    host=self.host, port=self.port,
                    use_ssl=self.use_ssl, verify_ssl=self.verify_ssl,
                    username_env_var=self.username_env_var,
                    password_env_var=self.password_env_var,
                    token_env_var=self.token_env_var,
                    job_filter=self.job_filter,
                )
                api_mode = "rest"
                for raw in raw_jobs:
                    job = {
                        "name": raw.get("jobName", raw.get("job_name", raw.get("name", ""))),
                        "type": raw.get("jobType", raw.get("job_type", "CMD")).upper(),
                        "machine": raw.get("machine"),
                        "condition": raw.get("condition"),
                        "description": raw.get("description"),
                        "box_name": raw.get("boxName", raw.get("box_name")),
                        "children": [],
                        "command": raw.get("command"),
                    }
                    if job["type"] == "BOX":
                        children_raw = _fetch_box_children_rest(
                            host=self.host, port=self.port,
                            use_ssl=self.use_ssl, verify_ssl=self.verify_ssl,
                            username_env_var=self.username_env_var,
                            password_env_var=self.password_env_var,
                            token_env_var=self.token_env_var,
                            box_name=job["name"],
                        )
                        job["children"] = [
                            c.get("jobName", c.get("job_name", c.get("name", "")))
                            for c in children_raw
                        ]
                    jobs.append(job)
            except Exception:
                api_mode = "cli"
                jobs = []

        if api_mode == "cli" or getattr(self, "use_cli", False):
            raw_cli = _fetch_jobs_cli(self.job_filter, getattr(self, "autosys_instance", None))
            box_to_children: dict[str, list[str]] = {}
            for j in raw_cli:
                parent = j.get("box_name")
                if parent:
                    box_to_children.setdefault(parent, []).append(j["name"])
            for j in raw_cli:
                if j.get("type", "").upper() == "BOX":
                    j["children"] = box_to_children.get(j["name"], [])
            jobs = raw_cli
            api_mode = "cli"

        exclude = set(getattr(self, "exclude_jobs", None) or [])
        include_types = {t.upper() for t in (getattr(self, "include_job_types", None) or ["CMD", "BOX", "FW"])}
        machine_filter = getattr(self, "machine_filter", None)

        filtered = [
            j for j in jobs
            if j["name"] not in exclude
            and j.get("type", "CMD").upper() in include_types
            and not (machine_filter and j.get("machine") and j["machine"] != machine_filter)
        ]

        state_path.write_text(json.dumps({
            "jobs": filtered,
            "api_mode": api_mode,
            "discovery_filter": self.job_filter,
        }, indent=2))
