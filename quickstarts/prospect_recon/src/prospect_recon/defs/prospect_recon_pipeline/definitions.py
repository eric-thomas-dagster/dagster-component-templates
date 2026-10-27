"""Prospect Recon -- the plain-Python glue this quickstart needs beyond the
declarative component instances in the sibling defs/ folders:

  - prospect_recon_candidate   -- real, deterministic port of the routine's
    request-file/dangling-ledger precedence + Calendar/Drive signal
    gathering, exclusion rules, dedup and tie-break (plan.md section 2).
    Not an LLM judgment call: the routine's own rules are fully
    deterministic once signals are gathered.
  - prospect_recon_evidence_df / prospect_recon_items_json -- bridges
    normalizing data between the ingestion/research/KB components, which
    each expect a specific shape (a DataFrame vs. a dict with a `text` key)
    that the neighboring component doesn't produce directly.
  - prospect_recon_brief -- structured extraction via forced function-
    calling, same mechanism agentic_pipeline's own `extract` op uses, but
    built as plain code rather than an AgenticPipelineComponent instance
    because its output_schema must be derived from `briefs/_TEMPLATE.md`
    fetched LIVE from the dagster-demo-factory repo at compute time (a
    component's YAML config is fixed at parse time, so it can't express
    "derive my own schema from a runtime fetch").
  - prospect_recon_commit_brief -- the hard integration contract: writes
    the brief + ledger update to dagster-demo-factory on main (direct push,
    falling back to branch+PR+merge on conflict), exactly like the real
    routine, since Demo Factory (unmodified, out of scope) reads from there.
  - prospect_recon_schedule -- cron "30 18 * * 1-5" (6:30pm ET weekdays),
    matching the real routine's own trigger.

Signal B's "derive the company from the file" step is an LLM judgment call in
the real routine (reading the file's actual content/context, not its
filename). `_classify_drive_company` now does the same: when Drive's
`inline_text_content` gave us real text, an LLM call (same forced
function-calling mechanism `prospect_recon_brief` uses) reads it and weighs it
against the open opportunities' domains already found via Calendar, returning
a confidence. `_guess_company_from_filename` is kept only as the last-resort
fallback when there is no readable content or the LLM itself has low/no
confidence -- never the primary mechanism. `google_drive_ingestion` now
extracts real text for PDF/.docx/.xlsx too (via pdfplumber/python-docx/openpyxl,
fixed separately -- see its own README), closing what used to be a real gap
here; the filename guess is now only reached for MIME types that genuinely
have no text representation (images, unknown binary formats) or when those
optional extraction libraries aren't installed.

`state/ledger.json`'s real shape (confirmed via a live fetch of
eric-thomas-dagster/dagster-demo-factory, not assumed) is two buckets, not a
flat dict keyed by domain or slug:
    {"brief-ready": [{"company", "slug", "status": "brief-ready", "demo_date",
                       "brief_path", "timestamp"}, ...],
     "built":       [{"company", "slug", "status": "deployed", "demo_date",
                       "brief_path", "timestamp", "deployed_at",
                       "location_name", "pr_url", "dagster_plus_url", ...}]}
Note the real inconsistency, kept faithfully: the bucket key is "built" but
the per-entry `status` string inside it is "deployed". Neither bucket's
entries carry a `domain` field -- only `company`/`slug` -- so "already
deployed" exclusion is matched by slug (`_slugify(company)` against each
`built` entry's own `slug`), a best-effort link since we don't control how the
real ledger's slugs were generated. Everything else (exclusion rules, dedup,
tie-break, request-file precedence, dangling-ledger detection, commit/PR
fallback) is a faithful, testable, deterministic port.
"""
import datetime as dt
import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
import pandas as pd
import yaml

DEMO_FACTORY_REPO = os.environ.get("PROSPECT_RECON_DEMO_FACTORY_REPO", "eric-thomas-dagster/dagster-demo-factory")

EXCLUDE_KEYWORDS = [
    "poc", "pilot", "trial", "check-in", "check in", "sync", "standup",
    "status", "weekly", "qbr", "renewal", "onboarding", "training",
    "implementation", "office hours",
]

# Discovery-half fields (used directly by the Gmail/Slack notification rows in
# prospect_recon_commit_brief) plus a handful of light build-directive fields
# useful as structured metadata. The real two-half document (discovery +
# "Build directives", matching briefs/_TEMPLATE.md's headings exactly --
# asset graph, fidelity, demo shape, checks, money shot, capability talk
# track, etc.) lives entirely in `markdown_body`: the model is instructed to
# write the whole thing as one coherent brief against the live-fetched
# template, the same way the real routine does, rather than decomposing every
# subsection (a table, several bulleted lists, a talk track) into its own
# schema field -- that decomposition would be far more fragile to reassemble
# than one prompt that says "fill out this exact template."
FALLBACK_BRIEF_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "company": {"type": "string"},
        "primary_contacts": {"type": "array", "items": {"type": "string"}},
        "use_case": {"type": "string"},
        "pain_points": {"type": "array", "items": {"type": "string"}},
        "technical_requirements": {"type": "array", "items": {"type": "string"}},
        "competitive_landscape": {"type": "string"},
        "timeline": {"type": "string"},
        "champion": {"type": "string"},
        "risks": {"type": "array", "items": {"type": "string"}},
        "next_steps": {"type": "array", "items": {"type": "string"}},
        "demo_thesis": {"type": "string"},
        "confidence": {"type": "string"},
        "biggest_guess": {"type": "string"},
        "conflicts": {"type": "string"},
        "ae_doc_summary": {"type": "string"},
        "ae_doc_modified": {"type": "string"},
        "demo_time": {"type": "string"},
        "attendees": {"type": "array", "items": {"type": "string"}},
        # build-directives half -- light structured summaries only, the full
        # prose for each lives in markdown_body alongside the discovery half.
        "demo_name": {"type": "string"},
        "money_shot": {"type": "string"},
        "asset_graph_summary": {"type": "string"},
        "fidelity": {"type": "string"},
        "demo_shape": {"type": "string"},
        "three_buckets_summary": {"type": "string"},
        "markdown_body": {"type": "string"},
    },
    "required": ["company", "use_case", "next_steps", "markdown_body"],
}

_FILENAME_STOPWORDS = {
    "discovery", "notes", "deck", "demo", "prep", "doc", "docx", "pdf", "pptx",
    "xlsx", "v1", "v2", "v3", "draft", "final", "copy", "the", "and", "for",
}


def _slugify(name: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(name or "").lower()).strip("-")


def _built_slugs(ledger: Dict[str, Any]) -> set:
    """Slugs already in the real ledger's `built` bucket (status: deployed) --
    the only bucket that means "don't rebuild this." See module docstring:
    entries carry no `domain` field, only `company`/`slug`, so this is the
    only link available for the "already deployed" exclusion rule."""
    return {
        (e.get("slug") or _slugify(e.get("company")))
        for e in (ledger.get("built") or [])
        if e.get("slug") or e.get("company")
    }


# ── pure helpers: Signal A (Calendar) ──────────────────────────────────────

def _event_text(ev: dict) -> str:
    return f"{ev.get('summary') or ''} {ev.get('description') or ''}".lower()


def _is_recurring(ev: dict) -> bool:
    return bool(ev.get("recurringEventId") or ev.get("recurring_event_id"))


def _matches_exclude_keyword(ev: dict) -> Optional[str]:
    text = _event_text(ev)
    for kw in EXCLUDE_KEYWORDS:
        if kw in text:
            return kw
    return None


def _attendee_domain(ev: dict, own_domain: str) -> Optional[str]:
    for email in (ev.get("attendees") or []):
        if email and "@" in email and email.split("@", 1)[1].lower() != own_domain.lower():
            return email.split("@", 1)[1].lower()
    return None


def _external_attendee_count(ev: dict, own_domain: str) -> int:
    return sum(
        1 for e in (ev.get("attendees") or [])
        if e and "@" in e and e.split("@", 1)[1].lower() != own_domain.lower()
    )


def _event_duration_minutes(ev: dict) -> float:
    try:
        start = pd.to_datetime(ev.get("start"), utc=True)
        end = pd.to_datetime(ev.get("end"), utc=True)
        return max(0.0, (end - start).total_seconds() / 60.0)
    except Exception:
        return 0.0


def filter_calendar_candidates(
    calendar_df: Optional[pd.DataFrame], *, own_domain: str, ledger: Dict[str, Any],
) -> Tuple[List[dict], List[dict]]:
    """Signal A: apply the routine's exclusion rules verbatim. Returns (candidates, skipped)."""
    candidates: List[dict] = []
    skipped: List[dict] = []
    if calendar_df is None or calendar_df.empty:
        return candidates, skipped
    for ev in calendar_df.to_dict("records"):
        label = ev.get("summary") or ev.get("id") or "(untitled)"
        if _is_recurring(ev):
            skipped.append({"summary": label, "reason": "recurring"})
            continue
        kw = _matches_exclude_keyword(ev)
        if kw:
            skipped.append({"summary": label, "reason": f"keyword:{kw}"})
            continue
        domain = _attendee_domain(ev, own_domain)
        if not domain:
            skipped.append({"summary": label, "reason": "no_external_attendee"})
            continue
        company = domain.split(".")[0]
        if _slugify(company) in _built_slugs(ledger):
            skipped.append({"summary": label, "reason": "already_deployed"})
            continue
        candidates.append({
            "source": "calendar", "domain": domain, "company": company,
            "demo_date": ev.get("start"), "dated": True,
            "summary": label, "attendees": ev.get("attendees") or [],
            "duration_minutes": _event_duration_minutes(ev),
            "external_attendee_count": _external_attendee_count(ev, own_domain),
            "html_link": ev.get("html_link"),
        })
    return candidates, skipped


# ── pure helpers: Signal B (Drive) ─────────────────────────────────────────

def _guess_company_from_filename(name: str) -> Optional[str]:
    """Last-resort company guess from a filename alone, used only when
    `_classify_drive_company` has no readable content or low/no LLM
    confidence -- see module docstring."""
    base = re.sub(r"\.[A-Za-z0-9]{2,4}$", "", name or "")
    tokens = re.split(r"[\s\-_]+", base)
    guesses = [
        t for t in tokens
        if t and t[0].isupper() and t.lower() not in _FILENAME_STOPWORDS and not t.isdigit()
    ]
    return guesses[0] if guesses else None


_DRIVE_CLASSIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "company": {"type": "string"},
        "domain": {"type": "string"},
        "confidence": {"type": "string", "enum": ["high", "medium", "low", "none"]},
    },
    "required": ["company", "confidence"],
}


def _classify_drive_company(content: Optional[str], filename: str, known_domains: List[str]) -> Tuple[Optional[str], Optional[str], str]:
    """The real routine's own judgment call: which company does this file belong
    to, decided by reading its actual content (cross-referenced against domains
    already found via Calendar), not by pattern-matching its filename. Returns
    (company, domain, confidence: high|medium|low|none). Falls back to the
    filename heuristic only when there's no readable content -- e.g. an image
    or a MIME type with no text representation, or the optional pdfplumber/
    python-docx/openpyxl extraction libraries aren't installed -- or the LLM
    itself has low/no confidence; the filename guess is the fallback, never
    the primary mechanism. `google_drive_ingestion` extracts real text from
    PDF/.docx/.xlsx now (not just plain-text/json/csv), so this path is
    reached far less often than when this function was first written."""
    if content and content.strip():
        try:
            import litellm

            prompt = (
                "Which company does this document belong to? Known open-opportunity "
                f"domains already found via Calendar: {known_domains or '(none yet)'}. If the "
                "document clearly names or matches one of these, use it. Otherwise infer the "
                "company from the content itself (letterhead, domain mentions, named "
                "attendees/contacts). Report your confidence honestly -- 'none' if you genuinely "
                "cannot tell; a wrong guess routed as the real candidate is worse than admitting "
                "you don't know.\n\n"
                f"Filename: {filename}\n\nContent (truncated):\n{content[:4000]}"
            )
            model = os.environ.get("PROSPECT_RECON_BRIEF_MODEL", "gpt-4o")
            resp = litellm.completion(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                tools=[{"type": "function", "function": {"name": "classify_company", "parameters": _DRIVE_CLASSIFY_SCHEMA}}],
                tool_choice={"type": "function", "function": {"name": "classify_company"}},
                api_key=os.environ.get("OPENAI_API_KEY"),
            )
            call = resp.choices[0].message.tool_calls[0]
            result = json.loads(call.function.arguments)
            if result.get("confidence") in ("high", "medium") and result.get("company"):
                domain = result.get("domain") or f"{_slugify(result['company']).replace('-', '')}.com"
                return result["company"], domain, result["confidence"]
        except Exception:
            pass  # any failure (no API key, network, malformed response) falls through to the filename guess

    guess = _guess_company_from_filename(filename)
    if not guess:
        return None, None, "none"
    return guess, f"{guess.lower()}.com", "low"


def filter_drive_candidates(
    *dfs: Optional[pd.DataFrame], ledger: Dict[str, Any], known_domains: Optional[List[str]] = None,
) -> List[dict]:
    """Signal B: merge recent-listing + search-pass results, dedupe by file id,
    attribute each to a company (content-based LLM judgment first, filename
    guess as the fallback -- see `_classify_drive_company`)."""
    frames = [d for d in dfs if d is not None and not d.empty]
    if not frames:
        return []
    merged = pd.concat(frames, ignore_index=True)
    seen_ids: set = set()
    candidates: List[dict] = []
    for row in merged.to_dict("records"):
        fid = row.get("id")
        if not fid or fid in seen_ids:
            continue
        seen_ids.add(fid)
        name = row.get("name") or ""
        company, domain, confidence = _classify_drive_company(row.get("content"), name, known_domains or [])
        if not company:
            continue
        if _slugify(company) in _built_slugs(ledger):
            continue
        candidates.append({
            "source": "drive", "domain": domain, "company": company,
            "demo_date": "TBD", "dated": False, "company_attribution_confidence": confidence,
            "file_name": name, "modified_time": row.get("modifiedTime"),
            "file_id": fid, "web_view_link": row.get("webViewLink"),
        })
    return candidates


# ── dedup + tie-break ───────────────────────────────────────────────────────

def select_candidate(
    calendar_candidates: List[dict], drive_candidates: List[dict], *, now: Optional[dt.datetime] = None,
) -> Optional[dict]:
    """Dedup by domain (calendar/dated wins a tie against a doc-only hit for the same
    company), then order: dated-and-upcoming (soonest first) > undated/doc-only
    (most-recently-shared first) > dated-but-passed (last); remaining ties broken by
    meeting length then external-attendee count -- verbatim per the routine."""
    now = now or dt.datetime.now(dt.timezone.utc)
    epoch = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    pool: List[dict] = []

    for c in calendar_candidates:
        c = dict(c)
        try:
            c["_demo_dt"] = pd.to_datetime(c["demo_date"], utc=True).to_pydatetime()
        except Exception:
            c["_demo_dt"] = None
        c["_upcoming"] = c["_demo_dt"] is not None and c["_demo_dt"] >= now
        pool.append(c)

    for c in drive_candidates:
        c = dict(c)
        c["_demo_dt"] = None
        c["_upcoming"] = False
        try:
            c["_shared_dt"] = pd.to_datetime(c.get("modified_time"), utc=True).to_pydatetime()
        except Exception:
            c["_shared_dt"] = epoch
        pool.append(c)

    by_domain: Dict[str, dict] = {}
    for c in pool:
        existing = by_domain.get(c["domain"])
        if existing is None or (c["dated"] and not existing["dated"]):
            by_domain[c["domain"]] = c
    deduped = list(by_domain.values())
    if not deduped:
        return None

    def bucket(c: dict) -> int:
        if c["dated"] and c["_upcoming"]:
            return 0
        if not c["dated"]:
            return 1
        return 2  # dated, passed

    def sort_key(c: dict):
        b = bucket(c)
        if b == 0:
            secondary = (c["_demo_dt"] or epoch).timestamp()
        elif b == 1:
            secondary = -(c.get("_shared_dt") or epoch).timestamp()
        else:
            secondary = -(c["_demo_dt"] or epoch).timestamp()
        tie1 = -(c.get("duration_minutes") or 0)
        tie2 = -(c.get("external_attendee_count") or 0)
        return (b, secondary, tie1, tie2)

    deduped.sort(key=sort_key)
    return deduped[0]


# ── request-file / dangling-ledger precedence ──────────────────────────────

def parse_frontmatter(md_text: str) -> Dict[str, Any]:
    """Parse a leading '---\\n...\\n---' YAML frontmatter block. {} if absent/invalid.

    YAML auto-parses an unquoted ISO date (e.g. `demo_date: 2026-11-05`) into a real
    `datetime.date`, not a string -- normalized back to its isoformat string here so
    every frontmatter consumer downstream (slug/path building, templating) can assume
    plain strings without each needing its own str() guard.
    """
    m = re.match(r"^---\s*\n(.*?)\n---\s*\n?", md_text or "", re.DOTALL)
    if not m:
        return {}
    try:
        parsed = yaml.safe_load(m.group(1)) or {}
    except Exception:
        return {}
    return {
        k: (v.isoformat() if isinstance(v, (dt.date, dt.datetime)) else v)
        for k, v in parsed.items()
    }


def fetch_ledger(github, repo: str = DEMO_FACTORY_REPO) -> Dict[str, Any]:
    content = github.get_file_contents(repo, "state/ledger.json")
    if content is None:
        return {}
    try:
        return json.loads(content["content"])
    except Exception:
        return {}


def find_request_candidate(github, repo: str = DEMO_FACTORY_REPO) -> Optional[dict]:
    """requests/*.md with frontmatter action: rebuild-brief, nearest demo_date first."""
    entries = github.list_directory(repo, "requests")
    files = [e for e in entries if e.get("type") == "file" and e.get("name") != "README.md"]
    requests: List[dict] = []
    for f in files:
        content = github.get_file_contents(repo, f["path"])
        if content is None:
            continue
        fm = parse_frontmatter(content["content"])
        if fm.get("action") == "rebuild-brief":
            requests.append({"path": f["path"], "sha": content["sha"], "frontmatter": fm, "content": content["content"]})
    if not requests:
        return None
    requests.sort(key=lambda r: r["frontmatter"].get("demo_date") or "9999-99-99")
    return requests[0]


def find_dangling_ledger_entry(ledger: Dict[str, Any], github, repo: str = DEMO_FACTORY_REPO) -> Optional[dict]:
    """A `brief-ready`-bucket entry whose brief_path file no longer exists means the
    prospect is waiting to be rewritten (e.g. scripts/reset_demo.sh ran) -- never report
    'no candidates' while this is true. The real ledger's `brief-ready` bucket is a list
    of entries (each already status: brief-ready by construction), not a flat dict."""
    for entry in (ledger.get("brief-ready") or []):
        brief_path = entry.get("brief_path")
        if brief_path and github.get_file_contents(repo, brief_path) is None:
            return {"slug": entry.get("slug") or _slugify(entry.get("company")), "entry": entry}
    return None


# ── candidate-selection asset ──────────────────────────────────────────────

@dg.asset(
    key=dg.AssetKey.from_user_string("prospect_recon_candidate"),
    ins={
        "calendar": dg.AssetIn(key=dg.AssetKey.from_user_string("calendar_raw")),
        "drive_recent": dg.AssetIn(key=dg.AssetKey.from_user_string("drive_recent_raw")),
        "drive_search": dg.AssetIn(key=dg.AssetKey.from_user_string("drive_search_raw")),
    },
    required_resource_keys={"github"},
    group_name="prospect_recon",
    kinds={"python"},
    description=(
        "Autonomous candidate selection: request-file/dangling-ledger precedence, then "
        "Calendar+Drive signal exclusion/dedup/tie-break -- a real, testable port of the "
        "routine's own decision rules, not an LLM judgment call."
    ),
)
def prospect_recon_candidate(
    context: dg.AssetExecutionContext, calendar: pd.DataFrame, drive_recent: pd.DataFrame, drive_search: pd.DataFrame,
) -> dict:
    github = context.resources.github
    own_domain = os.environ.get("PROSPECT_RECON_OWN_DOMAIN", "dagsterlabs.com")
    ledger = fetch_ledger(github)

    request = find_request_candidate(github)
    dangling = None if request else find_dangling_ledger_entry(ledger, github)

    if request is not None:
        fm = request["frontmatter"]
        company = fm.get("company") or "unknown"
        slug = fm.get("slug") or _slugify(company)
        context.log.info(f"prospect_recon_candidate: honoring request {request['path']!r} for {company!r}")
        return {
            "text": f"REQUEST: honoring requests/{request['path']}. Company: {company}.\n\n{request['content']}",
            "has_candidate": True, "source_kind": "request",
            "company": company, "domain": fm.get("domain"), "demo_date": fm.get("demo_date") or "TBD",
            "slug": slug, "request_file": request, "candidate": fm,
        }

    if dangling is not None:
        slug, entry = dangling["slug"], dangling["entry"]
        context.log.info(f"prospect_recon_candidate: dangling ledger entry for {slug!r} (brief_path missing) -- rewriting.")
        return {
            "text": f"DANGLING_LEDGER: {slug}'s brief was cleared (brief_path missing) -- rewriting from scratch.",
            "has_candidate": True, "source_kind": "dangling_ledger",
            "company": entry.get("company") or slug, "domain": entry.get("domain"),
            "demo_date": entry.get("demo_date") or "TBD", "slug": slug,
            "request_file": None, "candidate": entry,
        }

    cal_candidates, cal_skipped = filter_calendar_candidates(calendar, own_domain=own_domain, ledger=ledger)
    known_domains = [c["domain"] for c in cal_candidates]
    drive_candidates = filter_drive_candidates(drive_recent, drive_search, ledger=ledger, known_domains=known_domains)
    picked = select_candidate(cal_candidates, drive_candidates)

    if picked is None:
        if cal_skipped and not drive_candidates:
            reason = f"every calendar candidate was late-stage ({[s['reason'] for s in cal_skipped]}), no doc-only candidate either"
        else:
            reason = "no candidates from either signal"
        context.log.warning(f"prospect_recon_candidate: {reason}")
        return {
            "text": f"NO_CANDIDATE: {reason}",
            "has_candidate": False, "reason": reason,
            "company": None, "domain": None, "slug": None, "demo_date": None,
            "request_file": None, "candidate": None,
        }

    slug = _slugify(picked["company"])
    summary = (
        f"Company: {picked['company']} ({picked['domain']}). Demo date: {picked['demo_date']}. "
        f"Source: {picked['source']}. Signals: {json.dumps({k: v for k, v in picked.items() if not k.startswith('_')}, default=str)}"
    )
    context.add_output_metadata({"company": dg.MetadataValue.text(picked["company"]), "source": dg.MetadataValue.text(picked["source"])})
    return {
        "text": summary, "has_candidate": True, "source_kind": picked["source"],
        "company": picked["company"], "domain": picked["domain"],
        "demo_date": str(picked["demo_date"]), "slug": slug,
        "request_file": None, "candidate": picked,
    }


# ── bridge: research evidence -> context_engineering_pipeline's raw-rows DataFrame ──

@dg.asset(
    key=dg.AssetKey.from_user_string("prospect_recon_evidence_df"),
    ins={
        "candidate": dg.AssetIn(key=dg.AssetKey.from_user_string("prospect_recon_candidate")),
        "research": dg.AssetIn(key=dg.AssetKey.from_user_string("prospect_recon_research_findings")),
    },
    group_name="prospect_recon",
    kinds={"python"},
    description=(
        "Normalizes the research step's evidence trail (tool-call results + final findings) "
        "into the raw-text-rows DataFrame context_engineering_pipeline's upstream_asset_key expects."
    ),
)
def prospect_recon_evidence_df(context: dg.AssetExecutionContext, candidate: dict, research: dict) -> pd.DataFrame:
    opportunity_slug = candidate.get("slug") or "unknown"
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    rows: List[dict] = []
    for i, call in enumerate(research.get("tool_call_trace") or []):
        result = call.get("result") if isinstance(call, dict) else None
        text = result if isinstance(result, str) else (json.dumps(result, default=str) if result is not None else "")
        if not text.strip():
            continue
        tool_name = (call.get("tool") if isinstance(call, dict) else None) or f"tool_call_{i}"
        rows.append({
            "item_id": f"tool_call:{i}", "text": text, "source": "tool_call",
            "title": tool_name, "occurred_at": now_iso, "opportunity_slug": opportunity_slug,
        })
    final_text = (research.get("text") or "").strip()
    if final_text:
        rows.append({
            "item_id": "research:final", "text": final_text, "source": "research_summary",
            "title": "Research findings summary", "occurred_at": now_iso, "opportunity_slug": opportunity_slug,
        })
    if not rows:
        rows.append({
            "item_id": "research:none", "text": candidate.get("text") or "no research evidence",
            "source": "candidate", "title": "No research evidence",
            "occurred_at": now_iso, "opportunity_slug": opportunity_slug,
        })
    return pd.DataFrame(rows)


# ── bridge: KB DataFrame -> dict-with-text-key ──────────────────────────────

@dg.asset(
    key=dg.AssetKey.from_user_string("prospect_recon_items_json"),
    ins={"kb": dg.AssetIn(key=dg.AssetKey.from_user_string("prospect_recon_context_kb"))},
    group_name="prospect_recon",
    kinds={"python"},
    description="Bridges the KB's chunked DataFrame (dropping the embedding column) into a JSON-records string for the brief-extraction step.",
)
def prospect_recon_items_json(context: dg.AssetExecutionContext, kb: pd.DataFrame) -> dict:
    cols = [c for c in kb.columns if c != "embedding"]
    text = kb[cols].to_json(orient="records") if not kb.empty else "[]"
    return {"text": text, "n_items": int(len(kb))}


# ── brief extraction (dynamic schema from a live template fetch) ───────────

def derive_schema_from_template(template_text: str) -> dict:
    """Best-effort: turn `## Heading` sections of briefs/_TEMPLATE.md into light schema
    fields (used for `schema_source` metadata and a few structured extras). Falls back to
    FALLBACK_BRIEF_SCHEMA wholesale when the template is empty or has no `##` headings.

    The real template's structure (confirmed via a live fetch of
    eric-thomas-dagster/dagster-demo-factory) is far richer than one-string-per-heading --
    a markdown table ("Current stack"), several bulleted lists ("Public signals"), a
    multi-paragraph talk track -- so the full two-half document (discovery + "Build
    directives") is authored as one coherent `markdown_body` directly against the
    live-fetched template TEXT (see prospect_recon_brief's prompt), not reassembled from
    these per-heading fields. This function stays as a lighter structural signal, not the
    document's source of truth."""
    if not template_text or not template_text.strip():
        return FALLBACK_BRIEF_SCHEMA
    headings = re.findall(r"^##\s+(.+)$", template_text, re.MULTILINE)
    if not headings:
        return FALLBACK_BRIEF_SCHEMA
    properties: Dict[str, Any] = dict(FALLBACK_BRIEF_SCHEMA["properties"])
    for h in headings:
        field = re.sub(r"[^a-z0-9]+", "_", h.strip().lower()).strip("_")
        if field and field not in properties:
            properties[field] = {"type": "string"}
    return {"type": "object", "properties": properties, "required": ["company", "markdown_body"]}


# ── FastMCP context-governance client (minimal stdio call, duplicated inline --
# no shared helper files across components/quickstarts, see house convention) ──

_PROJECT_ROOT = os.environ.get(
    "PROSPECT_RECON_PROJECT_ROOT",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")),
)
GOVERNANCE_SERVER_PATH = os.path.join(_PROJECT_ROOT, "mcp_servers", "context_governance_server.py")


def _call_governance_tool(tool_name: str, tool_args: Dict[str, Any], *, timeout_seconds: float = 30.0) -> Dict[str, Any]:
    """Sync stdio MCP client call to mcp_servers/context_governance_server.py -- the FastMCP
    server that is the system of record for context-access receipts (see that file's
    docstring for the full Dagster/FastMCP/Prefect positioning). Same stdio transport
    agentic_pipeline's tool_use_loop uses for prospect_recon_research, duplicated minimally
    here since this is plain Python, not a declarative AgenticPipelineComponent step (see
    this module's own docstring for why prospect_recon_brief can't be one). Tests
    monkeypatch this function directly rather than spawning the real server subprocess."""
    import anyio

    async def _run() -> Dict[str, Any]:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        params = StdioServerParameters(command="python3", args=[GOVERNANCE_SERVER_PATH], env=dict(os.environ))
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, tool_args)
                structured = getattr(result, "structuredContent", None)
                if structured is not None:
                    return structured
                text = "\n".join(getattr(c, "text", "") for c in (result.content or []) if hasattr(c, "text"))
                return json.loads(text) if text else {}

    with anyio.fail_after(timeout_seconds):
        return anyio.run(_run)


@dg.asset(
    key=dg.AssetKey.from_user_string("prospect_recon_brief"),
    ins={
        "items": dg.AssetIn(key=dg.AssetKey.from_user_string("prospect_recon_items_json")),
        "candidate": dg.AssetIn(key=dg.AssetKey.from_user_string("prospect_recon_candidate")),
    },
    required_resource_keys={"github"},
    group_name="prospect_recon",
    kinds={"llm", "python"},
    description=(
        "Structured brief extraction via forced function-calling (same mechanism "
        "agentic_pipeline's `extract` op uses), authoring the FULL two-half document "
        "(discovery + Build directives) against briefs/_TEMPLATE.md fetched LIVE from "
        "dagster-demo-factory at compute time -- not an AgenticPipelineComponent instance, "
        "since a component's output_schema/prompt is fixed at YAML-parse time and can't "
        "express a runtime-derived template. Records a generation-time receipt through the "
        "FastMCP context-governance server (mcp_servers/context_governance_server.py), "
        "best-effort: a governance-server failure logs a warning but never blocks the brief."
    ),
)
def prospect_recon_brief(context: dg.AssetExecutionContext, items: dict, candidate: dict) -> dict:
    if not candidate.get("has_candidate"):
        text = candidate.get("text", "")
        return {"text": text, "extracted": {"company": "none", "markdown_body": text}, "schema_source": "no_candidate"}

    github = context.resources.github
    template = github.get_file_contents(DEMO_FACTORY_REPO, "briefs/_TEMPLATE.md")
    template_text = template["content"] if template else ""
    if template is None:
        context.log.warning("prospect_recon_brief: briefs/_TEMPLATE.md not found in dagster-demo-factory; using fallback schema.")
        schema, schema_source = FALLBACK_BRIEF_SCHEMA, "fallback"
    else:
        try:
            schema, schema_source = derive_schema_from_template(template_text), "live_template"
        except Exception as e:
            context.log.warning(f"prospect_recon_brief: template parse failed ({e}); using fallback schema.")
            schema, schema_source = FALLBACK_BRIEF_SCHEMA, "fallback"

    import litellm

    cand_entry = candidate.get("candidate") or {}
    known_attendees = cand_entry.get("attendees") or []
    template_instructions = template_text or (
        "(template unavailable -- use this structure: discovery half with Demo thesis / "
        "Meeting / Use case / Current stack / Pain / Data domains / Public signals / "
        "Conflicts and gaps, then a '# Build directives' half with Asset graph / Fidelity "
        "/ Demo shape / Native integrations to use / Community components to search for / "
        "Asset checks / Demo mode / Three buckets / Demo name / Money shot / Capability "
        "talk track / Explicitly out of scope)"
    )
    prompt = (
        f"You are writing a sales-engineering demo brief for {candidate.get('company')} "
        f"({candidate.get('domain')}), demo date {candidate.get('demo_date')}. This brief is "
        f"read aloud in a real meeting AND consumed unmodified by a second automated routine "
        f"(Demo Factory) that builds the actual demo from it -- so every build-directive "
        f"judgment call (asset-graph sizing and why, fidelity choice and why, demo shape, "
        f"which checks map to which named pain point, the money shot, the capability talk "
        f"track) must be a real decision grounded in the research below, never a placeholder "
        f"or a generic default.\n\n"
        f"Write 'markdown_body' as the COMPLETE brief matching this exact template's headings "
        f"and order verbatim -- keep every '#'/'##' heading text exactly as given, replace each "
        f"HTML comment with your own content, and say 'unknown' explicitly wherever you don't "
        f"know rather than inventing something plausible:\n\n{template_instructions}\n\n"
        f"Known attendees from the calendar: {known_attendees or 'none found'}.\n\n"
        f"Research findings (JSON records):\n{items.get('text', '')}"
    )
    model = os.environ.get("PROSPECT_RECON_BRIEF_MODEL", "gpt-4o")
    resp = litellm.completion(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        tools=[{"type": "function", "function": {"name": "write_brief", "parameters": schema}}],
        tool_choice={"type": "function", "function": {"name": "write_brief"}},
        api_key=os.environ.get("OPENAI_API_KEY"),
    )
    call = resp.choices[0].message.tool_calls[0]
    extracted = json.loads(call.function.arguments)

    receipt_id = None
    try:
        n_items = int(items.get("n_items") or 0)
        receipt = _call_governance_tool("write_receipt", {
            "context_name": f"prospect_recon:{candidate.get('slug')}",
            "item_ids": [f"kb_item:{i}" for i in range(n_items)] or ["kb_item:none"],
            "caller_id": os.environ.get("PROSPECT_RECON_CALLER_ID", "prospect_recon_brief"),
            "policy_version": "prospect-recon-v1",
            "purpose": "brief_generation",
        })
        receipt_id = receipt.get("receipt_id")
    except Exception as e:
        context.log.warning(f"prospect_recon_brief: governance receipt write failed ({e}); brief still generated (best-effort, see component docstring).")

    context.add_output_metadata({
        "schema_source": dg.MetadataValue.text(schema_source), "model": dg.MetadataValue.text(model),
        "governance_receipt_id": dg.MetadataValue.text(receipt_id or "none"),
    })
    return {
        "text": extracted.get("markdown_body", ""), "extracted": extracted,
        "schema_source": schema_source, "model": model, "governance_receipt_id": receipt_id,
    }


# ── commit to dagster-demo-factory (the hard integration contract) ─────────

def render_brief_markdown(extracted: dict, candidate: dict) -> str:
    """Real frontmatter shape, confirmed via a live fetch of a real brief
    (briefs/2026-11-03-publicis-media.md) and briefs/_TEMPLATE.md -- company/slug/domain/
    demo_date/demo_time/attendees/ae_doc/ae_doc_modified/overall_confidence/generated, then
    `markdown_body` verbatim (the model was instructed to write the full two-half document
    itself, matching the template's own headings -- see prospect_recon_brief's prompt)."""
    cand_entry = candidate.get("candidate") or {}
    attendees = cand_entry.get("attendees") or extracted.get("attendees") or []
    lines = [
        "---",
        f'company: "{extracted.get("company", "")}"',
        f'slug: "{candidate.get("slug", "")}"',
        f'domain: "{candidate.get("domain", "")}"',
        f'demo_date: "{candidate.get("demo_date", "TBD")}"',
        f'demo_time: "{extracted.get("demo_time", "")}"',
    ]
    if attendees:
        lines.append("attendees:")
        lines.extend(f'  - "{a}"' for a in attendees)
    else:
        lines.append("attendees: []")
    lines += [
        f'ae_doc: "{extracted.get("ae_doc_summary") or "none"}"',
        f'ae_doc_modified: "{extracted.get("ae_doc_modified", "")}"',
        f'overall_confidence: "{extracted.get("confidence", "")}"',
        f'generated: "{dt.datetime.now(dt.timezone.utc).isoformat()}"',
        "---", "",
    ]
    return "\n".join(lines) + "\n" + extracted.get("markdown_body", "").strip() + "\n"


def brief_path_for(candidate: dict) -> str:
    demo_date = str(candidate.get("demo_date") or "")
    if re.match(r"^\d{4}-\d{2}-\d{2}", demo_date):
        date_part = demo_date[:10]
    else:
        date_part = dt.date.today().isoformat()
    return f"briefs/{date_part}-{candidate.get('slug')}.md"


def commit_brief_and_ledger(github, candidate: dict, extracted: dict, *, repo: str = DEMO_FACTORY_REPO) -> dict:
    """Write the brief + update state/ledger.json (+ archive a consumed request file), one
    logical commit: direct push to main first, branch+PR+immediate-merge on conflict --
    exactly the routine's own fallback rule (never leave the PR open).

    state/ledger.json's real top-level shape (confirmed via a live fetch, not assumed) is
    two buckets, `"brief-ready"` and `"built"`, each a LIST of entries keyed by `slug`/
    `company` (no `domain` field) -- see module docstring. Writing a fresh brief always
    means: this slug moves (or newly lands) in `"brief-ready"`, removed from `"built"` if a
    rebuild-brief request targeted a previously-deployed prospect. Demo Factory (unmodified,
    out of scope) owns moving a slug from `"brief-ready"` into `"built"` once it deploys."""
    path = brief_path_for(candidate)
    content = render_brief_markdown(extracted, candidate)
    ledger_file = github.get_file_contents(repo, "state/ledger.json")
    ledger = json.loads(ledger_file["content"]) if ledger_file else {}
    ledger.setdefault("brief-ready", [])
    ledger.setdefault("built", [])
    slug = candidate.get("slug")
    ledger["brief-ready"] = [e for e in ledger["brief-ready"] if e.get("slug") != slug]
    ledger["built"] = [e for e in ledger["built"] if e.get("slug") != slug]
    ledger["brief-ready"].append({
        "company": candidate.get("company"), "slug": slug, "status": "brief-ready",
        "demo_date": candidate.get("demo_date"), "brief_path": path,
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
    })
    ledger_content = json.dumps(ledger, indent=2, sort_keys=True) + "\n"

    def _write(branch: str) -> None:
        existing_brief = github.get_file_contents(repo, path, ref=branch)
        github.create_or_update_file_contents(
            repo, path, content, f"prospect_recon: brief for {candidate.get('slug')}",
            branch=branch, sha=existing_brief["sha"] if existing_brief else None,
        )
        github.create_or_update_file_contents(
            repo, "state/ledger.json", ledger_content, f"prospect_recon: ledger update for {candidate.get('slug')}",
            branch=branch, sha=ledger_file["sha"] if ledger_file else None,
        )
        request_file = candidate.get("request_file")
        if request_file:
            done_path = f"requests/done/{candidate['slug']}-{dt.date.today().isoformat()}.md"
            github.create_or_update_file_contents(
                repo, done_path, request_file["content"], f"prospect_recon: archive request for {candidate['slug']}", branch=branch,
            )
            github.delete_file_contents(
                repo, request_file["path"], request_file["sha"], f"prospect_recon: consumed request for {candidate['slug']}", branch=branch,
            )

    try:
        _write("main")
        return {"branch": "main", "pr_number": None, "brief_path": path}
    except Exception as e:
        branch_name = f"prospect-recon/{candidate.get('slug')}-{dt.date.today().isoformat()}"
        github.create_branch(repo, branch_name, from_branch="main")
        _write(branch_name)
        pr = github.create_pull_request(
            repo, f"Prospect Recon: {candidate.get('company')}", head=branch_name, base="main",
            body="Automated brief + ledger update (direct push to main was rejected).",
        )
        github.merge_pull_request(repo, pr["number"], merge_method="squash")
        return {"branch": branch_name, "pr_number": pr["number"], "brief_path": path, "push_rejected_reason": str(e)}


@dg.asset(
    key=dg.AssetKey.from_user_string("prospect_recon_commit_brief"),
    ins={
        "candidate": dg.AssetIn(key=dg.AssetKey.from_user_string("prospect_recon_candidate")),
        "brief": dg.AssetIn(key=dg.AssetKey.from_user_string("prospect_recon_brief")),
    },
    required_resource_keys={"github"},
    group_name="prospect_recon",
    kinds={"github"},
    description=(
        "Commits the brief + ledger update (+ archives a consumed request file) to "
        "dagster-demo-factory on main -- the hard integration contract Demo Factory "
        "(unmodified) depends on. Also shapes a one-row DataFrame of notification "
        "template fields for the Gmail-draft and Slack-DM sinks."
    ),
)
def prospect_recon_commit_brief(context: dg.AssetExecutionContext, candidate: dict, brief: dict) -> pd.DataFrame:
    if not candidate.get("has_candidate"):
        context.log.info(f"prospect_recon_commit_brief: no candidate ({candidate.get('reason')}) -- nothing to commit.")
        return pd.DataFrame([{
            "company": "none", "domain": "", "demo_date": "", "attendees": "",
            "thesis": candidate.get("reason", "no candidate found"),
            "ae_doc_summary": "", "top_signals": "", "conflicts": "", "build_plan": "",
            "confidence": "", "biggest_guess": "", "github_brief_url": "",
        }])

    github = context.resources.github
    extracted = brief.get("extracted") or {}
    result = commit_brief_and_ledger(github, candidate, extracted)
    brief_url = f"https://github.com/{DEMO_FACTORY_REPO}/blob/main/{result['brief_path']}"
    attendees = ", ".join((candidate.get("candidate") or {}).get("attendees", []) or [])

    row = {
        "company": candidate.get("company"), "domain": candidate.get("domain"),
        "demo_date": candidate.get("demo_date"), "attendees": attendees,
        "thesis": extracted.get("demo_thesis") or (extracted.get("markdown_body", "") or "")[:280],
        "ae_doc_summary": extracted.get("ae_doc_summary") or "none found",
        "top_signals": "; ".join(extracted.get("pain_points") or extracted.get("risks") or []),
        "conflicts": extracted.get("conflicts") or "none noted",
        "build_plan": "; ".join(extracted.get("next_steps") or []),
        "confidence": extracted.get("confidence") or "",
        "biggest_guess": extracted.get("biggest_guess") or "",
        "github_brief_url": brief_url,
    }
    context.add_output_metadata({
        "brief_path": dg.MetadataValue.text(result["brief_path"]),
        "branch": dg.MetadataValue.text(result["branch"]),
        "pr_number": dg.MetadataValue.int(result["pr_number"] or 0),
    })
    return pd.DataFrame([row])


# ── schedule: weekdays 6:30pm ET, matching the real routine's own trigger ──

prospect_recon_job = dg.define_asset_job(
    name="prospect_recon_job",
    selection=dg.AssetSelection.groups("prospect_recon"),
)

prospect_recon_schedule = dg.ScheduleDefinition(
    name="prospect_recon_schedule",
    cron_schedule="30 18 * * 1-5",
    execution_timezone="America/New_York",
    job=prospect_recon_job,
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            prospect_recon_candidate,
            prospect_recon_evidence_df,
            prospect_recon_items_json,
            prospect_recon_brief,
            prospect_recon_commit_brief,
        ],
        jobs=[prospect_recon_job],
        schedules=[prospect_recon_schedule],
    )
