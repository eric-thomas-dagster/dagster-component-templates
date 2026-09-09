#!/usr/bin/env python3
"""Backfill `icon:` on every manifest entry.

Two-tier resolution:

  1. **Vendor icons** (`si:<slug>`) — Simple Icons brand mark for
     vendor-specific components. Matched by substring against component
     id, so `snowflake_workspace`, `snowflake_stream`, and
     `dataframe_to_snowflake` all resolve to `si:snowflake`. This is
     where most of the visual variety comes from — 900+ components,
     ~80 distinct vendor brands.

  2. **Category-generic Lucide icons** — the fallback when no vendor
     match. Better than the `Package` render for the ~50 truly generic
     transforms / IO managers / sensors that don't map to a brand.

The `si:` slugs are checked against Simple Icons' known catalog before
being written; unknown slugs are logged and skipped so we don't ship
broken `<img src="cdn.simpleicons.org/nonexistent-slug">` requests.

Idempotent: existing `si:*` icons are preserved unless --override is
passed. Existing generic-Lucide icons ARE upgraded to a matching
vendor icon when one becomes available (so shipping better maps later
takes effect on re-run).

Usage:
    python3 tools/backfill_icons.py                # write manifest.json in place
    python3 tools/backfill_icons.py --dry-run      # show proposed changes only
    python3 tools/backfill_icons.py --only kafka   # substring filter on id
    python3 tools/backfill_icons.py --override     # overwrite existing si: icons too

The written file preserves em-dashes and other non-ASCII characters
(ensure_ascii=False) so the diff stays minimal.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "manifest.json"


def _matches_id(needle: str, cid: str) -> bool:
    """Word-boundary-aware substring match. `hive` matches `apache_hive`
    but NOT `archive_fetcher`; `azure` matches `azure_blob` and
    `azuresql`. Boundary = start/end of string or non-alphanumeric char.

    Fast-path: needle already contains `_` or ends with `_` — treat as
    literal substring (users' compound keys like `s3_` / `_s3` express
    intent explicitly and don't need re-boundary-ing).
    """
    if "_" in needle:
        return needle in cid
    return re.search(rf'(?:^|[^a-z0-9]){re.escape(needle)}(?:[^a-z0-9]|$)', cid) is not None


# ─── Vendor icons (si: slugs) ─────────────────────────────────────────
#
# Ordered longest-substring-first per family to prevent shorter matches
# from winning ambiguous cases (e.g. `mssql` must match before `sql`).
# All keys are lower-case substrings tested against the lowered id.

VENDOR_ICONS: list[tuple[str, str]] = [
    # ─── Cloud providers (specific-service beats platform) ─────────
    # NB: many amazon* slugs aren't on Simple Icons. Reuse the base
    # `amazonwebservices` slug, which does exist and works as the
    # AWS-family visual.
    ("amazons3", "si:amazonwebservices"),
    ("s3_",      "si:amazonwebservices"),
    ("_s3",      "si:amazonwebservices"),
    ("dynamodb", "si:amazonwebservices"),
    ("kinesis",  "si:amazonwebservices"),
    ("sqs",      "si:amazonwebservices"),
    ("sns",      "si:amazonwebservices"),
    ("ecs",      "si:amazonwebservices"),
    ("emr",      "si:amazonwebservices"),
    ("redshift", "si:amazonwebservices"),
    ("athena",   "si:amazonwebservices"),
    ("cloudwatch", "si:amazonwebservices"),
    ("glue",     "si:amazonwebservices"),
    ("aws",      "si:amazonwebservices"),
    ("bigquery", "si:googlebigquery"),
    ("pubsub",   "si:googlecloud"),
    ("dataflow", "si:googlecloud"),
    ("gcs",      "si:googlecloudstorage"),
    ("gcp",      "si:googlecloud"),
    ("firestore", "si:firebase"),
    ("firebase", "si:firebase"),
    ("googlebigquery", "si:googlebigquery"),
    ("google",   "si:googlecloud"),
    ("adls",     "si:microsoftazure"),
    ("synapse",  "si:microsoftazure"),
    ("azuresql", "si:microsoftazure"),
    ("azure_",   "si:microsoftazure"),
    ("_azure",   "si:microsoftazure"),
    ("azure",    "si:microsoftazure"),
    ("microsoft", "si:microsoftazure"),

    # ─── Databases ──────────────────────────────────────────────────
    ("snowflake", "si:snowflake"),
    ("snowpark",  "si:snowflake"),
    ("snowpipe",  "si:snowflake"),
    ("postgres",  "si:postgresql"),
    ("postgre",   "si:postgresql"),
    ("psql",      "si:postgresql"),
    ("mysql",     "si:mysql"),
    ("mariadb",   "si:mariadb"),
    ("oracle",    "si:oracle"),
    ("mssql",     "si:microsoftsqlserver"),
    ("db2",       "si:ibm"),
    ("mongodb",   "si:mongodb"),
    ("mongo",     "si:mongodb"),
    ("redis",     "si:redis"),
    ("memcached", None),              # no SI
    ("cassandra", "si:apachecassandra"),
    ("neo4j",     "si:neo4j"),
    ("elasticsearch", "si:elasticsearch"),
    ("elastic_", "si:elastic"),
    ("clickhouse", "si:clickhouse"),
    ("duckdb",    "si:duckdb"),
    ("sqlite",    "si:sqlite"),
    ("influxdb",  "si:influxdb"),
    ("timescale", "si:timescale"),
    ("cockroach", "si:cockroachlabs"),
    ("apachedoris", "si:apache"),   # apachedoris slug missing; fall back to apache mark
    ("doris",     "si:apache"),
    ("starrocks", None),             # no SI; category fallback
    ("victoriametrics", "si:victoriametrics"),

    # ─── Messaging / streaming ──────────────────────────────────────
    ("kafka",     "si:apachekafka"),
    ("redpanda",  "si:apachekafka"),
    ("rabbitmq",  "si:rabbitmq"),
    ("nats",      None),             # no SI; category fallback
    ("mqtt",      "si:mqtt"),
    ("mosquitto", "si:eclipsemosquitto"),
    ("pulsar",    "si:apachepulsar"),

    # ─── Data platforms ─────────────────────────────────────────────
    ("dagster",   None),             # no SI (dagsterhq 404); use category fallback
    ("dbt",       "si:dbt"),
    ("airbyte",   "si:airbyte"),
    ("fivetran",  None),             # no SI
    ("airflow",   "si:apacheairflow"),
    ("prefect",   "si:prefect"),
    ("pyspark",   "si:apachespark"),
    ("spark",     "si:apachespark"),
    ("iceberg",   "si:apache"),       # apacheiceberg slug missing; use apache mark
    ("delta",     "si:databricks"),
    ("databricks", "si:databricks"),
    ("hive",      "si:apachehive"),
    ("hadoop",    "si:apachehadoop"),
    ("presto",    "si:presto"),
    ("trino",     "si:trino"),

    # ─── AI / ML ────────────────────────────────────────────────────
    ("openai",    "si:openai"),
    ("anthropic", "si:anthropic"),
    ("claude",    "si:anthropic"),
    ("gemini",    "si:googlegemini"),
    ("vertex",    "si:googlecloud"),
    ("huggingface", "si:huggingface"),
    ("mlflow",    "si:mlflow"),
    ("wandb",     "si:weightsandbiases"),
    ("weightsandbiases", "si:weightsandbiases"),
    ("langchain", "si:langchain"),
    ("pytorch",   "si:pytorch"),
    ("tensorflow", "si:tensorflow"),
    ("ollama",    "si:ollama"),
    ("litellm",   None),              # no SI; category fallback (Sparkles)
    ("perplexity", "si:perplexity"),
    ("cohere",    "si:cohere"),
    ("mistral",   "si:mistralai"),
    ("groq",      None),              # no SI

    # ─── BI / visualization ─────────────────────────────────────────
    ("tableau",   "si:tableau"),
    ("powerbi",   "si:powerbi"),
    ("power_bi",  "si:powerbi"),
    ("looker",    "si:looker"),
    ("superset",  "si:apachesuperset"),
    ("metabase",  "si:metabase"),
    ("streamlit", "si:streamlit"),

    # ─── Observability / monitoring ─────────────────────────────────
    ("grafana",   "si:grafana"),
    ("prometheus", "si:prometheus"),
    ("datadog",   "si:datadog"),
    ("dogstatsd", "si:datadog"),
    ("statsd",    "si:datadog"),
    ("newrelic",  "si:newrelic"),
    ("splunk",    "si:splunk"),
    ("honeycomb", "si:honeycomb"),
    ("opentelemetry", "si:opentelemetry"),
    ("otel",      "si:opentelemetry"),
    ("otlp",      "si:opentelemetry"),
    ("sentry",    "si:sentry"),
    ("posthog",   "si:posthog"),

    # ─── Alerting / paging ──────────────────────────────────────────
    ("pagerduty", "si:pagerduty"),
    ("opsgenie",  "si:atlassian"),

    # ─── Data catalogs ──────────────────────────────────────────────
    # Most catalog vendors don't have Simple Icons; fall through to
    # the category default (Link icon for `integration`, matches the
    # "connect Dagster lineage to X" story).
    ("datahub",   None),
    ("openmetadata", None),
    ("purview",   "si:microsoftazure"),
    ("collibra",  None),
    ("alation",   None),

    # ─── SaaS / apps ────────────────────────────────────────────────
    ("slack",     "si:slack"),
    ("teams",     "si:microsoftteams"),
    ("discord",   "si:discord"),
    ("github",    "si:github"),
    ("gitlab",    "si:gitlab"),
    ("bitbucket", "si:bitbucket"),
    ("jira",      "si:jira"),
    ("confluence", "si:confluence"),
    ("notion",    "si:notion"),
    ("stripe",    "si:stripe"),
    ("twilio",    "si:twilio"),
    ("sendgrid",  "si:twilio"),
    ("salesforce", "si:salesforce"),
    ("hubspot",   "si:hubspot"),
    ("shopify",   "si:shopify"),
    ("airtable",  "si:airtable"),
    ("zapier",    "si:zapier"),
    ("linear",    "si:linear"),
    ("zendesk",   "si:zendesk"),
    ("intercom",  "si:intercom"),
    ("gong",      None),              # no SI
    ("vanta",     None),              # no SI
    ("supabase",  "si:supabase"),
    ("okta",      "si:okta"),
    ("auth0",     "si:auth0"),
    ("jamf",      "si:jamf"),
    ("workday",   None),              # no SI
    ("mailchimp", "si:mailchimp"),
    ("klaviyo",   "si:klaviyo"),
    ("segment",   "si:segment"),

    # ─── Runtime / infra ────────────────────────────────────────────
    ("docker",    "si:docker"),
    ("kubernetes", "si:kubernetes"),
    ("k8s",       "si:kubernetes"),
    ("terraform", "si:terraform"),
    ("ansible",   "si:ansible"),
    ("minio",     "si:minio"),
    ("nginx",     "si:nginx"),
    ("cloudflare", "si:cloudflare"),
    ("vercel",    "si:vercel"),
    ("netlify",   "si:netlify"),

    # ─── SAP / enterprise ───────────────────────────────────────────
    ("sap",       "si:sap"),
    ("dynamics",  "si:microsoft"),
    ("msgraph",   "si:microsoft"),

    # ─── Payments / finance ─────────────────────────────────────────
    ("plaid",     None),              # no SI
    ("quickbooks", "si:quickbooks"),
    ("xero",      "si:xero"),

    # ─── Container / package ────────────────────────────────────────
    ("apache",    "si:apache"),
    ("ibm",       None),              # SI has 'ibm' but rendered oddly; category fallback

    # ─── Vector / RAG (mostly no SI slugs) ─────────────────────────
    ("pinecone",  None),
    ("weaviate",  None),
    ("qdrant",    "si:qdrant"),
    ("chromadb",  None),
    ("chroma",    None),

    # ─── File / doc formats ─────────────────────────────────────────
    ("excel",     "si:microsoftexcel"),
    ("sharepoint", "si:microsoftsharepoint"),
    ("googledrive", "si:googledrive"),
    ("googlesheets", "si:googlesheets"),
    ("dropbox",   "si:dropbox"),
    ("box",       "si:box"),
    ("onedrive",  "si:microsoftonedrive"),

    # ─── Misc ───────────────────────────────────────────────────────
    ("stackoverflow", "si:stackoverflow"),
    ("reddit",    "si:reddit"),
    ("twitter",   "si:twitter"),
    ("openflow",  "si:snowflake"),
    ("acord",     None),   # ACORD Corp — no si slug; force Lucide fallback
]


# Simple Icons slugs we know exist (used for validation; missing slugs
# get skipped rather than shipped as broken image URLs).
KNOWN_SI_SLUGS = {v[3:] for _, v in VENDOR_ICONS if v and v.startswith("si:")}


# ─── Favicon fallbacks ────────────────────────────────────────────────
#
# For vendors that don't ship a Simple Icons SVG, use their website
# favicon via Google's favicon service — better than a generic Lucide
# for brand recognition. Keys mirror VENDOR_ICONS keys (id substring
# match). Only fires when the VENDOR_ICONS lookup returns None (no
# Simple Icons slug available).

VENDOR_FAVICONS: dict[str, str] = {
    "dagster":       "dagster.io",
    "fivetran":      "fivetran.com",
    "datahub":       "datahubproject.io",
    "openmetadata":  "open-metadata.org",
    "collibra":      "collibra.com",
    "alation":       "alation.com",
    "gong":          "gong.io",
    "vanta":         "vanta.com",
    "pinecone":      "pinecone.io",
    "weaviate":      "weaviate.io",
    "chromadb":      "trychroma.com",
    "chroma":        "trychroma.com",
    "workday":       "workday.com",
    "plaid":         "plaid.com",
    "litellm":       "litellm.ai",
    "groq":          "groq.com",
    "memcached":     "memcached.org",
    "starrocks":     "starrocks.io",
    "nats":          "nats.io",
    "ibm":           "ibm.com",
}


# ─── Category-generic Lucide fallbacks ───────────────────────────────
#
# Only applied when a component has no vendor match AND either:
#   - has no icon at all, or
#   - has a generic Lucide icon we can improve upon.
#
# The goal is to make categorically similar components render similar
# icons at a glance in the grid.

CATEGORY_FALLBACK: dict[str, str] = {
    "ingestion":      "ArrowDownToLine",   # arrow into the system
    "sink":           "ArrowUpFromLine",   # arrow out of the system
    "source":         "Database",
    "transformation": "Shuffle",
    "sensor":         "Radar",
    "observation":    "Eye",
    "check":          "ShieldCheck",
    "resource":       "Plug",
    "io_manager":     "HardDrive",
    "integration":    "Link",
    "external":       "ExternalLink",
    "ai":             "Sparkles",
    "analytics":      "BarChart2",
    "jobs":           "Play",
    "decorator":      "Layers",
    "infrastructure": "Server",
    "dbt":            "si:dbt",
}


def resolve_icon(comp: dict) -> tuple[str | None, str]:
    """Return (icon, source). Source ∈ {'vendor', 'favicon', 'category', 'skip'}."""
    cid = (comp.get("id") or "").lower()
    for needle, slug in VENDOR_ICONS:
        if _matches_id(needle, cid):
            if slug is not None:
                return slug, "vendor"
            # No Simple Icons entry — try favicon fallback keyed on the
            # same needle. Preserves per-vendor visual distinction even
            # when Simple Icons doesn't ship a mark for the brand.
            fav = VENDOR_FAVICONS.get(needle)
            if fav:
                return f"favicon:{fav}", "favicon"
            # No favicon either — fall through to category default.
            break
    cat = comp.get("category")
    if cat and cat in CATEGORY_FALLBACK:
        return CATEGORY_FALLBACK[cat], "category"
    return None, "skip"


def should_replace(current: str | None, proposed: str | None, override: bool) -> bool:
    if proposed is None:
        return False
    if not current:
        return True
    if override:
        return current != proposed
    # Keep existing si: icons — those are curated. Upgrade generic
    # Lucide icons to si: vendor icons when we find a match.
    if current.startswith("si:"):
        return False
    if proposed.startswith("si:"):
        return current != proposed
    return False   # both are Lucide; leave existing alone


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default=None, help="substring filter on component id")
    ap.add_argument("--override", action="store_true",
                    help="Overwrite existing si: icons too (default: keep them).")
    args = ap.parse_args()

    text = MANIFEST.read_text()
    had_trailing_nl = text.endswith("\n")
    manifest = json.loads(text)

    changed = 0
    vendor_changes = 0
    favicon_changes = 0
    category_changes = 0
    upgrades = 0
    per_source: dict[str, int] = {}
    samples: list[tuple[str, str | None, str, str]] = []
    # samples only appended when `proposed is not None` (guarded by
    # should_replace), so the third slot is always a real str.

    for c in manifest["components"]:
        cid = c.get("id", "")
        if args.only and args.only not in cid:
            continue
        current = c.get("icon")
        proposed, source = resolve_icon(c)
        per_source[source] = per_source.get(source, 0) + 1
        if not should_replace(current, proposed, args.override):
            continue
        was_upgrade = current is not None
        c["icon"] = proposed
        changed += 1
        if source == "vendor":   vendor_changes   += 1
        if source == "favicon":  favicon_changes  += 1
        if source == "category": category_changes += 1
        if was_upgrade: upgrades += 1
        if len(samples) < 40:
            assert proposed is not None   # guarded by should_replace
            samples.append((cid, current, proposed, source))

    print(f"Proposed: {changed} icon changes")
    print(f"  vendor:   {vendor_changes}   (si: brand mark)")
    print(f"  favicon:  {favicon_changes}   (vendor site favicon)")
    print(f"  category: {category_changes}   (Lucide fallback)")
    print(f"  upgrades (had a generic icon): {upgrades}")
    print(f"  fresh (had no icon):           {changed - upgrades}")
    print()
    print(f"Per-source scan tally: {per_source}")
    print()
    print("Sample changes:")
    for cid, before, after, src in samples:
        print(f"  {cid:45s}  {str(before):24s} → {after:24s}  [{src}]")

    if args.dry_run:
        print("\n(--dry-run: manifest not written)")
        return 0

    out = json.dumps(manifest, indent=2, ensure_ascii=False)
    if had_trailing_nl and not out.endswith("\n"):
        out += "\n"
    MANIFEST.write_text(out)
    print(f"\nWrote {MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
