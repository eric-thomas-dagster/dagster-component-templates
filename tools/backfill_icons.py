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

Every `si:` slug below is hand-verified against the live simple-icons CDN
(cdn.jsdelivr.net/npm/simple-icons@latest/icons/<slug>.svg) at the time
it's added -- see `--validate-slugs` to re-check all of them at once (e.g.
after simple-icons renames/drops an icon in a later release). This is an
explicit, opt-in network check, not something that runs silently on every
backfill -- the normal `--dry-run`/default modes never touch the network,
they only read the already-curated table below.

Idempotent: existing `si:*` icons are preserved unless --override is
passed. Existing generic-Lucide icons ARE upgraded to a matching
vendor icon when one becomes available (so shipping better maps later
takes effect on re-run).

Usage:
    python3 tools/backfill_icons.py                # write manifest.json in place
    python3 tools/backfill_icons.py --dry-run      # show proposed changes only
    python3 tools/backfill_icons.py --only kafka   # substring filter on id
    python3 tools/backfill_icons.py --override     # overwrite existing si: icons too
    python3 tools/backfill_icons.py --validate-slugs  # network-check every si: slug against
                                                       # the live simple-icons CDN, report any 404s

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
    ("sagemaker", "si:amazonwebservices"),
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
    ("dataform", "si:googlecloud"),   # Google Cloud Dataform
    ("adls",     "si:microsoftazure"),
    ("synapse",  "si:microsoftazure"),
    ("azuresql", "si:microsoftazure"),
    ("azure_",   "si:microsoftazure"),
    ("_azure",   "si:microsoftazure"),
    ("azure",    "si:microsoftazure"),
    ("eventhub", "si:microsoftazure"),   # Azure Event Hub
    ("cosmosdb", "si:microsoftazure"),   # Azure Cosmos DB (si:azurecosmosdb 404s)
    ("fabric",   "si:microsoftazure"),   # Microsoft Fabric (no dedicated SI mark yet)
    ("key_vault", "si:microsoftazure"),  # Azure Key Vault
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
    ("jde",       "si:oracle"),   # JD Edwards EnterpriseOne is an Oracle product
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
    ("timescaledb", "si:timescale"),  # compound word; must precede "timescale" below
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
    ("cohere",    None),              # si:cohere 404 -- dropped from simple-icons
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
    ("honeycomb", None),              # si:honeycomb 404 -- dropped from simple-icons
    ("opentelemetry", "si:opentelemetry"),
    ("otel",      "si:opentelemetry"),
    ("otlp",      "si:opentelemetry"),
    ("dynatrace", "si:dynatrace"),
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
    ("msteams",   "si:microsoftteams"),  # compound word; "teams" alone wouldn't match it
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
    ("marketo",   "si:marketo"),
    ("hubspot",   "si:hubspot"),
    ("shopify",   "si:shopify"),
    ("airtable",  "si:airtable"),
    ("zapier",    "si:zapier"),
    # Must precede the bare "linear" entry below: linear_regression_model
    # is an ML component, not Linear.app, but "linear" alone matches it
    # (word-boundary on "_" either side). No vendor/favicon for this one
    # -- falls through to the "analytics" category default (BarChart2).
    ("linear_regression", None),
    ("linear",    "si:linear"),
    ("zendesk",   "si:zendesk"),
    ("intercom",  "si:intercom"),
    ("gong",      None),              # no SI
    ("vanta",     None),              # no SI
    ("supabase",  "si:supabase"),
    ("okta",      "si:okta"),
    ("auth0",     "si:auth0"),
    ("jamf",      None),              # si:jamf 404 -- dropped from simple-icons
    ("workday",   None),              # no SI
    ("mailchimp", "si:mailchimp"),
    ("klaviyo",   None),              # si:klaviyo 404 -- dropped from simple-icons
    # Must precede the bare "segment" entry below: launchdarkly_segment_update
    # is a LaunchDarkly audience-segment feature, not Segment.io, but
    # "segment" alone matches it (word-boundary on "_" either side).
    ("launchdarkly", None),           # si:launchdarkly 404 -- dropped from simple-icons
    ("segment",   None),              # si:segment 404 -- dropped from simple-icons

    # ─── Customer engagement / support (mostly no SI marks) ─────────
    ("amplitude", None),
    ("braze",     None),
    ("customerio", None),
    ("customer_io", None),
    ("drift",     None),
    ("freshdesk", None),
    ("freshservice", None),
    ("gorgias",   None),
    ("heap",      None),
    ("iterable",  None),
    ("kustomer",  None),
    ("onesignal", None),

    # ─── Recruiting / HR ──────────────────────────────────────────────
    ("lever",     None),
    ("workable",  None),

    # ─── Sales engagement / CRM ───────────────────────────────────────
    ("outreach",  None),
    ("salesloft", None),
    ("pipedrive", None),

    # ─── Productivity / project management ───────────────────────────
    ("monday",    None),
    ("smartsheet", None),
    ("wrike",     None),
    ("pandadoc",  None),

    # ─── Finance / ERP ─────────────────────────────────────────────────
    ("sage_intacct", None),

    # ─── Billing / usage metering ──────────────────────────────────────
    ("metronome", None),

    # ─── BI / search / data infra ──────────────────────────────────────
    ("sigma",     None),               # Sigma Computing (not the Greek-letter brand)
    ("typesense", None),
    ("cube",      None),               # Cube (cube.dev) semantic layer

    # ─── Web3 / staking ──────────────────────────────────────────────────
    ("figment",   None),

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
    ("servicenow", None),              # no SI
    ("cognos",    None),               # IBM Cognos -- no SI; favicon fallback
    ("tm1",       None),               # IBM Planning Analytics/TM1 -- no SI
    ("qlik",      None),               # no SI (covers Compose + Replicate)
    ("starburst", None),               # no SI

    # ─── Payments / finance ─────────────────────────────────────────
    ("plaid",     None),              # no SI
    ("quickbooks", "si:quickbooks"),
    ("xero",      "si:xero"),
    ("payscale",  None),              # no SI

    # ─── Billing / customer success (mostly no SI marks) ────────────
    ("chargebee", None),
    ("recurly",   None),
    ("zuora",     None),
    ("churnzero", None),
    ("gainsight", None),
    ("insightly", None),
    ("totango",   None),
    ("vitally",   None),
    ("copper",    None),               # Copper CRM
    ("zocdoc",    None),
    ("papertrail", None),              # SolarWinds Papertrail -- no SI

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


# Every si: slug referenced above, deduplicated -- the --validate-slugs
# mode checks each of these against the live CDN. (Previously this was
# computed and never actually used anywhere; see validate_known_slugs()
# below for the real check.)
KNOWN_SI_SLUGS: set[str] = {v[3:] for _, v in VENDOR_ICONS if v and v.startswith("si:")}


def _ssl_context():
    """Prefer certifi's CA bundle when available -- some Python installs
    (notably python.org's macOS installer without running "Install
    Certificates.command") ship with no usable system CA bundle at all,
    which makes every HTTPS request fail with CERTIFICATE_VERIFY_FAILED
    regardless of whether the remote resource actually exists. That
    failure must never be read as "the icon is missing" -- see the
    CHECK_ERROR handling in _slug_exists_on_cdn below."""
    import ssl
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


# Sentinel distinguishing "confirmed missing (404)" from "couldn't check"
# (network/SSL/timeout) -- these must never be conflated. A connectivity
# problem is not evidence an icon slug is wrong.
CHECK_ERROR = "error"


def _slug_exists_on_cdn(slug: str) -> "bool | str":
    """HEAD-check a single slug against the real simple-icons CDN (the
    same jsdelivr distribution the npm package ships, not the flakier
    third-party cdn.simpleicons.org proxy -- confirmed unreliable even for
    ubiquitous slugs like 'slack' during manual verification).

    Returns True (exists), False (confirmed 404), or CHECK_ERROR (network/
    SSL/timeout failure -- unknown, NOT evidence of a missing icon)."""
    import urllib.error
    import urllib.request

    url = f"https://cdn.jsdelivr.net/npm/simple-icons@latest/icons/{slug}.svg"
    req = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=10, context=_ssl_context()) as resp:
            return resp.status == 200
    except urllib.error.HTTPError as e:
        return e.code == 200
    except Exception:
        return CHECK_ERROR


def validate_known_slugs(slugs: set[str]) -> dict[str, "bool | str"]:
    """Network-check every slug in `slugs` against the live CDN. Returns
    {slug: True | False | CHECK_ERROR}. Used by --validate-slugs; never
    called during a normal (offline) backfill run."""
    return {slug: _slug_exists_on_cdn(slug) for slug in sorted(slugs)}


# ─── Favicon fallbacks ────────────────────────────────────────────────
#
# For vendors that don't ship a Simple Icons SVG, use their website
# favicon via Google's favicon service — better than a generic Lucide
# for brand recognition. Keys mirror VENDOR_ICONS keys (id substring
# match). Only fires when the VENDOR_ICONS lookup returns None (no
# Simple Icons slug available).

VENDOR_FAVICONS: dict[str, str] = {
    # Confirmed via --validate-slugs: these simple-icons entries 404 and
    # don't exist under any other title in the current catalog either
    # (not renamed, just dropped) -- favicon fallback instead.
    "cohere":        "cohere.com",
    "honeycomb":     "honeycomb.io",
    "jamf":          "jamf.com",
    "klaviyo":       "klaviyo.com",
    "segment":       "segment.com",
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
    "servicenow":    "servicenow.com",
    "cognos":        "ibm.com",
    "tm1":           "ibm.com",
    "qlik":          "qlik.com",
    "starburst":     "starburst.io",
    "payscale":      "payscale.com",
    "chargebee":     "chargebee.com",
    "recurly":       "recurly.com",
    "zuora":         "zuora.com",
    "churnzero":     "churnzero.net",
    "gainsight":     "gainsight.com",
    "insightly":     "insightly.com",
    "totango":       "totango.com",
    "vitally":       "vitally.io",
    "copper":        "copper.com",
    "zocdoc":        "zocdoc.com",
    "papertrail":    "papertrailapp.com",
    "launchdarkly":  "launchdarkly.com",
    "amplitude":     "amplitude.com",
    "braze":         "braze.com",
    "customerio":    "customer.io",
    "customer_io":   "customer.io",
    "drift":         "drift.com",
    "freshdesk":     "freshdesk.com",
    "freshservice":  "freshservice.com",
    "gorgias":       "gorgias.com",
    "heap":          "heap.io",
    "iterable":      "iterable.com",
    "kustomer":      "kustomer.com",
    "onesignal":     "onesignal.com",
    "lever":         "lever.co",
    "workable":      "workable.com",
    "outreach":      "outreach.io",
    "salesloft":     "salesloft.com",
    "pipedrive":     "pipedrive.com",
    "monday":        "monday.com",
    "smartsheet":    "smartsheet.com",
    "wrike":         "wrike.com",
    "pandadoc":      "pandadoc.com",
    "sage_intacct":  "sageintacct.com",
    "metronome":     "metronome.com",
    "sigma":         "sigmacomputing.com",
    "typesense":     "typesense.org",
    "cube":          "cube.dev",
    "figment":       "figment.io",
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
    # Keep existing si:/favicon: icons — those are curated. Upgrade a
    # generic Lucide icon to either vendor tier (si: or favicon:) when
    # we find a match; favicon: was previously never applied here even
    # though it's an intentional tier of the same resolution order.
    if current.startswith("si:") or current.startswith("favicon:"):
        return False
    if proposed.startswith("si:") or proposed.startswith("favicon:"):
        return current != proposed
    return False   # both are Lucide; leave existing alone


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", default=None, help="substring filter on component id")
    ap.add_argument("--override", action="store_true",
                    help="Overwrite existing si: icons too (default: keep them).")
    ap.add_argument("--validate-slugs", action="store_true",
                    help="Network-check every si: slug in VENDOR_ICONS against the live "
                         "simple-icons CDN and report any that 404 (e.g. renamed/dropped "
                         "icons). Does not modify manifest.json.")
    ap.add_argument("--fix-broken", action="store_true",
                    help="Network-validate every si: slug ACTUALLY IN USE in manifest.json "
                         "(a superset of VENDOR_ICONS -- components can carry si: values set "
                         "outside this tool entirely), then rewrite every component whose "
                         "icon is a CONFIRMED-404 si: slug to the current resolve_icon() "
                         "proposal. This is the one case where should_replace()'s normal "
                         "'never touch an existing si: icon' protection is bypassed -- only "
                         "for slugs this run confirms are broken, never for unverified ones. "
                         "Combine with --dry-run to preview without writing.")
    args = ap.parse_args()

    if args.fix_broken:
        text = MANIFEST.read_text()
        had_trailing_nl = text.endswith("\n")
        manifest = json.loads(text)

        used_slugs = {
            c["icon"][3:] for c in manifest["components"]
            if (c.get("icon") or "").startswith("si:")
        }
        results = validate_known_slugs(used_slugs | KNOWN_SI_SLUGS)
        confirmed_broken = {slug for slug, ok in results.items() if ok is False}
        errored = [slug for slug, ok in results.items() if ok == CHECK_ERROR]

        print(f"Validated {len(results)} si: slugs actually in use (+ curated table).")
        print(f"  confirmed broken: {len(confirmed_broken)}   couldn't check: {len(errored)}")
        if errored:
            print(f"  (unverified this run, left untouched: {sorted(errored)})")

        changed = 0
        samples = []
        for c in manifest["components"]:
            icon = c.get("icon") or ""
            if not icon.startswith("si:") or icon[3:] not in confirmed_broken:
                continue
            proposed, source = resolve_icon(c)
            if proposed is None or proposed == icon:
                continue
            samples.append((c["id"], icon, proposed, source))
            c["icon"] = proposed
            changed += 1

        print(f"\nFixing {changed} component(s) with a confirmed-broken si: icon:")
        for cid, before, after, src in samples:
            print(f"  {cid:40s}  {before:24s} -> {after:28s}  [{src}]")

        if args.dry_run:
            print("\n(--dry-run: manifest not written)")
            return 0

        out = json.dumps(manifest, indent=2, ensure_ascii=False)
        if had_trailing_nl and not out.endswith("\n"):
            out += "\n"
        MANIFEST.write_text(out)
        print(f"\nWrote {MANIFEST}")
        return 0

    if args.validate_slugs:
        results = validate_known_slugs(KNOWN_SI_SLUGS)
        missing = [slug for slug, ok in results.items() if ok is False]
        errored = [slug for slug, ok in results.items() if ok == CHECK_ERROR]
        ok_count = len(results) - len(missing) - len(errored)
        print(f"Checked {len(results)} si: slugs against cdn.jsdelivr.net/npm/simple-icons.")
        print(f"  OK: {ok_count}   confirmed missing: {len(missing)}   couldn't check: {len(errored)}")
        if errored:
            print(
                f"\n{len(errored)} slug(s) could not be checked (network/SSL/timeout -- "
                "NOT evidence they're wrong, just unverified this run):"
            )
            for slug in errored:
                print(f"  si:{slug}")
        if missing:
            print(f"\n{len(missing)} slug(s) CONFIRMED do not resolve (404) -- fix or remove these VENDOR_ICONS entries:")
            for slug in missing:
                print(f"  si:{slug}")
        if missing:
            return 1
        if errored:
            print("\nNo confirmed-broken slugs, but some couldn't be verified -- re-run to confirm.")
            return 2
        print("\nAll slugs resolve. Nothing to fix.")
        return 0

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
