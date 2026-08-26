#!/usr/bin/env python3
"""Backfill `keywords: [str]` on every manifest entry.

Keywords are a curated field of aliases / synonyms — the vocabulary a user
might type that isn't literally in the component's id/name/description.
Populating them dramatically improves recall on `dagster-component search`
without any change to component code.

Signal sources (in order of specificity):

  1. Explicit VENDOR_ALIASES table (see below) — hand-curated aliases for
     well-known vendors keyed on substrings of the component id.
  2. Category-generic terms — e.g. every `resource` gets `["connection",
     "connect"]`; every `io_manager` gets `["storage", "persistence"]`.
  3. From agent_hints.example_prompts, extract 3-6 word noun-phrases that
     don't already appear in id / name / description / tags — those are
     the terms a user would type but that the component's core fields
     don't capture.
  4. Existing keywords are preserved and merged (idempotent — safe to
     re-run).

Run:
    python3 tools/backfill_keywords.py                    # write manifest.json in place
    python3 tools/backfill_keywords.py --dry-run          # diff-only
    python3 tools/backfill_keywords.py --only postgres    # substring filter on id
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "manifest.json"


# ─── Vendor aliases ──────────────────────────────────────────────────────────
#
# Keys are substrings matched against component id (case-insensitive). All
# matches contribute their aliases. Order-independent; duplicates dedup.
#
# The goal is NOT to be exhaustive — it's to cover the queries an automation
# is most likely to type that today return zero hits.

VENDOR_ALIASES: dict[str, list[str]] = {
    # ─── SQL databases ────────────────────────────────────────────────
    "mssql": ["sql server", "microsoft sql", "microsoft sql server", "azure sql", "tsql", "sqlserver"],
    "postgres": ["postgresql", "psql", "psycopg", "postgre"],
    "mysql": ["maria", "mariadb", "aurora mysql"],
    "oracle": ["oracledb", "oracle db", "plsql", "pl/sql"],
    "db2": ["ibm db2", "database 2"],
    "sqlite": ["sqlite3"],
    "duckdb": ["duck db", "duck-db"],
    "snowflake": ["snow", "snowsql"],
    "bigquery": ["big query", "bq", "google bigquery", "gcp bigquery"],
    "redshift": ["amazon redshift", "aws redshift"],
    "athena": ["aws athena", "amazon athena"],
    "clickhouse": ["click house"],
    "cockroachdb": ["cockroach db", "cockroach"],
    "singlestore": ["memsql", "single store"],
    "vertica": ["hp vertica"],
    "teradata": ["tera data"],
    "sap_hana": ["hana", "s/4hana", "sap hana"],
    # ─── NoSQL / document / KV / graph / vector ──────────────────────
    "mongodb": ["mongo", "documentdb", "amazon documentdb", "atlas"],
    "cassandra": ["scylladb", "scylla", "datastax", "astra"],
    "neo4j": ["cypher", "graph database", "graphdb"],
    "redis": ["elasticache", "keydb", "dragonfly", "valkey"],
    "elasticsearch": ["elastic search", "opensearch", "es cluster"],
    "dynamodb": ["dynamo", "aws dynamodb"],
    "firestore": ["firebase firestore", "google firestore"],
    "cosmos": ["cosmosdb", "azure cosmos"],
    "chromadb": ["chroma", "chroma db"],
    "pinecone": ["vector db pinecone"],
    "pgvector": ["postgres vector", "postgresql vector"],
    "weaviate": ["weaviate vector"],
    "qdrant": ["qdrant vector"],
    "milvus": ["milvus vector"],
    # ─── Object storage / lake ───────────────────────────────────────
    "s3": ["aws s3", "amazon s3", "object store", "object storage", "bucket"],
    "gcs": ["gs", "google cloud storage", "gcp storage", "gcp bucket"],
    "adls": ["azure data lake", "abfs", "abfss", "azure storage", "adls gen2"],
    "azure_blob": ["azure blob storage", "az storage"],
    "minio": ["min io", "s3 compatible"],
    "iceberg": ["apache iceberg", "iceberg table"],
    "delta": ["delta lake", "deltalake", "databricks delta"],
    "hudi": ["apache hudi"],
    "sftp": ["sftp server", "secure ftp", "ssh ftp"],
    # ─── Streaming / messaging ───────────────────────────────────────
    "kafka": ["apache kafka", "confluent", "msk", "amazon msk", "azure event hubs kafka", "redpanda"],
    "kinesis": ["aws kinesis", "amazon kinesis"],
    "eventhubs": ["azure event hubs", "event hub"],
    "pubsub": ["gcp pubsub", "google pubsub", "cloud pubsub"],
    "rabbitmq": ["rabbit mq", "amqp"],
    "mqtt": ["mqtt broker", "iot messaging"],
    "nats": ["nats.io", "nats jetstream"],
    "pulsar": ["apache pulsar"],
    "sqs": ["aws sqs", "amazon sqs", "simple queue"],
    "servicebus": ["azure service bus"],
    # ─── ETL / EL / data movement ────────────────────────────────────
    "fivetran": ["fivetran connector", "hva"],
    "airbyte": ["airbyte connector"],
    "sling": ["sling data"],
    "meltano": ["singer"],
    "matillion": ["matillion etl"],
    "qlik_replicate": ["attunity", "qlik data movement"],
    "database_replication": [
        "cdc", "change data capture", "logical replication",
        "recurring", "recurring replication", "ongoing sync",
    ],
    "database_migration": ["one-time load", "lift and shift", "migration"],
    # ─── Warehouses vendors already covered above ────────────────────
    # ─── Compute / notebooks / containers ────────────────────────────
    "databricks": ["dbx", "databricks workflows", "dbfs", "spark databricks", "databricks sql"],
    "spark": ["apache spark", "pyspark"],
    "papermill": ["parameterized notebook", "notebook execution"],
    "jupyter": ["jupyter notebook", "ipynb"],
    "docker": ["container", "oci image"],
    "kubernetes": ["k8s"],
    # ─── SaaS / APIs ─────────────────────────────────────────────────
    "salesforce": ["sfdc", "salesforce crm", "sf-cli"],
    "hubspot": ["hubspot crm"],
    "stripe": ["stripe api", "stripe payments"],
    "shopify": ["shopify api", "shopify store"],
    "notion": ["notion pages", "notion api"],
    "github": ["gh api", "github api", "octocat"],
    "slack": ["slack api", "slack channel"],
    "jira": ["atlassian jira"],
    "servicenow": ["snow itsm", "snow"],
    "asana": ["asana tasks"],
    "linear": ["linear tickets"],
    "monday": ["monday.com"],
    "airtable": ["airtable base"],
    "gong": ["gong.io", "gong calls"],
    "zendesk": ["zendesk tickets"],
    "freshdesk": ["freshworks", "freshdesk tickets"],
    "vanta": ["vanta compliance", "soc2"],
    "personio": ["personio hris"],
    "google_ads": ["google adwords", "adwords"],
    "facebook_ads": ["meta ads", "facebook marketing"],
    "linkedin_ads": ["linkedin marketing"],
    "twitter_ads": ["x ads"],
    "pinterest_ads": ["pinterest marketing"],
    "tiktok_ads": ["tiktok marketing"],
    "matomo": ["piwik", "self hosted analytics"],
    "posthog": ["product analytics"],
    "google_analytics": ["ga4", "google analytics 4"],
    # ─── AI / LLM / ML ───────────────────────────────────────────────
    "litellm": ["multi-provider llm", "lite llm"],
    "openai": ["gpt", "gpt-4", "chatgpt", "o1", "o3"],
    "anthropic": ["claude"],
    "gemini": ["google gemini", "vertex ai"],
    "voyage": ["voyageai", "voyage ai"],
    "cohere": ["cohere embed"],
    "hugging_face": ["huggingface", "hf"],
    "mlflow": ["ml flow"],
    "wandb": ["weights and biases", "weights & biases", "weights biases"],
    "optuna": ["hyperparameter tuning", "bayesian search"],
    "sklearn": ["scikit-learn", "scikit learn"],
    "sentence_transformer": ["sbert"],
    "chroma": ["chromadb"],
    "mcp": ["model context protocol", "mcp tool"],
    "agentic_pipeline": [
        "multi-agent workflow", "llm pipeline", "agent orchestration",
        "critique loop", "debate", "route", "specialist agent",
    ],
    "llm_evaluator": ["llm as judge", "judge model", "answer quality", "eval prompt"],
    "provider_ab_evaluator": ["provider comparison", "model comparison", "ab test llm"],
    "rag_": ["retrieval augmented", "retrieval-augmented", "rag pipeline"],
    "embedding": ["vector embedding", "embed text"],
    # ─── Observability / lineage ─────────────────────────────────────
    "datahub": ["linkedin datahub"],
    "openmetadata": ["open metadata"],
    "purview": ["azure purview"],
    "alation": ["alation catalog"],
    "collibra": ["collibra catalog"],
    "otlp": ["opentelemetry", "otel", "open telemetry"],
    "statsd": ["dogstatsd", "datadog agent"],
    "prometheus": ["push gateway", "prom", "promql"],
    "splunk": ["splunk hec", "splunk enterprise"],
    "sentry": ["sentry.io", "error tracking"],
    "grafana": ["grafana loki", "grafana tempo"],
    "dbt": ["data build tool", "dbt-core", "dbt cloud"],
    # ─── Cross-cutting patterns ──────────────────────────────────────
    "upsert": ["idempotent write", "merge into", "on conflict", "partition rewrite"],
    "monitor": ["watch", "polling sensor", "arrival sensor"],
    "sensor": ["trigger", "event listener"],
    "external": ["declare only", "declaration", "mirror asset"],
    "workspace": ["multi asset workspace", "workspace-style component"],
    "orchestration": ["job orchestrator"],
    "schema": ["ddl", "table definition"],
    "ingestion": ["ingest", "extract load", "el"],
    "sink": ["writer", "output writer"],
    "compute_log": ["stdout capture", "log forwarder"],
}


# ─── Concept aliases keyed to specific component IDs ────────────────────────
#
# Cross-cutting patterns (like "upsert", "backfill", "quality gate") that
# aren't captured by a vendor id substring. Add the concept as a keyword to
# every component that supports the pattern, so a search for the concept
# surfaces the whole family.

CONCEPT_ALIASES: dict[str, list[str]] = {
    # Idempotent partition-rewrite writes (v0.10.99+ primitive).
    "upsert": [
        "polars_pipeline",
        "ml_pipeline",
        "rest_api_fetcher",
        "streaming_consumer",
        "kafka_to_database_asset",
        "kinesis_to_database_asset",
        "pubsub_to_database_asset",
        "eventhubs_to_database_asset",
        "sqs_to_database_asset",
        "servicebus_to_database_asset",
        "rabbitmq_to_database_asset",
        "nats_to_database_asset",
        "mqtt_to_database_asset",
        "pulsar_to_database_asset",
        "redis_streams_to_database_asset",
        "adls_to_database_asset",
        "s3_to_database_asset",
        "gcs_to_database_asset",
        "sftp_to_database_asset",
        "sql_to_database_asset",
        "warehouse_pipeline",
        "database_replication",
        "iceberg_ingestion",
        "delta_ingestion",
    ],
    "idempotent write": [
        "polars_pipeline",
        "ml_pipeline",
        "rest_api_fetcher",
        "warehouse_pipeline",
    ],
    "partition rewrite": [
        "polars_pipeline",
        "ml_pipeline",
        "rest_api_fetcher",
    ],
    "quality gate": [
        "provider_ab_evaluator",
        "llm_evaluator",
        "llm_judge",
        "mlflow_model_version_check",
    ],
    "merge gate": [
        "provider_ab_evaluator",
    ],
    "freight carrier": [
        "rest_api_fetcher",  # documented use case in the walkthrough
    ],
    "vendor api": [
        "rest_api_fetcher",
        "oauth_rest_ingestion",
        "graphql_asset",
        "openapi_asset",
    ],
    "per partition api": [
        "rest_api_fetcher",
    ],
    "human in the loop": [
        "human_approval_gate",
        "slack_approval_gate",
    ],
    "hitl": [
        "human_approval_gate",
        "slack_approval_gate",
    ],
    "backfill": [
        "polars_pipeline",
        "ml_pipeline",
        "warehouse_pipeline",
        "rest_api_fetcher",
    ],
    "crewai": [
        "agentic_pipeline",
    ],
    "langgraph": [
        "agentic_pipeline",
    ],
    "autogen": [
        "agentic_pipeline",
    ],
    "dspy": [
        "agentic_pipeline",
    ],
    "framework handoff": [
        "agentic_pipeline",
    ],
    "nl to sql": [
        "databricks_genie_query",
    ],
    "natural language sql": [
        "databricks_genie_query",
    ],
    "text to sql": [
        "databricks_genie_query",
    ],
    # cron_schedule wraps build_schedule_from_partitioned_job when partition
    # fields are supplied — attach the vocabulary a schedule-writing task
    # will actually type. Prevents rediscovery of the wheel as a custom
    # component (as happened in a customer project 2026-08).
    "build_schedule_from_partitioned_job": [
        "cron_schedule",
    ],
    "partitioned job schedule": [
        "cron_schedule",
    ],
    "partitioned asset schedule": [
        "cron_schedule",
    ],
    "schedule partitioned assets": [
        "cron_schedule",
    ],
    "cron over partitioned job": [
        "cron_schedule",
    ],
    "partitioned": [
        "cron_schedule",
        "interval_schedule",
        "asset_job",
        "per_partition_backfill_job",
        "partitioned_asset_launcher_job",
    ],
    "tool use loop": [
        "agentic_pipeline",
    ],
    "tool use agent": [
        "agentic_pipeline",
        "litellm_agent",
        "openai_agent",
        "anthropic_agent",
        "gemini_agent",
        "vercel_ai_gateway_agent",
        "snowflake_cortex_agent",
    ],
}


# ─── Category-generic aliases ────────────────────────────────────────────────

CATEGORY_ALIASES: dict[str, list[str]] = {
    "resource": ["connection", "connect"],
    "io_manager": ["storage", "persistence", "io"],
    "sensor": ["trigger", "watcher"],
    "observation": ["observability", "watch"],
    "external": ["declare only", "mirror"],
    "integration": ["3rd party", "external system"],
    "check": ["data quality", "assertion"],
    "transformation": ["transform", "reshape"],
    "ingestion": ["ingest", "read from"],
    "sink": ["write to", "output"],
    "ai": ["llm", "genai"],
    "analytics": ["report", "aggregate"],
    "infrastructure": ["infra"],
    "source": ["read", "load"],
    "jobs": ["job", "materialize batch"],
    "data-warehouse": ["warehouse", "dwh"],
    "dbt": ["data build tool"],
}


# ─── Extraction helpers ──────────────────────────────────────────────────────

STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "for", "in", "on",
    "with", "as", "into", "from", "any", "each", "one", "two", "n",
    "your", "you", "this", "that", "it", "its", "is", "are", "was",
    "be", "been", "will", "can", "may", "if", "then", "not",
    "component", "components", "asset", "assets", "step", "steps",
    "dagster", "yaml", "config", "returns", "return", "returns.", "returns,",
}


def _tokens(s: str) -> list[str]:
    """Words + hyphen/underscore-joined identifiers, lower-cased, stopwords out."""
    words = re.findall(r"[a-z0-9_\-]{3,}", (s or "").lower())
    return [w for w in words if w not in STOPWORDS]


def _existing_corpus(entry: dict) -> set[str]:
    """Words already in the component's id/name/description/tags/hints — no
    point re-adding these as keywords, they're already searchable."""
    parts = [
        entry.get("id", ""),
        entry.get("name", ""),
        entry.get("description", ""),
        " ".join(entry.get("tags") or []),
    ]
    ah = entry.get("agent_hints") or {}
    for v in ah.values():
        parts.append(json.dumps(v, default=str))
    return set(_tokens(" ".join(parts)))


def _from_vendor_aliases(component_id: str) -> list[str]:
    id_lower = component_id.lower()
    aliases: list[str] = []
    for pattern, alist in VENDOR_ALIASES.items():
        if pattern in id_lower:
            aliases.extend(alist)
    return aliases


def _from_concept_aliases(component_id: str) -> list[str]:
    """Pattern → aliases assignment keyed on exact component id. For
    cross-cutting concepts (upsert, quality gate, hitl, etc.) that
    aren't captured by an id substring."""
    aliases: list[str] = []
    for concept, target_ids in CONCEPT_ALIASES.items():
        if component_id in target_ids:
            aliases.append(concept)
    return aliases


def _from_category(category: str) -> list[str]:
    return CATEGORY_ALIASES.get(category, [])


def _from_example_prompts(entry: dict, existing: set[str]) -> list[str]:
    """Pull 2-4 word noun phrases from agent_hints.example_prompts that
    aren't already in the corpus. Keeps phrases (multi-word) since search
    tokenizes on whitespace — a keyword `"freight carrier"` matches both
    `freight` and `carrier` when the user types either one."""
    prompts = (entry.get("agent_hints") or {}).get("example_prompts") or []
    if not isinstance(prompts, list):
        return []
    phrases: list[str] = []
    for p in prompts[:5]:
        s = str(p).lower()
        # Extract 2-3 word runs of content words (no stopword between).
        words = re.findall(r"[a-z0-9_\-]+", s)
        i = 0
        while i < len(words) - 1:
            if words[i] in STOPWORDS:
                i += 1
                continue
            # Try 3-gram then 2-gram; require every word non-stopword.
            for n in (3, 2):
                if i + n <= len(words) and all(w not in STOPWORDS for w in words[i : i + n]):
                    phrase = " ".join(words[i : i + n])
                    # Only keep if it introduces a NEW token.
                    new_tokens = set(_tokens(phrase)) - existing
                    if len(new_tokens) >= 1 and phrase not in phrases:
                        phrases.append(phrase)
                    break
            i += 1
    return phrases[:6]


def _normalize_keywords(kws: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for k in kws:
        k = re.sub(r"\s+", " ", str(k).strip().lower())
        if not k:
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def backfill(manifest: dict, only: str | None) -> tuple[int, int, dict]:
    """Return (changed, unchanged, sample_diff)."""
    changed = 0
    unchanged = 0
    sample: dict = {}
    for entry in manifest.get("components", []):
        cid = entry.get("id", "")
        if only and only not in cid:
            continue
        existing_corpus = _existing_corpus(entry)
        existing_kws = list(entry.get("keywords") or [])
        additions = (
            _from_vendor_aliases(cid)
            + _from_concept_aliases(cid)
            + _from_category(entry.get("category", ""))
            + _from_example_prompts(entry, existing_corpus)
        )
        merged = _normalize_keywords(existing_kws + additions)
        if merged != _normalize_keywords(existing_kws):
            entry["keywords"] = merged
            changed += 1
            if len(sample) < 3:
                sample[cid] = {
                    "before": existing_kws,
                    "after": merged,
                }
        else:
            unchanged += 1
    return changed, unchanged, sample


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="Substring filter on component id")
    args = ap.parse_args()

    manifest = json.loads(MANIFEST.read_text())
    changed, unchanged, sample = backfill(manifest, args.only)

    print(f"changed:    {changed}")
    print(f"unchanged:  {unchanged}")
    print()
    for cid, diff in sample.items():
        print(f"── {cid} ──")
        print(f"  before ({len(diff['before'])}): {diff['before']}")
        print(f"  after  ({len(diff['after'])}): {diff['after']}")

    if args.dry_run:
        print("\n(dry run — manifest.json not written)")
        return

    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"\nWrote {MANIFEST.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
