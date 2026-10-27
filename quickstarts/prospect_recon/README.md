# Prospect Recon

A full-fidelity Dagster port of a real, personal Claude routine ("Prospect
Recon") run every weekday evening against an actual sales-engineering job:
autonomously decide which handed-off prospect to work on, research them
across Calendar/Drive/the web, write a structured brief, commit it to GitHub,
and notify a human. A second routine ("Demo Factory", **not** part of this
port) reads that brief overnight to build a demo -- unmodified, so this
quickstart writes into its exact expected files and format.

## Why this exists

This is not an illustrative toy. The real routine's full instructions (not
summarized) drove every design decision here, and it's meant to be run
against real accounts so it replaces the actual routine, not a demo of it.
It also exercises this repo's AI primitives for real: `context_engineering_pipeline`
(chunk/redact/classify/embed with a receipt-store-driven feedback loop),
`agentic_pipeline` (an agentic tool-use research loop + forced-function-call
extraction), and the context receipt store (real retrieval/outcome
provenance, not staged).

## What's real

**Everything**, when you provide real credentials (see `.env.example`):
real Calendar/Drive API calls, a real agentic LLM research loop against real
MCP tools (Drive search, Notion search, Tavily web search), a real embedded
ChromaDB knowledge base, a real forced-function-call brief extraction, a
real git commit (push, or branch+PR+merge on conflict) to whatever repo
`PROSPECT_RECON_DEMO_FACTORY_REPO` names, a real Gmail draft, and a real
Slack DM. There is no synthetic/offline mode -- unlike `receipts_learning_loop`,
this is meant to be watched running against your own accounts in the Dagster UI.

**Cost/safety note:** every run makes real LLM calls (KB embeddings, the
research tool-use loop, brief extraction) and, if a candidate is found, a
real commit + PR + merge against `PROSPECT_RECON_DEMO_FACTORY_REPO`. Point
`PROSPECT_RECON_DEMO_FACTORY_REPO` at a scratch/fork repo the first few times
you run this.

## Three products, three jobs

This quickstart is one leg of a **Dagster + FastMCP + Prefect** story, each
product doing the job it's actually good at:

- **Prefect builds the demo project.** That's Demo Factory, a separate,
  unmodified routine (out of scope here) that reads this quickstart's output
  from `dagster-demo-factory` overnight.
- **Dagster builds and shows the context.** The asset graph, materialization
  history, and the governed knowledge base itself
  (`prospect_recon_context_kb`) are real, inspectable Dagster assets -- not a
  staged illustration. This is the "I can see the history and assets" piece.
- **FastMCP governs access to that context and writes receipts.**
  `mcp_servers/context_governance_server.py` is the access boundary for the
  generation-time receipt: the moment `prospect_recon_brief` actually
  consumes the governed KB to produce an artifact Demo Factory will read, it
  goes through an MCP tool call (`write_receipt`) rather than a direct
  in-process resource call. Build-time provenance (chunking, classification,
  the research tool-call trail) keeps the existing, already-tested in-process
  `receipts:` wiring on `prospect_recon_context_kb` /
  `prospect_recon_research` -- there's no reason to break what's real and
  fast; the new governance boundary is specifically for the
  consumption/publish-time event, which is the one that matters most for
  "did this artifact come from governed context."

## Known, documented simplifications (see code comments for the full reasoning)

- **Signal B's company-from-file judgment call** now IS an LLM judgment call,
  matching the real routine: `_classify_drive_company` reads a Drive file's
  actual content (`google_drive_ingestion`'s `inline_text_content` now
  extracts real text from PDF/.docx/.xlsx too, via pdfplumber/python-docx/
  openpyxl, not just plain-text-ish MIME types -- fixed separately, see that
  component's own README) and weighs it against domains already found via
  Calendar. `_guess_company_from_filename` is kept only as the last-resort
  fallback, now reached only for file types with no text representation at
  all (images, unknown binary formats) or when those optional extraction
  libraries aren't installed, or when the LLM itself has low/no confidence.
- **The brief now authors the full two-half document** (discovery +
  "Build directives": asset graph sizing, fidelity, demo shape, checks, demo
  mode, three buckets, demo name, money shot, capability talk track --
  confirmed via a live fetch of `briefs/_TEMPLATE.md` and a real deployed
  brief, `briefs/2026-11-03-publicis-media.md`) as one coherent
  `markdown_body`, instructed directly against the live-fetched template
  text -- not decomposed into dozens of schema fields and reassembled, which
  would be far more fragile for a document this rich (a table, several
  bulleted lists, a multi-paragraph talk track). `derive_schema_from_template`
  / `FALLBACK_BRIEF_SCHEMA` stay as a lighter structural signal (schema_source
  metadata, a handful of structured fields the Gmail/Slack notifications use
  directly) in `defs/prospect_recon_pipeline/definitions.py`.
- **`state/ledger.json`'s real shape** (confirmed via a live fetch, not
  assumed) is two buckets -- `"brief-ready"` and `"built"`, each a list of
  entries keyed by `slug`/`company` (no `domain` field, and the bucket name
  `"built"` doesn't match its own entries' `status: "deployed"` string, kept
  faithfully). The dangling-ledger fallback reads the `"brief-ready"` bucket;
  the "already deployed" exclusion rule matches by slug against `"built"`.
- **The no-candidate short-circuit is partial.** When no real candidate is
  found, downstream assets still materialize (Dagster doesn't make this free
  to skip through generic pipeline components without modifying them, which
  was out of scope here) but the research step is explicitly instructed not
  to call any tools and finalize immediately, and the brief/commit/notify
  assets short-circuit to a minimal "no candidate, here's why" message --
  the same behavior the real routine's own "notify me (draft + push), stop"
  rule describes, at the cost of one still-cheap LLM call instead of zero.
- **Notion is a secondary output, not part of the real contract.** The real
  routine's instructions never call Notion despite it being a listed
  connector -- `notion_brief_page`'s YAML says so explicitly. Kept because
  an AE could plausibly start dropping discovery notes in Notion someday
  (the research step's MCP server already exposes `search_notion`/
  `read_notion_page` for exactly that, symmetric with Drive/Docs).
- **Multi-account (Dagster + Prefect) ingestion is deferred.** Every
  ingestion leg here is parameterized by its own credential env vars, so a
  second (e.g. Prefect-side) Google/Notion account is new `defs/` instances
  pointed at different credentials later -- not a redesign. Not built here;
  there's no FastMCP gateway on that side yet.

## Setup

```bash
cd quickstarts/prospect_recon
cp .env.example .env   # fill in real credentials -- see each var's comment
uv sync
set -a && source .env && set +a
dg dev
```

Materialize via the UI (`prospect_recon` asset group), or:

```bash
dg launch --select "prospect_recon_candidate+"
```

To close the receipt loop after a real sales call:

```bash
python3 -c "
from dagster_community_components.resources.context_receipt_store.component import ContextReceiptStore
store = ContextReceiptStore('./data/prospect-recon-receipts.sqlite3')
store.record_outcome('<receipt_id from the run's logs>', signal='thumbs_up')
"
```

## Asset graph

```
calendar_raw ─────────────┐
drive_recent_raw ─────────┼──▶ prospect_recon_candidate (request-file/ledger precedence,
drive_search_raw ─────────┘      then Calendar+Drive exclusion/dedup/tie-break)
                                           │
                                           ▼
                          prospect_recon_research (agentic tool_use_loop:
                                           │         Drive/Notion/web MCP search)
                                           ▼
                          prospect_recon_evidence_df (bridge)
                                           │
                                           ▼
                          prospect_recon_context_kb (chunk → redact → dedupe →
                                           │           classify → stamp → embed → index)
                                           ▼
                          prospect_recon_items_json (bridge)
                                           │
                                           ▼
                          prospect_recon_brief (forced-function-call extraction,
                                           │     full two-half doc, live template fetch)
                                           │
                                           ├──▶ mcp_servers/context_governance_server.py
                                           │      (FastMCP: write_receipt -- generation-
                                           │       time context-consumption receipt)
                                           ▼
                          prospect_recon_commit_brief (commit to dagster-demo-factory)
                                     │           │              │
                                     ▼           ▼              ▼
                        prospect_recon_gmail_draft  prospect_recon_slack_notify
                                     │
                                     ▼ (also, directly off prospect_recon_brief)
                        prospect_recon_notion_brief (secondary output)
```

Trigger: `prospect_recon_schedule`, cron `30 18 * * 1-5` (6:30pm America/New_York,
weekdays) -- matches the real routine's own trigger exactly.

## Tests

`python3 -m pytest tests -q -p no:cacheprovider -W ignore --import-mode=importlib`
-- mocks only the network boundary (`github`/`notion_client`/`litellm`/Google API
clients), same convention as every other component in this repo. The heaviest
coverage is on `prospect_recon_candidate`'s selection logic (one test per
exclusion rule, the dangling-ledger fallback, request-file precedence, and the
full tie-break ordering) since that's where a subtle bug would silently pick
the wrong prospect.
