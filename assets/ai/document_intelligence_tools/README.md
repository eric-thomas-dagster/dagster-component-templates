# `DocumentIntelligenceToolsComponent`

The missing building block for an "Agent Diligence Review" recipe: an MCP tool server an agent can call to
extract fields from ONE document, or ask for a schema suggestion -- rather than only having full
batch-pipeline assets available.

## Why this exists

Of a 13-item document-intelligence recipe set, most (bank statement -> JSON, annual report split->schema,
financial tables, chunking for RAG, LangChain RAG ingestion, vector metadata contract, S3 storage pipeline,
batch document intake, production webhooks) have a concrete building block already in this registry, but an
earlier gap audit found "Agent Diligence Review" had none: [`structured_document_extractor`](../structured_document_extractor)
only runs as a batch asset over a whole upstream DataFrame -- there was no single-document, callable entry
point an agent could reach for interactively (deciding what to look at next, one document at a time). A
follow-up re-check found two other items on that same list -- "spreadsheet financial review" and "footnote
citation review" -- had also been marked covered incorrectly; both remain open gaps (xlsx support elsewhere
in this repo is a generic flatten-to-markdown conversion with no cell-level fidelity, and no component does
footnote detection or reference-linking).

| Tool | What it answers |
|---|---|
| `extract(file_ref, output_fields, doc_label?, model?)` | Extract the named fields from ONE document and return them as JSON |
| `suggest_schema(sample_file_ref, doc_label?, model?, max_fields?)` | Given ONE sample document, propose a list of field names (+ one-line description each) worth extracting from documents like it |

Both tools are read-only / side-effect-free.

## `extract` is the same extraction core as `structured_document_extractor`, copied not reimplemented

`extract` is **not** new extraction logic. It is the same extraction core `structured_document_extractor`'s
own `extract_one` uses (which itself calls `build_extraction_messages`/`parse_extraction_response`), copied
into this file byte-faithfully per this repo's self-contained-component convention (no cross-component
imports -- every component is fully self-contained). The only things dropped going from
`structured_document_extractor` to this single-document tool are config surface that component's batch-asset
shape needs and a one-off tool call does not: `field_regions` (per-field bounding-box crops) and
`_crop_region_blocks` (the helper that builds them) are gone entirely, along with `batch_size`/
partitioning/`upstream_asset_key`/`path`-listing -- this component always processes exactly one document per
call. The actual LLM-call/parse logic (prompt construction, JSON-fence-stripping, field-shaping, the litellm
call itself) is unmodified.

Same file-reading behavior as `structured_document_extractor`'s `input_type: file` mode: images
(`.png`/`.jpg`/`.jpeg`/`.gif`/`.webp`/`.bmp`/`.tiff`) are base64-encoded into a vision content block; PDFs are
read via `pdfplumber` (a scanned/photographed PDF with no embedded text raises rather than silently sending
garbage); anything else is read as plain text.

## `suggest_schema` is new logic

No existing function covers this -- `suggest_schema` asks the LLM, via the same `litellm` library `extract`
already uses, to propose a JSON array of `{"field": "...", "description": "..."}` objects worth extracting
from documents like the one sample given. It reuses the same file-reading and
```json ...``` fence-stripping convention `extract`'s own `parse_extraction_response` already established. An
unparseable or wrongly-shaped reply raises a clear `ValueError` -- it never silently returns garbage.

## Extraction-quality controls (toolset-level, opt-in)

`additional_prompt`, `detect_selections`, and `refine` configure `extract`'s shared extraction core (same
behavior as the matching fields on `structured_document_extractor`), set once on the component/toolset and
applied to every `extract` call it serves. All default off/unset, so `extract` behaves exactly as before
unless you turn one on.

```yaml
type: dagster_community_components.DocumentIntelligenceToolsComponent
attributes:
  default_model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
  additional_prompt: "this is a redacted legal filing"
  detect_selections: true   # checkbox/radio/handwritten-mark detection -- image input only
  refine: true               # one corrective second pass
  refine_model: gpt-4o       # optional -- defaults to the call's own model for both passes
```

- **`additional_prompt`** -- extra domain/document context appended to the prompt. Steering, not a hard
  constraint.
- **`detect_selections`** -- adds an extra `selection_marks` key to `extract`'s `fields` result:
  `[{"label": "...", "checked": true/false}]`. A VISUAL signal -- only runs when `file_ref` resolves to an
  image; a text-layer PDF or raw text gets `selection_marks: None` (not attempted, logged as a warning), not
  a faked empty list.
- **`refine`** -- after the first-pass extraction succeeds, makes ONE additional LLM call that reviews it
  against the SAME original content (reused, not re-read) for OCR/table-structure/number-formatting errors
  and returns the corrected result instead. A single corrective pass, not a multi-mode (tables/text/
  formatting/layout) review pipeline -- `refine_model` can pick a different model for just this second call.
  Doubles the LLM calls (and cost/latency) per `extract` call.

## Deliberately NOT built here

Each of these is its own recipe gap, tracked separately -- do not expect them from this component:

- A "split" tool (page/section splitting for "Annual Report Split->Schema")
- A "tables" tool claiming real financial-table structure extraction
- A "get_citations" tool -- blocked on unbuilt citation/word-coordinate infrastructure, not faked here

## Auth

`api_key_env_var` (default `OPENAI_API_KEY`) names the environment variable holding the API key -- same
convention as `structured_document_extractor`'s own `api_key_env_var`. `api_base_env_var` optionally names
an environment variable holding an API base URL, for a self-hosted/local OpenAI-compatible model (Ollama, LM
Studio, vLLM...), e.g. `OLLAMA_API_BASE=http://localhost:11434` with `model: ollama/llava`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"document_intelligence_tools"` | Resource key the toolset is registered under. |
| `api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Environment variable name holding the API key. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `default_model` | `str` | `"gpt-4o"` | Fallback LLM model (litellm format) used when a tool call doesn't specify its own `model`. |
| `api_base_env_var` | `str` | — | Optional environment variable holding an API base URL, for a self-hosted or local OpenAI-compatible model (Ollama, LM Studio, vLLM...), e.g. OLLAMA_API_BASE=http://localhost:11434 with model 'ollama/llava'. Unset = the p… _(full docs in schema.json + component README)_ |
| `llm_max_retries` | `int` | `2` | Retry a tool call's LLM call up to this many times on transient errors (rate limits, timeouts) -- forwarded to litellm's own num_retries. |
| `max_content_chars` | `int` | `20000` | Truncate extracted document text to this many characters before prompting the LLM -- guards against blowing the model's context window or racking up cost on unusually large text-PDF/text-input documents. Doesn't apply to… _(full docs in schema.json + component README)_ |
| `allow` | `List[str]` | — | `f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."` |
| `max_result_chars` | `int` | `12000` | Upper bound on the serialized size of any tool result. |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this module, with DOCUMENT_INTELLIGENCE_TOOLS_* env vars set from this component's own config). Any mcp_servers-aware component can then… _(full docs in schema.json + component README)_ |
| `mcp_extra_env_vars` | `List[str]` | — | mcp_server_resource_key only. Extra env var NAMES to copy into the spawned MCP server's env. |
| `review_queue_approval_dir` | `str` | — | Approval directory used by AgentReviewQueueComponent (the SAME `approval_dir` value your review-queue asset scans). When set, `extract` writes a pending review record to `<review_queue_approval_dir>/pending/<key>.json` w… _(full docs in schema.json + component README)_ |
| `low_confidence_null_rate_threshold` | `float` | `0.3` | Only used when `review_queue_approval_dir` is set. Fraction (0-1) of requested `output_fields` that came back null/empty above which `extract` queues the result for review. There is no real confidence score anywhere in t… _(full docs in schema.json + component README)_ |
| `additional_prompt` | `str` | — | Extra domain/document context appended to every `extract` call's prompt, e.g. 'preserve signed amounts' or 'this is a redacted legal filing'. Steering, not a hard constraint -- the model can still omit a field it genuine… _(full docs in schema.json + component README)_ |
| `detect_selections` | `bool` | `false` | `extract` also asks the model to identify checkboxes, radio buttons, and handwritten selection marks, returned as an extra `selection_marks` key on the result's `fields`: [{'label': str, 'checked': bool}]. Selection-mark… _(full docs in schema.json + component README)_ |
| `refine` | `bool` | `false` | After `extract`'s first-pass extraction succeeds, make ONE additional LLM call that reviews the first pass against the SAME original content (reused, not re-read) for OCR/text-reading mistakes, wrong table structure, and… _(full docs in schema.json + component README)_ |
| `refine_model` | `str` | — | Model used for the refine pass only (litellm format). Unset = reuse the call's own model for both passes. Only used when refine=true. |

[//]: # (FIELDS:END)

## MCP server

Any `mcp_servers`-aware component (`mcp_tool_call`, `mcp_tool_picker`, the agent components, ...) can
reference this toolset via `server_resource_key` once `mcp_server_resource_key` is set:

```yaml
type: dagster_community_components.DocumentIntelligenceToolsComponent
attributes:
  default_model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
  mcp_server_resource_key: document_intelligence_tools_mcp
```

Or run it standalone for a local agent / Claude Code:
`python -m dagster_community_components.assets.ai.document_intelligence_tools.component`, with
`DOCUMENT_INTELLIGENCE_TOOLS_DEFAULT_MODEL` and `DOCUMENT_INTELLIGENCE_TOOLS_API_KEY_ENV_VAR` (and the env
var it names) set in the environment.

## See also

- [`structured_document_extractor`](../structured_document_extractor) -- the batch-asset extraction this
  component's `extract` tool shares its real extraction core with
- [`dagster_evidence_tools`](../dagster_evidence_tools) -- this component's closest-shaped sibling: a
  stateless, read-only MCP tool server with no practice_mode and no outcome ledger
