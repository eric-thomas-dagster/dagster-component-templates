# Structured Document Extractor

Extract structured fields from any document type using an LLM — one component, `document_type` picks the preset instead of picking a whole separate component per document type.

## Why this exists

Diffed directly against the source: `invoice_extractor`, `receipt_extractor`, `bank_statement_extractor`, `expense_report_extractor`, `purchase_order_extractor`, `shipping_label_extractor`, `contract_extractor`, `legal_document_extractor`, `insurance_claim_extractor`, `medical_record_extractor`, `resume_extractor`, `job_posting_extractor`, and `scientific_paper_extractor` are the same component — identical `upstream_asset_key` / `input_column` / `model` / `api_key_env_var` / `output_fields` / `batch_size` fields, identical LLM-call logic. The only real difference between any two of them is the *default value* of `output_fields` and a docstring. Those 13 components are kept for backward compatibility (existing YAML referencing them keeps working), but new usage should prefer this one.

## Two input modes

Exactly one of `upstream_asset_key` or `path` -- mirrors `file_ingestion`'s own `file_path` / `from_upstream` split:

```yaml
# 1. Read from an existing asset (e.g. file_lister, or anything else
#    that already produces a DataFrame of document content/paths)
type: dagster_component_templates.StructuredDocumentExtractorComponent
attributes:
  asset_name: invoice_fields
  upstream_asset_key: incoming_invoices
  document_type: invoice
  input_column: local_path
  input_type: file
  model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
```

```yaml
# 2. List documents itself, directly -- no separate file_lister asset
#    needed. Use this for a one-off extraction over a bucket/folder;
#    use `upstream_asset_key` (mode 1) when you want a shared, reusable
#    listing asset multiple extractors read from.
type: dagster_component_templates.StructuredDocumentExtractorComponent
attributes:
  asset_name: invoice_fields
  path: s3://my-bucket/invoices/**/*.pdf   # or gs://, abfss://, or a local path
  document_type: invoice
  model: gpt-4o
  api_key_env_var: OPENAI_API_KEY
  # download: true                         # default -- caches files locally first
  # download_dir: /data/cache/invoice_fields
  # max_files: 500                         # safety cap for a broad glob
```

Switch document types by changing one field — no new component to find or install:

```yaml
document_type: resume
document_type: medical_record
document_type: scientific_paper
```

## Presets

| `document_type` | Default `output_fields` |
|---|---|
| `invoice` | invoice_number, date, vendor, total_amount, line_items, tax, currency |
| `receipt` | merchant_name, merchant_address, date, time, items, subtotal, tax, total, payment_method, card_last_four, receipt_number |
| `bank_statement` | account_number, account_holder, bank_name, statement_period, opening_balance, closing_balance, transactions, total_credits, total_debits |
| `expense_report` | employee_name, employee_id, department, report_date, period_start, period_end, line_items, total_amount, currency, approver, status |
| `purchase_order` | po_number, vendor, buyer, issue_date, delivery_date, line_items, subtotal, tax, total, payment_terms, shipping_address, billing_address |
| `shipping_label` | tracking_number, carrier, service_type, sender_name, sender_address, recipient_name, recipient_address, weight, dimensions, ship_date, estimated_delivery |
| `contract` | contract_type, parties, effective_date, expiration_date, governing_law, payment_terms, termination_clause, liability_cap, signatures |
| `legal_document` | document_type, jurisdiction, court, case_number, parties, filing_date, key_dates, relief_sought, defined_terms, obligations, penalties |
| `insurance_claim` | claim_id, policy_number, insurer, claimant_name, claimant_contact, incident_date, incident_description, damage_type, claimed_amount, adjuster, status |
| `medical_record` | patient_name, dob, provider, visit_date, chief_complaint, diagnoses, icd_codes, medications, procedures, cpt_codes, follow_up |
| `resume` | name, email, phone, location, summary, skills, experience, education, certifications, languages |
| `job_posting` | job_title, company, location, remote_policy, employment_type, salary_range, required_skills, preferred_skills, experience_required, education_required, responsibilities, benefits, application_deadline |
| `scientific_paper` | title, authors, journal, publication_date, doi, abstract, keywords, methodology, key_findings, limitations, citations_count, data_availability |
| `custom` | none — you must set `output_fields` yourself |

Setting `output_fields` explicitly always overrides the preset, even when `document_type` is one of the presets above — start from a preset and tweak it:

```yaml
document_type: invoice
output_fields: [invoice_number, vendor, total_amount, po_reference]  # your own list, not the invoice preset
```

## Pairs with

`file_lister` — when you want a SHARED, reusable listing asset multiple extractors read from: install it, then point `upstream_asset_key` at it and `input_column: local_path`. For a one-off extraction over a bucket/folder, `path` mode above skips the separate asset entirely.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Output Dagster asset name |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `api_key_env_var` | `str` | `"OPENAI_API_KEY"` | Environment variable name holding the API key |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `batch_size` | `int` | `5` | Number of documents per LLM batch |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Dagster asset group name |
| `owners` | `List[str]` | — | Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com'] |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'} |
| `kinds` | `List[str]` | — | Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']} |
| `description` | `str` | — | Asset description shown in the Dagster catalog. |
| `deps` | `List[str]` | — | Lineage-only upstream asset keys (no data passed at runtime). |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned |
| `partition_start` | `str` | — | Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types. |
| `partition_date_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current date partition key. |
| `partition_dimensions` | `List[Dict[str, Any]]` | — | Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'. |
| `partition_static_column` | `Union[str, int]` | — | Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id'). |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on asset failure. Defines a RetryPolicy. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `input_column` | `Union[str, int]` | `"local_path"` | Column with document content (text or file path) -- 'local_path' pairs directly with file_lister's output. |
| `input_type` | `str` | `"file"` | Input type: 'text' (raw document text already in the column) or 'file' (read the file at that path). |
| `model` | `str` | `"gpt-4o"` | LLM model name (litellm format) |
| `output_fields` | `List[str]` | — | Fields to extract from each document. Overrides the document_type preset when set. Required when document_type='custom'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `upstream_asset_key` | `str` | — | Upstream asset key providing a DataFrame with document content. Mutually exclusive with `path` -- set exactly one. Use this when another asset (e.g. file_lister) already lists/produces the documents; use `path` when this… _(full docs in schema.json + component README)_ |
| `path` | `str` | — | Glob pattern or fsspec URI to list documents from directly (s3://, gs://, abfss:// / abfs:// / az://, or a local path), e.g. 's3://my-bucket/invoices/**/*.pdf'. Mutually exclusive with `upstream_asset_key` -- set exactly… _(full docs in schema.json + component README)_ |
| `download` | `bool` | `true` | When using `path`: download each matched file to a local cache directory first. Ignored when using `upstream_asset_key`. |
| `download_dir` | `str` | — | When using `path` with download=true: local cache directory. Auto-generated under the system temp dir if unset. |
| `max_files` | `int` | — | When using `path`: safety cap on how many matched files to process in one materialize. |
| `document_type` | `str` | `"custom"` | `'Picks a default output_fields preset: ' + ', '.join(sorted(_PRESET_FIELDS.keys())) + ", or 'custom' (requires output_fields to be set explicitly)."` |
| `dynamic_partition_name` | `str` | — | Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'. |
| `include_preview_metadata` | `bool` | `false` | Include a preview of the output data in metadata (first 5 rows as a markdown table). Used by builder UIs to render asset shape without warehouse access. |
| `preview_rows` | `int` | `25` | Rows to include in the preview metadata when include_preview_metadata is True. |

[//]: # (FIELDS:END)

## Requirements

`litellm` is a hard dependency. Works with any litellm-compatible model (OpenAI, Anthropic, etc.) — set `model` + `api_key_env_var` to match.

## Validation

`validation.level: code` — verified end-to-end against a real Dagster materialization (with litellm's `completion` call mocked to avoid real API spend during testing), both input modes: `upstream_asset_key` mode (file_lister → this component) and `path` mode (this component listing files directly, no separate asset). Preset resolution, the `custom` + no `output_fields` error path, the unknown-`document_type` error path, and the both-modes/neither-mode-set error path all behave as documented. Has NOT been run against a real LLM API call yet — live-test that before trusting it in production, and flip `validation.level` to `live` once confirmed.
