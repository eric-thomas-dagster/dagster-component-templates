# RPAOutputParserComponent

Normalize the vendor-specific `output_payload` metadata that the DCC RPA integration components emit into one flat DataFrame shape. Downstream `extract` / `classify` / `AgenticPipeline` steps consume a single shape regardless of which bot vendor produced the raw data.

Every RPA integration in the DCC (`uipath_orchestrator_integration`, `automation_anywhere_integration`, `blue_prism_integration`, `power_automate_integration`) emits an `output_payload` metadata dict on each materialization. This component reads those payloads from the event log and produces a single normalized DataFrame asset that any downstream step can consume without vendor branching.

## The vendor mess this solves

| Vendor | Native output field | Native ID field | Native status |
|---|---|---|---|
| UiPath Orchestrator | `OutputArguments` | `Job.Key` / `Job.Id` | `Successful` / `Faulted` |
| Automation Anywhere | `bot_output` | `executionId` | `COMPLETED` / `FAILED` |
| Blue Prism | `session.output` | `sessionId` | `Completed` / `Failed` |
| Power Automate | `run.trigger.outputs.body` | `runName` | `Succeeded` / `Failed` |

Downstream code that wants to extract structured fields would otherwise have to switch on vendor. This component collapses the four shapes into:

```
vendor  run_id  status  asset_key  materialized_at  output_field  raw_payload
```

## Integration pattern

```
uipath_extract_standard_pdf ──┐
aa_ocr_scanned_image ─────────┤
bp_edi_parser ────────────────┼──► rpa_output_parser ──► extract (JSON schema) ──► llm_evaluator ──► downstream
pa_email_extract ─────────────┘

Each upstream asset emits `output_payload` metadata on its materialization.
rpa_output_parser reads all upstream latest materializations, normalizes,
returns a list-of-dicts asset with one row per upstream.
```

## Example YAML

```yaml
type: dagster_community_components.RPAOutputParserComponent

attributes:
  asset_name: invoice_output_normalized
  upstream_asset_keys:
    - uipath_extract_standard_pdf
    - aa_ocr_scanned_image
    - bp_edi_parser
    - pa_email_extract
  fail_on_missing_upstream: false
```

## Output row shape

Each row in the resulting list-of-dicts asset has:

| Field | Type | Notes |
|---|---|---|
| `vendor` | `str` | `uipath` / `automation_anywhere` / `blue_prism` / `power_automate` / `unknown` |
| `run_id` | `str \| None` | Vendor-native run identifier (Job.Id / executionId / sessionId / runName) |
| `status` | `str \| None` | Vendor-native terminal status |
| `asset_key` | `str` | Upstream asset key that produced this row |
| `materialized_at` | `float \| None` | Unix timestamp of the upstream materialization |
| `output_field` | `Any` | Vendor's native output-carrying value (auto-picked from the vendor's known field name) |
| `raw_payload` | `dict \| None` | Full `output_payload` metadata dict for downstream custom parsing |

If an upstream has no materialization yet:
- `fail_on_missing_upstream: true` → raises `dg.Failure`
- `fail_on_missing_upstream: false` (default) → emits a row with `status='MISSING'`

## Downstream — the intended composition

```yaml
# Downstream extract step (from AgenticPipelineComponent) consumes the normalized asset:
type: dagster_community_components.AgenticPipelineComponent
attributes:
  source: {from: invoice_output_normalized}
  steps:
    - id: extract_fields
      op: extract
      output_schema:
        type: object
        properties:
          invoice_number: {type: string}
          vendor_name: {type: string}
          total_amount: {type: number}
          due_date: {type: string, format: date}
    - id: score
      op: llm_call
      # ... use llm_evaluator for real scoring
```

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Name of the resulting normalized DataFrame asset. |
| `upstream_asset_keys` | `List[str]` | Slash-separated asset keys of upstream RPA assets. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"rpa_normalized"` | Dagster asset group. |
| `kinds` | `List[str]` | `["python", "rpa", "normalized"]` | Asset kinds to attach. |
| `description` | `str` | `""` | Prose description shown in the Dagster UI. |

### Behavior

| Field | Type | Default | Description |
|---|---|---|---|
| `fail_on_missing_upstream` | `bool` | `false` | Raise on missing upstream vs. emit `status='MISSING'` row. |

## Requirements

```
dagster
```

## See also

- [`uipath_orchestrator_integration`](../../../integrations/uipath_orchestrator_integration/README.md)
- [`automation_anywhere_integration`](../../../integrations/automation_anywhere_integration/README.md)
- [`blue_prism_integration`](../../../integrations/blue_prism_integration/README.md)
- [`power_automate_integration`](../../../integrations/power_automate_integration/README.md)
- Full end-to-end walkthrough: [`rpa_meets_ai.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/rpa_meets_ai.md)
