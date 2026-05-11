# FHIR Resource Normalizer

Flatten FHIR R4/R5 JSON resources (`Patient`, `Observation`, `Encounter`, `Condition`, `MedicationRequest`, plus a generic fallback) into a flat pandas DataFrame ready for warehouse loads or analytics.

```yaml
type: dagster_component_templates.FhirResourceNormalizerComponent
attributes:
  asset_name: patients_flat
  upstream_asset_key: fhir_resources
  resource_column: resource
  resource_types: [Patient]
```

## Inspired by `hris_normalizer`

Same pattern: messy vendor JSON → canonical flat schema. Reuses `value_maps` for per-column normalization (case-insensitive by default).

```yaml
value_maps:
  gender:
    M: male       # FHIR sometimes encodes single-letter codes
    F: female
    male: male    # canonical pass-through
    female: female
```

## Resource-specific extractors

Each resource type gets a hand-tuned extractor with the most common useful fields:

| Resource | Extracted columns |
|---|---|
| `Patient` | `id`, `first_name`, `last_name`, `gender`, `birth_date`, `deceased`, `city`, `state`, `country`, `postal_code` |
| `Observation` | `id`, `patient_id`, `status`, `code_system`, `code`, `display`, `effective_dt`, `value`, `unit` |
| `Encounter` | `id`, `patient_id`, `status`, `class_code`, `class_display`, `start`, `end`, `reason_text` |
| `Condition` | `id`, `patient_id`, `code_system`, `code`, `display`, `clinical_status`, `onset_dt`, `recorded_dt` |
| `MedicationRequest` | `id`, `patient_id`, `status`, `intent`, `med_system`, `med_code`, `med_display`, `authored_on`, `dosage_text` |
| Other | Generic fallback: `resource_type`, `id`, `status`, `patient_id` |

## Input shape

The `resource_column` should hold either:
- **Parsed dicts** (most common when upstream is `synthetic_data_generator` `fhir_patients` or a JSON-API source)
- **JSON strings** (auto-parsed)

Anything else is recorded as an error.

## Filtering by type

A Bundle typically contains many resource types. To process them in separate pipelines, run multiple instances of this component with different `resource_types: [Patient]`, `[Observation]`, etc.

## Carry-over columns

Any non-resource column on the upstream row is carried into the output (e.g. `ingested_at`, `source_system`, `bundle_id`). The extractor's output takes precedence on collisions.

## Sister components

- `hris_normalizer` — same pattern for HR data
- `hl7_v2_parser` — older healthcare standard (pipe-delimited)
- `synthetic_data_generator` `fhir_patients` schema — common upstream for demos
- `dataframe_to_bigquery` / `dataframe_to_gcs` — common downstream sink
