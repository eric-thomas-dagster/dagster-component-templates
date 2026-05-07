# Validation Metadata Convention

The web UI at <https://dagster-component-ui.vercel.app/> shows a
"Trust & feedback" section on every component page. By default it
renders "Unverified" because there's no validation signal in the
manifest. This file documents the convention components can use to
opt in.

## Manifest field

Add a `validation` block to the component's manifest entry:

```json
{
  "id": "my_component",
  "validation": {
    "level": "live",
    "evidence": "https://github.com/.../examples/my_demo.md",
    "last_validated": "2026-05-06"
  }
}
```

## Levels

| Level | Meaning |
|---|---|
| `code` | Compile-validated. Code follows the SDK/spec but never run end-to-end. |
| `infra` | Validated against the right *kind* of infrastructure (e.g. spinning up a local container, mocking a service). Component does what it says, but not against the live external system. |
| `live` | Validated end-to-end against a live external system. The gold standard — there's a specific demo / walkthrough that runs the component against the real backend and gets the expected result. |

When you're building a component, default to omitting `validation`
(unverified). Only set it when you genuinely meet the bar.

## Evidence URL

Point at a walkthrough doc (`examples/<demo>.md`) or a README section
that demonstrates validation. The web UI will render this as a clickable
link in the trust section.

## Examples

```json
// Validated end-to-end against a live Azure subscription
{
  "id": "azure_data_factory",
  "validation": {
    "level": "live",
    "evidence": "https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/azure_data_factory.md",
    "last_validated": "2026-05-06"
  }
}

// Validated against synthetic data in a local demo
{
  "id": "scd_type_2",
  "validation": {
    "level": "live",
    "evidence": "https://raw.githubusercontent.com/eric-thomas-dagster/dagster-community-components-cli/main/examples/scd_type_2.md",
    "last_validated": "2026-05-06"
  }
}

// Code-validated only — built from the API spec, not yet run
{
  "id": "azure_openai_resource",
  "validation": {
    "level": "code",
    "evidence": "https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/resources/azure_openai_resource/README.md",
    "last_validated": "2026-05-06"
  }
}
```

## Web UI rendering (separate work)

The Vercel-deployed UI repo needs to read this field and render
something useful — e.g.:
- Green "✓ Validated live (2026-05-06)" badge with link to evidence
- Yellow "Validated against infrastructure" badge
- Gray "Code-validated" badge with caveat
- Default "Unverified" when the field is missing

That repo update is separate from this convention. This file documents
the producer side.

## Counts at last update

- 132 / 653 components have `validation: live` (validated end-to-end
  against a live system or via a worked walkthrough demo).
- The remaining are mostly code-validated; opt them in by validating
  + adding the manifest block.
