# Validation Metadata Convention

## Consumers

Today, exactly one consumer reads this field:

- The catalog site at <https://dagster-component-ui.vercel.app/> renders
  a "Trust & feedback" section per component driven by
  `manifest.json[].validation.level`.

Other tools that install / plan / recommend components from this
manifest (e.g. `dagster-community-components-cli`, Dagster Designer's
AI planner) **do not currently read this field**. So `validation:` is
a signal for humans browsing the catalog, not a runtime gate. If you
want it to gate installation, add a filter to your installer of choice.

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

- **949 components total** in the manifest.
- **613 (65%) have `validation: live`** — validated end-to-end against a
  live system or via a worked walkthrough demo.
- **336 (35%) have `validation: code`** — compile-validated to the SDK
  spec but not yet run against a live backend.
- **0 currently unverified** — every component has some validation level
  set. To gate strictly on the `live` bar, filter on
  `validation.level == "live"`.
- Regenerate these counts with:
  ```bash
  python3 -c "import json; d=json.load(open('manifest.json'));
  from collections import Counter;
  print(Counter((c.get('validation') or {}).get('level','unverified')
                for c in d['components']))"
  ```
