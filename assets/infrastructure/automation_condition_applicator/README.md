# Automation Condition Applicator

Apply Dagster `AutomationCondition`s **broadly** to many assets via selection rules — with **fall-through priority** (narrow rules override broad ones) and **preserve-existing semantics** (per-asset settings always win).

Solves the customer problem: *"I have 200 assets — 150 should run daily, 30 hourly, 5 are special. I don't want to set `automation_condition:` on every defs.yaml. Can I declare the rules in one place?"*

## Three rule shapes

### 1. Explicit cron — `on_cron(...)`

```yaml
- selection: "tag:cadence=hourly"
  cron: "0 * * * *"
  ignore_selection: "tag:cadence=daily"   # optional .ignore() filter
  allow_selection: "tag:cadence=hourly"   # optional .allow() filter
```

`ignore_selection` + `allow_selection` are the keys to the **"3 daily upstreams + 1 monthly upstream"** pattern:

```yaml
- selection: "group:revenue_marts"
  cron: "0 9 * * *"           # daily at 9am
  ignore_selection: "tag:cadence=monthly"   # don't block on the monthly upstream

- selection: "group:revenue_marts"
  cron: "0 9 1 * *"           # 9am on the 1st (month boundary)
  allow_selection: "tag:cadence=monthly"    # only THIS rule blocks on monthly
```

Both rules apply to the same assets — they AND together via Dagster's built-in composition.

### 2. Preset — named factory methods

```yaml
- selection: "*"
  preset: eager   # or "any_downstream_conditions", "on_missing", etc.
```

Any zero-arg method on `dg.AutomationCondition` works.

### 3. Derive from upstreams — automatic

```yaml
- selection: "group:silver"
  derive_from_upstreams: true
  strategy: most_frequent     # or 'least_frequent'
```

The component walks each matching asset's deps, inspects their cron schedules (from `automation_condition.cron_schedule` or `freshness_policy.cron_schedule`), and generates:

| Strategy | Behavior | Result for {3 daily + 1 monthly} upstreams |
|---|---|---|
| `most_frequent` | Fire on the fastest upstream's cron; ignore slower deps | `on_cron("0 9 * * *").ignore(<monthly_asset>)` |
| `least_frequent` | Fire on the slowest upstream's cron; require all deps fresh | `on_cron("0 0 1 * *")` |

`most_frequent` is the typical "keep up with the fast deps; let slow deps trail" pattern.

## Fall-through semantics — like CSS specificity

Rules are evaluated **top to bottom**. The first rule whose `selection` matches an asset wins — narrow rules listed first, broad catch-alls last:

```yaml
rules:
  - selection: "tag:cadence=hourly"      # narrow — wins for hourly-tagged assets
    cron: "0 * * * *"

  - selection: "group:silver"             # narrower than * — wins for silver assets not already hourly
    derive_from_upstreams: true

  - selection: "*"                         # broad catch-all — wins only if nothing above matched
    preset: eager
```

`preserve_existing: true` (default) acts like CSS `!important` on per-asset settings: if an asset already has `automation_condition` set explicitly (e.g. by its own `defs.yaml`'s `automation_condition:` field), no rule touches it. **Per-asset always wins.**

## Wiring in `definitions.py`

Components in Dagster can't post-process other components' output via `build_defs` alone — you need to invoke this applicator at the project level. Two ways:

### Helper (auto-discover all applicator components in defs/)

```python
from pathlib import Path
from dagster import definitions, load_from_defs_folder
from dagster_community_components.helpers.automation_applicator import (
    apply_automation_conditions_from_defs_folder,
)

@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return apply_automation_conditions_from_defs_folder(
        base,
        Path(__file__).parent,
    )
```

### Direct Python (skip YAML entirely)

```python
from pathlib import Path
from dagster import definitions, load_from_defs_folder
from dagster_community_components import apply_rules

@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return apply_rules(
        base,
        rules=[
            {"selection": "tag:cadence=hourly", "cron": "0 * * * *"},
            {"selection": "*", "preset": "eager"},
        ],
    )
```

Both produce the same result. Use YAML if you want a customer-editable config; use Python if you want full flexibility.

## Selection language

`selection` accepts any string parseable by `dg.AssetSelection.from_string()`:

| Selection | Matches |
|---|---|
| `"*"` or `"all"` | All assets |
| `"tag:cadence=hourly"` | Assets with tag `cadence=hourly` |
| `"group:silver"` | Assets in group `silver` |
| `"key:my_asset"` | Single asset |
| `"+my_asset"` | `my_asset` + everything downstream |
| `"my_asset+"` | `my_asset` + everything upstream |
| `"tag:tier=gold and group:facts"` | Boolean intersection |

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `preserve_existing` | `bool` | no | Default `true` — per-asset `automation_condition` wins |
| `rules` | `list[dict]` | yes | Ordered; first match wins |

Each rule:

| Key | Required | Notes |
|---|---|---|
| `name` | no | Human label (logged on apply) |
| `selection` | yes | AssetSelection string |
| `cron` | one-of | `"0 9 * * *"` → `on_cron(cron)` |
| `preset` | one-of | `"eager"` / `"any_downstream_conditions"` / etc. |
| `derive_from_upstreams` | one-of | `true` → auto from deps |
| `strategy` | for derive | `"most_frequent"` (default) / `"least_frequent"` |
| `ignore_selection` | for cron | AssetSelection — passed to `.ignore()` |
| `allow_selection` | for cron | AssetSelection — passed to `.allow()` |

## Validation

After wiring, run:

```bash
dg list defs --automation
```

To see which automation_condition resolved on each asset. Mismatches = your rules' order is wrong (fall-through hit too broadly).

## See also

- [`asset_job`](https://dagster-community-components-cli.vercel.app/c/asset_job) — bundle assets into a stable job
- [`cron_schedule`](https://dagster-community-components-cli.vercel.app/c/cron_schedule) — schedule a job at a fixed cadence (alternative to AutomationCondition)
- [Dagster AutomationCondition docs](https://docs.dagster.io/concepts/automation/declarative-automation)
- Walkthrough: [`automation_condition_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/automation_condition_pipeline.md)
