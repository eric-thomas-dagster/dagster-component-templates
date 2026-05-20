# Automation Condition Applicator

Apply Dagster `AutomationCondition`s **broadly** to many assets at once via declarative rules — without editing every `defs.yaml`. Fall-through priority (narrow > broad), preserve-existing semantics (per-asset > rules), automatic cadence derivation from upstream lineage, and a Python escape hatch for anything YAML can't express.

**Solves**: *"I have 200 assets — 150 should run daily, 30 hourly, 5 are special. I don't want to set `automation_condition:` on every defs.yaml."*

---

## Table of contents

- [Quick start](#quick-start)
- [Mental model — how it runs](#mental-model--how-it-runs)
- [Rule shapes](#rule-shapes) — pick one per rule
  - [`cron`](#shape-1-explicit-cron) — explicit
  - [`preset`](#shape-2-preset) — named factory
  - [`derive_from_upstreams`](#shape-3-derive-from-upstreams) — 6 strategies
  - [`python`](#shape-4-python-escape-hatch) — universal escape
- [Modifiers](#modifiers) — compose with any shape
  - [`label`](#modifier-label)
  - [`business_hours_only`](#modifier-business_hours_only)
  - [`min_interval_minutes`](#modifier-min_interval_minutes)
- [Selection language](#selection-language) — comprehensive
- [Fall-through priority](#fall-through-priority)
- [Cadence propagation](#cadence-propagation-through-chains)
- [Wiring (`definitions.py`)](#wiring-definitionspy)
- [Customer scenarios](#customer-scenarios)
- [Validating what got applied](#validating-what-got-applied)
- [Trade-offs, gotchas, limitations](#trade-offs-gotchas-limitations)
- [Fields reference](#fields-reference)
- [See also](#see-also)

---

## Quick start

> ⚠ **Important — this component requires TWO pieces, not one**:
> 1. Your rules (defined in YAML or Python — your choice)
> 2. **A call to `apply_rules(...)` in your `definitions.py`** — this is mandatory regardless of where rules live, because Dagster components can't post-process other components' output. The applicator mutates the merged Definitions AFTER all components have built; that can only happen at the project level.
>
> Without the `definitions.py` wiring, your rules sit dormant and no automation_condition ever gets applied.

### Easiest path — rules in Python, single file

If you don't need YAML config, write rules inline in `definitions.py` — one file, done:

```python
# definitions.py
from pathlib import Path
from dagster import definitions, load_from_defs_folder
from dagster_community_components import apply_rules

@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return apply_rules(base, rules=[
        {"selection": "tag:cadence=hourly", "cron": "0 * * * *"},
        {"selection": "group:silver", "derive_from_upstreams": True, "strategy": "most_frequent"},
        {"selection": "*", "preset": "eager"},
    ])
```

### Alternative — rules in YAML, applied from Python

If you want customer-editable YAML config separate from `definitions.py`:

```yaml
# defs/automation_rules/defs.yaml
type: dagster_community_components.AutomationConditionApplicatorComponent
attributes:
  preserve_existing: true
  rules:
    - name: critical_hourly
      selection: "tag:cadence=hourly"
      cron: "0 * * * *"
    - name: derive_silver_from_upstreams
      selection: "group:silver"
      derive_from_upstreams: true
      strategy: most_frequent
    - name: catchall_eager
      selection: "*"
      preset: eager
```

```python
# definitions.py — STILL required, even with YAML rules
import yaml
from pathlib import Path
from dagster import definitions, load_from_defs_folder
from dagster_community_components import apply_rules

@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    with open(Path(__file__).parent / "defs/automation_rules/defs.yaml") as f:
        config = yaml.safe_load(f)
    return apply_rules(base, rules=config["attributes"]["rules"])
```

Either approach produces the same result. **The `apply_rules` call in `definitions.py` is non-negotiable** — without it, the rules don't take effect.

---

## Mental model — how it runs

```
[components load → each build_defs() contributes assets]
       ↓
[Dagster merges all components into one base Definitions]
       ↓                              ← apply_rules sees the complete picture
[your definitions.py calls apply_rules(base, rules=[...])]
       ↓
[for each asset in TOPOLOGICAL ORDER (roots first, leaves last):
   if preserve_existing AND asset has its own condition → skip
   else: walk rules top-to-bottom; first selection match wins
         → build condition (using upstreams' POST-RULE effective crons)
         → record effective cron for downstream propagation]
       ↓
[final Definitions returned with each asset's automation_condition set]
```

Three guarantees:

1. **Complete picture** — by the time `apply_rules` runs, all components in the merged `Definitions` are visible. (Dynamic assets registered at runtime are NOT — see [Limitations](#trade-offs-gotchas-limitations).)
2. **Topological order** — assets process roots-first. When applying rules to asset D (which depends on C), C's rule has already been evaluated, so D's `derive_from_upstreams` sees C's *effective* cron.
3. **First match wins per asset** — rules are tried top-to-bottom; one rule applies per asset.

---

## Rule shapes

Each rule has exactly ONE of: `cron`, `preset`, `derive_from_upstreams`, or `python`. Plus optional [modifiers](#modifiers).

### Shape 1 — Explicit cron

`on_cron(...)` with optional `.ignore()` / `.allow()` filters.

```yaml
- selection: "group:gold"
  cron: "0 9 * * *"                       # daily at 9am UTC
  ignore_selection: "tag:cadence=monthly" # don't block on these deps
  allow_selection: "tag:cadence=hourly"   # only block on these deps (if set)
```

`ignore_selection` + `allow_selection` are how you express the "daily-with-monthly-boundary" pattern with two rules ANDed:

```yaml
rules:
  - selection: "group:revenue_marts"
    cron: "0 9 * * *"
    ignore_selection: "tag:cadence=monthly"
  - selection: "group:revenue_marts"
    cron: "0 9 1 * *"
    allow_selection: "tag:cadence=monthly"
```

(Or use `strategy: tiered` to get this automatically — see below.)

### Shape 2 — Preset

Any zero-arg method on `dg.AutomationCondition`. Most common: `eager`, `on_missing`, `any_downstream_conditions`.

```yaml
- selection: "*"
  preset: eager
```

### Shape 3 — Derive from upstreams

Walks each matching asset's deps, inspects their cron schedules, and generates an appropriate `AutomationCondition`. Six strategies:

#### `strategy: most_frequent` (default)

Fire on the **fastest** upstream's cron; ignore slower deps via `.ignore()`. Use when slower deps are nice-to-have but shouldn't block.

```yaml
- selection: "group:silver"
  derive_from_upstreams: true
  strategy: most_frequent
```

Example: with deps `[daily_asset, monthly_asset]` →  
`on_cron("0 9 * * *").ignore(monthly_asset)` — fires daily without waiting for the monthly.

#### `strategy: least_frequent`

Fire on the **slowest** upstream's cron; require all deps fresh. Use when downstream MUST see every dep's update.

Example: with deps `[daily_asset, monthly_asset]` → `on_cron("0 0 1 * *")` — fires monthly only.

#### `strategy: tiered`

Fire on the **fastest** upstream's cron, BUT on each slower tier's boundary, ALSO wait for that tier. The "daily-with-monthly-boundary" pattern as a single ANDed condition.

```yaml
- selection: "group:silver"
  derive_from_upstreams: true
  strategy: tiered
```

For deps `[daily ×3, monthly]` produces:

```
in_latest_time_window
& cron_tick_passed("0 9 * * *").since_last_handled       ← drives firing
& all_deps_updated_since_cron("0 9 * * *").ignore(monthly_keys)  ← daily tier
& all_deps_updated_since_cron("0 9 1 * *").allow(monthly_keys)   ← monthly tier
```

**Mid-month**: daily clauses fire, monthly clause already satisfied. **Month boundary (Feb 1)**: daily clauses fire, monthly clause demands the monthly to have updated since Feb 1 → blocks until it lands, then both fire together. Generalizes to N tiers (hourly + daily + weekly + monthly all work).

#### `strategy: staggered`

Like `most_frequent` but shifts the derived cron by `offset_minutes`. Use for pipeline pacing (e.g. 1-hour buffer after upstream completion).

```yaml
- selection: "group:gold"
  derive_from_upstreams: true
  strategy: staggered
  offset_minutes: 60         # required; positive only
```

Example: upstream `0 6 * * *` + offset 60 → `0 7 * * *`. Rejects cross-day-boundary offsets and non-fixed-time crons with clear errors.

#### `strategy: any_dep_updated`

No cron — fire whenever **any** upstream updates since the last fire. Event-driven; ignores upstream cron metadata entirely.

```yaml
- selection: "tag:cadence=event_driven"
  derive_from_upstreams: true
  strategy: any_dep_updated
```

Generates: `in_latest_time_window & any_deps_updated().since_last_handled() & ~any_deps_in_progress()`.

#### `strategy: all_deps_updated`

No cron — wait until **all** upstreams have updated since last fire. Strict join semantics; useful for stitched analytics where partial results are wrong.

Generates: `in_latest_time_window & all_deps_match(newly_updated).since_last_handled() & ~any_deps_in_progress()`.

### Shape 4 — Python escape hatch

For anything YAML can't express, including the [`@automation_condition` decorator](https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/arbitrary-python-automation-conditions):

```yaml
- selection: "tag:custom_logic"
  python: "my_project.automation:my_complex_condition"
```

```python
# my_project/automation.py
import dagster as dg

def my_complex_condition() -> dg.AutomationCondition:
    return (
        dg.AutomationCondition.in_latest_time_window()
        & dg.AutomationCondition.cron_tick_passed("0 * * * *").since_last_handled()
        & dg.AutomationCondition.any_deps_updated()
    ).with_label("my_complex_condition")
```

The callable can take **zero args** (one condition for all matching assets) or **one arg** (the asset spec — for per-asset customization). Must return an `AutomationCondition`.

---

## Modifiers

Compose with any cron-producing shape. Multiple modifiers can apply to one rule.

### Modifier: `label`

Override the auto-generated condition label (which is descriptive but verbose like `"tiered_on_cron(0 9 * * *)"`):

```yaml
- selection: "group:gold"
  cron: "0 9 * * *"
  label: "gold_daily_morning"   # shows in UI + logs
```

### Modifier: `business_hours_only`

Constrain the derived cron to working hours / days. **Both fields follow cron syntax** — defaults are 9-17 Mon-Fri (US convention) but you can override for any region.

```yaml
- selection: "group:gold"
  derive_from_upstreams: true
  strategy: most_frequent
  business_hours_only: true
  business_hours: "9-17"        # default; cron hour field
  business_days: "1-5"          # default; cron day-of-week field
```

Override examples:

| Region / schedule | YAML |
|---|---|
| Retail 8am-6pm, Mon-Sat | `business_hours: "8-18"`, `business_days: "1-6"` |
| 24/7 operations | `business_hours: "0-23"`, `business_days: "*"` |
| Specific hours only | `business_hours: "9,12,15"` (9am, noon, 3pm only) |
| Weekends only | `business_days: "0,6"` (Sun + Sat) |

**Timezone caveat**: cron evaluates in the Dagster instance's timezone (usually UTC). For non-UTC hours: either run Dagster in your local TZ, adjust hour values to UTC manually, or drop to the `python:` escape hatch and use `zoneinfo` for timezone-aware logic.

**Constraints**:
- Refuses for `tiered` strategy (multi-cron — can't constrain one without breaking tier composition).
- Refuses for `any_dep_updated` / `all_deps_updated` (no cron to constrain).
- Use `python:` escape hatch if you need these combinations.

### Modifier: `min_interval_minutes`

Debounce floor — replace the derived cron with a coarser one if it's faster than the floor.

```yaml
- selection: "group:expensive_dashboards"
  derive_from_upstreams: true
  strategy: most_frequent
  min_interval_minutes: 60      # never fire more often than hourly
```

Example: upstream every 5 min + `min_interval_minutes: 60` → `0 * * * *` (hourly). Upstream daily + same floor → daily preserved (already slower than floor).

Picks sane round crons: 5/10/15/30 minutely, hourly multiples (1/2/3/4/6/8/12), daily, weekly, monthly. Requires a cron-producing strategy (refuses for `any_dep_updated` / `all_deps_updated`).

---

## Selection language

The `selection` field accepts any string parseable by `dg.AssetSelection.from_string()` — or an already-built `AssetSelection` Python object.

### By identity

```yaml
selection: "*"                          # all assets (also "all")
selection: "key:my_asset"               # single asset by key
selection: "key:my_namespace/my_asset"  # namespaced key (slash-delimited)
```

### By group / tag / kind / owner / code location

```yaml
selection: "group:silver"               # by asset group
selection: "tag:cadence=daily"          # by tag key=value
selection: "tag:critical"               # by tag key (any value)
selection: "kind:snowflake"             # by asset kind (often auto-set by ingestion components)
selection: "kind:dbt"                   # all dbt assets
selection: "owner:team:platform"        # by owner — team or email
selection: "owner:alice@company.com"    # individual owner
selection: "code_location:warehouse"    # by code location (multi-project setups)
```

### By lineage (graph traversal)

```yaml
selection: "+orders"                    # orders + all upstream ancestors
selection: "orders+"                    # orders + all downstream descendants
selection: "+orders+"                   # orders + both directions
selection: "2+orders"                   # orders + 2 levels of upstream only
selection: "orders+3"                   # orders + 3 levels of downstream only
selection: "2+orders+3"                 # bounded both directions
```

### Boolean composition

```yaml
selection: "group:gold and tag:tier=critical"
selection: "tag:domain=finance or tag:domain=accounting"
selection: "group:silver and not tag:archived"
selection: "(group:silver or group:gold) and not kind:dbt"
```

### Python-direct (when calling `apply_rules` from `definitions.py`)

```python
import dagster as dg

apply_rules(base, rules=[
    {
        "selection": dg.AssetSelection.tag("cadence", "hourly") - dg.AssetSelection.groups("archive"),
        "cron": "0 * * * *",
    },
])
```

Use Python objects when you need set operations (`|`, `&`, `-`) the string parser doesn't expose.

### Resolution semantics

Each rule's selection is `.resolve()`-d against the asset graph once, at `apply_rules` time. The result is a concrete set of `AssetKey`s. Then for each asset: "is my key in the resolved set?" — first match wins.

Selections that match **zero assets** just no-op for that rule — no error, evaluation continues with the next rule. Safe to write speculative rules.

### Not supported

- **Regex on asset keys** — use `tag:` or `group:` instead (tag the assets you want patterns to match).
- **Dynamic / runtime selection** — selections resolve once at apply time. Assets added later don't get rules applied.
- **"All assets with daily upstream"** — covered by the `derive_from_upstreams` strategy, not the selection field.

---

## Fall-through priority

Rules are evaluated top to bottom. The `precedence` flag decides which match wins when selections overlap. **Pick whichever mental model fits your team — both are supported.**

### `precedence: "first_match"` (default) — iptables / firewall style

The FIRST matching rule wins. List **narrow rules first, broad catch-alls last**:

```yaml
precedence: first_match  # default
rules:
  - selection: "tag:cadence=hourly"   # narrow first → wins for matching assets
    cron: "0 * * * *"

  - selection: "group:silver"          # narrower than '*' → wins for silver
    derive_from_upstreams: true

  - selection: "*"                      # broad LAST → catches everything else
    preset: eager
```

⚠ **Pitfall**: putting `"*"` first shadows everything below. The first rule matches all assets, and no rule below ever fires.

### `precedence: "last_match"` — CSS cascade / dbt config style

The LAST matching rule wins. List **defaults first, overrides later**:

```yaml
precedence: last_match
rules:
  - selection: "*"                      # default first → applies to everything
    preset: eager

  - selection: "group:silver"          # override → wins for silver
    derive_from_upstreams: true

  - selection: "tag:cadence=hourly"   # final override → wins for hourly
    cron: "0 * * * *"
```

This is the more "natural" mental model for users who think *"set the default, then override the special cases"* — like CSS, dbt config layering, or YAML inheritance.

Same final result as `first_match`, just inverted rule ordering. Pick whichever feels natural.

### `preserve_existing: true` (default)

Acts like CSS `!important` on per-asset settings: if an asset already has `automation_condition` set explicitly (e.g. by its own component config or its `@asset(automation_condition=...)` decorator), **no rule touches it**. Per-asset always wins.

### Common pitfalls

1. **Catch-all listed first** — `"*"` matches everything, shadowing every rule below it. Always list `"*"` last.
2. **Narrow rule that never fires** — silently means the selection is wrong. Use the validation script below to verify.
3. **Composition expectation** — rules don't AND together. If you want narrow AND broad combined, write a custom `python:` rule that builds the composed condition.

### "Doubly-applied" rules — by design, no

A given asset only ever gets ONE rule applied. No double-application is possible from a single `apply_rules` call. To compose two rules' conditions on one asset, you write a `python:` rule combining them.

If you call `apply_rules` **multiple times** (e.g. layered policy from multiple `definitions.py` calls), the second call respects `preserve_existing` and won't overwrite what the first set. So composition by chaining is naturally protected.

---

## Cadence propagation through chains

The applicator processes assets in **topological order** so derived cadences flow downstream:

```
A (root, metadata.cron_schedule: daily)            → effective: daily
B (root, metadata.cron_schedule: monthly)          → effective: monthly
C (deps: [A, B]; rule: least_frequent)             → effective: monthly  ← bound by B
D (deps: [C]; rule: derive most_frequent)          → effective: monthly  ← inherits C's effective cadence
```

When D's rule asks "what's my upstream's cron?", the applicator answers with **C's post-rule effective cron (monthly)**, not C's raw metadata (none). This is what makes "an asset that resolves to monthly because of its rule" propagate correctly to downstream consumers.

Without topological propagation, D would see C as having no cron (C's automation_condition wasn't applied yet) and fall through to whatever the next rule said. The applicator gets this right.

Where the applicator looks for upstream crons (in order):

1. `effective_crons[key]` — post-rule cadence from prior pass in this `apply_rules` call
2. `asset.automation_condition.cron_schedule` — explicit per-asset condition
3. `asset.freshness_policy.cron_schedule` — Dagster freshness policy
4. `asset.metadata["cron_schedule"]` — convention metadata key

Tag your root assets with `metadata: {cron_schedule: "..."}` so the applicator can see them.

---

## Wiring (`definitions.py`)

Components in Dagster can't post-process other components' output via `build_defs` alone — you wire the applicator at the project level:

### Simple — one applicator

```python
from pathlib import Path
from dagster import definitions, load_from_defs_folder
from dagster_community_components import apply_rules

@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    return apply_rules(base, rules=[
        {"selection": "tag:cadence=hourly", "cron": "0 * * * *"},
        {"selection": "group:silver", "derive_from_upstreams": True, "strategy": "tiered"},
        {"selection": "*", "preset": "eager"},
    ], preserve_existing=True)
```

### Layered — multiple `apply_rules` calls

Each call respects what previous calls set (via `preserve_existing=True`):

```python
@definitions
def defs():
    base = load_from_defs_folder(path_within_project=Path(__file__).parent)
    # Team-specific rules first (narrower, set first → preserve_existing protects later)
    base = apply_rules(base, rules=team_rules, preserve_existing=True)
    # Global fallback policy second
    return apply_rules(base, rules=global_rules, preserve_existing=True)
```

### YAML-config + Python-applied

Define rules in `defs/automation_rules/defs.yaml` (any component-style YAML), then load and apply them:

```python
# definitions.py
import yaml
from pathlib import Path
from dagster_community_components import apply_rules

@definitions
def defs():
    base = load_from_defs_folder(...)
    with open(Path(__file__).parent / "defs/automation_rules/defs.yaml") as f:
        config = yaml.safe_load(f)
    return apply_rules(base, rules=config["attributes"]["rules"])
```

---

## Customer scenarios

### Scenario A — Brownfield "we have 200 assets, want consistent automation"

```yaml
rules:
  # Pre-existing critical assets with explicit settings stay (preserve_existing: true).
  # Add tag-based overrides for special cases:
  - selection: "tag:tier=realtime"
    cron: "*/5 * * * *"
  # Everything else → eager (default for new projects):
  - selection: "*"
    preset: eager
```

Onboarding: tag a handful of special assets, drop in the applicator. Done.

### Scenario B — Greenfield medallion architecture

```yaml
rules:
  - selection: "group:bronze"        # raw ingestion — refresh eagerly
    preset: eager

  - selection: "group:silver"        # follow upstream cadence
    derive_from_upstreams: true
    strategy: most_frequent

  - selection: "group:gold"          # publish at fixed time
    cron: "0 9 * * *"
    business_hours_only: true

  - selection: "*"                   # fallback
    preset: eager
```

Bronze hot-keeps, silver follows upstream, gold publishes daily during business hours.

### Scenario C — Per-team SLAs via tags

```yaml
rules:
  - selection: "tag:team=marketing and tag:sla=realtime"
    cron: "*/15 * * * *"
  - selection: "tag:team=marketing"
    cron: "0 6,12,18 * * *"
  - selection: "tag:team=finance"
    cron: "0 6 * * 1-5"             # weekdays only
  - selection: "*"
    preset: eager
```

Teams own their tags; SLAs translate to cron without code changes per asset.

### Scenario D — Mixed cadence with month-boundary gates

```yaml
rules:
  - selection: "group:revenue_marts"   # silver/gold with monthly + daily deps
    derive_from_upstreams: true
    strategy: tiered                    # auto-builds the daily + monthly AND
  - selection: "*"
    preset: eager
```

The `tiered` strategy produces the same ANDed condition the Dagster docs describe by hand — but per-asset and automatic.

### Scenario E — Event-driven ML assets

```yaml
rules:
  - selection: "kind:ml_feature"      # auto-set by your ingestion components
    derive_from_upstreams: true
    strategy: any_dep_updated         # fire whenever a feature lands
  - selection: "kind:ml_model"
    derive_from_upstreams: true
    strategy: all_deps_updated        # train only when all features fresh
  - selection: "*"
    preset: eager
```

---

## Validating what got applied

After wiring, inspect the final automation_conditions:

```python
# In a notebook or test:
from your_project.definitions import defs

resolved = defs()
for spec in sorted(resolved.resolve_all_asset_specs(), key=lambda s: s.key.to_user_string()):
    cond = spec.automation_condition
    label = getattr(cond, "label", None) or (type(cond).__name__ if cond else "(none)")
    print(f"{spec.key.to_user_string():40s} → {label}")
```

Or via CLI (if the version supports it):

```bash
dg list defs --automation
```

If a rule isn't applying:

1. Check **rule order** — did a broader rule above shadow it?
2. Check **`preserve_existing`** — did the asset already have a condition set?
3. Check **selection syntax** — `dg list defs --select <selection>` to see what your selection actually matches.
4. Check **upstream metadata** — `derive_from_upstreams` needs upstream crons to be findable in `metadata`, `freshness_policy`, or `automation_condition`.

---

## Trade-offs, gotchas, limitations

### Trade-offs

- **`Definitions.map_asset_specs` is in preview** (Dagster ~1.10). The applicator uses it; you'll see preview warnings at load. Not blocking, but the API may change in future versions.
- **Rule order is load-bearing**. First-match-wins means a misplaced `"*"` shadows everything below it. Use validation script to catch.
- **No AND-composition between rules** by default. To compose narrow + broad, write a custom `python:` rule. Default keeps the model predictable.

### Gotchas

- **`derive_from_upstreams` needs cron metadata on upstreams**. If your bronze layer doesn't expose cron in `metadata`, `freshness_policy`, or `automation_condition`, derive falls through (returns None). **Tag upstreams with `metadata: {cron_schedule: "..."}`** for the applicator to find them.
- **Per-asset wins** is the default. If a rule isn't applying to an asset, check whether the asset's own `@asset(automation_condition=...)` is set — `preserve_existing` protects it.
- **`business_hours_only` doesn't compose with `tiered`** — multi-cron conditions can't be cleanly constrained without breaking tier semantics. Use the `python:` escape hatch.
- **Cron timezone is the Dagster instance's TZ** (usually UTC). For local business hours: either run Dagster in your local TZ or adjust hours manually.

### Limitations

- **Dynamic / runtime assets** — assets added by sensors or runtime registration AFTER `apply_rules` runs don't get rules applied. Real but rare. For static `defs/`-folder projects, completeness is guaranteed.
- **Cross-project selections** — `apply_rules` only sees the `Definitions` it's given. Multi-project / workspace setups need rules invoked per code location.
- **No regex on asset key strings** — use tags / groups for pattern-matching.

---

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `rules` | `List[Dict[str, Any]]` | Ordered list of rules. First selection-match wins (fall-through). Each rule has 'selection' + one of: 'cron' (with optional 'ignore_selection'/'allow_selection'), 'preset', or 'derive_from_upstreams: true' (+ optional 'strategy'). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `preserve_existing` | `bool` | `true` | If true (default): assets that already have automation_condition set by their own component are NOT overwritten. Like CSS !important — explicit per-asset wins over broad rules. |
| `precedence` | `str` | `"first_match"` | How overlapping rule selections resolve. 'first_match' (default): list narrow rules first, broad catch-alls last — first match wins. 'last_match': list defaults first, overrides later — like CSS cascade / dbt config layering, last match wins. |

<!-- FIELDS:END -->

## See also

- [`asset_job`](https://dagster-community-components-cli.vercel.app/c/asset_job) — bundle assets into a stable named job
- [`cron_schedule`](https://dagster-community-components-cli.vercel.app/c/cron_schedule) — schedule a job at a fixed cadence (alternative to AutomationCondition)
- [Dagster — Declarative Automation](https://docs.dagster.io/guides/automate/declarative-automation/) — official docs
- [Dagster — Arbitrary Python AutomationConditions](https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/arbitrary-python-automation-conditions) — for the `python:` escape hatch
- [Dagster — AssetSelection syntax](https://docs.dagster.io/concepts/assets/asset-selection-syntax) — full selection language reference
- Walkthrough: [`automation_condition_pipeline.md`](https://github.com/eric-thomas-dagster/dagster-community-components-cli/blob/main/examples/automation_condition_pipeline.md) — end-to-end customer scenarios
