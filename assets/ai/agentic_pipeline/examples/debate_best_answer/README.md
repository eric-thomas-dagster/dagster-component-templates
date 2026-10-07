# Debate the best answer — registered debater agents, joined by `inputs:`

Two opposing debater agents argue a proposal, a separate arbitrator agent weighs both arguments and renders a verdict — three `delegate` steps, three registered persona agents, joined with typed `inputs:` ports (not the legacy single-`source:` chaining the other examples use).

```
source (a proposal)
   │         │
   ▼         ▼
┌─────────┐ ┌─────────────┐
│   for    │ │   against    │   both pick from the SAME pool
│ argument │ │  argument    │   (required_capabilities: [debate]
│(delegate)│ │ (delegate)   │   excludes the arbitrator; picker then
└────┬─────┘ └──────┬───────┘   distinguishes advocate vs skeptic by
     │              │           reading the task's FOR/AGAINST wording)
     └──────┬───────┘
            ▼
     ┌─────────────┐
     │   verdict    │   required_capabilities: [arbitrate] --
     │  (delegate,  │   zero ambiguity, picks the arbitrator
     │  inputs: for_arg,
     │  against_arg)│
     └─────────────┘
```

## Files

| File | What it is |
|---|---|
| `pipeline.yaml` | The `AgenticPipelineComponent` — 3 `delegate` steps. |
| `debater_advocate_agent.yaml` / `debater_skeptic_agent.yaml` | Both `capabilities: [debate]` — the picker distinguishes them by semantics, not tags (see below). |
| `arbitrator_agent.yaml` | `capabilities: [arbitrate]` — a distinct tag, since there's no ambiguity about who renders the verdict. |
| `debate_agent_server.py` | One script, three personas (`advocate`/`skeptic`/`arbitrator`), selected by `argv[1]`. |

## Two filtering mechanisms, used for two different jobs

The `for_argument`/`against_argument` steps both set `required_capabilities: [debate]` — this excludes the arbitrator *before* the picker runs (it isn't a debater), but does **not** distinguish advocate from skeptic, since both share the same tag. That distinction is genuine semantic matching: the picker reads each step's task ("Argue FOR..." vs "Argue AGAINST...") against each card's own skill description ("builds the strongest case in favor of it" vs "...against it, surfacing risks") and picks correctly — verified below, both picks were correct.

The `verdict` step sets `required_capabilities: [arbitrate]` instead — there's exactly one agent with that tag, so there's no ambiguity left for a picker to resolve. Use a shared tag + semantic picking when genuinely different registered options could plausibly fit (stance, specialty); use a distinct tag when only one answer is ever correct.

## `inputs:` ports — joining two prior steps into one task

`verdict`'s task references **both** prior arguments by name:

```yaml
task: "Given the proposal and these two arguments, weigh them and render a final verdict.\n\nFOR:\n{for_arg}\n\nAGAINST:\n{against_arg}"
inputs:
  for_arg: {from: for_argument}
  against_arg: {from: against_argument}
```

This is the same typed-named-input primitive every other op in this component already supports — building this example surfaced that `delegate` had never actually been wired up to use it, despite its own docstring claiming `source / inputs` support. Fixed in `_do_delegate` (now resolves `inputs:` and substitutes `{port_name}` into `task` before anything else happens) as part of building this example.

## What you'll actually see

Run for real against `gpt-4o-mini`, on the proposal *"our team should switch from a 2-week sprint cadence to continuous/weekly releases"*:

**FOR** (picked `debater_advocate`):
> "Switching... will significantly enhance our team's agility and responsiveness to market demands... continuous releases reduce the risk of large-scale failures, as smaller, incremental updates are easier to manage and troubleshoot."

**AGAINST** (picked `debater_skeptic`):
> "...the shift may lead to a lack of comprehensive testing and quality assurance... the pressure of constant releases could overwhelm team members, leading to burnout..."

**VERDICT** (picked `arbitrator`, genuinely weighing both — not just summarizing):
> "...The risk of overwhelming the team and compromising product quality could have long-term negative effects that outweigh the short-term advantages... A phased implementation or hybrid model may be a more prudent path forward rather than a full switch."

## Native `debate` op vs this delegate-based version

The native `debate` op does this in **one step**: `proposers: [{system_prompt: "..."}, {system_prompt: "..."}]` + an `arbitrator:` config, no agent cards needed — genuinely simpler for a one-off pipeline. This example's version is more work to set up, in exchange for: the advocate/skeptic/arbitrator personas become a *shared, reusable registry* — any other pipeline in the project can `delegate` to the same `arbitrator` agent for a different decision, without redefining its persona. Reach for native `debate` by default; reach for this pattern when the personas genuinely need to outlive any one pipeline.

## Running it for real

**Each component needs its own directory.** Dagster's component scanner only loads a component from a file literally named `defs.yaml`, one per directory — the three agent YAMLs as extra files sitting next to `pipeline.yaml` are never actually loaded as components. In your project's `defs/` folder, lay these out as four sibling directories, each containing one file renamed to `defs.yaml`:

```
defs/
  debate/
    defs.yaml                  # pipeline.yaml, renamed
  debater_advocate/
    defs.yaml                   # debater_advocate_agent.yaml, renamed
    debate_agent_server.py
  debater_skeptic/
    defs.yaml                   # debater_skeptic_agent.yaml, renamed
    debate_agent_server.py      # (a copy, or point command: at a shared location)
  arbitrator/
    defs.yaml                   # arbitrator_agent.yaml, renamed
    debate_agent_server.py
```

Set `OPENAI_API_KEY`, then materialize `debate_verdict` (pulls in `debate_for_argument` and `debate_against_argument` automatically). `delegate`'s sibling-discovery scans the shared *parent* directory (`defs/` above), so all three agents need to be true siblings of `debate`, not nested inside it.
