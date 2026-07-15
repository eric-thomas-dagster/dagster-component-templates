# Product feedback: how should Dagster represent a `no-op` dbt run?

**Context.** dbt's state-reuse feature (dbt Cloud) and incremental microbatch produce runs where some models complete with `status: "no-op"` or `status: "partial success"`. These models were NOT rebuilt — dbt intentionally skipped work because state said the model was already up-to-date, or because only some batches ran.

**Current behavior.** `dagster-dbt` today compares status against `NodeStatus.Success` with `==`. Anything else falls through — the OSS `@dbt_assets` op raises on missing required outputs, or the Cloud v2 sensor silently drops the event. Either way the run appears failed even though dbt itself succeeded. See dagster-io/dagster#34010 (PR filed 2026-07-15) for the immediate resilience fix.

**But #34010 might be the wrong semantic model.** It treats `no-op` as `success` — yields a full `AssetMaterialization` (or `Output`). That gets Bravida (and every other state-reuse customer) unblocked, but it conflates two distinct concepts:

- **Materialization** — the asset was rebuilt in the warehouse
- **Observation** — the asset was verified to be up-to-date; no work was done

Treating no-op as a full materialization has downstream consequences:

## What's problematic about `no-op → materialization`

1. **Dagster+ Insights double-counts materializations.** If a customer bills per-materialization or tracks materialization rate as a KPI, a state-reuse run inflates their metrics without any actual work being done. Their apparent cost/frequency skews.

2. **Freshness policies race with reality.** A `FreshnessPolicy` based on last-materialization-timestamp will re-arm every time state-reuse fires — but the underlying data hasn't changed. The user's freshness dashboard doesn't reflect data recency; it reflects state-reuse cadence.

3. **Backfill semantics get weird.** If a user launches a backfill over "assets last materialized before date X", a no-op run resets that timestamp — potentially causing them to skip a backfill they should have run.

4. **Downstream cache invalidation.** Cache layers that key on last-materialization-timestamp invalidate unnecessarily.

## What Dagster primitives are close, but don't quite fit

### `AssetObservation` (existing)

The semantically closest primitive. Purpose: "we observed this asset without changing it." But:
- Observations don't satisfy op output requirements. In the `@dbt_assets` in-op context, if we yield an Observation instead of an Output, the framework fails the op with "missing required outputs" — same failure mode we're trying to solve.
- Observations don't (today) update freshness. If asset freshness is measured by last-materialization-time, an observation doesn't advance it — which is arguably wrong for state-reuse (the asset IS fresh; state-reuse verified it).
- Dagster+ Insights doesn't (afaik) count observations toward cost the way it does materializations — which is actually the behavior we want.

### `ObserveResult` (newer, in-op-context observation)

Newer typed result for in-op observations. Better fit than raw `AssetObservation` for the OSS `@dbt_assets` path — it lives inside op execution. But same freshness/insights caveats.

### `@observable_source_asset` (existing)

Existing primitive for "declare an external source and observe its freshness periodically." Not the right fit — dbt models aren't external sources; they're first-class assets.

## The design question

Do we need a third primitive? Or extend `AssetObservation` / `ObserveResult`?

Options:

**Option A: Extend `ObserveResult` to satisfy op-output requirements.**
An `ObserveResult` yielded by an `@dbt_assets` op should count as "this asset's output was produced" for framework purposes, AND signal to Insights + freshness "no cost was incurred; the asset is fresh but wasn't rebuilt." This is the smallest framework change and probably the right one.

**Option B: New primitive — `ReuseResult` (or `NoOpResult`).**
Explicit "this asset was not materialized in this run because state said so." Feels like it should just be `ObserveResult` with a well-known metadata key, not a new primitive.

**Option C: Metadata-flag on `Output` / `AssetMaterialization`.**
Ship `#34010` as-is, add a well-known metadata key like `dagster/materialization_kind: "reused"`. Then Insights/freshness/backfill can query the metadata and de-count reused materializations. Ugly — every consumer has to remember to check the flag.

**Recommendation: Option A** — extend `ObserveResult` to satisfy op-output requirements. This gives dbt state-reuse a clean semantic home: it verifies the asset is fresh, records that verification in the asset graph, but doesn't count as a rebuild event for cost/freshness/backfill purposes. `dagster-dbt` then chooses between `Output` (fresh materialization) and `ObserveResult` (state-reused) based on the dbt status — the framework handles both cleanly.

## Why this matters beyond dbt

The same pattern applies to any tool that supports "state-based skip":

- **Airflow's sensor + skip** — same shape.
- **Fivetran's "no rows changed" sync** — same shape.
- **Snowflake / BigQuery merge with no changes** — same shape.
- **Any incremental-materialization framework** — same shape.

If Dagster nails the "asset was verified fresh, no work done" primitive, all of these integrations can adopt it cleanly. Today each one has to invent its own workaround (or, like `dagster-dbt`, hit the same class of bug).

## Recommended next step

Cross-functional discussion between the `dagster-dbt` maintainers, the Insights team, and the freshness / automation-condition team. This is a framework-level design decision, not a `dagster-dbt` bug. Filing this as a follow-up to #34010 so the resilience fix can ship immediately, and the semantic question gets its own dedicated conversation.

**Owner ask:** whoever picks this up, please also loop in the Insights team about how they'd want to bill / display "verified fresh" events distinctly from full materializations.
