"""dbt Docs Enriched Project Component.

Extends the official dagster-dbt DbtProjectComponent to surface rich metadata
from the dbt manifest that is not captured by default:

- Clickable link to the dbt docs site for each model/source/snapshot
- Exposures that consume each model (BI dashboards, notebooks, ML models)
- Metrics and semantic models referencing each model
- Contract enforcement status and column-level constraints
- Full `meta` dict (beyond the dagster-specific sub-keys)
- Source freshness SLA thresholds and loaded_at_field
- dbt model access level (public / protected / private)
- Language (sql vs python models)
- doc block contents referenced by nodes
- Per-model partitions_def read from `meta.dagster.partitions_def`
- Per-model automation_condition read from `meta.dagster.automation_condition`

This component is a thin subclass of DbtProjectComponent — all dbt execution,
asset key mapping, and check generation work identically.

Per-model partitioning:

    ```yaml
    # dbt model schema.yml
    - name: fct_fuel_margin_daily
      config:
        meta:
          dagster:
            partitions_def:
              type: daily
              start_date: "2025-08-01"
    ```

    Supported types: `daily`, `hourly`, `weekly`, `monthly`, `static`,
    `dynamic`. Models without this meta field remain unpartitioned. See
    `_partitions_def_from_meta` for the full shape.

Per-model automation_condition:

    ```yaml
    - name: fct_daily_pnl
      config:
        meta:
          dagster:
            automation_condition:
              preset: eager
              # OR: preset: on_deploy_if_code_changed
              # OR: cron: "0 9 * * *"
    ```

    Shares the preset vocabulary of AutomationConditionApplicatorComponent
    (`eager`, `on_missing`, `any_downstream_conditions`,
    `on_deploy_if_code_changed`, or a bare `cron:`).
"""
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import dagster as dg
from pydantic import Field

# Constants used by dagster-dbt to store internal metadata on AssetSpecs.
# These are stable keys — dagster-dbt uses them to round-trip unique_id back
# from a resolved AssetSpec.
_UNIQUE_ID_KEY = "dagster_dbt/unique_id"
_MANIFEST_KEY = "dagster_dbt/manifest"


# ─── Asset overrides (inline; kept per-component to preserve self-containment) ─
#
# Per-asset override applied after enumeration. Today supports `depends_on` —
# a list of upstream Dagster asset keys (strings; slash-delimited becomes a
# hierarchical AssetKey). Extend with more fields as needed (group, tags,
# description). Matches the pattern used by the official Databricks workspace
# component's `attributes.asset_overrides.<key>.depends_on`.


@dataclass
class AssetOverride(dg.Resolvable):
    depends_on: Optional[List[str]] = None


def _resolve_override_deps(
    asset_overrides: Optional[Dict[str, "AssetOverride"]],
    lookup_key: str,
) -> List[dg.AssetKey]:
    if not asset_overrides:
        return []
    ov = asset_overrides.get(lookup_key)
    if not ov or not ov.depends_on:
        return []
    return [dg.AssetKey(d.split("/")) if "/" in d else dg.AssetKey(d) for d in ov.depends_on]


def _get_str_meta(metadata: dict, key: str) -> Optional[str]:
    """Extract a string from a dagster metadata dict, unwrapping MetadataValue if needed."""
    val = metadata.get(key)
    if val is None:
        return None
    if isinstance(val, str):
        return val
    # MetadataValue objects expose .value or .text
    if hasattr(val, "value"):
        return str(val.value)
    if hasattr(val, "text"):
        return val.text
    return str(val)


def _partitions_def_from_meta(meta: Mapping[str, Any]) -> Optional[Any]:
    """Convert a `meta.dagster.partitions_def` dict from a dbt manifest node
    into a concrete Dagster PartitionsDefinition. Returns None if the shape
    is invalid, absent, or references an unsupported type.

    Supported YAML shapes (matching Dagster's declarative PartitionsDefinitionModels):

        partitions_def:
          type: daily
          start_date: "2025-01-01"
          end_date: null              # optional
          timezone: "America/New_York"  # optional
          minute_offset: 0            # optional
          hour_offset: 0              # optional

        partitions_def:
          type: hourly
          start_date: "2025-01-01-00:00"
          # + end_date / timezone / minute_offset (same as daily)

        partitions_def:
          type: weekly
          start_date: "2025-01-06"
          # + end_date / timezone / minute_offset / hour_offset / day_offset

        partitions_def:
          type: monthly
          start_date: "2025-01-01"
          # + end_date / timezone / minute_offset / hour_offset / day_offset

        partitions_def:
          type: static
          values: ["US", "CA", "MX"]

        partitions_def:
          type: dynamic
          name: filenames
    """
    if not meta or not isinstance(meta, Mapping):
        return None
    ptype = meta.get("type")
    if not ptype:
        return None
    try:
        if ptype == "daily":
            return dg.DailyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
                hour_offset=meta.get("hour_offset", 0),
            )
        if ptype == "hourly":
            return dg.HourlyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
            )
        if ptype == "weekly":
            return dg.WeeklyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
                hour_offset=meta.get("hour_offset", 0),
                day_offset=meta.get("day_offset", 0),
            )
        if ptype == "monthly":
            return dg.MonthlyPartitionsDefinition(
                start_date=meta["start_date"],
                end_date=meta.get("end_date"),
                timezone=meta.get("timezone"),
                minute_offset=meta.get("minute_offset", 0),
                hour_offset=meta.get("hour_offset", 0),
                day_offset=meta.get("day_offset", 1),
            )
        if ptype == "static":
            values = meta.get("values")
            if not values:
                return None
            return dg.StaticPartitionsDefinition(list(values))
        if ptype == "dynamic":
            name = meta.get("name")
            if not name:
                return None
            return dg.DynamicPartitionsDefinition(name=name)
    except (KeyError, TypeError, ValueError):
        return None
    return None


def _automation_condition_from_meta(meta: Mapping[str, Any]) -> Optional[Any]:
    """Convert a `meta.dagster.automation_condition` dict from a dbt manifest
    node into a concrete AutomationCondition. Returns None on invalid shape.

    Supported shapes (mirrors the AutomationConditionApplicatorComponent
    preset vocabulary):

        automation_condition:
          preset: eager                       # or on_missing / any_downstream_conditions

        automation_condition:
          preset: on_deploy_if_code_changed   # synthetic composite

        automation_condition:
          cron: "0 9 * * *"
    """
    if not meta or not isinstance(meta, Mapping):
        return None
    # Preset shortcut
    preset = meta.get("preset")
    if preset:
        # Synthetic composite — mirrors the applicator's Shape 2 preset path.
        if preset == "on_deploy_if_code_changed":
            return (
                dg.AutomationCondition.code_version_changed().since_last_handled()
                & ~dg.AutomationCondition.in_progress()
            )
        method = getattr(dg.AutomationCondition, preset, None)
        if method is None or not callable(method):
            return None
        try:
            result = method()
        except Exception:
            return None
        return result if isinstance(result, dg.AutomationCondition) else None
    # Explicit cron
    cron = meta.get("cron")
    if cron:
        try:
            return dg.AutomationCondition.on_cron(cron)
        except Exception:
            return None
    return None


try:
    from dagster_dbt.components.dbt_project.component import DbtProjectComponent as _DbtProjectComponent

    @dataclass
    class DbtDocsEnrichedProjectComponent(_DbtProjectComponent):
        """Extends DbtProjectComponent with rich dbt docs metadata on every asset.

        Drop-in replacement for DbtProjectComponent. By default it is identical —
        same dbt execution, selection, and check generation, no extra metadata.
        Opt in to specific enrichments by setting the include_* flags or dbt_docs_url.

        Example:
            ```yaml
            type: dagster_component_templates.DbtDocsEnrichedProjectComponent
            attributes:
              project: "{{ project_root }}/dbt_project"
              dbt_docs_url: "https://dbt-docs.internal.mycompany.com"
              include_exposures: true
              include_metrics: true
              include_semantic_models: true
              cli_args:
                - build
            ```
        """

        # --- enrichment configuration ---
        dbt_docs_url: Optional[str] = None
        """Base URL of your hosted dbt docs site. If set, each asset gets a
        clickable URL: {dbt_docs_url}/#!/{resource_type}/{unique_id}"""

        include_exposures: bool = False
        """Annotate each model with downstream exposures (dashboards, notebooks, etc.)"""

        include_metrics: bool = False
        """Annotate each model with dbt metrics that reference it."""

        include_semantic_models: bool = False
        """Annotate each model with semantic model definitions referencing it."""

        include_contracts: bool = False
        """Surface contract enforcement status and column-level constraints."""

        include_meta: bool = False
        """Surface the full node.meta dict (beyond dagster-specific sub-keys)."""

        include_source_freshness: bool = False
        """Surface freshness SLA thresholds on source assets."""

        include_doc_blocks: bool = False
        """Resolve and embed doc block contents."""

        manifest_path: Optional[str] = None
        """Override path to manifest.json. Defaults to {project_dir}/target/manifest.json."""

        asset_overrides: Optional[Dict[str, AssetOverride]] = None
        """Per-asset overrides keyed by the emitted asset's stringified key (e.g.
        `my_dbt_model` or `analytics/orders`). Today supports
        `depends_on: [upstream_key, ...]` to add Dagster asset dependencies —
        merged into each matching spec's deps. Matches the pattern used by the
        official Databricks workspace component."""

        # ------------------------------------------------------------------
        # Internal helpers
        # ------------------------------------------------------------------

        def _resolve_manifest(self, state_path: Optional[Path]) -> Optional[dict]:
            """Load the dbt manifest.json. Tries manifest_path override first,
            then falls back to the project manager's compiled manifest path."""
            candidates: list[Path] = []
            if self.manifest_path:
                candidates.append(Path(self.manifest_path))
            try:
                project = self._project_manager.get_project(state_path)
                candidates.append(Path(project.manifest_path))
            except Exception:
                pass
            for candidate in candidates:
                try:
                    return json.loads(candidate.read_text())
                except (FileNotFoundError, PermissionError, json.JSONDecodeError):
                    continue
            return None

        def _enrich_spec(self, spec: dg.AssetSpec, manifest: dict) -> dg.AssetSpec:
            """Add dbt docs metadata to a single AssetSpec."""
            unique_id = _get_str_meta(dict(spec.metadata), _UNIQUE_ID_KEY)
            if not unique_id:
                return spec

            all_nodes: dict = {
                **manifest.get("nodes", {}),
                **manifest.get("sources", {}),
                **manifest.get("snapshots", {}),
            }
            node = all_nodes.get(unique_id)
            if not node:
                return spec

            extra: dict[str, dg.MetadataValue] = {}
            resource_type: str = node.get("resource_type", "model")
            child_map: dict = manifest.get("child_map", {})
            child_ids: list[str] = child_map.get(unique_id, [])

            # 1. dbt docs URL
            if self.dbt_docs_url:
                url = f"{self.dbt_docs_url}/#!/{resource_type}/{unique_id}"
                extra["dbt_docs/url"] = dg.MetadataValue.url(url)

            # 2. Exposures consuming this model
            if self.include_exposures:
                exposure_ids = [c for c in child_ids if c.startswith("exposure.")]
                if exposure_ids:
                    exposures = []
                    for eid in exposure_ids:
                        exp = manifest.get("exposures", {}).get(eid, {})
                        entry: dict = {
                            "name": exp.get("name"),
                            "type": exp.get("type"),
                            "description": exp.get("description"),
                            "maturity": exp.get("maturity"),
                        }
                        owner = exp.get("owner", {})
                        if owner:
                            entry["owner"] = owner.get("email") or owner.get("name")
                        if exp.get("url"):
                            entry["url"] = exp["url"]
                        if exp.get("label"):
                            entry["label"] = exp["label"]
                        exposures.append(entry)
                    extra["dbt_docs/exposures"] = dg.MetadataValue.json(exposures)

            # 3. Metrics referencing this model
            if self.include_metrics:
                metric_ids = [c for c in child_ids if c.startswith("metric.")]
                if metric_ids:
                    metrics = []
                    for mid in metric_ids:
                        m = manifest.get("metrics", {}).get(mid, {})
                        metrics.append({
                            "name": m.get("name"),
                            "label": m.get("label"),
                            "type": m.get("type"),
                            "description": m.get("description"),
                            "time_granularity": m.get("time_granularity"),
                        })
                    extra["dbt_docs/metrics"] = dg.MetadataValue.json(metrics)

            # 4. Semantic models
            if self.include_semantic_models:
                sm_ids = [c for c in child_ids if c.startswith("semantic_model.")]
                if sm_ids:
                    sms = []
                    for smid in sm_ids:
                        sm = manifest.get("semantic_models", {}).get(smid, {})
                        sms.append({
                            "name": sm.get("name"),
                            "label": sm.get("label"),
                            "description": sm.get("description"),
                            "primary_entity": sm.get("primary_entity"),
                            "measures": [m.get("name") for m in sm.get("measures", [])],
                            "dimensions": [d.get("name") for d in sm.get("dimensions", [])],
                            "entities": [e.get("name") for e in sm.get("entities", [])],
                        })
                    extra["dbt_docs/semantic_models"] = dg.MetadataValue.json(sms)

            # 5. Contract enforcement + column constraints
            if self.include_contracts:
                contract = node.get("contract", {})
                if contract.get("enforced"):
                    extra["dbt_docs/contract_enforced"] = dg.MetadataValue.bool(True)
                    # Per-column constraints
                    col_constraints = {}
                    for col_name, col_info in node.get("columns", {}).items():
                        constraints = col_info.get("constraints", [])
                        if constraints:
                            col_constraints[col_name] = constraints
                    if col_constraints:
                        extra["dbt_docs/column_constraints"] = dg.MetadataValue.json(col_constraints)

            # 6. Full meta dict (non-dagster keys)
            if self.include_meta:
                meta = node.get("meta", {})
                non_dagster = {k: v for k, v in meta.items() if k != "dagster"}
                if non_dagster:
                    extra["dbt_docs/meta"] = dg.MetadataValue.json(non_dagster)

            # 7. Source freshness (sources only)
            if self.include_source_freshness and resource_type == "source":
                freshness = node.get("freshness")
                if freshness and any(freshness.get(k) for k in ["warn_after", "error_after"]):
                    extra["dbt_docs/freshness"] = dg.MetadataValue.json(freshness)
                loaded_at = node.get("loaded_at_field")
                if loaded_at:
                    extra["dbt_docs/loaded_at_field"] = dg.MetadataValue.text(loaded_at)
                loader = node.get("loader")
                if loader:
                    extra["dbt_docs/loader"] = dg.MetadataValue.text(loader)

            # 8. Model access level (public / protected / private)
            access = node.get("config", {}).get("access")
            if access and access != "protected":  # protected is default, skip noise
                extra["dbt_docs/access"] = dg.MetadataValue.text(access)

            # 9. Language (only surface non-SQL — Python models are notable)
            language = node.get("language")
            if language and language != "sql":
                extra["dbt_docs/language"] = dg.MetadataValue.text(language)

            # 10. Patch path (YAML file where this model is documented)
            patch_path = node.get("patch_path")
            if patch_path:
                # Strip the project prefix: "project://models/staging/schema.yml"
                display = patch_path.split("://")[-1] if "://" in patch_path else patch_path
                extra["dbt_docs/patch_path"] = dg.MetadataValue.text(display)

            # 11. Doc block contents (opt-in — can be verbose)
            if self.include_doc_blocks:
                doc_block_names = node.get("doc_blocks", [])
                if doc_block_names:
                    docs_lookup = manifest.get("docs", {})
                    resolved_blocks: dict[str, str] = {}
                    for block_name in doc_block_names:
                        for doc_uid, doc_node in docs_lookup.items():
                            if doc_node.get("name") == block_name:
                                resolved_blocks[block_name] = doc_node.get("block_contents", "")
                                break
                    if resolved_blocks:
                        extra["dbt_docs/doc_blocks"] = dg.MetadataValue.json(resolved_blocks)

            # meta.dagster.partitions_def / .automation_condition — per-model
            # config that keeps partitioning + automation in the dbt project.
            dagster_meta = node.get("meta", {}).get("dagster", {}) or {}
            per_model_partitions = _partitions_def_from_meta(dagster_meta.get("partitions_def") or {})
            per_model_automation = _automation_condition_from_meta(dagster_meta.get("automation_condition") or {})

            enriched = spec
            if extra:
                enriched = enriched.merge_attributes(metadata=extra)
            if per_model_partitions is not None:
                enriched = enriched.replace_attributes(partitions_def=per_model_partitions)
            if per_model_automation is not None:
                enriched = enriched.replace_attributes(automation_condition=per_model_automation)
            return enriched

        # ------------------------------------------------------------------
        # Override build_defs_from_state
        # ------------------------------------------------------------------

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            base_defs = super().build_defs_from_state(context, state_path)

            manifest = self._resolve_manifest(state_path)
            if manifest is None:
                context.log.warning(  # type: ignore[attr-defined]
                    "DbtDocsEnrichedProjectComponent: could not load manifest.json — "
                    "returning base dbt definitions without docs enrichment. "
                    "Set manifest_path explicitly if the default target/ location is non-standard."
                ) if hasattr(context, "log") else None
                return base_defs

            def enrich(spec: dg.AssetSpec) -> dg.AssetSpec:
                try:
                    enriched = self._enrich_spec(spec, manifest)
                except Exception:
                    # Never break the load — degrade gracefully
                    enriched = spec

                # Apply per-asset override deps if set. Look up by the spec's
                # stringified asset key (matches Databricks pattern).
                if self.asset_overrides:
                    lookup_key = spec.key.to_user_string()
                    override_deps = _resolve_override_deps(self.asset_overrides, lookup_key)
                    if override_deps:
                        try:
                            existing = list(enriched.deps or [])
                            enriched = enriched.merge_attributes(
                                deps=existing + list(override_deps)
                            )
                        except Exception:
                            pass
                return enriched

            return base_defs.map_resolved_asset_specs(func=enrich)

except ImportError:
    # dagster-dbt not installed — provide a stub that accepts the same field
    # shape so YAML validates; build_defs raises a helpful error.
    class DbtDocsEnrichedProjectComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Stub: requires dagster-dbt to be installed.

        Install with: pip install dagster-dbt
        """

        # Mirror the DbtProjectComponent surface so example.yaml validates.
        project: Optional[str] = Field(default=None)
        cli_args: Optional[Any] = Field(default=None)
        translation: Optional[Any] = Field(default=None)
        select: Optional[str] = Field(default=None)
        exclude: Optional[str] = Field(default=None)

        # DbtDocsEnrichedProjectComponent's own enrichment flags
        dbt_docs_url: Optional[str] = Field(default=None)
        include_exposures: bool = Field(default=False)
        include_metrics: bool = Field(default=False)
        include_semantic_models: bool = Field(default=False)
        include_contracts: bool = Field(default=False)
        include_meta: bool = Field(default=False)
        include_source_freshness: bool = Field(default=False)
        include_doc_blocks: bool = Field(default=False)
        asset_overrides: Optional[Dict[str, AssetOverride]] = Field(default=None)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            raise ImportError(
                "DbtDocsEnrichedProjectComponent requires dagster-dbt. "
                "Install with: pip install dagster-dbt"
            )
