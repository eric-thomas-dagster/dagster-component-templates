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

This component is a thin subclass of DbtProjectComponent — all dbt execution,
asset key mapping, partitions, and check generation work identically.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import dagster as dg

# Constants used by dagster-dbt to store internal metadata on AssetSpecs.
# These are stable keys — dagster-dbt uses them to round-trip unique_id back
# from a resolved AssetSpec.
_UNIQUE_ID_KEY = "dagster_dbt/unique_id"
_MANIFEST_KEY = "dagster_dbt/manifest"


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


try:
    from dagster_dbt.components.dbt_project.component import DbtProjectComponent as _DbtProjectComponent

    @dataclass
    class DbtDocsEnrichedProjectComponent(_DbtProjectComponent):
        """Extends DbtProjectComponent with rich dbt docs metadata on every asset.

        Drop-in replacement for DbtProjectComponent in defs.yaml. All dbt execution,
        selection, and check generation work identically. Adds a layer of metadata
        enrichment by reading the compiled manifest.json after dbt prepare.

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

        include_exposures: bool = True
        """Annotate each model with downstream exposures (dashboards, notebooks, etc.)"""

        include_metrics: bool = True
        """Annotate each model with dbt metrics that reference it."""

        include_semantic_models: bool = True
        """Annotate each model with semantic model definitions referencing it."""

        include_contracts: bool = True
        """Surface contract enforcement status and column-level constraints."""

        include_meta: bool = True
        """Surface the full node.meta dict (beyond dagster-specific sub-keys)."""

        include_source_freshness: bool = True
        """Surface freshness SLA thresholds on source assets."""

        include_doc_blocks: bool = False
        """Resolve and embed doc block contents. Off by default (can be verbose)."""

        manifest_path: Optional[str] = None
        """Override path to manifest.json. Defaults to {project_dir}/target/manifest.json."""

        # ------------------------------------------------------------------
        # Internal helpers
        # ------------------------------------------------------------------

        def _resolve_manifest(self, state: Any) -> Optional[dict]:
            """Load the dbt manifest.json. Tries multiple locations."""
            candidates: list[Path] = []

            if self.manifest_path:
                candidates.append(Path(self.manifest_path))

            # Try to get project_dir from state or the project object
            for obj in [state, getattr(self, "project", None)]:
                for attr in ["project_dir", "target_path", "manifest_path"]:
                    val = getattr(obj, attr, None)
                    if val:
                        p = Path(str(val))
                        if attr == "project_dir":
                            candidates.append(p / "target" / "manifest.json")
                        elif attr == "target_path":
                            candidates.append(p / "manifest.json")
                        else:
                            candidates.append(p)

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

            if not extra:
                return spec

            return spec.merge_attributes(metadata=extra)

        # ------------------------------------------------------------------
        # Override build_defs_from_state
        # ------------------------------------------------------------------

        def build_defs_from_state(self, context: dg.ComponentLoadContext, state: Any) -> dg.Definitions:
            base_defs = super().build_defs_from_state(context, state)

            manifest = self._resolve_manifest(state)
            if manifest is None:
                context.log.warning(  # type: ignore[attr-defined]
                    "DbtDocsEnrichedProjectComponent: could not load manifest.json — "
                    "returning base dbt definitions without docs enrichment. "
                    "Set manifest_path explicitly if the default target/ location is non-standard."
                ) if hasattr(context, "log") else None
                return base_defs

            def enrich(spec: dg.AssetSpec) -> dg.AssetSpec:
                try:
                    return self._enrich_spec(spec, manifest)
                except Exception:
                    # Never break the load — degrade gracefully
                    return spec

            return base_defs.map_resolved_asset_specs(enrich)

except ImportError:
    # dagster-dbt not installed — provide a helpful stub
    class DbtDocsEnrichedProjectComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Stub: requires dagster-dbt to be installed.

        Install with: pip install dagster-dbt
        """

        dbt_docs_url: Optional[str] = dg.Field(default=None)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            raise ImportError(
                "DbtDocsEnrichedProjectComponent requires dagster-dbt. "
                "Install with: pip install dagster-dbt"
            )
