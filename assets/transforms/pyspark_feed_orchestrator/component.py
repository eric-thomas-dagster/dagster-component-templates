"""PySparkFeedOrchestratorComponent — folder-scan sibling of pyspark_pipeline.

Points at a folder of feed config files (`*.json` or `*.yaml/.yml`) — each
one describes ONE pipeline in the same shape `pyspark_pipeline` accepts,
plus the control-flow step types (`condition` / `checkpoint` / `for_each`).

For each feed file:
  - Loads the config.
  - Extracts asset keys the feed materializes (from `sinks[].from` + the
    step it references) → becomes the emitted asset keys.
  - Extracts asset keys the feed depends on (from `steps[].source.kind:
    upstream` + `sources[].kind: table` when tables get produced by ANOTHER
    feed in the same folder). Automatically resolves cross-feed edges when
    two feeds share a table name.
  - Instantiates a PySparkPipelineComponent with the parsed shape.
  - Emits its Definitions (multi_asset + optional ScheduleDefinition).

Feed shape (superset of pyspark_pipeline; JSON or YAML):

    {
      "name": "customer_daily_features",
      "description": "...",
      "schedule": "0 6 * * *",           # optional cron
      "owners": ["team@corp.com"],
      "group_name": "spark_feeds",
      "variables": {"target_date": "2026-08-30"},
      "steps": [
        {"id": "customers", "source": {"kind": "parquet", "path": "..."},
         "operations": [...]},
        {"id": "guard", "type": "condition", "when": "customers.row_count > 0"},
        ...
      ],
      "sinks": [
        {"from": "features", "kind": "parquet", "path": "features/", "mode": "overwrite"}
      ]
    }

Cross-feed lineage: when a feed's step reads `{"kind": "table", "table":
"warehouse.customers"}`, the orchestrator checks if any other feed in the
folder writes a sink to `warehouse.customers`. If yes → the assets are
wired; if no → the source appears as an external asset (dashed border).

Same feed configs unchanged, unified observability, actual lineage. Ports
the customer prototype pattern to a first-class DCC component.
"""
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import dagster as dg
from dagster import (
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
)
from pydantic import Field

# Reuse pyspark_pipeline component's execute path — but since the no-shared-
# code rule requires each component be self-contained, we spawn a
# PySparkPipelineComponent PER feed and let IT own the actual execution.
# The orchestrator's job is discovery + cross-file lineage resolution.


def _load_feed(path: Path) -> Dict[str, Any]:
    """Load a feed file. Supports .json / .yaml / .yml."""
    suffix = path.suffix.lower()
    text = path.read_text()
    if suffix == ".json":
        return json.loads(text)
    if suffix in (".yaml", ".yml"):
        import yaml
        return yaml.safe_load(text)
    raise ValueError(f"pyspark_feed_orchestrator: unsupported feed extension {suffix!r} on {path}")


def _write_targets(feed: Dict[str, Any]) -> List[str]:
    """All `sinks[].table` / `sinks[].path` (converted to asset-key strings)."""
    out: List[str] = []
    for sink in feed.get("sinks") or []:
        # Prefer explicit `asset_key` field, then `table`, then last resort:
        # a path-derived key (base file/dir name without extension).
        key = sink.get("asset_key") or sink.get("table")
        if not key and sink.get("path"):
            key = Path(sink["path"]).with_suffix("").name
        if key and key not in out:
            out.append(key)
    return out


def _read_sources(feed: Dict[str, Any]) -> List[str]:
    """All potential upstream asset keys read across steps + nested for_each."""
    out: List[str] = []

    def _walk(steps: List[Dict[str, Any]]) -> None:
        for s in steps:
            stype = (s.get("type") or "").lower()
            if stype == "for_each":
                _walk(s.get("steps") or [])
                continue
            if stype in {"condition", "checkpoint"}:
                continue
            src = s.get("source") or {}
            kind = (src.get("kind") or "").lower()
            if kind == "upstream":
                k = src.get("upstream_asset_key")
                if k and k not in out:
                    out.append(k)
            elif kind == "table":
                k = src.get("table")
                if k and k not in out:
                    out.append(k)
            elif kind == "ref":
                # Intra-feed reference — not a cross-feed dep.
                pass

    _walk(feed.get("steps") or [])
    return out


class PySparkFeedOrchestratorComponent(dg.Component, Model, Resolvable):
    """Scan a folder of feed files and emit one Dagster multi_asset per feed.

    Example:

    ```yaml
    type: dagster_component_templates.PySparkFeedOrchestratorComponent
    attributes:
      feeds_dir: feeds
      spark_config:
        spark.master: "local[*]"
      checkpoint_dir: checkpoints
      group_name: spark_feeds
    ```
    """

    feeds_dir: str = Field(
        description=(
            "Path (relative to component YAML dir, or absolute) to the folder "
            "containing feed config files. Every `.json` / `.yaml` / `.yml` "
            "file at the top level becomes one Dagster multi_asset via "
            "PySparkPipelineComponent."
        ),
    )
    file_glob: str = Field(
        default="*.json,*.yaml,*.yml",
        description="Comma-separated glob patterns for feed files (default matches JSON + YAML).",
    )
    spark_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Base SparkConf applied to every feed. Feed-level `spark_config` overlays.",
    )
    spark_app_name_prefix: str = Field(
        default="dagster-pyspark-feed",
        description="Spark app name is `<prefix>-<feed_name>`.",
    )
    checkpoint_dir: Optional[str] = Field(
        default=None,
        description=(
            "Checkpoint directory passed to every feed's PySparkPipelineComponent. "
            "Each feed writes `<checkpoint_dir>/<feed_name>.json`."
        ),
    )
    group_name: Optional[str] = Field(
        default=None,
        description="Fallback Dagster group name (feed-level `group_name` overrides).",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Fallback asset kinds. Default: ['pyspark', 'spark'].",
    )
    tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Fallback catalog tags (feed-level tags overlay).",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Resolve feeds_dir against the component's YAML location.
        base = _resolve_base(context)
        feeds_dir = Path(self.feeds_dir)
        if not feeds_dir.is_absolute():
            feeds_dir = (base / feeds_dir).resolve()

        if not feeds_dir.exists():
            # Bail cleanly — likely dg check before the feeds folder exists yet.
            context.log.warning(  # type: ignore[attr-defined]
                f"PySparkFeedOrchestratorComponent: feeds_dir={feeds_dir!s} does not exist yet; "
                "no assets emitted."
            ) if hasattr(context, "log") else None
            return Definitions()

        # Enumerate feeds.
        patterns = [p.strip() for p in self.file_glob.split(",") if p.strip()]
        feed_files: List[Path] = []
        seen: set = set()
        for pat in patterns:
            for f in sorted(feeds_dir.glob(pat)):
                if f.is_file() and str(f) not in seen:
                    seen.add(str(f))
                    feed_files.append(f)

        if not feed_files:
            return Definitions()

        # Load + inventory produced/consumed keys across every feed.
        parsed: List[Tuple[Path, Dict[str, Any]]] = []
        produced_keys: set = set()
        for f in feed_files:
            try:
                feed = _load_feed(f)
            except Exception as e:  # noqa: BLE001
                raise ValueError(f"pyspark_feed_orchestrator: failed to parse {f.name}: {e}") from e
            if not isinstance(feed, dict):
                raise ValueError(f"pyspark_feed_orchestrator: {f.name} did not parse to a dict")
            feed.setdefault("name", f.stem)
            parsed.append((f, feed))
            produced_keys.update(_write_targets(feed))

        # Build one PySparkPipelineComponent per feed. External sources become
        # explicit AssetSpecs so the graph shows them as first-class nodes with
        # dashed borders (not implicit "auto-external" stubs).
        from ..pyspark_pipeline.component import PySparkPipelineComponent

        all_defs_children: List[Definitions] = []
        external_specs: List[dg.AssetSpec] = []
        external_seen: set = set()

        for path, feed in parsed:
            feed_name = feed.get("name") or path.stem
            read_srcs = _read_sources(feed)
            # Externals — sources referenced but NOT produced by any feed in this folder.
            for src in read_srcs:
                if src not in produced_keys and src not in external_seen:
                    external_seen.add(src)
                    external_specs.append(
                        dg.AssetSpec(
                            key=dg.AssetKey.from_user_string(src),
                            group_name="external",
                            kinds={"external"},
                            description=f"External input to spark feed {feed_name!r}.",
                        )
                    )

            # Merge component-level defaults into feed-level values.
            merged_spark_cfg = dict(self.spark_config or {})
            merged_spark_cfg.update(feed.get("spark_config") or {})

            # Route ALL feed sinks — first sink's target defines the asset name.
            targets = _write_targets(feed)
            if not targets:
                # No sinks — treat the feed as a no-op observation asset keyed on feed name.
                asset_name = feed_name
            else:
                asset_name = targets[0]

            child = PySparkPipelineComponent(
                asset_name=asset_name,
                spark_config=merged_spark_cfg,
                spark_app_name=f"{self.spark_app_name_prefix}-{feed_name}",
                steps=feed.get("steps") or [],
                sinks=feed.get("sinks") or [],
                variables=feed.get("variables"),
                checkpoint_dir=feed.get("checkpoint_dir") or self.checkpoint_dir,
                group_name=feed.get("group_name") or self.group_name,
                description=feed.get("description"),
                owners=feed.get("owners"),
                asset_tags={**(self.tags or {}), **(feed.get("tags") or {})} or None,
                kinds=feed.get("kinds") or self.kinds,
                deps=[k for k in read_srcs if k not in {t for t in targets}] or None,
            )
            child_defs = child.build_defs(context)
            all_defs_children.append(child_defs)

            # Attach a schedule if the feed declares one.
            cron = feed.get("schedule")
            if cron:
                sched_name = f"{feed_name}_schedule"
                sched = dg.ScheduleDefinition(
                    name=sched_name,
                    cron_schedule=cron,
                    target=dg.AssetSelection.assets(dg.AssetKey.from_user_string(asset_name)),
                    execution_timezone=feed.get("timezone"),
                )
                all_defs_children.append(Definitions(schedules=[sched]))

        # Combine every feed's Definitions + external specs into one.
        if external_specs:
            all_defs_children.append(Definitions(assets=external_specs))

        return Definitions.merge(*all_defs_children)


def _resolve_base(context: ComponentLoadContext) -> Path:
    """Best-effort resolution of the component YAML directory."""
    for attr in ("path", "component_path", "yaml_path", "file_path"):
        val = getattr(context, attr, None)
        if val is None:
            continue
        if callable(val):
            try:
                val = val()
            except Exception:  # noqa: BLE001
                continue
            if val is None:
                continue
        p = Path(val)
        return p if p.is_dir() else p.parent
    return Path.cwd()
