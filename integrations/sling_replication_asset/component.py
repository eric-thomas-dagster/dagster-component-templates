"""SlingReplicationAssetComponent.

Wraps `dagster-sling` to run Sling replication YAMLs as Dagster assets — one asset per stream in the replication. Each materialization runs the configured stream from source to target.
"""
import json
from pathlib import Path
from typing import Any, Mapping, Optional

import dagster as dg
from pydantic import Field


def _parse_sling_json_table(raw: str) -> list:
    """Parses `sling conns exec --force_json` output ({"fields": [...], "rows": [[...]]}) into row dicts."""
    parsed = json.loads(raw.strip())
    fields = parsed["fields"]
    return [dict(zip(fields, row)) for row in parsed["rows"]]


def _incremental_stream_specs(replication_config: Mapping[str, Any]) -> dict:
    """For every stream configured with Sling's `mode: incremental` and an `update_key`, returns
    {stream_name: (update_key, object_name)} -- `object_name` is the table reference exactly as
    it lives on the target connection (e.g. "main.events"), NOT prefixed with the connection
    name -- `sling conns exec <conn> <sql>` already scopes the query to that connection, so a
    `<conn>.<table>`-qualified reference (dagster_sling's own display-metadata convention, not
    a queryable identifier) fails to resolve there.

    Sling has no separate per-stream state file by default — for incremental mode it queries
    MAX(update_key) on the TARGET table itself to pick each run's starting watermark (confirmed
    against Sling's own docs: https://docs.slingdata.io/examples/database-to-database/incremental,
    and against this install's `~/.sling/.sling.db`, which holds no per-stream state at all).
    The target table IS Sling's state, so that's exactly what we query here too.
    """
    from dagster_sling.asset_decorator import get_streams_from_replication

    defaults = replication_config.get("defaults") or {}
    out: dict = {}
    for stream in get_streams_from_replication(replication_config):
        cfg = stream["config"] or {}
        mode = cfg.get("mode", defaults.get("mode"))
        update_key = cfg.get("update_key", defaults.get("update_key"))
        if mode != "incremental" or not update_key:
            continue
        object_name = cfg.get("object") or stream["name"]
        out[stream["name"]] = (update_key, object_name)
    return out


def _query_incremental_watermark(sling, target_name, object_name: str, update_key: str, log) -> Optional[str]:
    if not target_name:
        return None
    select_stmt = f"select max({update_key}) as watermark from {object_name}"
    try:
        raw = sling.run_sling_cli(["conns", "exec", target_name, select_stmt], force_json=True)
        rows = _parse_sling_json_table(raw)
    except Exception as e:
        log.warning(f"Could not read back incremental watermark for {object_name}.{update_key}: {e}")
        return None
    if not rows:
        return None
    value = next(iter(rows[0].values()), None)
    return str(value) if value is not None else None


class SlingReplicationAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Sling YAML replication as Dagster assets via dagster-sling."""

    replication_config_path: str = Field(description="Path to a Sling replication YAML file (relative to project root, or absolute).")
    group_name: Optional[str] = Field(default="sling", description="Asset group applied to every stream in the replication.")
    name: Optional[str] = Field(default="sling_replication", description="Op name for the replication run.")
    emit_incremental_watermarks: bool = Field(
        default=True,
        description="For streams configured with Sling's `mode: incremental` and an `update_key`, query the target table's MAX(update_key) after each materialization and attach it as asset metadata (`incremental_watermark`, `update_key`). Sling has no separate per-stream state file by default -- the target table is its state -- so this surfaces the same watermark value Sling itself would use as the next run's starting point.",
    )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Sling Replication Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_sling import SlingResource, sling_assets, DagsterSlingTranslator
        from dagster_sling.sling_replication import validate_replication

        # Resolve path relative to project root when not absolute.
        cfg_path = Path(self.replication_config_path)
        if not cfg_path.is_absolute():
            root = getattr(context, "path", None) or Path.cwd()
            cfg_path = Path(root) / cfg_path

        # Bail out cleanly when the replication YAML isn't present yet —
        # e.g. running `dg check` before the user has authored it. The
        # component still validates; the assets show up once the file lands.
        if not cfg_path.exists():
            return dg.Definitions(resources={"sling": SlingResource()})

        # Group_name is applied via a subclassed translator so every stream
        # in the replication picks up the same group.
        _group = self.group_name

        class _GroupTranslator(DagsterSlingTranslator):
            def get_group_name(self, stream_definition):  # type: ignore[override]
                return _group

        replication_config = validate_replication(cfg_path)
        target_name = replication_config.get("target")
        _incremental_streams = (
            _incremental_stream_specs(replication_config)
            if self.emit_incremental_watermarks
            else {}
        )

        @sling_assets(
            replication_config=cfg_path,
            name=self.name,
            dagster_sling_translator=_GroupTranslator() if _group else None,
        )
        def _sling_assets(context, sling: SlingResource):
            for result in sling.replicate(context=context):
                stream_name = result.metadata.get("stream_name") if result.metadata else None
                spec = _incremental_streams.get(stream_name) if stream_name else None
                if spec is not None:
                    update_key, object_name = spec
                    watermark = _query_incremental_watermark(
                        sling, target_name, object_name, update_key, context.log
                    )
                    if watermark is not None:
                        result = result._replace(
                            metadata={
                                **result.metadata,
                                "incremental_watermark": watermark,
                                "update_key": update_key,
                            }
                        )
                yield result

        return dg.Definitions(assets=[_sling_assets], resources={"sling": SlingResource()})
