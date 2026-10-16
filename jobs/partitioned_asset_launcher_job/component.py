"""PartitionedAssetLauncherJobComponent.

Launcher job that turns a run-config form (or an external RunConfig POST)
into a *derived* partition key + a partitioned materialization of a
downstream asset selection.

The pattern: user launches this job with owner/repo/issue_number config →
op formats the partition key from `partition_key_template` → registers
the dynamic partition → materializes the target assets in-process with
that partition key set. Two runs in Dagster history: the launcher run
(fast, only registers + kicks off) and the actual materialization run
(the pipeline itself, tagged with the partition key).

Sibling of `python_callable_job` — one YAML, one launcher; but instead
of running an arbitrary Python callable, it launches a partitioned run
of another asset selection.
"""

from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field, create_model


# ── Runtime helpers ──────────────────────────────────────────────────

def _build_config_cls(config_schema: Dict[str, Dict[str, Any]]) -> type:
    """Dynamically build a Pydantic `dg.Config` class from the YAML shape:

        config_schema:
          owner:        {type: str, default: dagster-io}
          repo:         {type: str, default: dagster}
          issue_number: {type: int}

    Fields without a `default` are required — Dagster surfaces them as
    required in the launchpad's config form.
    """
    TYPE_MAP: Dict[str, Any] = {
        "str": str, "string": str,
        "int": int, "integer": int,
        "float": float, "number": float,
        "bool": bool, "boolean": bool,
    }
    fields: Dict[str, Any] = {}
    for name, spec in config_schema.items():
        py_type = TYPE_MAP.get(str(spec.get("type", "str")).lower(), str)
        if "default" in spec:
            fields[name] = (py_type, spec["default"])
        else:
            fields[name] = (py_type, ...)
    return create_model("LauncherConfig", __base__=dg.Config, **fields)



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class PartitionedAssetLauncherJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Launch a partitioned materialization from a run-config form.

    Reads a user-provided run config (owner/repo/issue_number, or whatever
    the caller declares), formats a partition key from
    `partition_key_template`, registers it on a dynamic partitions
    definition, then materializes the target asset selection with that
    partition key.
    """

    job_name: str = Field(
        description="Dagster job name for the launcher itself (shows in the UI as a job you can materialize)."
    )
    target_asset_keys: List[str] = Field(
        description=(
            "Asset keys to materialize in the launched partitioned run. "
            "These assets MUST be dynamic-partitioned on `dynamic_partitions_name` "
            "(typically declared in a sibling AgenticPipelineComponent / other "
            "partitioned-multi-asset component). Multi-part keys are dot-joined: "
            "'foo/bar' → AssetKey(['foo', 'bar'])."
        )
    )
    dynamic_partitions_name: str = Field(
        description=(
            "Name of the DynamicPartitionsDefinition on the target assets. "
            "The launcher registers the computed partition key on this def "
            "before firing the materialization."
        )
    )
    partition_key_template: str = Field(
        description=(
            "Format template composing the partition key from config field "
            "values. e.g. `'{owner}/{repo}#{issue_number}'` — each `{name}` "
            "placeholder maps to a config_schema field. The target asset can "
            "invert this via its own `partition_key_parser:` field to reach "
            "each value as `{partition.owner}` / `{partition.repo}` / etc."
        )
    )
    config_schema: Dict[str, Dict[str, Any]] = Field(
        description=(
            "Config fields exposed on the launcher's launchpad form (and via "
            "run_config for programmatic launches). Shape: "
            "`{field_name: {type: str|int|float|bool, default: <value>}}`. "
            "Fields without `default` are required."
        )
    )
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Tags applied to both the launcher run and (via inherited context) the materialization run."
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=(), csv=(), tags=('tags',))

    @classmethod
    def get_form_config(cls):
        """UI-editable via the Dagster / Dagster+ Components tab
        (dagster>=1.13.8 + `flagComponentInstanceUI`)."""
        from dagster.components.resolved.form_config import ComponentFormConfig

        return ComponentFormConfig(label="Partitioned Asset Launcher", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _job_name = self.job_name
        _target_keys = list(self.target_asset_keys)
        _dyn_name = self.dynamic_partitions_name
        _key_template = self.partition_key_template
        _cfg_schema = dict(self.config_schema)
        _tags = dict(self.tags) if self.tags else None

        ConfigCls = _build_config_cls(_cfg_schema)

        @dg.op(name=f"{_job_name}_op")
        def _launch_op(context: dg.OpExecutionContext, config: ConfigCls):  # type: ignore[valid-type]
            # Format the partition key from config field values.
            try:
                partition_key = _key_template.format(**config.model_dump())
            except KeyError as e:
                raise dg.Failure(
                    description=(
                        f"partition_key_template references {e!r} but that field "
                        f"is not in config_schema. Declared fields: "
                        f"{list(_cfg_schema.keys())}"
                    )
                ) from e

            # Register the dynamic partition (idempotent — Dagster de-dupes).
            context.instance.add_dynamic_partitions(_dyn_name, [partition_key])
            context.log.info(
                f"registered dynamic partition {partition_key!r} on "
                f"partitions_def {_dyn_name!r}"
            )

            # Resolve target asset defs via the repo. This works because
            # the launcher and target components live in the same code
            # location (Definitions).
            repo_def = context.repository_def
            target_asset_keys = {dg.AssetKey.from_user_string(k) for k in _target_keys}
            all_defs = list(repo_def.assets_defs_by_key.values())
            asset_defs = []
            seen_ids = set()
            for ad in all_defs:
                if ad.keys & target_asset_keys:
                    if id(ad) not in seen_ids:
                        asset_defs.append(ad)
                        seen_ids.add(id(ad))
            resolved_keys = {k for ad in asset_defs for k in ad.keys}
            missing = target_asset_keys - resolved_keys
            if missing:
                raise dg.Failure(
                    description=(
                        f"target_asset_keys not found in this code location: "
                        f"{sorted(k.to_user_string() for k in missing)}. "
                        f"Available: {sorted(k.to_user_string() for k in repo_def.assets_defs_by_key.keys())[:20]}..."
                    )
                )

            # Materialize the target assets in-process with the derived
            # partition key. Creates an inner Dagster run tracked in the
            # target asset's history.
            context.log.info(
                f"launching materialization of {len(asset_defs)} asset def(s) "
                f"[{len(resolved_keys)} keys] partition_key={partition_key!r}"
            )
            result = dg.materialize(
                assets=asset_defs,
                partition_key=partition_key,
                instance=context.instance,
                tags={
                    "dagster/launched_by": _job_name,
                    **({} if _tags is None else _tags),
                },
            )
            if not result.success:
                raise dg.Failure(
                    description=(
                        f"target materialization failed for partition "
                        f"{partition_key!r}; see the inner run for details."
                    )
                )
            context.log.info(
                f"materialization succeeded — partition {partition_key!r} complete"
            )

        @dg.job(name=_job_name, tags=_tags)
        def _the_job():
            _launch_op()

        return dg.Definitions(jobs=[_the_job])
