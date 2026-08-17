"""MLflow Workspace Component.

Auto-enumerates MLflow experiments and registered models via the Tracking
REST API. Emits one Dagster asset per experiment (runs metadata) and one
per registered model (versions metadata).

Follows the canonical `workspace: <Resource>` pattern used by
dagster-databricks / dagster-fivetran / dagster-powerbi — secrets travel
inline in the `workspace:` block via `{{ env.XXX }}` Jinja templating, and
the runtime component reads them off `self.workspace.<attr>`.

Aligns with the same convention as `SnowflakeWorkspaceComponent`:
- `@public` class
- `translation:` callable field
- `@public get_asset_spec(props)` hook
- `polling_sensor` (alias `generate_sensor`) opt-in
- `defs_state` + `defs_state_config` property
- `StateBackedComponent` inheritance with `write_state_to_path` +
  `build_defs_from_state`
- `MLflowObjectProps` @record + `DagsterMLflowTranslator` +
  `MLflowComponentTranslator`

Note: The official `dagster-mlflow` package ships only a legacy
`mlflow_tracking` `ResourceDefinition` (no `ConfigurableResource` at time
of writing). We ship a minimal `MLflowResource(ConfigurableResource)`
inline so the workspace: block works with `resolve_fields()` and looks the
same as the Snowflake / Fivetran / Databricks workspaces.
"""

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

import dagster as dg
from dagster import (
    AssetKey,
    AssetSpec,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    Resolver,
)
from dagster._annotations import public
from dagster.components.component.state_backed_component import StateBackedComponent
from dagster.components.resolved.base import resolve_fields
from dagster.components.utils.defs_state import (
    DefsStateConfig,
    DefsStateConfigArgs,
    ResolvedDefsStateConfig,
)
from dagster.components.utils.translation import (
    ComponentTranslator,
    TranslationFn,
    TranslationFnResolver,
    create_component_translator_cls,
)
from dagster_shared.record import record
from pydantic import Field


@record
class MLflowObjectProps:
    """Data passed to translation callables for each imported MLflow object.

    Mirrors the shape of `FivetranConnectorTableProps` / `SnowflakeObjectProps`
    — a single record describing the object so `translation:` callables can
    filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'experiment' / 'registered_model'.
        object_name: The MLflow object's name (experiment name / registered
            model name).
        extra: Kind-specific metadata (experiment_id for experiments, etc.).
    """
    object_kind: str
    object_name: str
    extra: Optional[Dict[str, Any]] = None


class MLflowResource(dg.ConfigurableResource):
    """MLflow workspace connection.

    Mirrors the shape of dagster_databricks.DatabricksWorkspace /
    dagster_fivetran.FivetranWorkspace / dagster_powerbi.PowerBIWorkspace —
    a `ConfigurableResource` holding just the connection fields. Values
    typically arrive via `{{ env.XXX }}` templating from YAML.

    Named `MLflowResource` (not `MLflowWorkspace`) so it matches the
    community `mlflow_resource` component and reads naturally next to the
    official `dagster_snowflake.SnowflakeResource` / `dagster_databricks.
    DatabricksWorkspace` conventions.
    """

    tracking_uri: str = Field(
        description="MLflow tracking server URI, e.g. https://mlflow.acme.com."
    )
    username: Optional[str] = Field(
        default=None, description="Optional basic-auth username."
    )
    password: Optional[str] = Field(
        default=None, description="Optional basic-auth password."
    )
    verify_ssl: bool = Field(
        default=True,
        description="TLS cert verification. Set false for self-signed dev environments.",
    )


# Backward-compat alias — early docs / examples used `MLflowWorkspace` for
# the resource. Keep the old name importable so downstream code that does
# `from ... import MLflowWorkspace` doesn't break.
MLflowWorkspace = MLflowResource


@dataclass
class MLflowSelector(dg.Resolvable):
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
        import fnmatch
        if self.exclude_by_name and name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(name, p) for p in self.by_pattern):
            return True
        return False


def _enumerate_mlflow(tracking_uri, verify_ssl, auth=None) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    if auth:
        session.auth = auth

    out: dict = {"experiments": [], "models": []}
    try:
        r = session.get(
            f"{tracking_uri.rstrip('/')}/api/2.0/mlflow/experiments/search",
            params={"max_results": 1000},
            timeout=60,
        )
        r.raise_for_status()
        for e in (r.json() or {}).get("experiments", []):
            eid = e.get("experiment_id")
            ename = e.get("name")
            if eid and ename:
                out["experiments"].append({"id": eid, "name": ename})
    except Exception:  # noqa: BLE001
        pass
    try:
        r = session.get(
            f"{tracking_uri.rstrip('/')}/api/2.0/mlflow/registered-models/search",
            params={"max_results": 1000},
            timeout=60,
        )
        r.raise_for_status()
        for m in (r.json() or {}).get("registered_models", []):
            mname = m.get("name")
            if mname:
                out["models"].append({"name": mname})
    except Exception:  # noqa: BLE001
        pass
    return out


@public
class MLflowWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Component for importing MLflow experiments and registered models
    as Dagster assets.

    Supports importing:
    - Experiments (as observation assets carrying runs metadata)
    - Registered models (as observation assets carrying model-version rows)

    Example (canonical `workspace:` block, mirrors dagster-databricks):
        ```yaml
        type: dagster_community_components.MLflowWorkspaceComponent
        attributes:
          workspace:
            tracking_uri: "{{ env.MLFLOW_TRACKING_URI }}"
            username: "{{ env.MLFLOW_USER }}"      # optional
            password: "{{ env.MLFLOW_PASSWORD }}"  # optional
            verify_ssl: true
          experiment_selector:
            by_pattern: ["prod_*", "staging_*"]
          model_selector:
            by_pattern: ["*_classifier", "*_regressor"]
        ```
    """

    # ── Connection: workspace: block IS an MLflowResource ─────────────
    # Canonical shape — mirrors dagster-databricks / dagster-fivetran /
    # dagster-powerbi workspace components (all have `workspace: <Resource>`).
    workspace: Annotated[
        MLflowResource,
        Resolver(
            lambda context, model: MLflowResource(
                **resolve_fields(model, MLflowResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "MLflow connection as an MLflowResource (tracking_uri + optional "
            "basic-auth username/password + verify_ssl). Secrets typically "
            "arrive via `{{ env.XXX }}` Jinja templating in defs.yaml."
        ),
    )

    # Optional user-side customization hook. Matches the convention used by
    # FivetranAccountComponent / PowerBIWorkspaceComponent / SnowflakeWorkspaceComponent
    # — a callable that takes (base_spec, props) and returns a modified
    # AssetSpec. Applied to each imported MLflow object; wired via
    # `MLflowComponentTranslator`.
    translation: Annotated[
        Optional[TranslationFn[MLflowObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate MLflow object properties into "
            "Dagster asset specs. Called for each imported experiment / "
            "registered model. If unset, the base translator's default "
            "AssetSpec is used."
        ),
    )

    experiment_selector: Optional[MLflowSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for experiment names.",
    )
    model_selector: Optional[MLflowSelector] = Field(
        default=None,
        description="Optional inclusion/exclusion filter for registered-model names.",
    )
    runs_limit: int = Field(
        default=100,
        description="Max runs fetched per experiment on each materialization.",
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Group name for all imported assets.",
    )
    asset_key_prefix: List[str] = Field(
        default_factory=lambda: ["mlflow"],
        description="Key prefix used for all emitted AssetKeys.",
    )
    compute_kind: str = Field(
        default="mlflow",
        description="Compute kind tag for all imported assets.",
    )

    poll_interval_seconds: int = Field(
        default=60,
        description=(
            "Minimum seconds between MLflow polling-sensor evaluations. Only "
            "consulted when `polling_sensor: true`. Matches Snowflake's "
            "`poll_interval_seconds` convention."
        ),
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a polling sensor that detects new MLflow runs "
            "landing in enumerated experiments and emits AssetObservation "
            "events into Dagster's event log. Useful when MLflow training "
            "jobs are triggered outside Dagster (e.g., a data scientist "
            "manually kicks off `mlflow.start_run()`) and you want the "
            "downstream Dagster graph to react. Matches the `polling_sensor` "
            "convention on FivetranAccountComponent and "
            "SnowflakeWorkspaceComponent. Off by default — the MLflow "
            "workspace has no cheap change-signal, so opt in explicitly."
        ),
        alias="generate_sensor",  # backward-compat: old YAML still resolves
    )

    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Overridden per-deploy for prod runs against Dagster Cloud."
        ),
    )

    @public
    def get_asset_spec(self, props: MLflowObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given MLflow object.

        This method can be overridden in a subclass to customize how MLflow
        objects are converted to Dagster asset specs. By default, it delegates
        to the configured translator (which respects the `translation:` field).

        Args:
            props: The MLflowObjectProps carrying object kind, name, and any
                kind-specific metadata.

        Returns:
            An AssetSpec that represents the MLflow object as a Dagster asset.

        Example:
            Override this method to add custom tags based on the object kind:

            .. code-block:: python

                from dagster_community_components import MLflowWorkspaceComponent

                class CustomMLflowWorkspaceComponent(MLflowWorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "mlflow_object_kind": props.object_kind,
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "MLflowComponentTranslator":
        # Cached lazily so subclasses can still override get_asset_spec cleanly.
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = MLflowComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Key on tracking URI so multiple MLflow servers don't collide in
        # the shared local-filesystem state dir. Hashed to keep the key
        # filesystem-safe.
        uri_hash = hashlib.sha256(self.workspace.tracking_uri.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{uri_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def _apply_translation(
        self,
        kwargs: Dict[str, Any],
        kind: str,
        name: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fold the translation callable into per-asset kwargs.

        Builds an ``MLflowObjectProps`` and calls ``self.get_asset_spec(props)``,
        which delegates to ``MLflowComponentTranslator`` (base spec + optional
        user ``translation:`` callable).

        Backward-compat: when no ``translation:`` callable is set, the base
        translator returns the default AssetSpec and this method is a no-op —
        all pre-existing per-asset kwargs (name, key, group_name, metadata,
        tags, kinds) win. When a ``translation:`` callable IS set, its
        AssetSpec's key / tags / metadata / kinds / owners flow into the
        kwargs (translation-provided values win over inferred ones).
        """
        if self.translation is None:
            return kwargs

        props = MLflowObjectProps(
            object_kind=kind,
            object_name=name,
            extra=extra,
        )
        base_spec = self.get_asset_spec(props)
        merged = dict(kwargs)
        # Translation callable can rename the key. Drop `name=` when key is
        # supplied so `@asset` doesn't reject the double-declaration.
        merged.pop("name", None)
        merged["key"] = base_spec.key
        if base_spec.metadata:
            existing_meta = dict(merged.get("metadata") or {})
            existing_meta.update(base_spec.metadata)
            merged["metadata"] = existing_meta
        if base_spec.tags:
            existing_tags = dict(merged.get("tags") or {})
            existing_tags.update(base_spec.tags)
            merged["tags"] = existing_tags
        if base_spec.kinds:
            existing_kinds = set(merged.get("kinds") or set())
            existing_kinds.update(base_spec.kinds)
            merged["kinds"] = existing_kinds
        if base_spec.owners:
            merged["owners"] = list(base_spec.owners)
        if base_spec.group_name and "group_name" not in kwargs:
            merged["group_name"] = base_spec.group_name
        return merged

    async def write_state_to_path(self, state_path: Path) -> None:
        """Enumerate MLflow experiments + registered models and cache them.

        Runs the same Tracking REST calls that the previous inline discovery
        used, applies the ``experiment_selector`` / ``model_selector`` filters,
        and writes the surviving rows to ``state_path`` as a JSON dict keyed
        by object kind. ``build_defs_from_state`` re-hydrates from this
        snapshot so no MLflow HTTP calls fire at Dagster load time.
        """
        uri = self.workspace.tracking_uri
        auth = None
        if self.workspace.username and self.workspace.password:
            auth = (self.workspace.username, self.workspace.password)
        snapshot = _enumerate_mlflow(uri, self.workspace.verify_ssl, auth)
        if self.experiment_selector is not None:
            snapshot["experiments"] = [
                e for e in snapshot["experiments"]
                if self.experiment_selector.matches(e["name"])
            ]
        if self.model_selector is not None:
            snapshot["models"] = [
                m for m in snapshot["models"]
                if self.model_selector.matches(m["name"])
            ]
        state_path.write_text(json.dumps(snapshot, indent=2))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached MLflow workspace state.

        Reads the JSON dict written by ``write_state_to_path`` and turns each
        experiment/model entry into a materializable ``@asset``. Runtime
        MLflow calls (fetching runs, model versions) still fire on each
        materialization — only the discovery moved to state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state = json.loads(state_path.read_text())
        assets = []
        experiment_asset_keys: List[tuple[str, AssetKey]] = []
        for e in state.get("experiments", []):
            asset_def = self._build_experiment_asset(e["id"], e["name"])
            assets.append(asset_def)
            # Track (experiment_id, asset_key) pairs so the polling sensor can
            # emit AssetObservation events keyed to the right catalog entries.
            key = next(iter(asset_def.keys)) if hasattr(asset_def, "keys") else None
            if key is not None:
                experiment_asset_keys.append((e["id"], key))
        for m in state.get("models", []):
            assets.append(self._build_model_asset(m["name"]))

        sensors = []
        if self.polling_sensor and experiment_asset_keys:
            sensors.append(self._build_runs_polling_sensor(experiment_asset_keys))

        return Definitions(
            assets=assets,
            sensors=sensors if sensors else None,
        )

    def _build_runs_polling_sensor(
        self, experiment_asset_keys: List[tuple[str, "AssetKey"]]
    ):
        """Build a polling sensor that detects new MLflow runs and emits
        AssetObservation events on the enumerated experiment assets.

        Cursor shape: ``{experiment_id: <latest_run_start_time_ms>}``.
        On each tick, ``POST /api/2.0/mlflow/runs/search`` per experiment
        with the cursor value as the filter — new runs since last check
        become one observation per run.
        """
        _self = self
        # Snapshot the mapping so the sensor closure doesn't hold onto
        # component instance state that could change under it at reload.
        exp_keys = list(experiment_asset_keys)

        @dg.sensor(
            name=f"{self.group_name or 'mlflow'}_runs_polling_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
            default_status=dg.DefaultSensorStatus.STOPPED,
        )
        def _mlflow_runs_polling_sensor(context: dg.SensorEvaluationContext):
            import requests
            cursor = json.loads(context.cursor) if context.cursor else {}
            new_cursor = dict(cursor)

            uri = _self.workspace.tracking_uri.rstrip("/")
            session = requests.Session()
            session.verify = _self.workspace.verify_ssl
            if _self.workspace.username and _self.workspace.password:
                session.auth = (_self.workspace.username, _self.workspace.password)

            observations: List[dg.AssetObservation] = []
            for exp_id, asset_key in exp_keys:
                since_ms = int(cursor.get(exp_id, 0))
                # MLflow filter syntax on runs.search — attributes.start_time > <ms>.
                # `order_by` ascending so we can walk chronologically + update the
                # cursor to the newest run seen without leaving gaps.
                body: dict = {
                    "experiment_ids": [exp_id],
                    "max_results": 1000,
                    "order_by": ["attributes.start_time ASC"],
                }
                if since_ms > 0:
                    body["filter"] = f"attributes.start_time > {since_ms}"
                try:
                    r = session.post(
                        f"{uri}/api/2.0/mlflow/runs/search", json=body, timeout=60,
                    )
                    r.raise_for_status()
                    runs = (r.json() or {}).get("runs", [])
                except Exception as e:  # noqa: BLE001
                    context.log.warning(
                        f"MLflow runs.search failed for experiment {exp_id}: {e}"
                    )
                    continue

                latest_seen = since_ms
                for run in runs:
                    info = run.get("info") or {}
                    start_time = int(info.get("start_time") or 0)
                    status = info.get("status")
                    run_id = info.get("run_id")
                    if start_time <= since_ms:
                        continue
                    observations.append(
                        dg.AssetObservation(
                            asset_key=asset_key,
                            metadata={
                                "mlflow_run_id": dg.MetadataValue.text(str(run_id or "")),
                                "mlflow_run_status": dg.MetadataValue.text(str(status or "")),
                                "mlflow_run_start_time_ms": dg.MetadataValue.int(start_time),
                                "mlflow_experiment_id": dg.MetadataValue.text(exp_id),
                            },
                        )
                    )
                    if start_time > latest_seen:
                        latest_seen = start_time
                if latest_seen > since_ms:
                    new_cursor[exp_id] = latest_seen

            if not observations:
                return dg.SensorResult(
                    skip_reason=dg.SkipReason(
                        "No new MLflow runs observed across enumerated experiments."
                    ),
                    cursor=json.dumps(new_cursor),
                )
            return dg.SensorResult(
                asset_events=observations,
                cursor=json.dumps(new_cursor),
            )

        return _mlflow_runs_polling_sensor

    def _build_experiment_asset(self, exp_id: str, exp_name: str):
        _self = self
        safe = "".join(c if c.isalnum() or c == "_" else "_" for c in exp_name)[:40] or exp_id
        key = AssetKey([*self.asset_key_prefix, "experiment", safe])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={
                "mlflow_experiment_id": dg.MetadataValue.text(exp_id),
                "mlflow_experiment_name": dg.MetadataValue.text(exp_name),
            },
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="experiment",
            name=exp_name,
            extra={"experiment_id": exp_id},
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            try:
                import pandas as pd
                import requests
            except ImportError as e:
                raise Exception("pandas or requests library not installed") from e
            uri = _self.workspace.tracking_uri
            session = requests.Session()
            session.verify = _self.workspace.verify_ssl
            if _self.workspace.username and _self.workspace.password:
                session.auth = (_self.workspace.username, _self.workspace.password)

            r = session.post(
                f"{uri.rstrip('/')}/api/2.0/mlflow/runs/search",
                json={"experiment_ids": [exp_id], "max_results": _self.runs_limit},
                timeout=60,
            )
            r.raise_for_status()
            runs = (r.json() or {}).get("runs", [])
            rows = []
            for run in runs:
                info = run.get("info") or {}
                data = run.get("data") or {}
                row = {
                    "run_id": info.get("run_id"),
                    "status": info.get("status"),
                    "start_time": info.get("start_time"),
                    "end_time": info.get("end_time"),
                    "artifact_uri": info.get("artifact_uri"),
                }
                for m in data.get("metrics", []):
                    row[f"metric_{m.get('key')}"] = m.get("value")
                for p in data.get("params", []):
                    row[f"param_{p.get('key')}"] = p.get("value")
                rows.append(row)
            df = pd.DataFrame(rows)
            context.add_output_metadata({"row_count": len(df), "experiment": exp_name})
            return df

        return _asset

    def _build_model_asset(self, model_name: str):
        _self = self
        safe = "".join(c if c.isalnum() or c == "_" else "_" for c in model_name)[:60] or "model"
        key = AssetKey([*self.asset_key_prefix, "model", safe])

        base_kwargs: Dict[str, Any] = dict(
            key=key,
            group_name=self.group_name,
            compute_kind=self.compute_kind,
            metadata={"mlflow_model_name": dg.MetadataValue.text(model_name)},
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind="registered_model",
            name=model_name,
            extra=None,
        )

        @dg.asset(**asset_kwargs)
        def _asset(context: dg.AssetExecutionContext):
            try:
                import pandas as pd
                import requests
            except ImportError as e:
                raise Exception("pandas or requests library not installed") from e
            uri = _self.workspace.tracking_uri
            session = requests.Session()
            session.verify = _self.workspace.verify_ssl
            if _self.workspace.username and _self.workspace.password:
                session.auth = (_self.workspace.username, _self.workspace.password)
            r = session.get(
                f"{uri.rstrip('/')}/api/2.0/mlflow/model-versions/search",
                params={"filter": f"name = '{model_name}'", "max_results": 1000},
                timeout=60,
            )
            r.raise_for_status()
            versions = (r.json() or {}).get("model_versions", [])
            df = pd.DataFrame(versions)
            context.add_output_metadata({"row_count": len(df), "model": model_name})
            return df

        return _asset


class DagsterMLflowTranslator:
    """Base translator for MLflow workspace objects → AssetSpec.

    Follows the shape of `DagsterFivetranTranslator` / `DagsterPowerBITranslator` /
    `DagsterSnowflakeTranslator`. Subclass this and override `get_asset_spec()`
    to fully customize how MLflow objects become Dagster assets — an
    alternative to the runtime `translation:` callable on the component.
    """

    def get_asset_spec(self, props: MLflowObjectProps) -> AssetSpec:
        """Default AssetSpec for an MLflow object.

        Key = ["mlflow", <object_kind>, <object_name>] (lowercased for
        consistency with the rest of the Dagster catalog). Kind is set to
        the MLflow object type. Metadata carries the object kind + name.
        """
        return AssetSpec(
            key=AssetKey(["mlflow", props.object_kind, props.object_name.lower()]),
            kinds={"mlflow", props.object_kind},
            metadata={
                "mlflow/object_kind": props.object_kind,
                "mlflow/object_name": props.object_name,
            },
        )


class MLflowComponentTranslator(
    create_component_translator_cls(MLflowWorkspaceComponent, DagsterMLflowTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[MLflowWorkspaceComponent],
):
    """Bridges `MLflowWorkspaceComponent.translation` (runtime callable)
    with the base `DagsterMLflowTranslator` (class-level override).

    Mirrors `FivetranComponentTranslator` / `PowerBIComponentTranslator` /
    `SnowflakeComponentTranslator`.
    """

    def __init__(self, component: "MLflowWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: MLflowObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
