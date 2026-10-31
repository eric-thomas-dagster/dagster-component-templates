"""Microsoft Fabric Workspace integration component.

Imports a Fabric workspace's items as Dagster external assets. Uses the
Fabric REST API (`https://api.fabric.microsoft.com/v1`) and Microsoft
Entra ID OAuth via a service principal (client_id / client_secret /
tenant_id) or ``DefaultAzureCredential``.

Items imported:
  - Lakehouse       (observable_source_asset)
  - Warehouse       (observable_source_asset)
  - Notebook        (materializable @asset that triggers a notebook job)
  - DataPipeline    (materializable @asset that triggers the pipeline)
  - Dataflow Gen2   (materializable @asset that refreshes the dataflow)
  - SemanticModel   (observable_source_asset)
  - Report          (observable_source_asset)

Aligns with the canonical `workspace:` convention shared by
``SnowflakeWorkspaceComponent`` / ``MLflowWorkspaceComponent`` /
``dagster_databricks.DatabricksWorkspaceComponent`` /
``dagster_powerbi.PowerBIWorkspaceComponent``:

- ``@public`` class
- ``translation:`` callable field
- ``@public get_asset_spec(props)`` hook
- ``polling_sensor`` (alias ``generate_sensor``) opt-in
- ``defs_state`` + ``defs_state_config`` property
- ``StateBackedComponent`` inheritance with ``write_state_to_path`` +
  ``build_defs_from_state`` -- Fabric REST enumeration lives entirely in
  the state-write path so no HTTP fires at Dagster load time
- ``FabricObjectProps`` @record + ``DagsterFabricTranslator`` +
  ``FabricComponentTranslator``

Fabric shares the Microsoft Entra auth stack with Power BI --
``FabricResource`` accepts the same service-principal triple
(``client_id`` / ``client_secret`` / ``tenant_id``) as
``dagster_powerbi.PowerBIServicePrincipal``. A future PR could collapse
these onto a shared ``EntraServicePrincipal`` resource; today we keep
them separate so ``FabricResource`` can be installed without pulling in
the Power BI SDK.

Reference: https://learn.microsoft.com/rest/api/fabric/
"""

import hashlib
import json
import time
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
from pydantic import ConfigDict, Field


_FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_ONELAKE_STORAGE_SCOPE = "https://storage.azure.com/.default"
_ONELAKE_DFS_BASE = "https://onelake.dfs.fabric.microsoft.com"

# Fabric item types we know how to import. Lakehouse/Warehouse/SemanticModel/
# Report become observable source assets; Notebook/DataPipeline/Dataflow
# become materializable trigger assets.
_RUNNABLE_ITEM_TYPES = {"Notebook", "DataPipeline", "Dataflow"}
_DATASET_ITEM_TYPES = {"Lakehouse", "Warehouse", "SemanticModel", "Report"}

# Fabric's own Job Scheduler status enum (verified against the current REST
# API docs, learn.microsoft.com/rest/api/fabric/core/job-scheduler/
# get-item-job-instance): NotStarted, InProgress, Completed, Failed,
# Cancelled, Deduped. All but the first two are terminal. "Deduped" means
# another instance of the same job type was already running and this one
# was skipped -- it is NOT the same as a normal completion, and was
# previously treated as "still in progress" here, so a run that collided
# with an already-running instance just polled until it hit the timeout.


def _infer_fabric_parameter_type(value: Any) -> str:
    """Map a plain Python value to one of Fabric's documented
    ItemJobParameterType values (Integer / Number / Text / Boolean --
    DateTime/Guid/VariableReference/Automatic are also valid but not
    inferable from a bare Python value)."""
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Integer"
    if isinstance(value, float):
        return "Number"
    return "Text"


def _to_fabric_parameters(params: Optional[Dict[str, Any]]) -> Optional[List[Dict[str, Any]]]:
    """Convert a plain {name: value} dict into Fabric's documented Job
    Scheduler request shape: a top-level `parameters` list of
    {name, value, type} objects. Used for DataPipeline/Dataflow and any
    other non-Notebook item type -- see `_to_notebook_execution_data` for
    Notebook, which needs a DIFFERENT shape (corrected below).

    CORRECTION to an earlier (round-1) claim in this file's history: this
    top-level `Parameter[]` list is NOT the same shape used across every
    item type -- that was wrong. The generic REST reference
    (learn.microsoft.com/rest/api/fabric/core/job-scheduler/run-on-demand-item-job)
    only shows this shape in its generic/"Execute" job-type examples, and
    separately documents `executionData` as "fixed static data, defined by
    the specific item job type" -- i.e. genuinely per-item-type. Confirmed
    via TWO independent official Microsoft sources for Notebook specifically:
    Microsoft's own apache-airflow-providers-microsoft-fabric ships a
    dedicated `MSFabricNotebookJobParameters` dataclass that sends ONLY
    `{"executionData": {"parameters": {<name>: {"value": ..., "type": ...}}}}`
    (a dict keyed by name, never the top-level list) for RunNotebook; the
    official `fab` CLI's own job-run docs show the identical inner shape.
    """
    if not params:
        return None
    return [
        {"name": str(k), "value": v, "type": _infer_fabric_parameter_type(v)}
        for k, v in params.items()
    ]


def _infer_notebook_parameter_type(value: Any) -> str:
    """Map a plain Python value to a Fabric Notebook executionData parameter
    type: 'string' | 'int' | 'float' | 'bool' -- a DIFFERENT, lowercase
    vocabulary from `_infer_fabric_parameter_type`'s ItemJobParameterType
    (Text/Boolean/Number/Integer), confirmed via Microsoft's own
    `MSFabricNotebookJobParameters` dataclass and the `fab` CLI's job-run
    docs (both independently show this exact lowercase vocabulary for
    Notebook's own executionData.parameters, not the generic Parameter[]
    enum)."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "string"


def _to_notebook_execution_data(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Convert a plain {name: value} dict into the executionData shape a
    Fabric Notebook's RunNotebook job actually reads, per Microsoft's own
    `MSFabricNotebookJobParameters`/`fab` CLI job-run docs:

        {"executionData": {"parameters": {<name>: {"value": ..., "type": ...}}}}

    NOT the generic top-level `parameters: Parameter[]` list `_to_fabric_parameters`
    builds for DataPipeline/Dataflow -- that shape is for item types that read
    the generic job-parameter mechanism; a Notebook reads its own parameters
    out of `executionData` instead, with its own (lowercase) type vocabulary.
    """
    if not params:
        return None
    return {
        "parameters": {
            str(k): {"value": v, "type": _infer_notebook_parameter_type(v)}
            for k, v in params.items()
        }
    }


_RETRIABLE_KEYWORDS = ("capacity", "throttl", "toomanyrequests", "paused", "suspended")
_AUTH_KEYWORDS = ("unauthorized", "forbidden", "insufficientprivileges", "authentication", "accountisdisabled")


def _looks_retriable(error_code: str, message: str) -> bool:
    """Heuristic fallback ONLY used when Fabric's own `failureReason.isRetriable`
    flag is absent (None) -- when Fabric tells us directly, that's authoritative
    and this is never consulted. Matches the same capacity/throttle vocabulary
    the original gap report suggested as a fallback for exactly this case."""
    text = f"{error_code} {message}".lower()
    return any(k in text for k in _RETRIABLE_KEYWORDS)


def _classify_fabric_failure(
    status: str,
    error_code: str,
    message: str,
    is_retriable: Optional[bool],
    http_status_code: Optional[int] = None,
) -> str:
    """Best-effort, honest failure classification for an "at a glance" label.

    Four buckets, not five: "auth" and "infrastructure" (capacity/throttling,
    confirmed either by Fabric's own isRetriable flag or the same keyword
    heuristic as _looks_retriable) and "collision" (Deduped) are all
    distinguishable from real signals. A fifth "fabric platform bug" vs.
    "the notebook's own code/data" split was also requested, but errorCode
    alone doesn't reliably tell those apart without a verified, comprehensive
    Fabric error-code catalog this component doesn't have -- rather than
    fabricate that distinction, both land in "other" with the real errorCode/
    message still attached so a human can tell the difference themselves.
    The one case this component CAN confidently call "data" is a notebook's
    own exit-value signal (see `data_failure_exit_values`), not a generic
    Failed/Cancelled status.

    `http_status_code` is only available on the initial trigger-request path
    (the polled job-status path has no HTTP status of its own, only Fabric's
    job-level status string) -- a 401/403 there is authoritative for "auth"
    even when Fabric's errorCode doesn't match the keyword list below.
    """
    if status == "Deduped":
        return "collision"
    if http_status_code in (401, 403):
        return "auth"
    text = f"{error_code} {message}".lower()
    if any(k in text for k in _AUTH_KEYWORDS):
        return "auth"
    if is_retriable or _looks_retriable(error_code, message):
        return "infrastructure"
    return "other"


def _extract_fabric_error(resp) -> Dict[str, Any]:
    """Best-effort parse of Fabric's documented ErrorResponse shape
    (errorCode / message / isRetriable / moreDetails) from a non-2xx
    response. Falls back to the raw response text if the body isn't JSON
    or doesn't match the expected shape."""
    try:
        body = resp.json()
    except Exception:
        return {"errorCode": "", "message": resp.text[:300], "isRetriable": None}
    if isinstance(body, dict):
        return {
            "errorCode": body.get("errorCode") or "",
            "message": body.get("message") or resp.text[:300],
            "isRetriable": body.get("isRetriable"),
        }
    return {"errorCode": "", "message": resp.text[:300], "isRetriable": None}


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Same shared shape as `databricks_clean_room_notebook_run`/`snowflake_task`/
    the other ~670 components across this repo carrying this helper (verified
    before assuming a bespoke daily-only field was needed here -- the repo
    already has a mature, tested, multi-axis convention for exactly this).

    `end_offset`/`timezone` are real, documented kwargs on every time-window
    partitions definition (Daily/Weekly/Monthly/Hourly/TimeWindow -- verified
    via `inspect.signature` before adding these, not assumed). Without them,
    a daily set only ever holds *completed* UTC days -- a notebook meant to
    run every 15 minutes under an eager AutomationCondition and process
    *today* would instead re-run yesterday's partition with yesterday's data
    (component-feedback: "partitioned this way it would re-run yesterday's
    partition with yesterday's load date").

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi/static without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
                end_offset=_end_offset, timezone=_timezone,
            )
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(
            start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule,
            end_offset=end_offset, timezone=timezone,
        )
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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


@record
class FabricObjectProps:
    """Data passed to translation callables for each imported Fabric object.

    Mirrors the shape of ``SnowflakeObjectProps`` / ``MLflowObjectProps`` --
    a single record describing the object plus its parent workspace so
    ``translation:`` callables can filter, rename, add tags, etc.

    Attributes:
        object_kind: One of 'lakehouse' / 'warehouse' / 'notebook' /
            'data_pipeline' / 'dataflow' / 'semantic_model' / 'report'.
        object_name: The Fabric item's display name.
        workspace_id: The Fabric workspace GUID the item lives in.
        extra: Kind-specific metadata (item_id, item type as returned by
            the Fabric REST API, description, workspace metadata, etc.).
    """
    object_kind: str
    object_name: str
    workspace_id: str
    extra: Optional[Dict[str, Any]] = None


class FabricResource(dg.ConfigurableResource):
    """Microsoft Fabric workspace connection.

    Shape mirrors ``dagster_powerbi.PowerBIServicePrincipal`` -- same Entra
    ID auth surface (client_id / client_secret / tenant_id), same
    ``login.microsoftonline.com`` token exchange. If any of the three
    service-principal fields are unset, falls back to
    ``DefaultAzureCredential`` (env vars, Azure CLI, managed identity,
    etc.) so local dev "just works" via `az login`.

    Named ``FabricResource`` (not ``FabricWorkspace``) so it reads
    naturally alongside the community ``mlflow_resource`` /
    ``snowflake_resource`` components and doesn't shadow the
    ``workspace:`` field on the component itself.
    """

    workspace_id: str = Field(
        description="Fabric workspace GUID (from Fabric portal, workspace settings).",
    )
    tenant_id: Optional[str] = Field(
        default=None,
        description="Entra tenant ID for service-principal auth. When set with client_id + client_secret, uses ClientSecretCredential. When any of the three is missing, falls back to DefaultAzureCredential.",
    )
    client_id: Optional[str] = Field(
        default=None,
        description="Entra application (service principal) client ID.",
    )
    client_secret: Optional[str] = Field(
        default=None,
        description="Entra application (service principal) client secret.",
    )
    max_wait_seconds: int = Field(
        default=1800,
        description="Max wait when polling a triggered notebook/pipeline/dataflow job for completion.",
    )
    poll_interval_seconds: int = Field(
        default=15,
        description="Poll interval when waiting on a triggered job.",
    )

    def _credential(self):
        try:
            from azure.identity import ClientSecretCredential, DefaultAzureCredential
        except ImportError as e:
            raise ImportError(
                "azure-identity is required for FabricResource -- install with `pip install azure-identity`."
            ) from e
        if self.tenant_id and self.client_id and self.client_secret:
            return ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
        return DefaultAzureCredential()

    def get_token(self) -> str:
        return self._credential().get_token(_FABRIC_SCOPE).token

    def get_storage_token(self) -> str:
        """Separate OAuth scope for OneLake file reads (the ADLS Gen2-compatible
        API), distinct from the Fabric API's own `_FABRIC_SCOPE`. Verified
        against OneLake's own docs (learn.microsoft.com/fabric/onelake/
        onelake-access-api): OneLake only accepts tokens in the Storage
        audience, i.e. `https://storage.azure.com/.default`."""
        return self._credential().get_token(_ONELAKE_STORAGE_SCOPE).token


# Backward-compat alias -- the resource may also be referenced as
# ``FabricWorkspace`` (matches ``PowerBIWorkspace`` naming).
FabricWorkspace = FabricResource



# BEGIN upstream-asset-key normalization (inlined per component; keep copies identical — see tests/test_upstream_keys.py)
from typing import List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def normalize_upstream_keys(single, plural=None) -> List[str]:
    """Merge ``upstream_asset_key`` / ``upstream_asset_keys`` into one clean list.

    Each argument may be a str, a list/tuple of str, or None. Entries are
    stripped, empties dropped, and duplicates removed (order preserved).
    """
    out: List[str] = []
    for value in (single, plural):
        if value is None:
            continue
        items = [value] if isinstance(value, str) else value
        if not isinstance(items, (list, tuple)):
            raise ValueError(
                f"upstream asset key(s) must be a string or a list of strings, got {value!r}"
            )
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"upstream asset keys must be strings, got {item!r}")
            item = item.strip()
            if item and item not in out:
                out.append(item)
    return out


def coerce_upstream_input(data, *, component: str, mode: str, optional: bool = True):
    """Rewrite raw component input so both spellings/shapes land on the declared field(s).

    mode "single": declared field is ``upstream_asset_key`` (str); >1 key is an error.
    mode "multi":  declared field is ``upstream_asset_keys`` (list).
    mode "both":   both fields are declared; everything is merged into the plural one.
    """
    if not isinstance(data, dict):
        return data
    if "upstream_asset_key" not in data and "upstream_asset_keys" not in data:
        return data
    keys = normalize_upstream_keys(data.get("upstream_asset_key"), data.get("upstream_asset_keys"))
    out = {k: v for k, v in data.items() if k not in ("upstream_asset_key", "upstream_asset_keys")}
    if mode == "single":
        if len(keys) > 1:
            raise ValueError(f"{component} takes exactly one upstream asset, got {keys}")
        out["upstream_asset_key"] = keys[0] if keys else (None if optional else "")
    else:
        out["upstream_asset_keys"] = keys or (None if optional else [])
    return out
# END upstream-asset-key normalization


@public
class FabricWorkspaceComponent(StateBackedComponent, Model, Resolvable):
    """Import a Microsoft Fabric workspace's items as Dagster assets.

    Discovery runs once at state-write time and caches the workspace
    contents to disk. On subsequent Dagster loads no Fabric REST calls
    fire until an asset is materialized. Runtime triggering
    (notebook/pipeline/dataflow jobs) still happens inside asset compute
    functions.

    Example (canonical `workspace:` block, mirrors dagster-powerbi):
        ```yaml
        type: dagster_community_components.FabricWorkspaceComponent
        attributes:
          workspace:
            workspace_id: "{{ env.FABRIC_WORKSPACE_ID }}"
            tenant_id:     "{{ env.AZURE_TENANT_ID }}"
            client_id:     "{{ env.AZURE_CLIENT_ID }}"
            client_secret: "{{ env.AZURE_CLIENT_SECRET }}"
          import_lakehouses: true
          import_warehouses: true
          import_notebooks:  true
          import_pipelines:  true
        ```
    """

    # `polling_sensor` carries `alias="generate_sensor"` below for backcompat
    # with the pre-rename field name -- populate_by_name lets BOTH the current
    # field name and the old alias validate. Without it, Pydantic accepts only
    # the alias and rejects the current field name as an unknown extra input.
    model_config = ConfigDict(populate_by_name=True)

    workspace: Annotated[
        FabricResource,
        Resolver(
            lambda context, model: FabricResource(
                **resolve_fields(model, FabricResource, context)  # ty: ignore[invalid-argument-type]
            ),
        ),
    ] = Field(
        description=(
            "Fabric connection as a FabricResource (workspace_id + optional "
            "tenant_id/client_id/client_secret for service-principal auth). "
            "Secrets typically arrive via `{{ env.XXX }}` Jinja templating "
            "in defs.yaml. Falls back to DefaultAzureCredential when the "
            "service-principal triple is unset."
        ),
    )

    translation: Annotated[
        Optional[TranslationFn[FabricObjectProps]],
        TranslationFnResolver(template_vars_for_translation_fn=lambda data: {"props": data}),
    ] = Field(
        default=None,
        description=(
            "Function used to translate Fabric object properties into "
            "Dagster asset specs. Called for each imported lakehouse / "
            "warehouse / notebook / pipeline / dataflow / semantic model / "
            "report. If unset, the base translator's default AssetSpec is used."
        ),
    )

    import_lakehouses: bool = Field(default=True)
    import_warehouses: bool = Field(default=True)
    import_notebooks: bool = Field(
        default=False,
        description="Notebooks become materializable @assets -- materializing them runs the notebook job.",
    )
    import_pipelines: bool = Field(
        default=True,
        description="Data Pipelines become materializable @assets.",
    )
    import_dataflows: bool = Field(
        default=False,
        description="Dataflow Gen2 become materializable @assets.",
    )
    import_semantic_models: bool = Field(default=False)
    import_reports: bool = Field(default=False)

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex applied to Fabric item display names for inclusion.",
    )
    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex applied to Fabric item display names for exclusion.",
    )
    include_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Explicit, exact-match allow-list of item display names "
            "(case-insensitive) -- bypasses filter_by_name_pattern entirely "
            "when set, the same include_names convention as "
            "snowflake_workspace. Use this when you know the real list of "
            "item names and don't want to guess at (or wait on confirmation "
            "of) a naming convention a regex would have to match. "
            "exclude_name_pattern still applies on top, as a safety veto."
        ),
    )
    filter_by_description_pattern: Optional[str] = Field(
        default=None,
        description=(
            "Regex applied to a Fabric item's own description field (same "
            "filter_by_comment_pattern convention as snowflake_workspace, "
            "adapted to Fabric's equivalent metadata field) -- a "
            "deterministic alternative to filter_by_name_pattern for teams "
            "that annotate cadence/ownership/tier via item descriptions "
            "rather than naming convention."
        ),
    )

    group_name: str = Field(default="fabric")
    upstream_asset_keys: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Asset keys that all imported assets wait for (lineage-only).",
    )

    key: Optional[str] = Field(
        default=None,
        description=(
            "Disambiguator for the cached discovery-state key. Without this, two "
            "FabricWorkspaceComponent instances pointed at the SAME workspace_id but "
            "different import_*/filter settings (e.g. one importing only notebooks "
            "matching 'part_a', another only pipelines) would hash to the same state "
            "key and silently share one state file. Set a distinct `key:` per instance "
            "when you split one workspace across multiple component instances."
        ),
    )

    default_parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Parameters passed to every triggered Notebook/DataPipeline/Dataflow job "
            "(e.g. a load date), merged with `item_parameters` below. A plain "
            "{name: value} dict -- converted internally to Fabric's documented "
            "Parameter[] request shape ({name, value, type}, type inferred from the "
            "Python value). Not all item/job types accept parameters -- Fabric's own "
            "API returns errorCode FeatureNotAvailable for ones that don't, surfaced "
            "as a clear Failure rather than a generic HTTP error."
        ),
    )
    item_parameters: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Per-item parameter overrides, keyed by the Fabric item's display name "
            "(e.g. {'my_notebook': {'load_date': '2024-01-01'}}). Merged OVER "
            "default_parameters for that specific item -- lets one notebook wait for "
            "a different input than another from the same component instance."
        ),
    )
    execution_data: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Raw executionData sent on every triggered job -- Fabric's own term for "
            "'fixed static data, defined by the specific item job type' (confirmed via "
            "the Job Scheduler REST reference: executionData and parameters can be sent "
            "TOGETHER in the same request). Unlike default_parameters/item_parameters, "
            "this is NOT converted to any shape -- sent exactly as given, merged into "
            "whatever executionData object is otherwise built for the item type (e.g. "
            "merged alongside a Notebook's own auto-built executionData.parameters). "
            "Needed for job types whose required config isn't exposed as a dedicated "
            "field yet -- e.g. Spark Job Definition's executableFile/mainClass/"
            "commandLineArguments/additionalLibraryUris/defaultLakehouseId/environmentId "
            "(confirmed via learn.microsoft.com/rest/api/fabric/sparkjobdefinition/"
            "background-jobs/run-on-demand-spark-job-definition), though note Spark Job "
            "Definition ALSO uses a different URL path "
            "(/sparkJobDefinitions/{id}/jobs/sparkjob/instances, not /items/{id}/jobs/...) "
            "that this component doesn't build yet -- full native support for triggering "
            "Spark Job Definition/DBT Job/Copy Job/Materialized Lake Views items isn't "
            "implemented; DBT Job and Copy Job's exact job-type strings also haven't been "
            "confirmed against a primary Fabric doc page, only an unverified third-party "
            "source."
        ),
    )
    item_execution_data: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description="Per-item executionData overrides, keyed by Fabric display name (same convention as item_parameters). Merged OVER execution_data for that specific item.",
    )
    load_date_parameter: Optional[str] = Field(
        default=None,
        description=(
            "If set, injects today's date (in load_date_timezone, ISO format) as this "
            "parameter name into every triggered job, alongside default_parameters/"
            "item_parameters. Override for one run via the 'fabric/load_date_override' "
            "run tag (e.g. for a manual backfill), recorded in run history as a plain tag. "
            "If this is also set to the SAME name as partition_parameter_name on a "
            "partitioned run, the partition key wins over 'today' -- e.g. a backfill of "
            "partition 2026-01-03 sends load_date=2026-01-03, not today's date. Without "
            "this collision (different names, or no partition_parameter_name), "
            "load_date_parameter always computes 'today' as before. The override run "
            "tag still wins over both."
        ),
    )
    load_date_timezone: str = Field(
        default="UTC",
        description="IANA timezone name used to compute 'today' for load_date_parameter.",
    )

    max_retries: int = Field(
        default=1,
        description="Max retries requested for a Fabric failure Fabric itself flags as retriable (failureReason.isRetriable, or a capacity/throttling keyword match as a fallback when that flag is absent).",
    )
    retry_backoff_seconds: int = Field(
        default=30,
        description="Base seconds to wait before a retriable-failure retry. Grows exponentially per attempt (retry_backoff_seconds * 2^attempt) when the run's own retry_number is available.",
    )

    pool: Optional[str] = Field(
        default=None,
        description="Dagster concurrency pool name applied to every runnable (Notebook/DataPipeline/Dataflow) asset -- prevents overlapping runs of the same Fabric item. Overridable per item via item_pools.",
    )
    item_pools: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "Per-item concurrency pool overrides, keyed by the Fabric item's display "
            "name (same convention as item_parameters). Overrides `pool` for that "
            "specific item -- e.g. to give one notebook its own limit distinct from "
            "the rest of the workspace instead of sharing a single component-wide pool."
        ),
    )

    lakehouse_id: Optional[str] = Field(
        default=None,
        description=(
            "OneLake item (GUID) to read exit_value_file from. Required when "
            "exit_value_file is set. A Notebook has no OneLake storage of its own -- "
            "notebookutils.fs resolves relative paths against its attached DEFAULT "
            "Lakehouse, not the notebook item itself -- so exit_value_file must name "
            "a real storage-bearing item (a Lakehouse or Warehouse), not the "
            "triggered item's own id."
        ),
    )
    exit_value_file: Optional[str] = Field(
        default=None,
        description=(
            "Relative OneLake path under lakehouse_id's Files/ root (e.g. "
            "'exit_value.txt', or 'exit_values/{load_date}.txt' when load_date_parameter "
            "is also set) that the notebook ALSO writes its notebookutils.notebook.exit(...) "
            "value to. Read after the job reports Completed, via OneLake's ADLS Gen2-compatible "
            "API (a separate 'https://storage.azure.com/.default' OAuth scope from the "
            "Fabric API itself) -- the Job Scheduler API has no way to return this value "
            "directly, so a Completed job with no actual rows processed otherwise looks "
            "identical to a real success. A read failure (auth, 404, or anything else) "
            "raises a real dg.Failure rather than silently skipping the data-failure check -- "
            "this is deliberately fail-closed, since the entire point of configuring this "
            "field is to catch bad data that would otherwise pass as a green materialization."
        ),
    )
    data_failure_exit_values: Optional[List[str]] = Field(
        default=None,
        description=(
            "If exit_value_file's content exactly matches one of these strings (e.g. "
            "'no_rows_processed'), an otherwise-Completed job is raised as a data-quality "
            "dg.Failure instead of succeeding silently. Only used when exit_value_file is set."
        ),
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None. Same canonical shape used across this repo's other ~670 partitioned components. Applies to every runnable (Notebook/DataPipeline/Dataflow) asset built by this component instance.",
    )
    partition_start: Optional[str] = Field(default=None, description="Required for time-based partition_type values (ISO date, e.g. '2024-01-01').")
    partition_values: Optional[Union[str, List[str]]] = Field(default=None, description="Comma-separated or list values for partition_type='static'/'multi'.")
    dynamic_partition_name: Optional[str] = Field(default=None, description="Required when partition_type='dynamic'.")
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partitioning as a list of per-dimension specs, instead of the flat partition_type fields. Set one or the other, not both.",
    )
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.",
    )
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window (e.g. today's partition for a "
            "notebook meant to run intraday under an eager AutomationCondition). "
            "Overridable per item via item_partitions."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default). Overridable per item via item_partitions.",
    )
    partition_parameter_name: Optional[str] = Field(
        default=None,
        description=(
            "When set (and the asset is partitioned), injects the run's partition key "
            "as this Fabric parameter name on every triggered job, merged with "
            "default_parameters/item_parameters (those take precedence if they also "
            "set the same key). Mirrors the same partition-key-to-pipeline-parameter "
            "convention as azure_data_factory. If this is also set to the SAME name as "
            "load_date_parameter, the partition key wins as the load date instead of "
            "'today' -- see load_date_parameter. An unpartitioned downstream asset "
            "still waits on ALL partitions to materialize before it's considered up "
            "to date -- standard Dagster partition semantics, not specific to this "
            "component. Overridable per item via item_partitions."
        ),
    )
    item_partitions: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Per-item partitioning overrides, keyed by the Fabric item's display name "
            "(same convention as item_parameters/item_pools). When an item has an "
            "entry here, it FULLY REPLACES the component-wide partition_type/"
            "partition_start/partition_values/dynamic_partition_name/"
            "partition_dimensions/partition_cron_schedule/partition_cron_fmt/"
            "partition_parameter_name fields for that item -- including explicitly "
            "un-partitioning it (an empty dict, or one with no partition_type) even "
            "when the component-wide partition_type IS set. Items with no entry here "
            "fall back to the component-wide fields. Lets one live notebook be daily-"
            "partitioned without forcing every other item from the same component "
            "instance (a static-per-workspace collector, an unpartitioned semantic "
            "model) to inherit the same partitions_def."
        ),
    )

    polling_sensor: bool = Field(
        default=False,
        description=(
            "If true, adds a polling sensor that detects new Fabric item job "
            "completions and emits AssetObservation events. Matches the "
            "`polling_sensor` convention on FivetranAccountComponent and "
            "SnowflakeWorkspaceComponent. Off by default -- opt in explicitly."
        ),
        alias="generate_sensor",
    )
    sensor_check_interval_seconds: int = Field(
        default=300,
        description=(
            "Seconds between polling-sensor ticks. Only used when polling_sensor=True. "
            "Distinct from workspace.poll_interval_seconds, which governs the in-asset "
            "trigger-and-wait loop for runs Dagster itself kicks off."
        ),
    )

    defs_state: ResolvedDefsStateConfig = Field(
        default_factory=DefsStateConfigArgs.local_filesystem,
        description=(
            "State backend for cached workspace discovery. Local filesystem by "
            "default. Overridden per-deploy for prod runs against Dagster Cloud."
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="multi", optional=True)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, csv=("partition_values",))

    @model_validator(mode="after")
    def _validate_exit_value_config(self):
        if self.exit_value_file and not self.lakehouse_id:
            raise ValueError(
                "lakehouse_id is required when exit_value_file is set. A Notebook has "
                "no OneLake storage of its own -- name the Lakehouse/Warehouse item "
                "the notebook actually writes exit_value_file to."
            )
        return self

    @public
    def get_asset_spec(self, props: FabricObjectProps) -> AssetSpec:
        """Generates an AssetSpec for a given Fabric object.

        This method can be overridden in a subclass to customize how Fabric
        objects are converted to Dagster asset specs. By default, it delegates
        to the configured translator (which respects the ``translation:`` field).

        Args:
            props: The FabricObjectProps carrying object kind, name, workspace
                ID, and any kind-specific metadata.

        Returns:
            An AssetSpec that represents the Fabric object as a Dagster asset.

        Example:
            Override this method to add custom tags based on the object kind:

            .. code-block:: python

                from dagster_community_components import FabricWorkspaceComponent

                class CustomFabricWorkspaceComponent(FabricWorkspaceComponent):
                    def get_asset_spec(self, props):
                        base_spec = super().get_asset_spec(props)
                        return base_spec.replace_attributes(
                            tags={
                                **base_spec.tags,
                                "fabric_object_kind": props.object_kind,
                            }
                        )
        """
        return self._base_translator.get_asset_spec(props)

    @property
    def _base_translator(self) -> "FabricComponentTranslator":
        cached = getattr(self, "__base_translator_cached", None)
        if cached is None:
            cached = FabricComponentTranslator(self)
            object.__setattr__(self, "__base_translator_cached", cached)
        return cached

    @property
    def defs_state_config(self) -> DefsStateConfig:
        # Hash workspace_id + the optional instance disambiguator (`key`), not
        # workspace_id alone -- otherwise two instances over the same workspace
        # with different import_*/filter settings silently share one state file.
        hash_input = self.workspace.workspace_id + (self.key or "")
        wsid_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
        default_key = f"{self.__class__.__name__}[{wsid_hash}]"
        return DefsStateConfig.from_args(self.defs_state, default_key=default_key)

    def _matches_filter(self, name: str, description: Optional[str] = None) -> bool:
        """Check if an item should be included based on filters.

        Precedence: exclude_name_pattern always vetoes, regardless of any
        other match. Then: include_names (explicit, exact allow-list) OR
        filter_by_name_pattern OR filter_by_description_pattern -- an item
        is included if it satisfies ANY configured inclusion criterion, the
        same include_names/filter_by_comment_pattern union/OR precedence
        snowflake_workspace already uses (replicated here for parity, not
        reinvented).
        """
        import re

        if self.exclude_name_pattern and re.search(self.exclude_name_pattern, name):
            return False

        has_inclusion_criteria = bool(
            self.include_names or self.filter_by_name_pattern or self.filter_by_description_pattern
        )
        if not has_inclusion_criteria:
            return True

        if self.include_names and name.upper() in {n.upper() for n in self.include_names}:
            return True
        if self.filter_by_name_pattern and re.search(self.filter_by_name_pattern, name):
            return True
        if self.filter_by_description_pattern and description:
            if re.search(self.filter_by_description_pattern, description, re.IGNORECASE):
                return True
        return False

    def _apply_translation(
        self,
        kwargs: Dict[str, Any],
        kind: str,
        name: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fold the translation callable into per-asset kwargs.

        Builds a ``FabricObjectProps`` and calls ``self.get_asset_spec(props)``,
        which delegates to ``FabricComponentTranslator`` (base spec + optional
        user ``translation:`` callable).

        Backward-compat: when no ``translation:`` callable is set, the base
        translator returns the default AssetSpec and this method is a no-op --
        all pre-existing per-asset kwargs (name, key, group_name, metadata,
        tags, kinds, deps) win. When a ``translation:`` callable IS set, its
        AssetSpec's key / tags / metadata / kinds / owners / deps /
        automation_condition / freshness_policy / description flow into the
        kwargs (translation-provided values win over inferred ones; `deps`
        and `tags`/`metadata`/`kinds` are merged rather than replaced, so a
        `translation:` can ADD a per-item dependency without having to
        re-declare `upstream_asset_keys`'s own deps).

        Before this fix, only key/metadata/tags/kinds/owners/group_name were
        copied from the translated AssetSpec -- setting `deps` or
        `automation_condition` inside a `translation:` callable silently did
        nothing, with no error raised, and `upstream_asset_keys` had no way
        to vary per item as a result (component-feedback: translation
        silently drops deps and automation_condition).
        """
        if self.translation is None:
            return kwargs

        props = FabricObjectProps(
            object_kind=kind,
            object_name=name,
            workspace_id=self.workspace.workspace_id,
            extra=extra,
        )
        base_spec = self.get_asset_spec(props)
        merged = dict(kwargs)
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
        if base_spec.deps:
            existing_deps = list(merged.get("deps") or [])
            existing_dep_keys = {getattr(d, "asset_key", d) for d in existing_deps}
            for dep in base_spec.deps:
                dep_key = getattr(dep, "asset_key", dep)
                if dep_key not in existing_dep_keys:
                    existing_deps.append(dep)
                    existing_dep_keys.add(dep_key)
            merged["deps"] = existing_deps
        if base_spec.automation_condition is not None:
            merged["automation_condition"] = base_spec.automation_condition
        if base_spec.freshness_policy is not None:
            merged["freshness_policy"] = base_spec.freshness_policy
        if base_spec.description:
            merged["description"] = base_spec.description
        return merged

    def _list_items(self) -> List[dict]:
        """Enumerate all items in the Fabric workspace via the REST API."""
        import requests
        token = self.workspace.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        url = f"{_FABRIC_API_BASE}/workspaces/{self.workspace.workspace_id}/items"

        items: List[dict] = []
        next_link: Optional[str] = url
        while next_link:
            resp = requests.get(next_link, headers=headers, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            next_link = data.get("continuationUri") or data.get("@odata.nextLink")
        return items

    async def refresh_state(self, project_root: Path) -> str:
        """Refresh LOCAL_FILESYSTEM state atomically.

        The stock ``StateBackedComponent.refresh_state`` does ``shutil.rmtree(state_path)`` and only then
        runs ``write_state_to_path`` (network calls, seconds long), so for the whole fetch -- and again
        while the file is half-written -- a concurrent reader (run worker, step subprocess, ``dg list defs``;
        every ``dg`` command refreshes in dev mode) sees missing/partial state: empty Definitions or a
        JSONDecodeError. Here the new state is built beside the old and swapped in with a rename, so the
        previous state stays readable until the new one is complete (and survives a failed refresh)."""
        import os
        import shutil
        import uuid

        from dagster._core.storage.defs_state.base import DefsStateStorage
        from dagster.components.utils.project_paths import get_local_state_path
        from dagster_shared.serdes.objects.models.defs_state_info import (
            LOCAL_STATE_VERSION,
            DefsStateManagementType,
        )

        config = self.defs_state_config
        if config.management_type != DefsStateManagementType.LOCAL_FILESYSTEM:
            return await super().refresh_state(project_root)
        state_path = get_local_state_path(config.key, project_root)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        token = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"
        work = state_path.parent / f".{state_path.name}.refresh-{token}"
        old = state_path.parent / f".{state_path.name}.old-{token}"
        try:
            await self._write_state_to_path_async(work)
            if work.exists():
                if work.is_dir() and state_path.is_dir():
                    try:
                        os.rename(state_path, old)
                        os.rename(work, state_path)
                    except OSError:  # e.g. Windows: directory held open by another process
                        shutil.rmtree(state_path, ignore_errors=True)
                        os.rename(work, state_path)
                else:
                    if state_path.is_dir():
                        shutil.rmtree(state_path, ignore_errors=True)
                    elif work.is_dir() and state_path.exists():
                        state_path.unlink()
                    os.replace(work, state_path)
        finally:
            for _p in (work, old):
                if _p.is_dir():
                    shutil.rmtree(_p, ignore_errors=True)
                else:
                    _p.unlink(missing_ok=True)
        state_storage = DefsStateStorage.get()
        if state_storage:
            state_storage.set_latest_version(config.key, LOCAL_STATE_VERSION)
        return LOCAL_STATE_VERSION

    async def write_state_to_path(self, state_path: Path) -> None:
        """Enumerate Fabric workspace items and cache them.

        Runs the Fabric REST discovery (`GET /workspaces/{ws}/items`),
        applies the ``filter_by_name_pattern`` / ``exclude_name_pattern``
        regexes, and writes the surviving rows to ``state_path`` as a JSON
        dict bucketed by item type. ``build_defs_from_state`` re-hydrates
        from this snapshot so no Fabric HTTP calls fire at Dagster load
        time.
        """
        items = self._list_items()
        buckets: Dict[str, List[dict]] = {
            "lakehouses": [],
            "warehouses": [],
            "notebooks": [],
            "pipelines": [],
            "dataflows": [],
            "semantic_models": [],
            "reports": [],
        }
        for item in items:
            item_type = item.get("type", "")
            name = item.get("displayName") or item.get("name") or "unnamed"
            if not self._matches_filter(name, description=item.get("description")):
                continue
            row = {
                "id": item.get("id"),
                "displayName": name,
                "type": item_type,
                "description": item.get("description"),
                "workspaceId": item.get("workspaceId") or self.workspace.workspace_id,
            }
            if item_type == "Lakehouse":
                buckets["lakehouses"].append(row)
            elif item_type == "Warehouse":
                buckets["warehouses"].append(row)
            elif item_type == "Notebook":
                buckets["notebooks"].append(row)
            elif item_type == "DataPipeline":
                buckets["pipelines"].append(row)
            elif item_type == "Dataflow":
                buckets["dataflows"].append(row)
            elif item_type == "SemanticModel":
                buckets["semantic_models"].append(row)
            elif item_type == "Report":
                buckets["reports"].append(row)

        state = {
            "workspace_id": self.workspace.workspace_id,
            **buckets,
        }
        state_path.write_text(json.dumps(state, indent=2, default=str))

    def build_defs_from_state(
        self,
        context: ComponentLoadContext,
        state_path: Optional[Path],
    ) -> Definitions:
        """Build Dagster definitions from cached Fabric workspace state.

        Reads the JSON dict written by ``write_state_to_path`` and turns
        each item row into an ``observable_source_asset`` (lakehouse /
        warehouse / semantic model / report) or a materializable ``@asset``
        (notebook / pipeline / dataflow). Runtime Fabric REST calls
        (job triggers, polling) still fire on each materialization -- only
        the discovery moved to state.
        """
        if state_path is None or not state_path.exists():
            return Definitions()
        state: Dict[str, Any] = json.loads(state_path.read_text())

        upstream_keys = [dg.AssetKey.from_user_string(k) for k in (self.upstream_asset_keys or [])]
        assets = []
        # Populated by _build_runnable_asset below with the FINAL,
        # translation-resolved AssetKey each runnable item actually got (not
        # a re-derived default) -- consumed by the polling sensor further
        # down so sensor/asset identity never splits, the same class of bug
        # found in azure_data_factory's and ssis_workspace's observation
        # sensors (component-feedback/2026-09-03-azure-data-factory-demo-mode-seam.md,
        # component-feedback/2026-09-24-ssis-workspace-gaps.md).
        runnable_items: List[Dict[str, Any]] = []

        if self.import_lakehouses:
            for row in state.get("lakehouses", []):
                assets.append(self._build_dataset_asset(row, "Lakehouse", "lakehouse"))
        if self.import_warehouses:
            for row in state.get("warehouses", []):
                assets.append(self._build_dataset_asset(row, "Warehouse", "warehouse"))
        if self.import_semantic_models:
            for row in state.get("semantic_models", []):
                assets.append(self._build_dataset_asset(row, "SemanticModel", "semantic_model"))
        if self.import_reports:
            for row in state.get("reports", []):
                assets.append(self._build_dataset_asset(row, "Report", "report"))

        if self.import_notebooks:
            for row in state.get("notebooks", []):
                assets.append(
                    self._build_runnable_asset(row, "Notebook", "notebook", upstream_keys, runnable_items)
                )
        if self.import_pipelines:
            for row in state.get("pipelines", []):
                assets.append(
                    self._build_runnable_asset(row, "DataPipeline", "data_pipeline", upstream_keys, runnable_items)
                )
        if self.import_dataflows:
            for row in state.get("dataflows", []):
                assets.append(
                    self._build_runnable_asset(row, "Dataflow", "dataflow", upstream_keys, runnable_items)
                )

        sensors = []
        if self.polling_sensor and runnable_items:
            sensors.append(self._build_polling_sensor(runnable_items))

        return Definitions(assets=assets, sensors=sensors)

    def _build_polling_sensor(self, runnable_items: List[Dict[str, Any]]):
        """Poll every imported Notebook/DataPipeline/Dataflow's Fabric job-run
        history and emit AssetObservation for completions Dagster didn't itself
        trigger (another scheduler, or a user running it manually in Fabric).

        One token fetch per tick, shared across every item. Cursor is a JSON
        dict of {item_id: last-seen COMPLETED job instance id}, so a rerun of
        the same instance id (job history keeps ~100 recent entries per
        Fabric's own API limit) is never re-emitted.
        """
        _self = self

        @dg.sensor(
            name=f"{self.group_name}_fabric_job_observation_sensor",
            minimum_interval_seconds=self.sensor_check_interval_seconds,
            asset_selection=dg.AssetSelection.assets(*[i["key"] for i in runnable_items]),
        )
        def _fabric_job_observation_sensor(context: dg.SensorEvaluationContext):
            import requests

            try:
                prev_seen: Dict[str, str] = json.loads(context.cursor) if context.cursor else {}
            except Exception:
                prev_seen = {}
            new_seen = dict(prev_seen)

            try:
                token = _self.workspace.get_token()
            except Exception as e:
                return dg.SensorResult(skip_reason=f"Fabric auth failed: {e}")
            headers = {"Authorization": f"Bearer {token}"}

            events = []
            for item in runnable_items:
                url = (
                    f"{_FABRIC_API_BASE}/workspaces/{_self.workspace.workspace_id}"
                    f"/items/{item['item_id']}/jobs/instances"
                )
                try:
                    resp = requests.get(url, headers=headers, timeout=30)
                    resp.raise_for_status()
                    instances = resp.json().get("value", [])
                except Exception as e:
                    context.log.warning(f"Fabric job-instance poll failed for {item['item_id']!r}: {e}")
                    continue

                completed = [i for i in instances if i.get("status") == "Completed"]
                if not completed:
                    continue
                # Instances aren't guaranteed sorted; pick the most recently started.
                latest = max(completed, key=lambda i: i.get("startTimeUtc") or "")
                if prev_seen.get(item["item_id"]) == latest["id"]:
                    continue
                new_seen[item["item_id"]] = latest["id"]
                events.append(dg.AssetObservation(
                    asset_key=item["key"],
                    metadata={
                        "fabric_job_instance_id": latest["id"],
                        "fabric_invoke_type": latest.get("invokeType"),
                        "fabric_start_time_utc": latest.get("startTimeUtc"),
                        "fabric_end_time_utc": latest.get("endTimeUtc"),
                    },
                    tags={"dagster/data_version": str(latest["id"])},
                ))

            if not events:
                return dg.SensorResult(skip_reason="No new Fabric job completions", cursor=json.dumps(new_seen))
            return dg.SensorResult(asset_events=events, cursor=json.dumps(new_seen))

        return _fabric_job_observation_sensor

    def _build_dataset_asset(self, row: dict, item_type: str, kind: str):
        """Build an observable_source_asset for a Lakehouse / Warehouse /
        SemanticModel / Report -- items with no direct run-on-demand
        semantics.
        """
        item_id = row.get("id")
        name = row.get("displayName") or "unnamed"
        default_name = f"fabric_{item_type.lower()}_{name}"

        base_kwargs: Dict[str, Any] = dict(
            name=default_name,
            group_name=self.group_name,
            # Per-item kinds: `fabric` + the specific workload type
            # (`lakehouse` / `warehouse` / `semantic_model` / `report`).
            # Renders as badges in the Dagster catalog and enables
            # `kind:lakehouse` filtering. No first-class Dagster icons
            # for these — they render as text-only badges, still useful.
            kinds={"fabric", kind},
            metadata={
                "fabric_item_id": item_id,
                "fabric_item_type": item_type,
                "workspace_id": self.workspace.workspace_id,
            },
            description=f"Fabric {item_type}: {name}",
        )
        asset_kwargs = self._apply_translation(
            base_kwargs,
            kind=kind,
            name=name,
            extra={
                "item_id": item_id,
                "item_type": item_type,
                "description": row.get("description"),
                "workspace_id": row.get("workspaceId"),
            },
        )

        @dg.observable_source_asset(**asset_kwargs)
        def _ext():
            return dg.DataVersion(item_id or "unknown")

        return _ext

    def _resolve_item_partitions(self, name: str):
        """Returns (partitions_def, partition_parameter_name) for one item.

        An `item_partitions` entry for this display name FULLY REPLACES the
        component-wide partition_type/etc fields for that item -- partition
        config is a cohesive unit (type+start+values+cron all go together),
        so a per-item override means "this item has its own partitioning
        scheme entirely," including explicitly un-partitioning it (an empty
        dict, or one with no partition_type) even when the component-wide
        partition_type IS set. Without this, partitioning one live notebook
        would force every other item from the same component instance --
        a static-per-workspace collector, an unpartitioned semantic model --
        to inherit the exact same partitions_def too (component-feedback:
        "partitioning the live notebook would also partition Part B's
        notebook... and force the unpartitioned semantic model to wait on
        all partitions").
        """
        item_cfg = (self.item_partitions or {}).get(name)
        if item_cfg is not None:
            partitions_def = _build_partitions_def(
                item_cfg.get("partition_type"),
                item_cfg.get("partition_start"),
                item_cfg.get("partition_values"),
                item_cfg.get("dynamic_partition_name"),
                item_cfg.get("partition_dimensions"),
                cron_schedule=item_cfg.get("partition_cron_schedule"),
                cron_fmt=item_cfg.get("partition_cron_fmt") or "%Y-%m-%d",
                end_offset=item_cfg.get("partition_end_offset", 0),
                timezone=item_cfg.get("partition_timezone"),
            )
            partition_parameter_name = item_cfg.get("partition_parameter_name", self.partition_parameter_name)
            return partitions_def, partition_parameter_name
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset,
            timezone=self.partition_timezone,
        )
        return partitions_def, self.partition_parameter_name

    def _runnable_asset_kwargs(self, row: dict, item_type: str, kind: str, upstream_keys) -> Dict[str, Any]:
        """Build the `@dg.asset(...)` kwargs for a single runnable item.

        Split out from `_build_runnable_asset` specifically so a subclass can
        override just this piece (e.g. to add run config, a custom
        partitions_def, or other per-item @asset kwargs this base component
        doesn't expose) without having to copy the whole method, including
        the compute-function body.
        """
        item_id = row.get("id")
        name = row.get("displayName") or "unnamed"
        default_name = f"fabric_{item_type.lower()}_{name}"

        base_kwargs: Dict[str, Any] = dict(
            key=dg.AssetKey.from_user_string(default_name),
            group_name=self.group_name,
            deps=upstream_keys,
            # Per-item kinds: `fabric` + the specific workload type
            # (`notebook` / `data_pipeline` / `dataflow`).
            # Renders as badges in the Dagster catalog and enables
            # `kind:notebook` filtering across the whole workspace.
            kinds={"fabric", kind},
            metadata={
                "fabric_item_id": item_id,
                "fabric_item_type": item_type,
                "workspace_id": self.workspace.workspace_id,
            },
            description=f"Fabric {item_type} run: {name}",
        )
        item_pool = (self.item_pools or {}).get(name) or self.pool
        if item_pool:
            base_kwargs["pool"] = item_pool
        partitions_def, _ = self._resolve_item_partitions(name)
        if partitions_def is not None:
            base_kwargs["partitions_def"] = partitions_def
        return self._apply_translation(
            base_kwargs,
            kind=kind,
            name=name,
            extra={
                "item_id": item_id,
                "item_type": item_type,
                "description": row.get("description"),
                "workspace_id": row.get("workspaceId"),
            },
        )

    def _build_runnable_asset(self, row: dict, item_type: str, kind: str, upstream_keys, runnable_items: Optional[List[Dict[str, Any]]] = None):
        """Build a materializable @asset that triggers a Fabric notebook /
        pipeline / dataflow job and polls it to completion.

        When `runnable_items` is passed, appends this item's FINAL,
        translation-resolved {item_id, item_type, key} -- consumed by the
        polling sensor so it observes against the asset's real key, not a
        re-derived default that can drift from a translation: override.
        """
        item_id = row.get("id")
        name = row.get("displayName") or "unnamed"
        _self = self

        asset_kwargs = self._runnable_asset_kwargs(row, item_type, kind, upstream_keys)

        if runnable_items is not None:
            runnable_items.append({"item_id": item_id, "item_type": item_type, "key": asset_kwargs["key"]})

        resolved_parameters: Dict[str, Any] = dict(self.default_parameters or {})
        resolved_parameters.update((self.item_parameters or {}).get(name) or {})
        resolved_execution_data: Dict[str, Any] = dict(self.execution_data or {})
        resolved_execution_data.update((self.item_execution_data or {}).get(name) or {})

        @dg.asset(**asset_kwargs)
        def _runnable(context: dg.AssetExecutionContext):
            if context.run.tags.get("fabric/skip") == "true":
                context.log.info("fabric/skip=true run tag set -- skipping the actual Fabric trigger.")
                return dg.MaterializeResult(
                    metadata={"fabric_item_id": item_id, "fabric_item_type": item_type, "fabric_skipped": True},
                )

            _, item_partition_parameter_name = _self._resolve_item_partitions(name)
            partition_key = context.partition_key if context.has_partition_key else None

            run_parameters = dict(resolved_parameters)
            if item_partition_parameter_name and partition_key is not None:
                run_parameters.setdefault(item_partition_parameter_name, partition_key)
                context.log.info(f"partitioned run: passing {item_partition_parameter_name}={partition_key} to Fabric")

            load_date = None
            if _self.load_date_parameter:
                override = context.run.tags.get("fabric/load_date_override")
                collides_with_partition = (
                    partition_key is not None
                    and _self.load_date_parameter == item_partition_parameter_name
                )
                if override:
                    load_date = override
                elif collides_with_partition:
                    # load_date_parameter and partition_parameter_name name the SAME
                    # Fabric parameter -- the partition key is the more specific,
                    # deliberately-targeted value (e.g. a backfill of one historical
                    # day), so it wins over "today" instead of being silently
                    # clobbered by it (component-feedback: a backfill of 2026-01-03
                    # silently sent today's date instead, a real wrong-data bug).
                    load_date = partition_key
                    context.log.info(
                        f"load_date_parameter collides with partition_parameter_name "
                        f"({_self.load_date_parameter!r}) -- using partition key "
                        f"{partition_key!r} as the load date, not today's date."
                    )
                else:
                    import datetime
                    from zoneinfo import ZoneInfo
                    load_date = datetime.datetime.now(ZoneInfo(_self.load_date_timezone)).date().isoformat()
                run_parameters[_self.load_date_parameter] = load_date

            state = _self._trigger_item_run(
                context, item_id, item_type, run_parameters or None,
                extra_execution_data=resolved_execution_data or None,
            )

            metadata: Dict[str, Any] = {
                "fabric_item_id": item_id,
                "fabric_item_type": item_type,
                "fabric_job_instance_id": state.get("id", ""),
                "fabric_job_status": state.get("status", ""),
                "fabric_job_instance_url": state.get("_instance_url", ""),
            }
            if load_date is not None:
                metadata["fabric/load_date"] = load_date
            if partition_key is not None:
                metadata["fabric/partition_key"] = partition_key

            if _self.exit_value_file:
                exit_value_path = _self.exit_value_file
                if "{load_date}" in exit_value_path:
                    if load_date is None:
                        raise dg.Failure(
                            description=(
                                f"exit_value_file ({exit_value_path!r}) uses a {{load_date}} "
                                "placeholder, but load_date_parameter isn't set on this "
                                "component instance, so there's no load_date to substitute."
                            ),
                            metadata=metadata,
                        )
                    exit_value_path = exit_value_path.replace("{load_date}", load_date)
                exit_value = _self._read_onelake_exit_value(_self.lakehouse_id, exit_value_path, context.log)
                metadata["fabric/exit_value"] = exit_value
                if _self.data_failure_exit_values and exit_value in _self.data_failure_exit_values:
                    raise dg.Failure(
                        description=(
                            f"Fabric job for item {item_id} reported Completed, but its "
                            f"notebook signalled a data failure via exit value: {exit_value!r}."
                        ),
                        metadata={**metadata, "failure_class": "data"},
                    )

            return dg.MaterializeResult(metadata=metadata)

        return _runnable

    def _read_onelake_exit_value(self, lakehouse_id: str, exit_value_file: str, asset_log) -> str:
        """Read a small text file from OneLake via its ADLS Gen2-compatible
        API -- the only way to get a notebook's own exit value, since the
        Job Scheduler API doesn't surface `notebookutils.notebook.exit(...)`.

        GET https://onelake.dfs.fabric.microsoft.com/{workspace}/{lakehouse}/Files/{path}
        verified against OneLake's own docs (learn.microsoft.com/fabric/
        onelake/onelake-access-api): GUIDs for both workspace and item need
        no item-type suffix, and the response body IS the raw file content
        (no JSON envelope) -- confirmed against the underlying ADLS Gen2
        "Path - Read" API reference. Uses a SEPARATE OAuth scope
        (`https://storage.azure.com/.default`) from the Fabric API itself.

        `lakehouse_id` is deliberately NOT the triggered item's own id: a
        Notebook has no OneLake storage of its own (notebookutils.fs resolves
        relative paths against its attached DEFAULT Lakehouse -- confirmed
        via Microsoft's own notebookutils file-system docs and the OneLake
        overview's item-type list, which names lakehouses/warehouses/
        eventhouses/KQL databases, not notebooks, as OneLake data items). The
        caller must pass the actual storage-bearing item's id via the
        `lakehouse_id` component field.

        Fails CLOSED (raises `dg.Failure`) on any read failure -- auth,
        non-2xx, or a network exception -- rather than warning and returning
        None. The entire point of configuring exit_value_file is to catch bad
        data that would otherwise pass as a green materialization; silently
        skipping that check on a transient read failure would defeat it.
        """
        import requests
        try:
            token = self.workspace.get_storage_token()
        except Exception as e:
            raise dg.Failure(
                description=f"OneLake: couldn't acquire a storage-scoped token to read exit_value_file: {e}",
                metadata={"failure_class": "auth"},
            ) from e
        url = f"{_ONELAKE_DFS_BASE}/{self.workspace.workspace_id}/{lakehouse_id}/Files/{exit_value_file}"
        try:
            resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        except Exception as e:
            raise dg.Failure(description=f"OneLake: exit-value read raised: {e}") from e
        if resp.status_code >= 300:
            raise dg.Failure(
                description=f"OneLake: couldn't read exit-value file ({resp.status_code}): {resp.text[:200]}",
                metadata={
                    "failure_class": "auth" if resp.status_code in (401, 403) else "other",
                    "onelake_status_code": resp.status_code,
                },
            )
        return resp.text.strip()

    def _cancel_job_instance(self, instance_url: str, headers: dict, asset_log) -> None:
        """Best-effort cancellation of a still-running Fabric job instance.

        POST {instance_url}/cancel -- https://learn.microsoft.com/rest/api/fabric/core/job-scheduler/cancel-item-job-instance
        Used on timeout and when polling is interrupted by an unexpected
        exception, so a Dagster-side timeout/interrupt doesn't leave an
        orphaned Fabric job running. Never called once the job has already
        reached one of its own terminal states (Completed/Failed/Cancelled/
        Deduped) -- Fabric would just answer with a benign `JobAlreadyCompleted`
        error in that case, so there's nothing useful to cancel.
        """
        import requests
        try:
            resp = requests.post(f"{instance_url}/cancel", headers=headers, timeout=30)
            if resp.status_code >= 300:
                asset_log.warning(f"Fabric: cancel request failed ({resp.status_code}): {resp.text[:200]}")
            else:
                asset_log.info("Fabric: cancellation requested.")
        except Exception as e:
            asset_log.warning(f"Fabric: cancel request raised: {e}")

    def _retry_backoff(self, context: dg.AssetExecutionContext) -> int:
        """Exponential backoff for a retriable-failure RetryRequested:
        retry_backoff_seconds * 2^attempt. `context.retry_number` isn't
        available outside a real step execution context (e.g. direct
        asset invocation in a test), so this falls back to attempt 0 --
        the base backoff, not an error -- in that case."""
        try:
            attempt = context.retry_number
        except Exception:
            attempt = 0
        return self.retry_backoff_seconds * (2 ** attempt)

    def _trigger_item_run(
        self,
        context: dg.AssetExecutionContext,
        item_id: str,
        item_type: str,
        parameters: Optional[Dict[str, Any]] = None,
        extra_execution_data: Optional[Dict[str, Any]] = None,
    ) -> dict:
        """Trigger a Fabric on-demand item job and poll until done.

        POST /workspaces/{ws}/items/{id}/jobs/instances?jobType=<T>
        The response Location header carries the job instance URL we poll.

        Raises `dg.Failure` for Failed / Cancelled / Deduped / Timeout, and
        `dg.RetryRequested` when Fabric's own `failureReason.isRetriable`
        flag says the failure is transient. Previously this method RETURNED
        the state dict in every case including failure, so a failed Fabric
        run showed up as a green Dagster materialization, with nothing
        downstream any the wiser (component-feedback: a failed Fabric run
        is a green Dagster materialization).

        Accepts the full `context` (not just a logger) so a subclass can
        read the partition key / retry number / run config, matching the
        same `context` a subclassed `execute()` hook gets on
        `FivetranAccountComponent`.
        """
        import requests
        asset_log = context.log

        try:
            token = self.workspace.get_token()
        except Exception as e:
            # Auth failures previously escaped as a raw azure.core/azure.identity
            # exception; wrap them so the Dagster UI shows a clear description.
            raise dg.Failure(
                description=f"[auth] Fabric authentication failed: {e}",
                metadata={"fabric_item_id": item_id, "fabric_item_type": item_type, "failure_class": "auth"},
            ) from e
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        job_type_map = {
            "DataPipeline": "Pipeline",
            "Notebook": "RunNotebook",
            "Dataflow": "Refresh",
        }
        if item_type not in job_type_map:
            # Previously silently defaulted to "Pipeline" for ANY unmapped
            # item_type -- a real landmine: a typo'd item_type, or an item
            # kind this component doesn't natively discover/import, would
            # silently be triggered as if it were a Data Pipeline instead of
            # failing clearly. Spark Job Definition/DBT Job/Copy Job/
            # Materialized Lake Views are NOT supported here yet (see
            # extra_execution_data's docstring) -- raise rather than guess.
            raise dg.Failure(
                description=(
                    f"Fabric item type {item_type!r} has no known job type mapping "
                    f"(supported: {sorted(job_type_map)}). This component doesn't yet "
                    "support triggering Spark Job Definition/DBT Job/Copy Job/"
                    "Materialized Lake Views items."
                ),
                metadata={"fabric_item_id": item_id, "fabric_item_type": item_type},
            )
        job_type = job_type_map[item_type]
        run_url = (
            f"{_FABRIC_API_BASE}/workspaces/{self.workspace.workspace_id}"
            f"/items/{item_id}/jobs/instances?jobType={job_type}"
        )
        if item_type == "Notebook":
            # Notebooks read their parameters out of executionData, not the
            # generic top-level `parameters` list -- see
            # _to_notebook_execution_data's docstring for the two
            # independent Microsoft sources confirming this.
            execution_data = _to_notebook_execution_data(parameters) or {}
            if extra_execution_data:
                execution_data = {**execution_data, **extra_execution_data}
            body = {"executionData": execution_data} if execution_data else None
        else:
            fabric_params = _to_fabric_parameters(parameters)
            body = {}
            if fabric_params:
                body["parameters"] = fabric_params
            if extra_execution_data:
                # Confirmed via the Job Scheduler REST reference's own
                # example that `executionData` and `parameters` can be sent
                # TOGETHER in the same request (each item type defines its
                # own executionData schema -- see extra_execution_data's
                # field description for the ones we've verified so far,
                # e.g. Spark Job Definition's executableFile/mainClass/...).
                body["executionData"] = extra_execution_data
            body = body or None
        resp = requests.post(run_url, json=body, headers=headers, timeout=60)
        if resp.status_code >= 300:
            error = _extract_fabric_error(resp)
            trigger_is_retriable = error["isRetriable"]
            if trigger_is_retriable is None:
                trigger_is_retriable = resp.status_code in (429, 430) or _looks_retriable(error["errorCode"], error["message"])
            failure_class = _classify_fabric_failure(
                "Failed", error["errorCode"], error["message"], trigger_is_retriable,
                http_status_code=resp.status_code,
            )
            md = {
                "fabric_item_id": item_id,
                "fabric_item_type": item_type,
                "fabric_error_code": error["errorCode"],
                "fabric_status_code": resp.status_code,
                "failure_class": failure_class,
            }
            failure_exc = dg.Failure(
                description=f"[{failure_class}] Fabric run trigger failed ({resp.status_code}): {error['message']}",
                metadata=md,
            )
            if trigger_is_retriable:
                raise dg.RetryRequested(max_retries=self.max_retries, seconds_to_wait=self._retry_backoff(context)) from failure_exc
            raise failure_exc

        instance_url = resp.headers.get("Location")
        if not instance_url:
            asset_log.warning("Fabric: no Location header on job trigger response; cannot poll")
            return {"status": "Triggered", "instance_url": None}

        asset_log.info(f"Fabric job triggered. Instance: {instance_url}")
        elapsed = 0
        reached_fabric_terminal_state = False
        try:
            while elapsed < self.workspace.max_wait_seconds:
                state_resp = requests.get(instance_url, headers=headers, timeout=60)
                state_resp.raise_for_status()
                state = state_resp.json()
                status = state.get("status", "Unknown")
                asset_log.info(f"Fabric job: status={status} elapsed={elapsed}s")

                if status == "Completed":
                    reached_fabric_terminal_state = True
                    state["_instance_url"] = instance_url
                    return state

                if status == "Deduped":
                    reached_fabric_terminal_state = True
                    raise dg.Failure(
                        description=(
                            f"Fabric job for item {item_id} was Deduped: another instance of "
                            "the same job type was already running, so Fabric skipped this one. "
                            "Not a failure of this run's own logic -- a scheduling collision."
                        ),
                        metadata={
                            "fabric_item_id": item_id,
                            "fabric_item_type": item_type,
                            "fabric_job_instance": state.get("id", ""),
                            "failure_class": "collision",
                        },
                    )

                if status in ("Failed", "Cancelled"):
                    reached_fabric_terminal_state = True
                    failure = state.get("failureReason") or {}
                    error_code = failure.get("errorCode") or ""
                    message = failure.get("message") or f"Fabric job ended with status {status}"
                    is_retriable = failure.get("isRetriable")
                    failure_class = _classify_fabric_failure(status, error_code, message, is_retriable)
                    md = {
                        "fabric_item_id": item_id,
                        "fabric_item_type": item_type,
                        "fabric_status": status,
                        "fabric_error_code": error_code,
                        "fabric_job_instance": state.get("id", ""),
                        "failure_class": failure_class,
                    }
                    failure_exc = dg.Failure(
                        description=f"[{failure_class}] Fabric job {status.lower()}: {message}",
                        metadata=md,
                    )
                    if is_retriable is None:
                        is_retriable = _looks_retriable(error_code, message)
                    if is_retriable:
                        # Fabric's own failureReason.isRetriable flag when present; a
                        # capacity/throttle keyword fallback only when that flag is
                        # absent (None) -- never overrides an explicit isRetriable=False.
                        asset_log.warning(f"Fabric job failed with a retriable error ({error_code}): {message}")
                        backoff = self._retry_backoff(context)
                        raise dg.RetryRequested(max_retries=self.max_retries, seconds_to_wait=backoff) from failure_exc
                    raise failure_exc

                time.sleep(self.workspace.poll_interval_seconds)
                elapsed += self.workspace.poll_interval_seconds

            asset_log.warning(f"Fabric job timed out after {elapsed}s.")
            raise dg.Failure(
                description=f"Fabric job for item {item_id} timed out after {elapsed}s waiting for completion.",
                metadata={
                    "fabric_item_id": item_id,
                    "fabric_item_type": item_type,
                    "fabric_job_instance_url": instance_url,
                },
            )
        finally:
            # Cancel on timeout, or on any other exception interrupting the poll
            # loop -- but not when Fabric's own job already reached a terminal
            # state on its own (Completed/Failed/Cancelled/Deduped), where
            # there's nothing left running to cancel.
            if not reached_fabric_terminal_state:
                self._cancel_job_instance(instance_url, headers, asset_log)


class DagsterFabricTranslator:
    """Base translator for Fabric workspace objects to AssetSpec.

    Follows the shape of ``DagsterSnowflakeTranslator`` /
    ``DagsterMLflowTranslator`` / ``DagsterPowerBITranslator``. Subclass
    this and override ``get_asset_spec()`` to fully customize how Fabric
    objects become Dagster assets -- an alternative to the runtime
    ``translation:`` callable on the component.
    """

    def get_asset_spec(self, props: FabricObjectProps) -> AssetSpec:
        """Default AssetSpec for a Fabric object.

        Key = ["fabric", <object_kind>, <object_name>] (lowercased for
        consistency with the rest of the Dagster catalog). Kind carries
        both "fabric" and the specific item kind. Metadata carries the
        item kind + name + workspace ID.
        """
        return AssetSpec(
            key=AssetKey(["fabric", props.object_kind, props.object_name.lower()]),
            kinds={"fabric", props.object_kind},
            metadata={
                "fabric/object_kind": props.object_kind,
                "fabric/object_name": props.object_name,
                "fabric/workspace_id": props.workspace_id,
            },
        )


class FabricComponentTranslator(
    create_component_translator_cls(FabricWorkspaceComponent, DagsterFabricTranslator),  # ty: ignore[unsupported-base]
    ComponentTranslator[FabricWorkspaceComponent],
):
    """Bridges ``FabricWorkspaceComponent.translation`` (runtime callable)
    with the base ``DagsterFabricTranslator`` (class-level override).

    Mirrors ``SnowflakeComponentTranslator`` / ``MLflowComponentTranslator``
    / ``FivetranComponentTranslator`` / ``PowerBIComponentTranslator``.
    """

    def __init__(self, component: "FabricWorkspaceComponent"):
        self._component = component

    def get_asset_spec(self, props: FabricObjectProps) -> AssetSpec:
        base_asset_spec = super().get_asset_spec(props)
        if self.component.translation is None:
            return base_asset_spec
        return self.component.translation(base_asset_spec, props)
