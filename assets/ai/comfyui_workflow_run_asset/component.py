"""ComfyUIWorkflowRunAssetComponent.

Submits a ComfyUI workflow (the node graph you build in ComfyUI's own UI,
exported in API format) to a ComfyUI server as a Dagster asset: queues it,
polls for completion, downloads every generated image, and saves them to
`output_dir`.

The workflow's own sophistication -- checkpoints, LoRAs, ControlNet,
inpainting/masking, img2img, upscaling, batching -- lives entirely in the
node graph JSON itself, not in this component. This component's job is
generic: submit whatever graph it's given, optionally overriding specific
node inputs at materialize time (`node_overrides`, with the same
`{partition_key}`-style token substitution every other source component in
this registry uses), so one exported workflow can be reused across many
materializations instead of hand-editing JSON per run.

Request/response shapes verified directly against the official ComfyUI API
reference, not from memory:
  - `POST /prompt` -- body `{"prompt": <workflow graph>, "client_id": ...}`,
    response `{"prompt_id", "number", "node_errors"}`. A non-empty
    `node_errors` means the graph itself failed validation (missing nodes,
    bad inputs) -- caught before ever polling.
  - `GET /history/{prompt_id}` -- `{"<prompt_id>": {"outputs": {"<node_id>":
    {"images": [{"filename", "subfolder", "type"}]}}, "status": {
    "status_str", "completed", "messages"}}}`.
  - `GET /view?filename=...&subfolder=...&type=...` -- raw image bytes.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

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


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(
                start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"],
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
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists.
    A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value


def _partition_vars(context):
    """{partition_key}, and for time partitions {partition_date}/{partition_start}/{partition_end}."""
    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass
    return tokens


def _apply_node_overrides(workflow: Dict[str, Any], node_overrides: Dict[str, Dict[str, Any]], tokens: Dict[str, str]) -> Dict[str, Any]:
    """Deep-copy `workflow` and set workflow[node_id]['inputs'][input_name] = value for every
    override, with {partition_*} tokens substituted into string values first."""
    import copy

    out = copy.deepcopy(workflow)
    for node_id, inputs in (node_overrides or {}).items():
        if node_id not in out:
            raise ValueError(f"node_overrides: node id {node_id!r} not found in the workflow graph.")
        out[node_id].setdefault("inputs", {})
        for input_name, value in inputs.items():
            out[node_id]["inputs"][input_name] = _substitute_partition(value, tokens)
    return out


def _submit_prompt(comfyui_url: str, workflow: Dict[str, Any], client_id: str, api_key: Optional[str], request_timeout_seconds: int) -> Dict[str, Any]:
    import requests

    body: Dict[str, Any] = {"prompt": workflow, "client_id": client_id}
    if api_key:
        body["extra_data"] = {"api_key_comfy_org": api_key}
    resp = requests.post(f"{comfyui_url}/prompt", json=body, timeout=request_timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"ComfyUI /prompt submission failed: HTTP {resp.status_code} {resp.text[:1000]!r}")
    return resp.json()


def _poll_history(comfyui_url: str, prompt_id: str, log, poll_interval_seconds: float, timeout_seconds: int, request_timeout_seconds: int) -> Dict[str, Any]:
    import time
    import requests

    deadline = time.monotonic() + timeout_seconds
    while True:
        resp = requests.get(f"{comfyui_url}/history/{prompt_id}", timeout=request_timeout_seconds)
        if resp.status_code >= 400:
            raise RuntimeError(f"ComfyUI /history poll failed: HTTP {resp.status_code} {resp.text[:1000]!r}")
        body = resp.json()
        entry = body.get(prompt_id)
        if entry is not None and entry.get("status", {}).get("completed"):
            return entry
        if time.monotonic() >= deadline:
            raise TimeoutError(f"ComfyUI prompt {prompt_id} did not complete within {timeout_seconds}s.")
        log.info(f"comfyui_workflow_run_asset: prompt {prompt_id} still running, polling...")
        time.sleep(poll_interval_seconds)


def _download_images(comfyui_url: str, entry: Dict[str, Any], output_dir: str, request_timeout_seconds: int) -> List[str]:
    import os
    import requests

    os.makedirs(output_dir, exist_ok=True)
    paths = []
    for node_output in (entry.get("outputs") or {}).values():
        for image in node_output.get("images") or []:
            resp = requests.get(
                f"{comfyui_url}/view",
                params={"filename": image["filename"], "subfolder": image.get("subfolder", ""), "type": image.get("type", "output")},
                timeout=request_timeout_seconds,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"ComfyUI /view failed for {image['filename']!r}: HTTP {resp.status_code}")
            out_path = os.path.join(output_dir, image["filename"])
            with open(out_path, "wb") as f:
                f.write(resp.content)
            paths.append(out_path)
    return paths


class ComfyUIWorkflowRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a ComfyUI workflow (exported in API format) as a Dagster asset: queue, poll, download images.

    The workflow's sophistication (checkpoints, LoRAs, ControlNet,
    inpainting, img2img, upscaling, batching) lives entirely in the graph
    JSON itself, authored in ComfyUI's own UI -- this component's job is
    generic submission + polling + image retrieval, with `node_overrides`
    letting one exported workflow be reused across materializations instead
    of hand-editing JSON per run.
    """

    asset_name: str = Field(description="Dagster asset key.")
    comfyui_url: str = Field(description="Base URL of the ComfyUI server, e.g. 'http://comfyui-server:8188'.")
    workflow_path: Optional[str] = Field(default=None, description="Local path to an API-format workflow JSON file (exported from ComfyUI's UI). Mutually exclusive with workflow.")
    workflow: Optional[Dict[str, Any]] = Field(default=None, description="Inline API-format workflow graph. Mutually exclusive with workflow_path.")
    node_overrides: Optional[Dict[str, Dict[str, Any]]] = Field(
        default=None,
        description=(
            "Override specific node inputs before submission, e.g. {'6': {'text': 'a cat in space'}} to "
            "set a CLIPTextEncode node's prompt. {partition_key}/{partition_date}/{partition_start}/"
            "{partition_end} tokens are substituted into string values on a partitioned run."
        ),
    )
    client_id: Optional[str] = Field(default=None, description="ComfyUI client id. Defaults to a fresh UUID per materialization.")
    api_key_env_var: Optional[str] = Field(default=None, description="Env var holding a Comfy Cloud API key (api_key_comfy_org), for workflows using hosted API nodes.")
    output_dir: str = Field(default="comfyui_output", description="Local directory generated images are saved to.")

    poll_interval_seconds: float = Field(default=2.0, gt=0.0, description="Seconds between history polls.")
    timeout_seconds: int = Field(default=600, ge=1, description="Max time to wait for the workflow to complete.")
    request_timeout_seconds: int = Field(default=60, ge=1, description="HTTP request timeout.")
    fail_on_node_errors: bool = Field(default=True, description="Raise if ComfyUI rejects the graph at submission (node_errors in the /prompt response).")
    fail_on_execution_error: bool = Field(default=True, description="Raise if the workflow's status_str is 'error' once completed.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['comfyui', 'image'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.",
    )
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), csv=("partition_values",), tags=("asset_tags",))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if bool(self.workflow_path) == bool(self.workflow):
            raise ValueError("Set exactly one of workflow_path or workflow.")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
        )

        _inferred_kinds = list(self.kinds or ["comfyui", "image"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(int(self.freshness_max_lag_minutes), self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Run a ComfyUI workflow on {self.comfyui_url}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _comfyui_workflow_run_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            import json
            import os
            import uuid

            if self.workflow_path:
                with open(self.workflow_path) as f:
                    workflow = json.load(f)
            else:
                workflow = self.workflow

            tokens = _partition_vars(context)
            workflow = _apply_node_overrides(workflow, self.node_overrides or {}, tokens)

            client_id = self.client_id or str(uuid.uuid4())
            api_key = os.environ.get(self.api_key_env_var) if self.api_key_env_var else None

            submit_resp = _submit_prompt(self.comfyui_url, workflow, client_id, api_key, self.request_timeout_seconds)
            node_errors = submit_resp.get("node_errors") or {}
            if node_errors and self.fail_on_node_errors:
                raise ValueError(f"ComfyUI rejected the workflow graph: {node_errors}")
            prompt_id = submit_resp["prompt_id"]
            context.log.info(f"comfyui_workflow_run_asset: submitted prompt_id={prompt_id}")

            entry = _poll_history(self.comfyui_url, prompt_id, context.log, self.poll_interval_seconds, self.timeout_seconds, self.request_timeout_seconds)
            status = entry.get("status") or {}
            if status.get("status_str") == "error" and self.fail_on_execution_error:
                raise RuntimeError(f"ComfyUI workflow {prompt_id} ended in error: {status.get('messages')}")

            image_paths = _download_images(self.comfyui_url, entry, self.output_dir, self.request_timeout_seconds)
            context.log.info(f"comfyui_workflow_run_asset: downloaded {len(image_paths)} image(s) to {self.output_dir}")

            metadata: Dict[str, Any] = {
                "prompt_id": dg.MetadataValue.text(prompt_id),
                "status": dg.MetadataValue.text(status.get("status_str", "unknown")),
                "image_count": dg.MetadataValue.int(len(image_paths)),
            }
            if image_paths:
                metadata["image_paths"] = dg.MetadataValue.json(image_paths)
                metadata["first_image"] = dg.MetadataValue.path(image_paths[0])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_comfyui_workflow_run_asset])
