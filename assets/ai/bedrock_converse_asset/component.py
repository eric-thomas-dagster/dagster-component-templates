"""BedrockConverseAssetComponent — call AWS Bedrock's Converse API directly
(no litellm passthrough) to extract/generate structured output per row of an
upstream DataFrame, with optional JSON-schema validation of the result.

## Why this exists

No component in this registry called `boto3.client("bedrock-runtime").converse(...)`
directly before this one (confirmed via a dedicated search: Bedrock only ever
appeared as a `bedrock/<model>` model-string option inside litellm-based
components -- `rag_pipeline`, `agentic_pipeline`, `litellm_agent`,
`embeddings_generator`, `context_engineering_pipeline`,
`litellm_inference_asset`). litellm's passthrough works for most models, but
a native Converse-API integration is simpler to reason about, supports
Bedrock's raw-bytes image protocol natively, and matches this registry's
existing direct-SDK AI components (`anthropic_llm`, `anthropic_llm_batch`,
`gemini_llm`) rather than its litellm-based ones.

Field conventions (`upstream_asset_key`/`input_column`/`input_type`,
partition-aware row filtering) match `structured_document_extractor` and
`image_llm_extractor` so swapping between a litellm-based extractor and this
native-Bedrock one is a small config change, not a rewrite.

## Demo mode

`demo_mode: true` (the default) never calls AWS. `_build_bedrock_client` is
swapped for an internal `_DemoBedrockClient` whose `.converse(...)` method
returns a deterministic synthetic response in the exact real response shape
(`output.message.content[0].text`, `usage.*`, `metrics.latencyMs`), seeded
off a stable per-row key (the row's own input value) rather than a
`demo_seed` field -- matching this repo's established determinism
convention. Set `demo_mode: false` for a real Bedrock call.

## Validation notes

Bedrock Converse's image content block takes RAW bytes, not base64 --
verified directly against AWS's own `ImageSource` API reference ("If you use
an AWS SDK, you don't need to encode the image bytes in base64."), not
inferred from an unverified example. `model_id`'s default
(`qwen.qwen3-vl-235b-a22b`) is Bedrock's current vision-capable Qwen3 model
ID, verified against AWS's own Bedrock model-card page.
"""

import json
import random
import time
from typing import Any, Dict, List, Literal, Optional, Union

import dagster as dg
import pandas as pd
from pydantic import Field


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


def _build_bedrock_client(region_name: str) -> Any:
    """Isolated client construction -- tests monkeypatch
    `bedrock_converse_asset_component._build_bedrock_client` to return a fake
    client instead of calling the real (paid, AWS-account-specific) API."""
    import boto3

    return boto3.client("bedrock-runtime", region_name=region_name)


def _percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    idx = min(len(s) - 1, max(0, round(pct * (len(s) - 1))))
    return s[idx]


class _DemoBedrockClient:
    """Deterministic, zero-cost stand-in for `boto3.client("bedrock-runtime")`.
    Produces a real Converse-shaped response (output/usage/metrics) without
    calling AWS, seeded off a stable per-row key so repeated demo runs are
    reproducible."""

    def converse(self, *, modelId: str, messages: List[Dict[str, Any]], inferenceConfig: Optional[Dict[str, Any]] = None, **_ignored) -> Dict[str, Any]:
        seed_text = json.dumps(messages, sort_keys=True, default=str)
        rng = random.Random(seed_text)
        schema = getattr(self, "_output_schema", None)
        if schema and schema.get("properties"):
            payload = {name: self._fake_value(rng, prop) for name, prop in schema["properties"].items()}
            text = json.dumps(payload)
        else:
            text = f"demo response {rng.randint(1000, 9999)}"
        input_tokens = max(1, len(seed_text) // 4)
        output_tokens = max(1, len(text) // 4)
        return {
            "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
            "usage": {"inputTokens": input_tokens, "outputTokens": output_tokens, "totalTokens": input_tokens + output_tokens},
            "metrics": {"latencyMs": rng.randint(150, 900)},
            "stopReason": "end_turn",
        }

    @staticmethod
    def _fake_value(rng: random.Random, prop: Dict[str, Any]) -> Any:
        t = prop.get("type")
        if t == "integer":
            return rng.randint(0, 1000)
        if t == "number":
            return round(rng.uniform(0, 1000), 2)
        if t == "boolean":
            return rng.choice([True, False])
        if t == "array":
            return []
        return rng.choice(prop.get("enum") or [f"demo_{rng.randint(1, 999)}"])


def _extract_response_text(response: Dict[str, Any]) -> str:
    return response["output"]["message"]["content"][0]["text"]


def _parse_and_validate(raw: str, output_schema: Optional[Dict[str, Any]]):
    """Returns (parsed_dict_or_None, valid: bool). Strips markdown code fences
    the model sometimes wraps JSON in (same tolerance as this repo's other
    JSON-extracting LLM components)."""
    if not output_schema:
        return None, True
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None, False
    if not isinstance(parsed, dict):
        return None, False
    try:
        import jsonschema

        jsonschema.Draft202012Validator(output_schema).validate(parsed)
        return parsed, True
    except Exception:
        return parsed, False


class BedrockConverseAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Call AWS Bedrock's Converse API once per row of an upstream DataFrame
    and attach the result as new column(s).

    Set `output_schema` (a JSON schema with `properties`) to parse the
    model's response as JSON, validate it, and add one output column per
    schema property (missing/invalid fields become None). Leave it unset to
    store the model's raw text response in `output_column`.

    Example:
        ```yaml
        type: dagster_community_components.BedrockConverseAssetComponent
        attributes:
          asset_name: invoice_baseline_extraction
          upstream_asset_key: carrier_invoices
          input_column: file_path
          input_type: image
          aws_region: us-east-2
          prompt_template: "Extract invoice_number, carrier_name, and total_amount as JSON."
          output_schema:
            type: object
            properties:
              invoice_number: {type: string}
              carrier_name: {type: string}
              total_amount: {type: number}
          demo_mode: true
        ```
    """

    asset_name: str = Field(description="Dagster asset key emitted by this component.")
    upstream_asset_key: Union[str, List[str]] = Field(description="Upstream asset key providing a DataFrame with one row per item to send to Bedrock.")
    input_column: str = Field(default="text", description="Column holding the input -- raw text (input_type='text') or a path to an image file (input_type='image').")
    input_type: Literal["text", "image"] = Field(default="text", description="'text': input_column is sent as the prompt's text content. 'image': input_column is a path to an image file, read and sent as a raw-bytes image content block.")
    image_format: Optional[Literal["png", "jpeg", "gif", "webp"]] = Field(default=None, description="Override the image format Bedrock is told to expect. Unset = inferred from the file extension.")

    model_id: str = Field(
        default="qwen.qwen3-vl-235b-a22b",
        description="Bedrock model ID for the Converse API, e.g. 'qwen.qwen3-vl-235b-a22b' (vision-capable Qwen3) or 'qwen.qwen3-235b-a22b-v1:0' (text-only).",
    )
    aws_region: str = Field(description="AWS region for the bedrock-runtime client.")
    prompt_template: str = Field(
        default="{input}",
        description="Prompt sent to the model. For input_type='text', '{input}' is replaced with the row's input_column value; the literal text is used as-is otherwise (e.g. for input_type='image', where the image itself is the primary content and this is an instruction).",
    )
    output_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description="JSON schema (with 'properties') the model's JSON response is validated against. One output column is added per property; missing or schema-invalid responses fill those columns with None. Leave unset to store the raw text response in output_column instead.",
    )
    output_column: str = Field(default="llm_response", description="Column name for the raw text response when output_schema is unset. Ignored (schema properties become the output columns instead) when output_schema is set.")
    max_tokens: int = Field(default=1024, description="inferenceConfig.maxTokens for the Converse call.")
    temperature: float = Field(default=0.0, description="inferenceConfig.temperature for the Converse call.")
    min_valid_rate: Optional[float] = Field(
        default=None,
        description="Only used when output_schema is set: minimum fraction of rows whose response must validate against output_schema. Emits a blocking (ERROR) or non-blocking (WARN) 'output_schema_valid_rate' check per min_valid_rate_severity. Leave unset for no check.",
    )
    min_valid_rate_severity: Literal["ERROR", "WARN"] = Field(default="WARN", description="Severity of the output_schema_valid_rate check when min_valid_rate is set.")

    # Demo mode.
    demo_mode: bool = Field(
        default=True,
        description="Return deterministic synthetic Converse responses instead of calling AWS Bedrock. Set false for a real account.",
    )

    # AWS credentials (optional if the environment's default credential chain already works).
    aws_access_key_id: Optional[str] = Field(default=None)
    aws_secret_access_key: Optional[str] = Field(default=None)
    aws_session_token: Optional[str] = Field(default=None)

    # Catalog / governance.
    group_name: Optional[str] = Field(default=None, description="Asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds. Default: ['bedrock', 'aws', 'llm'].")

    # Partitioning -- same shape as modal_pipes_asset/ecs_pipes_asset, this repo's shared convention.
    partition_type: Optional[str] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(default=None)
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d")
    partition_end_offset: int = Field(default=0)
    partition_timezone: Optional[str] = Field(default=None)
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Any] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_date_column: Optional[str] = Field(default=None, description="Upstream column holding each row's date, used to filter to the current partition's rows when time-partitioned. Unset = no row filtering.")
    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(default=None)
    backfill_policy_max_partitions_per_run: Optional[int] = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("owners", "kinds"), csv=(), tags=("tags",))

    @model_validator(mode="before")
    @classmethod
    def _coerce_upstream_asset_keys(cls, data):
        return coerce_upstream_input(data, component=cls.__name__, mode="single", optional=False)

    def _get_client(self):
        if self.demo_mode:
            client = _DemoBedrockClient()
            client._output_schema = self.output_schema
            return client
        return _build_bedrock_client(self.aws_region)

    def _build_content(self, row_value: Any) -> List[Dict[str, Any]]:
        if self.input_type == "image":
            path = str(row_value)
            with open(path, "rb") as f:
                image_bytes = f.read()
            fmt = self.image_format or path.lower().rsplit(".", 1)[-1]
            if fmt == "jpg":
                fmt = "jpeg"
            return [
                {"image": {"format": fmt, "source": {"bytes": image_bytes}}},
                {"text": self.prompt_template},
            ]
        text = self.prompt_template.format(input=row_value) if "{input}" in self.prompt_template else self.prompt_template
        return [{"text": text}]

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Bedrock Converse Asset", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self
        asset_key = dg.AssetKey.from_user_string(self.asset_name)
        upstream_key = dg.AssetKey.from_user_string(self.upstream_asset_key)
        kinds_set = set(self.kinds or []) | {"bedrock", "aws", "llm"}
        tag_map = dict(self.tags or {})
        for k in kinds_set:
            tag_map[f"dagster/kind/{k}"] = ""

        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
            end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _backfill_policy_obj = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            _backfill_policy_obj = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            _backfill_policy_obj = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        check_specs = []
        if self.output_schema and self.min_valid_rate is not None:
            check_specs.append(dg.AssetCheckSpec(
                name="output_schema_valid_rate", asset=asset_key,
                blocking=(self.min_valid_rate_severity == "ERROR"),
            ))

        @dg.asset(
            key=asset_key,
            description=self.description or f"Bedrock Converse ({self.model_id}) output",
            group_name=self.group_name, owners=self.owners or [], tags=tag_map, kinds=kinds_set,
            partitions_def=partitions_def,
            backfill_policy=_backfill_policy_obj,
            check_specs=check_specs,
            ins={"upstream": dg.AssetIn(key=upstream_key)},
        )
        def _bedrock_converse_asset(context: dg.AssetExecutionContext, upstream: Any):
            if hasattr(upstream, "value") and hasattr(upstream, "metadata"):
                upstream = upstream.value
            if isinstance(upstream, dict):
                frames = [v for v in upstream.values() if isinstance(v, pd.DataFrame)]
                upstream = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            df = upstream
            if component.partition_date_column and component.partition_date_column in df.columns and context.has_partition_key:
                dates = pd.to_datetime(df[component.partition_date_column], errors="coerce").dt.strftime("%Y-%m-%d")
                df = df[dates == context.partition_key]

            client = component._get_client()
            schema_props = list((component.output_schema or {}).get("properties", {}).keys())
            rows: List[Dict[str, Any]] = []
            latencies: List[float] = []
            input_tokens_total = 0
            output_tokens_total = 0
            valid_count = 0
            failures = 0

            for value in df[component.input_column]:
                try:
                    content = component._build_content(value)
                    response = client.converse(
                        modelId=component.model_id,
                        messages=[{"role": "user", "content": content}],
                        inferenceConfig={"temperature": component.temperature, "maxTokens": component.max_tokens},
                    )
                    text = _extract_response_text(response)
                    usage = response.get("usage") or {}
                    input_tokens_total += usage.get("inputTokens", 0) or 0
                    output_tokens_total += usage.get("outputTokens", 0) or 0
                    latency = (response.get("metrics") or {}).get("latencyMs")
                    if latency is not None:
                        latencies.append(latency)

                    if schema_props:
                        parsed, valid = _parse_and_validate(text, component.output_schema)
                        if valid:
                            valid_count += 1
                        rows.append({p: (parsed or {}).get(p) for p in schema_props})
                    else:
                        rows.append({component.output_column: text})
                except Exception as e:
                    context.log.warning(f"{component.asset_name}: Bedrock call failed for row: {e}")
                    failures += 1
                    rows.append({p: None for p in schema_props} if schema_props else {component.output_column: None})

            result_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(rows)], axis=1)

            metadata: Dict[str, Any] = {
                "row_count": dg.MetadataValue.int(len(result_df)),
                "failures": dg.MetadataValue.int(failures),
                "model_id": component.model_id,
                "total_input_tokens": dg.MetadataValue.int(input_tokens_total),
                "total_output_tokens": dg.MetadataValue.int(output_tokens_total),
                "latency_p50_ms": dg.MetadataValue.float(float(_percentile(latencies, 0.5) or 0.0)),
                "latency_p95_ms": dg.MetadataValue.float(float(_percentile(latencies, 0.95) or 0.0)),
            }
            schema_valid_rate = None
            if schema_props:
                schema_valid_rate = valid_count / len(rows) if rows else 0.0
                metadata["schema_valid_rate"] = dg.MetadataValue.float(schema_valid_rate)

            yield dg.Output(result_df, metadata=metadata)

            if check_specs:
                passed = schema_valid_rate is not None and schema_valid_rate >= component.min_valid_rate
                yield dg.AssetCheckResult(
                    check_name="output_schema_valid_rate", passed=passed,
                    severity=dg.AssetCheckSeverity.ERROR if component.min_valid_rate_severity == "ERROR" else dg.AssetCheckSeverity.WARN,
                    metadata={"value": schema_valid_rate, "threshold": component.min_valid_rate},
                )

        return dg.Definitions(assets=[_bedrock_converse_asset])


def _build_partitions_def(
    partition_type, partition_start, partition_values,
    dynamic_partition_name, partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )
    if partition_dimensions and partition_type:
        raise ValueError("Set either partition_type or partition_dimensions, not both.")

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily": return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly": return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly": return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly": return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("static partition requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        return MultiPartitionsDefinition({d["name"]: _build_axis(d) for d in partition_dimensions})

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(f"partition_type={partition_type!r} requires partition_start.")
    if partition_type == "daily": return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly": return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly": return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly": return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError("partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only.")
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values: raise ValueError("static requires values")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError("dynamic requires dynamic_partition_name")
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    raise ValueError(f"unknown partition_type: {partition_type!r}")
