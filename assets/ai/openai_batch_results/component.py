"""OpenaiBatchResultsComponent — fetch and parse a completed OpenAI Batch API run.

Paired with `openai_batch_submit` (which submits the batch) and
`openai_batch_status_sensor` (which watches for completion and fires the run
that materializes this asset). `batch_id` can be set statically in YAML for
manual/backfill runs, or supplied per-run via `config.batch_id` -- the sensor
wires this through `run_config={"ops": {<op_name>: {"config": {"batch_id":
...}}}}`, where `<op_name>` is this asset's key rendered through
`AssetKey.to_python_identifier()` (see openai_batch_status_sensor's
docstring for why that matters).

Typed output (`output_schema`) follows the exact dotted-path-import
convention used by `assets/infrastructure/smart_retry`'s `kind: python`
handling (`module.path:ClassName` -> `importlib.import_module` + `getattr`).
A row whose `raw_output` fails JSON parsing or Pydantic validation is never
allowed to fail the whole asset: it's marked `invalid_output=True` with the
raw text preserved and the typed columns left null for that row.
"""
import importlib
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import dagster as dg
from pydantic import Field

_RAW_COLUMNS = ["custom_id", "raw_output", "error"]


def _build_openai_client(api_key: str) -> Any:
    """Isolated client construction -- tests monkeypatch
    `openai_batch_results_component._build_openai_client` to return a fake
    client instead of calling the real (paid) API."""
    from openai import OpenAI

    return OpenAI(api_key=api_key)


def _resolve_dotted_class(dotted: str) -> Any:
    """`module.path:ClassName` (or `module.path.ClassName` as a fallback) ->
    the class object. Mirrors smart_retry's dotted-path resolution."""
    if ":" in dotted:
        module_path, cls_name = dotted.rsplit(":", 1)
    else:
        module_path, cls_name = dotted.rsplit(".", 1)
    mod = importlib.import_module(module_path.strip())
    cls = getattr(mod, cls_name.strip(), None)
    if cls is None:
        raise ValueError(f"output_schema {dotted!r}: {cls_name!r} not found in {module_path!r}.")
    return cls


def _parse_jsonl_lines(text: str) -> List[Dict[str, Any]]:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def _rows_from_output_file(client: Any, output_file_id: Optional[str]) -> List[Dict[str, Any]]:
    """Successful + per-row-errored requests from the batch's output file."""
    if not output_file_id:
        return []
    text = client.files.content(output_file_id).text
    rows = []
    for obj in _parse_jsonl_lines(text):
        custom_id = obj.get("custom_id")
        resp = obj.get("response") or {}
        err = obj.get("error")
        status_code = resp.get("status_code")
        if err or status_code != 200:
            err_text = json.dumps(err) if err else f"non-200 status_code: {status_code}"
            rows.append({"custom_id": custom_id, "raw_output": None, "error": err_text})
            continue
        try:
            content = resp["body"]["choices"][0]["message"]["content"]
            rows.append({"custom_id": custom_id, "raw_output": content, "error": None})
        except (KeyError, IndexError, TypeError) as e:
            rows.append({"custom_id": custom_id, "raw_output": None, "error": f"malformed response body: {e}"})
    return rows


def _rows_from_error_file(client: Any, error_file_id: Optional[str]) -> List[Dict[str, Any]]:
    """Request-level failures (e.g. malformed input lines) from the batch's
    separate error file -- distinct from per-row API errors already captured
    in the output file."""
    if not error_file_id:
        return []
    text = client.files.content(error_file_id).text
    rows = []
    for obj in _parse_jsonl_lines(text):
        custom_id = obj.get("custom_id")
        err = obj.get("error")
        rows.append({"custom_id": custom_id, "raw_output": None, "error": json.dumps(err) if err else "unknown error"})
    return rows


class _OpenaiBatchResultsRunConfig(dg.Config):
    """Per-run override for batch_id, wired via run_config by
    openai_batch_status_sensor. Falls back to the component's static
    `batch_id` field when unset (manual/backfill runs)."""

    batch_id: Optional[str] = None


class OpenaiBatchResultsComponent(dg.Component, dg.Model, dg.Resolvable):
    """Fetch, parse, and (optionally) typed-validate a completed OpenAI batch.

    Example:
        ```yaml
        type: dagster_component_templates.OpenaiBatchResultsComponent
        attributes:
          asset_name: support_reply_results
          output_schema: "my_project.schemas:SupportReply"
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    batch_id: Optional[str] = Field(
        default=None,
        description=(
            "OpenAI batch id to fetch results for. Settable statically for manual/backfill runs; "
            "normally supplied dynamically via run_config.batch_id by openai_batch_status_sensor, "
            "which takes precedence over this static value when present."
        ),
    )
    output_schema: Optional[str] = Field(
        default=None,
        description=(
            "Dotted path to a Pydantic model for typed output, e.g. 'my_project.schemas:SupportReply' "
            "(module.path:ClassName). Each row's raw_output is parsed as JSON and validated against "
            "this model; on success the model's fields are merged in as extra columns. On ANY failure "
            "(bad JSON, validation error) the row is marked invalid_output=True with raw_output kept "
            "as-is -- it never fails the asset."
        ),
    )
    api_key_env_var: str = Field(default="OPENAI_API_KEY", description="Env var holding the OpenAI API key.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'support', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['openai', 'llm']. Defaults to ['openai', 'llm'] if not set.",
    )
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_name = self.asset_name
        static_batch_id = self.batch_id
        output_schema = self.output_schema
        api_key_env_var = self.api_key_env_var
        _kinds = set(self.kinds or ["openai", "llm"])

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            description=self.description or "Parsed results of a completed OpenAI Batch API run.",
            group_name=self.group_name,
            kinds=_kinds,
            tags=self.asset_tags or None,
            owners=self.owners or None,
        )
        def _asset(context: dg.AssetExecutionContext, config: _OpenaiBatchResultsRunConfig) -> pd.DataFrame:
            batch_id = config.batch_id or static_batch_id
            if not batch_id:
                raise ValueError(
                    "OpenaiBatchResultsComponent: no batch_id -- set it statically in YAML, or supply "
                    "it via run_config (as openai_batch_status_sensor does)."
                )

            api_key = os.environ.get(api_key_env_var)
            if not api_key:
                raise ValueError(f"{api_key_env_var} not set. Get a key at https://platform.openai.com/api-keys")
            client = _build_openai_client(api_key)

            batch = client.batches.retrieve(batch_id)
            if batch.status != "completed":
                raise Exception(
                    f"Batch {batch_id} is not completed (status={batch.status!r}). This asset should "
                    f"only run once openai_batch_status_sensor confirms completion."
                )

            rows = _rows_from_output_file(client, getattr(batch, "output_file_id", None))
            rows += _rows_from_error_file(client, getattr(batch, "error_file_id", None))

            df = pd.DataFrame(rows, columns=_RAW_COLUMNS)

            if output_schema:
                model_cls = _resolve_dotted_class(output_schema)
                invalid_flags: List[bool] = []
                parsed_rows: List[Dict[str, Any]] = []
                for raw, err in zip(df["raw_output"], df["error"]):
                    if err is not None or raw is None:
                        invalid_flags.append(False)
                        parsed_rows.append({})
                        continue
                    try:
                        instance = model_cls.model_validate_json(raw)
                        parsed_rows.append(instance.model_dump())
                        invalid_flags.append(False)
                    except Exception:
                        invalid_flags.append(True)
                        parsed_rows.append({})
                df["invalid_output"] = invalid_flags
                parsed_df = pd.DataFrame(parsed_rows, index=df.index)
                df = pd.concat([df, parsed_df], axis=1)
            else:
                df["invalid_output"] = False

            row_count = len(df)
            errored_count = int(df["error"].notna().sum())
            succeeded_count = row_count - errored_count
            invalid_output_count = int(df["invalid_output"].sum())

            context.add_output_metadata({
                "row_count": dg.MetadataValue.int(row_count),
                "succeeded_count": dg.MetadataValue.int(succeeded_count),
                "errored_count": dg.MetadataValue.int(errored_count),
                "invalid_output_count": dg.MetadataValue.int(invalid_output_count),
            })
            return df

        return dg.Definitions(assets=[_asset])
