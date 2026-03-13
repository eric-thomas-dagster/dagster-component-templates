"""AWS Step Functions Asset Component.

Extends StateBackedComponent so the AWS Step Functions API is called once at
prepare time (write_state_to_path) and the discovered state machine list is cached
on disk. build_defs_from_state builds one asset per state machine with zero network
calls, keeping code-server reloads instant.

On first load (state_path is None) returns empty Definitions — run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sanitize_name(name: str) -> str:
    """Convert a state machine name to a valid Dagster asset key segment."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _make_boto3_client(region_name: str, aws_profile: Optional[str], role_arn: Optional[str]):
    """Return a boto3 SFN client, optionally assuming a cross-account role."""
    import boto3

    session = boto3.Session(profile_name=aws_profile, region_name=region_name)

    if role_arn:
        sts = session.client("sts")
        creds = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="DagsterStepFunctionsComponent",
        )["Credentials"]
        session = boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=region_name,
        )

    return session.client("stepfunctions")


def _list_state_machines(
    sfn,
    name_prefix: Optional[str],
    exclude_names: Optional[list[str]],
) -> list[dict]:
    """Page through ListStateMachines and apply filters."""
    exclude_set = set(exclude_names or [])
    machines = []
    paginator = sfn.get_paginator("list_state_machines")
    for page in paginator.paginate():
        for sm in page.get("stateMachines", []):
            name = sm["name"]
            if name_prefix and not name.startswith(name_prefix):
                continue
            if name in exclude_set:
                continue
            machines.append({
                "name": name,
                "arn": sm["stateMachineArn"],
                "type": sm.get("type", "STANDARD"),
                "creation_date_str": sm["creationDate"].isoformat()
                if hasattr(sm["creationDate"], "isoformat")
                else str(sm["creationDate"]),
            })
    return machines


def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    return dg.AssetSpec(
        key=dg.AssetKey.from_user_string(ov["key"]) if "key" in ov else base.key,
        description=ov.get("description", base.description),
        group_name=ov.get("group_name", base.group_name),
        metadata={**(base.metadata or {}), **(ov.get("metadata") or {})},
        tags={**(base.tags or {}), **(ov.get("tags") or {})},
        kinds=set(ov["kinds"]) if "kinds" in ov else base.kinds,
        deps=list(base.deps or []) + extra_deps,
    )


def _apply_item_overrides(
    default_spec: dg.AssetSpec,
    item_name: str,
    overrides: Optional[dict],
) -> list[dg.AssetSpec]:
    """Apply assets_by_X overrides. Returns list (usually 1, but >1 if one item → multiple assets)."""
    if not overrides or item_name not in overrides:
        return [default_spec]
    ov = overrides[item_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


def _build_step_functions_defs(
    machines: list[dict],
    region_name: str,
    aws_profile: Optional[str],
    role_arn: Optional[str],
    group_name: str,
    key_prefix: Optional[str],
    wait_for_completion: bool,
    poll_interval_seconds: int,
    execution_timeout_seconds: int,
    assets_by_state_machine_name: Optional[dict] = None,
) -> dg.Definitions:
    """Build one @dg.asset per state machine (no network calls at def-build time)."""
    from dagster import AssetExecutionContext

    terminal_statuses = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}

    assets = []
    for sm in machines:
        safe_name = _sanitize_name(sm["name"])
        key_parts = [key_prefix, safe_name] if key_prefix else [safe_name]
        asset_key = dg.AssetKey(key_parts)
        sm_arn = sm["arn"]
        sm_name = sm["name"]

        default_spec = dg.AssetSpec(
            key=asset_key,
            description=f"AWS Step Functions state machine: {sm_name}",
            group_name=group_name,
            kinds={"aws", "step_functions"},
            metadata={
                "step_functions/state_machine_arn": dg.MetadataValue.text(sm_arn),
                "step_functions/state_machine_type": dg.MetadataValue.text(sm.get("type", "STANDARD")),
                "step_functions/region": dg.MetadataValue.text(region_name),
            },
        )
        expanded_specs = _apply_item_overrides(default_spec, sm_name, assets_by_state_machine_name)

        if len(expanded_specs) == 1:
            spec = expanded_specs[0]

            @dg.asset(
                key=spec.key,
                group_name=spec.group_name,
                description=spec.description,
                metadata=dict(spec.metadata or {}),
                tags=dict(spec.tags or {}),
                kinds=spec.kinds or {"aws", "step_functions"},
                deps=list(spec.deps or []),
            )
            def _asset(
                context: AssetExecutionContext,
                _arn=sm_arn,
                _name=sm_name,
                _wait=wait_for_completion,
                _poll=poll_interval_seconds,
                _timeout=execution_timeout_seconds,
                _region=region_name,
                _profile=aws_profile,
                _role=role_arn,
            ) -> dg.MaterializeResult:
                sfn = _make_boto3_client(_region, _profile, _role)

                context.log.info(f"Starting Step Functions execution for: {_name}")
                start_resp = sfn.start_execution(
                    stateMachineArn=_arn,
                    input=json.dumps({}),
                )
                execution_arn = start_resp["executionArn"]
                context.log.info(f"Execution started: {execution_arn}")

                if not _wait:
                    return dg.MaterializeResult(
                        metadata={
                            "execution_arn": dg.MetadataValue.text(execution_arn),
                            "status": dg.MetadataValue.text("RUNNING"),
                        }
                    )

                elapsed = 0
                status = "RUNNING"
                while elapsed < _timeout:
                    time.sleep(_poll)
                    elapsed += _poll
                    desc = sfn.describe_execution(executionArn=execution_arn)
                    status = desc["status"]
                    context.log.info(
                        f"Execution {execution_arn} status: {status} "
                        f"(elapsed {elapsed}s)"
                    )
                    if status in terminal_statuses:
                        break

                if status != "SUCCEEDED":
                    raise Exception(
                        f"Step Functions execution {execution_arn} ended with status: {status}"
                    )

                return dg.MaterializeResult(
                    metadata={
                        "execution_arn": dg.MetadataValue.text(execution_arn),
                        "status": dg.MetadataValue.text(status),
                    }
                )

            assets.append(_asset)

        else:
            # One state machine → multiple Dagster assets
            _expanded_specs = expanded_specs

            @dg.multi_asset(specs=_expanded_specs, name=f"sfn_{safe_name}_multi")
            def _multi_asset(
                context: AssetExecutionContext,
                _arn=sm_arn,
                _name=sm_name,
                _wait=wait_for_completion,
                _poll=poll_interval_seconds,
                _timeout=execution_timeout_seconds,
                _region=region_name,
                _profile=aws_profile,
                _role=role_arn,
                _specs=_expanded_specs,
            ):
                sfn = _make_boto3_client(_region, _profile, _role)

                context.log.info(f"Starting Step Functions execution for: {_name}")
                start_resp = sfn.start_execution(
                    stateMachineArn=_arn,
                    input=json.dumps({}),
                )
                execution_arn = start_resp["executionArn"]
                context.log.info(f"Execution started: {execution_arn}")

                if not _wait:
                    for spec in _specs:
                        yield dg.MaterializeResult(
                            asset_key=spec.key,
                            metadata={
                                "execution_arn": dg.MetadataValue.text(execution_arn),
                                "status": dg.MetadataValue.text("RUNNING"),
                            },
                        )
                    return

                elapsed = 0
                status = "RUNNING"
                while elapsed < _timeout:
                    time.sleep(_poll)
                    elapsed += _poll
                    desc = sfn.describe_execution(executionArn=execution_arn)
                    status = desc["status"]
                    context.log.info(
                        f"Execution {execution_arn} status: {status} "
                        f"(elapsed {elapsed}s)"
                    )
                    if status in terminal_statuses:
                        break

                if status != "SUCCEEDED":
                    raise Exception(
                        f"Step Functions execution {execution_arn} ended with status: {status}"
                    )

                for spec in _specs:
                    yield dg.MaterializeResult(
                        asset_key=spec.key,
                        metadata={
                            "execution_arn": dg.MetadataValue.text(execution_arn),
                            "status": dg.MetadataValue.text(status),
                        },
                    )

            assets.append(_multi_asset)

    if not assets:
        return dg.Definitions()

    return dg.Definitions(assets=assets)


# ── StateBackedComponent variant ──────────────────────────────────────────────

if _HAS_STATE_BACKED:
    @dataclass
    class StepFunctionsAssetComponent(StateBackedComponent, dg.Resolvable):
        """AWS Step Functions Asset Component.

        Connects to AWS Step Functions at prepare time, discovers all matching
        state machines, and creates one Dagster asset per state machine. At
        execution time each asset starts a Step Functions execution and (optionally)
        waits for it to reach a terminal status.

        Uses StateBackedComponent so the state machine list is fetched once and
        cached — code-server reloads are instant. Refresh with:
          dagster dev   (automatic in dev)
          dg utils refresh-defs-state   (CI/CD / image build)

        Example:
            ```yaml
            type: dagster_component_templates.StepFunctionsAssetComponent
            attributes:
              region_name: us-east-1
              name_prefix: "data-pipeline-"
              group_name: step_functions
              wait_for_completion: true
            ```
        """

        region_name: str = dg.Field(description="AWS region where state machines live")
        aws_profile: Optional[str] = dg.Field(
            default=None,
            description="AWS named profile (uses default credential chain if omitted)",
        )
        role_arn: Optional[str] = dg.Field(
            default=None,
            description="IAM Role ARN to assume for cross-account access",
        )
        name_prefix: Optional[str] = dg.Field(
            default=None,
            description="Only include state machines whose names start with this prefix",
        )
        exclude_names: Optional[list[str]] = dg.Field(
            default=None,
            description="State machine names to exclude from asset generation",
        )
        group_name: str = dg.Field(
            default="step_functions",
            description="Dagster asset group name for all generated assets",
        )
        key_prefix: Optional[str] = dg.Field(
            default=None,
            description="Asset key prefix prepended to every state machine asset key",
        )
        wait_for_completion: bool = dg.Field(
            default=True,
            description="Poll until execution reaches a terminal status before returning",
        )
        poll_interval_seconds: int = dg.Field(
            default=10,
            description="Seconds between describe_execution polls",
        )
        execution_timeout_seconds: int = dg.Field(
            default=3600,
            description="Maximum seconds to wait for an execution before raising",
        )
        assets_by_state_machine_name: Optional[dict] = dg.Field(
            default=None,
            description="Override AssetSpec per state machine name. Value can be a single override dict or a list of dicts (one item → multiple assets).",
        )
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=ResolvedDefsStateConfig
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"StepFunctionsAssetComponent[{self.region_name}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Discover state machines via AWS API and cache list to disk."""
            sfn = _make_boto3_client(self.region_name, self.aws_profile, self.role_arn)
            machines = _list_state_machines(sfn, self.name_prefix, self.exclude_names)
            state_path.write_text(json.dumps(machines, default=str))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build asset definitions from cached state — no network calls."""
            if state_path is None or not state_path.exists():
                if hasattr(context, "log"):
                    context.log.warning(  # type: ignore[union-attr]
                        "StepFunctionsAssetComponent: no cached state found. "
                        "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                    )
                return dg.Definitions()

            machines = json.loads(state_path.read_text())
            return _build_step_functions_defs(
                machines=machines,
                region_name=self.region_name,
                aws_profile=self.aws_profile,
                role_arn=self.role_arn,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                execution_timeout_seconds=self.execution_timeout_seconds,
                assets_by_state_machine_name=self.assets_by_state_machine_name,
            )


else:
    # Fallback: StateBackedComponent not available in this dagster version.
    # Calls the AWS API on every build_defs invocation (no caching).
    class StepFunctionsAssetComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """AWS Step Functions Asset Component (fallback: no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """

        region_name: str = dg.Field(description="AWS region where state machines live")
        aws_profile: Optional[str] = dg.Field(default=None)
        role_arn: Optional[str] = dg.Field(default=None)
        name_prefix: Optional[str] = dg.Field(default=None)
        exclude_names: Optional[list[str]] = dg.Field(default=None)
        group_name: str = dg.Field(default="step_functions")
        key_prefix: Optional[str] = dg.Field(default=None)
        wait_for_completion: bool = dg.Field(default=True)
        poll_interval_seconds: int = dg.Field(default=10)
        execution_timeout_seconds: int = dg.Field(default=3600)
        assets_by_state_machine_name: Optional[dict] = dg.Field(default=None)

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            sfn = _make_boto3_client(self.region_name, self.aws_profile, self.role_arn)
            machines = _list_state_machines(sfn, self.name_prefix, self.exclude_names)
            return _build_step_functions_defs(
                machines=machines,
                region_name=self.region_name,
                aws_profile=self.aws_profile,
                role_arn=self.role_arn,
                group_name=self.group_name,
                key_prefix=self.key_prefix,
                wait_for_completion=self.wait_for_completion,
                poll_interval_seconds=self.poll_interval_seconds,
                execution_timeout_seconds=self.execution_timeout_seconds,
                assets_by_state_machine_name=self.assets_by_state_machine_name,
            )
