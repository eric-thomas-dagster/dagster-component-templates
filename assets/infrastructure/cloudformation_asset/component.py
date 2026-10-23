"""CloudFormation Asset Component.

Deploy or update AWS CloudFormation stacks as the first step in a Dagster
pipeline. Downstream data assets (S3 buckets, RDS instances, Glue databases,
etc.) are declared as deps so they won't run until infrastructure is ready.

Idempotent: uses change sets for updates, so re-running the asset when the
stack is already up-to-date is safe (no-op).
"""
import time
from typing import Dict, List, Optional, Union
import dagster as dg
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


class CloudFormationAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Deploy or update an AWS CloudFormation stack as a Dagster asset.

    Uses create-or-update semantics: if the stack does not exist it is created;
    if it already exists a change set is used to apply updates. When there are
    no changes the asset completes successfully without modifying the stack.

    Stack outputs are captured and surfaced as Dagster asset metadata so they
    are visible in the Dagster UI after each materialization.

    Example:
        ```yaml
        type: dagster_component_templates.CloudFormationAssetComponent
        attributes:
          asset_name: provision_data_lake
          stack_name: my-company-data-platform
          template_file: "{{ project_root }}/infra/data_platform.yaml"
          parameters:
            Environment: production
            BucketName: my-company-data-lake
          region_name: us-east-1
          group_name: infrastructure
          description: Provision S3 data lake, Glue catalog, and IAM roles
        ```
    """

    asset_name: str = Field(description="Dagster asset name for this CloudFormation stack")
    stack_name: str = Field(description="CloudFormation stack name")
    template_file: Optional[str] = Field(
        default=None,
        description="Local path to the CloudFormation template (YAML or JSON). "
        "Mutually exclusive with template_url.",
    )
    template_url: Optional[str] = Field(
        default=None,
        description="S3 URL to the CloudFormation template "
        "(e.g. https://s3.amazonaws.com/bucket/template.yaml). "
        "Mutually exclusive with template_file.",
    )
    parameters: Optional[dict[str, str]] = Field(
        default=None,
        description="Stack parameters as a key/value dict. "
        "Values are passed as ParameterValue; existing values are not reused.",
    )
    capabilities: list[str] = Field(
        default=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
        description="CloudFormation capabilities to acknowledge "
        "(e.g. CAPABILITY_IAM, CAPABILITY_NAMED_IAM, CAPABILITY_AUTO_EXPAND).",
    )
    region_name: str = Field(
        default="us-east-1",
        description="AWS region in which to manage the stack.",
    )
    aws_profile: Optional[str] = Field(
        default=None,
        description="AWS CLI profile name to use for credentials. "
        "Defaults to the default credential chain when not set.",
    )
    role_arn: Optional[str] = Field(
        default=None,
        description="ARN of an IAM role for CloudFormation to assume when managing the stack. "
        "Requires iam:PassRole permission for the calling principal.",
    )
    on_failure: str = Field(
        default="ROLLBACK",
        description="Action on stack creation failure: ROLLBACK, DO_NOTHING, or DELETE.",
    )
    timeout_minutes: int = Field(
        default=60,
        description="Maximum minutes to wait for a stack operation to complete.",
    )
    poll_interval_seconds: int = Field(
        default=15,
        description="Seconds between stack status polls.",
    )
    tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="AWS resource tags to apply to the stack (and all resources it creates).",
    )
    group_name: str = Field(
        default="infrastructure",
        description="Dagster asset group name.",
    )
    description: Optional[str] = Field(
        default=None,
        description="Human-readable description of what this stack provisions.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Dagster asset keys that this asset depends on "
        "(slash-separated, e.g. 'upstream/my_asset').",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('tags', 'asset_tags'))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _stack_exists(self, cf_client, stack_name: str) -> bool:
        """Return True if the stack exists (in any non-DELETE state)."""
        try:
            resp = cf_client.describe_stacks(StackName=stack_name)
            stacks = resp.get("Stacks", [])
            # A stack in REVIEW_IN_PROGRESS is a placeholder; treat as non-existent
            if stacks and stacks[0]["StackStatus"] != "REVIEW_IN_PROGRESS":
                return True
            return False
        except Exception as exc:
            msg = str(exc)
            if "does not exist" in msg or "ValidationError" in msg:
                return False
            raise

    def _build_template_kwargs(self) -> dict:
        """Return the template body or URL kwarg for CF API calls."""
        if self.template_file:
            with open(self.template_file, "r") as fh:
                body = fh.read()
            return {"TemplateBody": body}
        if self.template_url:
            return {"TemplateURL": self.template_url}
        raise ValueError(
            "Either template_file or template_url must be specified."
        )

    def _build_parameters(self, partition_tokens=None) -> list[dict]:
        if not self.parameters:
            return []
        return [
            {"ParameterKey": k, "ParameterValue": v}
            for k, v in _substitute_partition(self.parameters, partition_tokens).items()
        ]

    def _build_tags(self) -> list[dict]:
        if not self.tags:
            return []
        return [{"Key": k, "Value": v} for k, v in self.tags.items()]

    def _poll_until_done(self, cf_client, stack_name: str, context: dg.AssetExecutionContext):
        """Poll describe_stacks until the stack reaches a terminal status."""
        deadline = time.time() + self.timeout_minutes * 60
        while time.time() < deadline:
            time.sleep(self.poll_interval_seconds)
            resp = cf_client.describe_stacks(StackName=stack_name)
            stack = resp["Stacks"][0]
            status = stack["StackStatus"]
            reason = stack.get("StackStatusReason", "")
            context.log.info(f"Stack {stack_name}: {status} — {reason}")
            if status.endswith("_COMPLETE"):
                return stack
            if status.endswith("_FAILED") or status == "ROLLBACK_COMPLETE":
                raise Exception(
                    f"Stack {stack_name} reached failed status {status}: {reason}"
                )
        raise Exception(
            f"Stack {stack_name} did not reach a terminal state within "
            f"{self.timeout_minutes} minutes."
        )

    def _extract_outputs(self, stack: dict) -> dict:
        outputs = {}
        for output in stack.get("Outputs", []):
            key = output.get("OutputKey", "")
            value = output.get("OutputValue", "")
            outputs[key] = value
        return outputs

    # ------------------------------------------------------------------
    # build_defs
    # ------------------------------------------------------------------


    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `stack_name` and `parameters` instead.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `stack_name` and `parameters` instead.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        # retry_policy_* settings -> RetryPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            None,  # dynamic_partition_name: not a field of this component
            None,  # partition_dimensions: not a field of this component
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d",
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _retry_policy_obj = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
        
            _retry_policy_obj = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        _self = self

        # Resolve asset deps
        dep_specs = []
        if _self.deps:
            for dep in _self.deps:
                dep_specs.append(dg.AssetKey(dep.split("/")))

        @dg.asset(partitions_def=partitions_def, retry_policy=_retry_policy_obj, freshness_policy=_freshness_policy_obj, 
            key=dg.AssetKey([_self.asset_name]),
            group_name=_self.group_name,
            description=_self.description or f"CloudFormation stack: {_self.stack_name}",
            deps=dep_specs,
            kinds={"cloudformation", "aws", "infrastructure"},
        )
        def cloudformation_asset(context: dg.AssetExecutionContext):
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            _tokens = _partition_vars(context)
            try:
                import boto3
                from botocore.exceptions import ClientError
            except ImportError:
                raise Exception(
                    "boto3 is not installed. Run: pip install boto3"
                )

            # --- Build boto3 session / client ---
            session_kwargs = {"region_name": _self.region_name}
            if _self.aws_profile:
                session_kwargs["profile_name"] = _self.aws_profile

            session = boto3.Session(**session_kwargs)

            if _self.role_arn:
                sts = session.client("sts")
                assumed = sts.assume_role(
                    RoleArn=_self.role_arn,
                    RoleSessionName="dagster-cloudformation-asset",
                )
                creds = assumed["Credentials"]
                cf = session.client(
                    "cloudformation",
                    aws_access_key_id=creds["AccessKeyId"],
                    aws_secret_access_key=creds["SecretAccessKey"],
                    aws_session_token=creds["SessionToken"],
                )
            else:
                cf = session.client("cloudformation")

            template_kwargs = _self._build_template_kwargs()
            parameters = _self._build_parameters(_tokens)
            tags = _self._build_tags()
            stack_name = _substitute_partition(_self.stack_name, _tokens)

            # --- Create or Update ---
            if not _self._stack_exists(cf, stack_name):
                # Stack does not exist — create it
                context.log.info(f"Creating stack {stack_name}...")
                create_kwargs = dict(
                    StackName=stack_name,
                    Parameters=parameters,
                    Capabilities=_self.capabilities,
                    OnFailure=_self.on_failure,
                    TimeoutInMinutes=_self.timeout_minutes,
                    Tags=tags,
                    **template_kwargs,
                )
                if _self.role_arn:
                    create_kwargs["RoleARN"] = _self.role_arn
                cf.create_stack(**create_kwargs)
                context.log.info(f"Stack {stack_name} creation initiated.")
            else:
                # Stack exists — create a change set to apply updates
                change_set_name = f"dagster-update-{int(time.time())}"
                context.log.info(
                    f"Stack {stack_name} exists. Creating change set {change_set_name}..."
                )
                cs_kwargs = dict(
                    StackName=stack_name,
                    ChangeSetName=change_set_name,
                    ChangeSetType="UPDATE",
                    Parameters=parameters,
                    Capabilities=_self.capabilities,
                    Tags=tags,
                    **template_kwargs,
                )
                if _self.role_arn:
                    cs_kwargs["RoleARN"] = _self.role_arn
                cf.create_change_set(**cs_kwargs)

                # Wait for change set to reach a terminal state
                cs_deadline = time.time() + 300  # 5-minute timeout for CS creation
                while time.time() < cs_deadline:
                    time.sleep(5)
                    cs_resp = cf.describe_change_set(
                        StackName=stack_name,
                        ChangeSetName=change_set_name,
                    )
                    cs_status = cs_resp["Status"]
                    cs_reason = cs_resp.get("StatusReason", "")
                    context.log.info(
                        f"Change set {change_set_name}: {cs_status} — {cs_reason}"
                    )
                    if cs_status in ("CREATE_COMPLETE",):
                        break
                    if cs_status == "FAILED":
                        no_changes_phrases = [
                            "didn't contain changes",
                            "No updates are to be performed",
                            "submitted information didn't contain changes",
                        ]
                        if any(phrase in cs_reason for phrase in no_changes_phrases):
                            context.log.info(
                                f"No changes detected for stack {stack_name}. Skipping update."
                            )
                            # Clean up the empty change set
                            try:
                                cf.delete_change_set(
                                    StackName=stack_name,
                                    ChangeSetName=change_set_name,
                                )
                            except Exception:
                                pass
                            # Fetch current stack state and return
                            resp = cf.describe_stacks(StackName=stack_name)
                            stack = resp["Stacks"][0]
                            outputs = _self._extract_outputs(stack)
                            return dg.MaterializeResult(
                                metadata={
                                    "stack_name": stack_name,
                                    "status": stack["StackStatus"],
                                    "outputs": outputs,
                                    "change_set_result": "no_changes",
                                }
                            )
                        raise Exception(
                            f"Change set {change_set_name} failed: {cs_reason}"
                        )
                else:
                    raise Exception(
                        f"Change set {change_set_name} did not complete within 5 minutes."
                    )

                # Execute the change set
                context.log.info(f"Executing change set {change_set_name}...")
                cf.execute_change_set(
                    StackName=stack_name,
                    ChangeSetName=change_set_name,
                )

            # --- Poll until stack reaches terminal state ---
            stack = _self._poll_until_done(cf, stack_name, context)
            outputs = _self._extract_outputs(stack)

            context.log.info(
                f"Stack {stack_name} completed with status {stack['StackStatus']}. "
                f"Outputs: {outputs}"
            )

            return dg.MaterializeResult(
                metadata={
                    "stack_name": stack_name,
                    "status": stack["StackStatus"],
                    "outputs": outputs,
                }
            )

        return dg.Definitions(assets=[cloudformation_asset])


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


# BEGIN partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port.py)
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

    # Both shapes set: ambiguous. Pick one.
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
# END partitions-def builder


# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

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
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
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
# END partition-vars helper

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
