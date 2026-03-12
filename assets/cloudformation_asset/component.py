"""CloudFormation Asset Component.

Deploy or update AWS CloudFormation stacks as the first step in a Dagster
pipeline. Downstream data assets (S3 buckets, RDS instances, Glue databases,
etc.) are declared as deps so they won't run until infrastructure is ready.

Idempotent: uses change sets for updates, so re-running the asset when the
stack is already up-to-date is safe (no-op).
"""
import time
from typing import Optional
import dagster as dg
from pydantic import Field


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
    tags: Optional[dict[str, str]] = Field(
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

    def _build_parameters(self) -> list[dict]:
        if not self.parameters:
            return []
        return [
            {"ParameterKey": k, "ParameterValue": v}
            for k, v in self.parameters.items()
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self

        # Resolve asset deps
        dep_specs = []
        if _self.deps:
            for dep in _self.deps:
                dep_specs.append(dg.AssetKey(dep.split("/")))

        @dg.asset(
            key=dg.AssetKey([_self.asset_name]),
            group_name=_self.group_name,
            description=_self.description or f"CloudFormation stack: {_self.stack_name}",
            deps=dep_specs,
            kinds={"cloudformation", "aws", "infrastructure"},
        )
        def cloudformation_asset(asset_context: dg.AssetExecutionContext):
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
            parameters = _self._build_parameters()
            tags = _self._build_tags()
            stack_name = _self.stack_name

            # --- Create or Update ---
            if not _self._stack_exists(cf, stack_name):
                # Stack does not exist — create it
                asset_context.log.info(f"Creating stack {stack_name}...")
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
                asset_context.log.info(f"Stack {stack_name} creation initiated.")
            else:
                # Stack exists — create a change set to apply updates
                change_set_name = f"dagster-update-{int(time.time())}"
                asset_context.log.info(
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
                    asset_context.log.info(
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
                            asset_context.log.info(
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
                asset_context.log.info(f"Executing change set {change_set_name}...")
                cf.execute_change_set(
                    StackName=stack_name,
                    ChangeSetName=change_set_name,
                )

            # --- Poll until stack reaches terminal state ---
            stack = _self._poll_until_done(cf, stack_name, asset_context)
            outputs = _self._extract_outputs(stack)

            asset_context.log.info(
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
