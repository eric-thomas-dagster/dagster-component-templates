"""AWS SageMaker Component.

Import AWS SageMaker training jobs, batch transform jobs, processing jobs, and pipelines
as Dagster assets for orchestrating machine learning workflows.
"""

import re
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    sensor,
    SensorEvaluationContext,
    AssetMaterialization,
    Resolvable,
    Model,
    MetadataValue,
)
from pydantic import Field


class AWSSageMakerComponent(Component, Model, Resolvable):
    """Component for importing AWS SageMaker entities as Dagster assets.

    Supports importing:
    - Training Jobs (start model training)
    - Batch Transform Jobs (batch inference)
    - Processing Jobs (data processing)
    - Pipelines (ML workflow orchestration)

    Example:
        ```yaml
        type: dagster_component_templates.AWSSageMakerComponent
        attributes:
          aws_region: us-east-1
          aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
          aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
          import_training_jobs: true
          import_transform_jobs: true
          import_processing_jobs: true
          import_pipelines: true
        ```
    """

    aws_region: str = Field(
        description="AWS region"
    )

    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID (optional if using IAM role)"
    )

    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key (optional if using IAM role)"
    )

    aws_session_token: Optional[str] = Field(
        default=None,
        description="AWS session token for temporary credentials (optional)"
    )

    import_training_jobs: bool = Field(
        default=True,
        description="Import training job definitions as materializable assets"
    )

    import_transform_jobs: bool = Field(
        default=True,
        description="Import transform job definitions as materializable assets"
    )

    import_processing_jobs: bool = Field(
        default=True,
        description="Import processing job definitions as materializable assets"
    )

    import_pipelines: bool = Field(
        default=True,
        description="Import pipeline definitions as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    filter_by_tags: Optional[str] = Field(
        default=None,
        description="Comma-separated tag keys to filter entities (e.g., 'env,team')"
    )

    poll_interval_seconds: int = Field(
        default=60,
        description="Sensor poll interval in seconds"
    )

    generate_sensor: bool = Field(
        default=True,
        description="Generate observation sensor for completed jobs"
    )

    group_name: str = Field(
        default="aws_sagemaker",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the AWS SageMaker component"
    )

    def _get_boto3_session(self) -> boto3.Session:
        """Create boto3 session with credentials."""
        session_kwargs = {"region_name": self.aws_region}

        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key

        if self.aws_session_token:
            session_kwargs["aws_session_token"] = self.aws_session_token

        return boto3.Session(**session_kwargs)

    def _matches_filters(self, name: str, tags: Optional[List[Dict[str, str]]] = None) -> bool:
        """Check if entity matches name and tag filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        # Tag filter
        if self.filter_by_tags and tags:
            required_keys = [k.strip() for k in self.filter_by_tags.split(",")]
            tag_dict = {tag["Key"]: tag["Value"] for tag in tags}
            if not all(key in tag_dict for key in required_keys):
                return False

        return True

    def _list_training_job_definitions(self, session: boto3.Session) -> List[str]:
        """List recent training jobs to use as templates."""
        sagemaker = session.client("sagemaker")
        job_names = []

        try:
            # Get recent training jobs (last 30 days)
            creation_time_after = datetime.utcnow() - timedelta(days=30)

            paginator = sagemaker.get_paginator("list_training_jobs")
            for page in paginator.paginate(
                CreationTimeAfter=creation_time_after,
                StatusEquals="Completed",
                MaxResults=100
            ):
                for job in page["TrainingJobSummaries"]:
                    job_name = job["TrainingJobName"]
                    tags_response = sagemaker.list_tags(ResourceArn=job["TrainingJobArn"])
                    tags = tags_response.get("Tags", [])

                    if self._matches_filters(job_name, tags):
                        job_names.append(job_name)

        except ClientError as e:
            raise Exception(f"Failed to list training jobs: {e}")

        return job_names

    def _list_transform_job_definitions(self, session: boto3.Session) -> List[str]:
        """List recent transform jobs to use as templates."""
        sagemaker = session.client("sagemaker")
        job_names = []

        try:
            creation_time_after = datetime.utcnow() - timedelta(days=30)

            paginator = sagemaker.get_paginator("list_transform_jobs")
            for page in paginator.paginate(
                CreationTimeAfter=creation_time_after,
                StatusEquals="Completed",
                MaxResults=100
            ):
                for job in page["TransformJobSummaries"]:
                    job_name = job["TransformJobName"]

                    if self._matches_filters(job_name):
                        job_names.append(job_name)

        except ClientError as e:
            raise Exception(f"Failed to list transform jobs: {e}")

        return job_names

    def _list_processing_job_definitions(self, session: boto3.Session) -> List[str]:
        """List recent processing jobs to use as templates."""
        sagemaker = session.client("sagemaker")
        job_names = []

        try:
            creation_time_after = datetime.utcnow() - timedelta(days=30)

            paginator = sagemaker.get_paginator("list_processing_jobs")
            for page in paginator.paginate(
                CreationTimeAfter=creation_time_after,
                StatusEquals="Completed",
                MaxResults=100
            ):
                for job in page["ProcessingJobSummaries"]:
                    job_name = job["ProcessingJobName"]

                    if self._matches_filters(job_name):
                        job_names.append(job_name)

        except ClientError as e:
            raise Exception(f"Failed to list processing jobs: {e}")

        return job_names

    def _list_pipelines(self, session: boto3.Session) -> List[Dict[str, str]]:
        """List SageMaker pipelines."""
        sagemaker = session.client("sagemaker")
        pipelines = []

        try:
            paginator = sagemaker.get_paginator("list_pipelines")
            for page in paginator.paginate():
                for pipeline in page["PipelineSummaries"]:
                    pipeline_name = pipeline["PipelineName"]

                    if self._matches_filters(pipeline_name):
                        pipelines.append({
                            "name": pipeline_name,
                            "arn": pipeline["PipelineArn"]
                        })

        except ClientError as e:
            raise Exception(f"Failed to list pipelines: {e}")

        return pipelines

    def _get_training_job_assets(self, session: boto3.Session) -> List:
        """Generate training job assets."""
        assets = []
        job_names = self._list_training_job_definitions(session)

        for job_name in job_names:
            asset_key = f"training_job_{job_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "job_name": job_name,
                    "job_type": "training",
                    "aws_region": self.aws_region,
                },
            )
            def training_asset(context: AssetExecutionContext, job_name=job_name):
                """Create new training job based on template."""
                session = self._get_boto3_session()
                sagemaker = session.client("sagemaker")

                # Get original job as template
                try:
                    orig_job = sagemaker.describe_training_job(TrainingJobName=job_name)

                    # Create new job name with timestamp
                    new_job_name = f"{job_name}-{int(datetime.utcnow().timestamp())}"

                    context.log.info(f"Creating training job: {new_job_name}")

                    # Create new training job (simplified - would need full config)
                    # Note: This is a template - actual implementation needs all training job parameters

                    metadata = {
                        "original_job": job_name,
                        "new_job_name": new_job_name,
                        "algorithm": orig_job.get("AlgorithmSpecification", {}).get("TrainingImage", ""),
                        "instance_type": orig_job.get("ResourceConfig", {}).get("InstanceType", ""),
                        "note": "Template job - implement full training job creation logic"
                    }

                    return metadata

                except ClientError as e:
                    context.log.error(f"Failed to create training job: {e}")
                    raise

            assets.append(training_asset)

        return assets

    def _get_transform_job_assets(self, session: boto3.Session) -> List:
        """Generate transform job assets."""
        assets = []
        job_names = self._list_transform_job_definitions(session)

        for job_name in job_names:
            asset_key = f"transform_job_{job_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "job_name": job_name,
                    "job_type": "transform",
                    "aws_region": self.aws_region,
                },
            )
            def transform_asset(context: AssetExecutionContext, job_name=job_name):
                """Create new transform job based on template."""
                session = self._get_boto3_session()
                sagemaker = session.client("sagemaker")

                try:
                    orig_job = sagemaker.describe_transform_job(TransformJobName=job_name)

                    new_job_name = f"{job_name}-{int(datetime.utcnow().timestamp())}"

                    context.log.info(f"Creating transform job: {new_job_name}")

                    metadata = {
                        "original_job": job_name,
                        "new_job_name": new_job_name,
                        "model_name": orig_job.get("ModelName", ""),
                        "instance_type": orig_job.get("TransformResources", {}).get("InstanceType", ""),
                        "note": "Template job - implement full transform job creation logic"
                    }

                    return metadata

                except ClientError as e:
                    context.log.error(f"Failed to create transform job: {e}")
                    raise

            assets.append(transform_asset)

        return assets

    def _get_processing_job_assets(self, session: boto3.Session) -> List:
        """Generate processing job assets."""
        assets = []
        job_names = self._list_processing_job_definitions(session)

        for job_name in job_names:
            asset_key = f"processing_job_{job_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "job_name": job_name,
                    "job_type": "processing",
                    "aws_region": self.aws_region,
                },
            )
            def processing_asset(context: AssetExecutionContext, job_name=job_name):
                """Create new processing job based on template."""
                session = self._get_boto3_session()
                sagemaker = session.client("sagemaker")

                try:
                    orig_job = sagemaker.describe_processing_job(ProcessingJobName=job_name)

                    new_job_name = f"{job_name}-{int(datetime.utcnow().timestamp())}"

                    context.log.info(f"Creating processing job: {new_job_name}")

                    metadata = {
                        "original_job": job_name,
                        "new_job_name": new_job_name,
                        "instance_type": orig_job.get("ProcessingResources", {}).get("ClusterConfig", {}).get("InstanceType", ""),
                        "note": "Template job - implement full processing job creation logic"
                    }

                    return metadata

                except ClientError as e:
                    context.log.error(f"Failed to create processing job: {e}")
                    raise

            assets.append(processing_asset)

        return assets

    def _get_pipeline_assets(self, session: boto3.Session) -> List:
        """Generate pipeline assets."""
        assets = []
        pipelines = self._list_pipelines(session)

        for pipeline in pipelines:
            pipeline_name = pipeline["name"]
            asset_key = f"pipeline_{pipeline_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "pipeline_name": pipeline_name,
                    "aws_region": self.aws_region,
                },
            )
            def pipeline_asset(context: AssetExecutionContext, pipeline_name=pipeline_name):
                """Start SageMaker pipeline execution."""
                session = self._get_boto3_session()
                sagemaker = session.client("sagemaker")

                try:
                    # Start pipeline execution
                    response = sagemaker.start_pipeline_execution(
                        PipelineName=pipeline_name,
                        PipelineExecutionDisplayName=f"execution-{int(datetime.utcnow().timestamp())}"
                    )

                    execution_arn = response["PipelineExecutionArn"]
                    context.log.info(f"Pipeline execution started: {execution_arn}")

                    metadata = {
                        "pipeline_name": pipeline_name,
                        "execution_arn": execution_arn,
                        "status": "Executing"
                    }

                    return metadata

                except ClientError as e:
                    context.log.error(f"Failed to start pipeline: {e}")
                    raise

            assets.append(pipeline_asset)

        return assets

    def _get_observation_sensor(self, session: boto3.Session):
        """Generate sensor to observe SageMaker jobs and pipelines."""

        @sensor(
            name=f"{self.group_name}_observation_sensor",
            minimum_interval_seconds=self.poll_interval_seconds,
        )
        def sagemaker_observation_sensor(context: SensorEvaluationContext):
            """Sensor to observe AWS SageMaker jobs and pipeline executions."""
            sagemaker = session.client("sagemaker")

            # Get cursor (last check time)
            cursor = context.cursor
            if cursor:
                last_check = datetime.fromisoformat(cursor)
            else:
                last_check = datetime.utcnow() - timedelta(hours=1)

            now = datetime.utcnow()

            # Observe completed training jobs
            if self.import_training_jobs:
                try:
                    paginator = sagemaker.get_paginator("list_training_jobs")
                    for page in paginator.paginate(
                        StatusEquals="Completed",
                        LastModifiedTimeAfter=last_check,
                        LastModifiedTimeBefore=now
                    ):
                        for job in page["TrainingJobSummaries"]:
                            job_name = job["TrainingJobName"]

                            if self._matches_filters(job_name):
                                asset_key = f"training_job_{job_name}"

                                yield AssetMaterialization(
                                    asset_key=asset_key,
                                    metadata={
                                        "job_name": MetadataValue.text(job_name),
                                        "status": MetadataValue.text("Completed"),
                                        "observed_at": MetadataValue.text(now.isoformat()),
                                    },
                                )
                except ClientError as e:
                    context.log.warning(f"Failed to list training jobs: {e}")

            # Update cursor
            context.update_cursor(now.isoformat())

        return sagemaker_observation_sensor

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        session = self._get_boto3_session()

        assets = []
        sensors = []

        # Import training jobs
        if self.import_training_jobs:
            assets.extend(self._get_training_job_assets(session))

        # Import transform jobs
        if self.import_transform_jobs:
            assets.extend(self._get_transform_job_assets(session))

        # Import processing jobs
        if self.import_processing_jobs:
            assets.extend(self._get_processing_job_assets(session))

        # Import pipelines
        if self.import_pipelines:
            assets.extend(self._get_pipeline_assets(session))

        # Generate observation sensor
        if self.generate_sensor:
            sensors.append(self._get_observation_sensor(session))

        return Definitions(
            assets=assets,
            sensors=sensors,
        )
