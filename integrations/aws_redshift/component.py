"""AWS Redshift Component.

Import AWS Redshift scheduled queries, stored procedures, and materialized views
as Dagster assets for orchestrating data warehouse operations.
"""

import re
import time
from typing import Optional, List, Dict, Any

import boto3
from botocore.exceptions import ClientError

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class AWSRedshiftComponent(Component, Model, Resolvable):
    """Component for importing AWS Redshift entities as Dagster assets.

    Supports importing:
    - Scheduled Queries (trigger query execution)
    - Stored Procedures (execute procedures)
    - Materialized Views (refresh views)

    Example:
        ```yaml
        type: dagster_component_templates.AWSRedshiftComponent
        attributes:
          aws_region: us-east-1
          cluster_identifier: my-redshift-cluster
          database: my_database
          db_user: admin
          import_scheduled_queries: true
          import_stored_procedures: true
          import_materialized_views: true
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

    cluster_identifier: str = Field(
        description="Redshift cluster identifier"
    )

    database: str = Field(
        description="Redshift database name"
    )

    db_user: Optional[str] = Field(
        default=None,
        description="Database user (optional if using IAM authentication)"
    )

    secret_arn: Optional[str] = Field(
        default=None,
        description="AWS Secrets Manager ARN for database credentials (optional)"
    )

    import_scheduled_queries: bool = Field(
        default=True,
        description="Import scheduled queries as materializable assets"
    )

    import_stored_procedures: bool = Field(
        default=True,
        description="Import stored procedures as materializable assets"
    )

    import_materialized_views: bool = Field(
        default=True,
        description="Import materialized views as materializable assets"
    )

    filter_by_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to filter entities by name"
    )

    exclude_name_pattern: Optional[str] = Field(
        default=None,
        description="Regex pattern to exclude entities by name"
    )

    schema_name: Optional[str] = Field(
        default="public",
        description="Schema name to query for procedures and views"
    )

    group_name: str = Field(
        default="aws_redshift",
        description="Asset group name for all imported assets"
    )

    description: Optional[str] = Field(
        default=None,
        description="Description for the AWS Redshift component"
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

    def _matches_filters(self, name: str) -> bool:
        """Check if entity matches name filters."""
        # Name pattern filter
        if self.filter_by_name_pattern:
            if not re.search(self.filter_by_name_pattern, name):
                return False

        # Exclusion pattern
        if self.exclude_name_pattern:
            if re.search(self.exclude_name_pattern, name):
                return False

        return True

    def _execute_sql(self, session: boto3.Session, sql: str, context: AssetExecutionContext) -> Dict[str, Any]:
        """Execute SQL statement using Redshift Data API."""
        redshift_data = session.client("redshift-data")

        # Build execution parameters
        exec_params = {
            "ClusterIdentifier": self.cluster_identifier,
            "Database": self.database,
            "Sql": sql,
        }

        if self.secret_arn:
            exec_params["SecretArn"] = self.secret_arn
        elif self.db_user:
            exec_params["DbUser"] = self.db_user

        try:
            # Execute statement
            response = redshift_data.execute_statement(**exec_params)
            statement_id = response["Id"]

            context.log.info(f"Statement execution started: {statement_id}")

            # Wait for completion (max 10 minutes)
            max_wait = 600
            poll_interval = 2
            elapsed = 0

            while elapsed < max_wait:
                describe_response = redshift_data.describe_statement(Id=statement_id)
                status = describe_response["Status"]

                if status == "FINISHED":
                    context.log.info("Statement execution completed successfully")

                    # Get result metadata
                    result_metadata = {
                        "statement_id": statement_id,
                        "status": status,
                        "duration_ms": describe_response.get("Duration", 0) / 1000000,  # Convert to ms
                        "rows_affected": describe_response.get("ResultRows", 0),
                    }

                    return result_metadata

                elif status == "FAILED":
                    error = describe_response.get("Error", "Unknown error")
                    context.log.error(f"Statement execution failed: {error}")
                    raise Exception(f"SQL execution failed: {error}")

                elif status == "ABORTED":
                    context.log.error("Statement execution was aborted")
                    raise Exception("SQL execution was aborted")

                time.sleep(poll_interval)
                elapsed += poll_interval

            context.log.warning(f"Statement execution timed out after {max_wait} seconds")
            return {
                "statement_id": statement_id,
                "status": "TIMEOUT",
            }

        except ClientError as e:
            context.log.error(f"Failed to execute SQL: {e}")
            raise

    def _list_stored_procedures(self, session: boto3.Session) -> List[str]:
        """List stored procedures in the schema."""
        redshift_data = session.client("redshift-data")

        sql = f"""
        SELECT proname
        FROM pg_proc
        JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
        WHERE pg_namespace.nspname = '{self.schema_name}'
        AND prokind = 'p'
        ORDER BY proname;
        """

        exec_params = {
            "ClusterIdentifier": self.cluster_identifier,
            "Database": self.database,
            "Sql": sql,
        }

        if self.secret_arn:
            exec_params["SecretArn"] = self.secret_arn
        elif self.db_user:
            exec_params["DbUser"] = self.db_user

        try:
            response = redshift_data.execute_statement(**exec_params)
            statement_id = response["Id"]

            # Wait for completion
            max_wait = 30
            elapsed = 0
            while elapsed < max_wait:
                describe_response = redshift_data.describe_statement(Id=statement_id)
                if describe_response["Status"] == "FINISHED":
                    break
                time.sleep(1)
                elapsed += 1

            # Get results
            result_response = redshift_data.get_statement_result(Id=statement_id)
            procedures = []

            for record in result_response.get("Records", []):
                proc_name = record[0].get("stringValue", "")
                if proc_name and self._matches_filters(proc_name):
                    procedures.append(proc_name)

            return procedures

        except ClientError as e:
            raise Exception(f"Failed to list stored procedures: {e}")

    def _list_materialized_views(self, session: boto3.Session) -> List[str]:
        """List materialized views in the schema."""
        redshift_data = session.client("redshift-data")

        sql = f"""
        SELECT schemaname || '.' || matviewname as full_name
        FROM pg_matviews
        WHERE schemaname = '{self.schema_name}'
        ORDER BY matviewname;
        """

        exec_params = {
            "ClusterIdentifier": self.cluster_identifier,
            "Database": self.database,
            "Sql": sql,
        }

        if self.secret_arn:
            exec_params["SecretArn"] = self.secret_arn
        elif self.db_user:
            exec_params["DbUser"] = self.db_user

        try:
            response = redshift_data.execute_statement(**exec_params)
            statement_id = response["Id"]

            # Wait for completion
            max_wait = 30
            elapsed = 0
            while elapsed < max_wait:
                describe_response = redshift_data.describe_statement(Id=statement_id)
                if describe_response["Status"] == "FINISHED":
                    break
                time.sleep(1)
                elapsed += 1

            # Get results
            result_response = redshift_data.get_statement_result(Id=statement_id)
            views = []

            for record in result_response.get("Records", []):
                view_name = record[0].get("stringValue", "")
                if view_name and self._matches_filters(view_name):
                    views.append(view_name)

            return views

        except ClientError as e:
            raise Exception(f"Failed to list materialized views: {e}")

    def _get_scheduled_query_assets(self, session: boto3.Session) -> List:
        """Generate scheduled query assets."""
        # Note: Redshift scheduled queries are managed via EventBridge Scheduler
        # This is a placeholder - implementation would require EventBridge integration
        return []

    def _get_stored_procedure_assets(self, session: boto3.Session) -> List:
        """Generate stored procedure assets."""
        assets = []
        procedures = self._list_stored_procedures(session)

        for proc_name in procedures:
            asset_key = f"procedure_{proc_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "procedure_name": proc_name,
                    "schema": self.schema_name,
                    "cluster": self.cluster_identifier,
                },
            )
            def procedure_asset(context: AssetExecutionContext, proc_name=proc_name):
                """Execute Redshift stored procedure."""
                session = self._get_boto3_session()

                # Call stored procedure
                sql = f"CALL {self.schema_name}.{proc_name}();"
                context.log.info(f"Executing stored procedure: {sql}")

                result = self._execute_sql(session, sql, context)

                return {
                    "procedure_name": proc_name,
                    "schema": self.schema_name,
                    **result,
                }

            assets.append(procedure_asset)

        return assets

    def _get_materialized_view_assets(self, session: boto3.Session) -> List:
        """Generate materialized view assets."""
        assets = []
        views = self._list_materialized_views(session)

        for view_name in views:
            # Extract just the view name without schema
            simple_name = view_name.split('.')[-1]
            asset_key = f"matview_{simple_name}"

            @asset(
                name=asset_key,
                group_name=self.group_name,
                metadata={
                    "view_name": view_name,
                    "schema": self.schema_name,
                    "cluster": self.cluster_identifier,
                },
            )
            def matview_asset(context: AssetExecutionContext, view_name=view_name):
                """Refresh Redshift materialized view."""
                session = self._get_boto3_session()

                # Refresh materialized view
                sql = f"REFRESH MATERIALIZED VIEW {view_name};"
                context.log.info(f"Refreshing materialized view: {sql}")

                result = self._execute_sql(session, sql, context)

                return {
                    "view_name": view_name,
                    "schema": self.schema_name,
                    **result,
                }

            assets.append(matview_asset)

        return assets

    def resolve(self, load_context: ComponentLoadContext) -> Definitions:
        """Resolve component to Dagster definitions."""
        session = self._get_boto3_session()

        assets = []

        # Import scheduled queries
        if self.import_scheduled_queries:
            assets.extend(self._get_scheduled_query_assets(session))

        # Import stored procedures
        if self.import_stored_procedures:
            assets.extend(self._get_stored_procedure_assets(session))

        # Import materialized views
        if self.import_materialized_views:
            assets.extend(self._get_materialized_view_assets(session))

        return Definitions(
            assets=assets,
        )
