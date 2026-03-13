#!/usr/bin/env python3
"""
Update components with new destinations and environment-aware routing.
"""

import re
from pathlib import Path

# New Pydantic fields to add
NEW_DESTINATION_FIELDS = '''
    # ClickHouse fields
    clickhouse_host: Optional[str] = Field(default="localhost", description="ClickHouse server host")
    clickhouse_port: Optional[int] = Field(default=9000, description="ClickHouse server port")
    clickhouse_database: Optional[str] = Field(default="default", description="ClickHouse database name")
    clickhouse_username: Optional[str] = Field(default=None, description="ClickHouse username")
    clickhouse_password: Optional[str] = Field(default=None, description="ClickHouse password")

    # MSSQL fields
    mssql_host: Optional[str] = Field(default="localhost", description="SQL Server host")
    mssql_port: Optional[int] = Field(default=1433, description="SQL Server port")
    mssql_database: Optional[str] = Field(default=None, description="SQL Server database name")
    mssql_username: Optional[str] = Field(default=None, description="SQL Server username")
    mssql_password: Optional[str] = Field(default=None, description="SQL Server password")

    # Athena fields
    athena_query_result_bucket: Optional[str] = Field(default=None, description="S3 bucket for query results")
    athena_database: Optional[str] = Field(default="default", description="Athena database name")
    athena_aws_access_key_id: Optional[str] = Field(default=None, description="AWS access key ID")
    athena_aws_secret_access_key: Optional[str] = Field(default=None, description="AWS secret access key")
    athena_region: Optional[str] = Field(default="us-east-1", description="AWS region")

    # MySQL fields
    mysql_host: Optional[str] = Field(default="localhost", description="MySQL server host")
    mysql_port: Optional[int] = Field(default=3306, description="MySQL server port")
    mysql_database: Optional[str] = Field(default=None, description="MySQL database name")
    mysql_username: Optional[str] = Field(default=None, description="MySQL username")
    mysql_password: Optional[str] = Field(default=None, description="MySQL password")

    # Filesystem fields
    filesystem_bucket_path: Optional[str] = Field(default=None, description="Local or S3 path")
    filesystem_format: Optional[str] = Field(default="parquet", description="Output file format")

    # Synapse fields
    synapse_host: Optional[str] = Field(default=None, description="Azure Synapse workspace hostname")
    synapse_database: Optional[str] = Field(default=None, description="Synapse database name")
    synapse_username: Optional[str] = Field(default=None, description="Synapse username")
    synapse_password: Optional[str] = Field(default=None, description="Synapse password")

    # Environment-aware routing
    use_environment_routing: bool = Field(
        default=False,
        description="Automatically select destination based on Dagster deployment environment"
    )
    destination_local: Optional[str] = Field(default=None, description="Destination for local development")
    destination_branch: Optional[str] = Field(default=None, description="Destination for branch deployments")
    destination_prod: Optional[str] = Field(default=None, description="Destination for production")
'''

# New destination config cases to add to _build_destination_config
NEW_DESTINATION_CASES = '''        elif self.destination == "clickhouse":
            return {
                "credentials": {
                    "database": self.clickhouse_database,
                    "username": self.clickhouse_username,
                    "password": self.clickhouse_password,
                    "host": self.clickhouse_host,
                    "port": self.clickhouse_port,
                }
            }
        elif self.destination == "mssql":
            return {
                "credentials": {
                    "database": self.mssql_database,
                    "username": self.mssql_username,
                    "password": self.mssql_password,
                    "host": self.mssql_host,
                    "port": self.mssql_port,
                }
            }
        elif self.destination == "athena":
            return {
                "credentials": {
                    "query_result_bucket": self.athena_query_result_bucket,
                    "database": self.athena_database,
                    "aws_access_key_id": self.athena_aws_access_key_id,
                    "aws_secret_access_key": self.athena_aws_secret_access_key,
                    "region_name": self.athena_region,
                }
            }
        elif self.destination == "mysql":
            return {
                "credentials": {
                    "database": self.mysql_database,
                    "username": self.mysql_username,
                    "password": self.mysql_password,
                    "host": self.mysql_host,
                    "port": self.mysql_port,
                }
            }
        elif self.destination == "filesystem":
            config = {
                "bucket_url": self.filesystem_bucket_path if self.filesystem_bucket_path else "/tmp/dlt_data",
            }
            if self.filesystem_format:
                config["format"] = self.filesystem_format
            return config
        elif self.destination == "synapse":
            return {
                "credentials": {
                    "database": self.synapse_database,
                    "username": self.synapse_username,
                    "password": self.synapse_password,
                    "host": self.synapse_host,
                }
            }'''

# Environment routing method
ENVIRONMENT_ROUTING_METHOD = '''
    def _get_effective_destination(self) -> Optional[str]:
        """Get destination based on environment routing if enabled."""
        import os

        if not self.use_environment_routing:
            return self.destination

        # Check Dagster Cloud environment variables
        is_branch = os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "").lower() == "true"
        deployment_name = os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "")

        # Determine which destination to use
        if is_branch and self.destination_branch:
            return self.destination_branch
        elif deployment_name and not is_branch and self.destination_prod:
            # In Dagster Cloud but not a branch deployment = production
            return self.destination_prod
        elif not deployment_name and self.destination_local:
            # Not in Dagster Cloud = local development
            return self.destination_local
        else:
            # Fallback to main destination field
            return self.destination
'''

def update_component_file(file_path: Path) -> bool:
    """Update component.py with new destinations and environment routing."""
    content = file_path.read_text()

    # Check if already updated
    if 'clickhouse_host' in content:
        return False

    try:
        # 1. Add new destination fields after databricks_schema
        content = re.sub(
            r'(databricks_schema: Optional\[str\] = Field\(default="default", description="Databricks schema name"\))\n',
            r'\1\n' + NEW_DESTINATION_FIELDS + '\n',
            content
        )

        # 2. Add environment routing method after persist_and_return field
        content = re.sub(
            r'(persist_and_return: bool = Field\([^)]+\))\n\n(\s+def _build_destination_config)',
            r'\1\n' + ENVIRONMENT_ROUTING_METHOD + '\n\2',
            content
        )

        # 3. Add new destination cases before the final else in _build_destination_config
        content = re.sub(
            r'(\s+elif self\.destination == "databricks":.*?return config\n)(\s+else:\n\s+return \{\})',
            r'\1' + NEW_DESTINATION_CASES + '\n\2',
            content,
            flags=re.DOTALL
        )

        # 4. Update build_defs to use _get_effective_destination()
        content = re.sub(
            r'(\s+)# Determine destination\n\s+use_destination = destination if destination else "duckdb"',
            r'''\1# Determine destination (with environment routing if enabled)
\1effective_destination = self._get_effective_destination() if hasattr(self, '_get_effective_destination') else destination
\1use_destination = effective_destination if effective_destination else "duckdb"''',
            content
        )

        # Also need to update where destination is checked
        content = re.sub(
            r'destination_config = self\._build_destination_config\(\) if destination else \{\}',
            r'destination_config = self._build_destination_config() if effective_destination else {}',
            content
        )

        # Update dataset_name logic
        content = re.sub(
            r'dataset_name=asset_name if destination else f"\{asset_name\}_temp"',
            r'dataset_name=asset_name if effective_destination else f"{asset_name}_temp"',
            content
        )

        # Update persist check
        content = re.sub(
            r'if destination and not persist_and_return:',
            r'if effective_destination and not persist_and_return:',
            content
        )

        # Update metadata destination references
        content = re.sub(
            r'f"Data persisted to \{destination\}',
            r'f"Data persisted to {effective_destination}',
            content
        )
        content = re.sub(
            r'"destination": destination,',
            r'"destination": effective_destination,',
            content
        )
        content = re.sub(
            r'\{"status": \["persisted"\], "destination": \[destination\]',
            r'{"status": ["persisted"], "destination": [effective_destination]',
            content
        )

        file_path.write_text(content)
        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Update all ingestion components."""
    assets_dir = Path("/Users/ericthomas/dagster_components/dagster-component-templates/assets")

    component_files = []
    for component_py in assets_dir.glob("*_ingestion/component.py"):
        if component_py.parent.name != "csv_file_ingestion":
            component_files.append(component_py)

    print(f"Found {len(component_files)} dlt ingestion components\n")

    updated_count = 0
    for component_file in sorted(component_files):
        component_name = component_file.parent.name
        print(f"Processing {component_name}...")
        if update_component_file(component_file):
            updated_count += 1
            print(f"  ✅ Updated")
        else:
            print(f"  ⚠️  Skipped (already updated or error)")
        print()

    print(f"\n✅ Updated {updated_count}/{len(component_files)} components")

if __name__ == "__main__":
    main()
